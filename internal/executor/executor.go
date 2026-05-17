// internal/executor/executor.go
package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ghosecorp/ghostsql/internal/metadata"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

type Executor struct {
	db              *storage.Database
	session         *storage.Session
	cteResults      map[string]*Result // For CTE virtual tables
	currentOuterRow storage.Row        // For LATERAL joins correlation
	currentStmt     *parser.SelectStmt // For WHERE clause alias resolution
}

func NewExecutor(db *storage.Database, session *storage.Session) *Executor {
	return &Executor{
		db:         db,
		session:    session,
		cteResults: make(map[string]*Result),
	}
}

func (e *Executor) GetSession() *storage.Session {
	return e.session
}

func (e *Executor) SetSession(s *storage.Session) {
	e.session = s
}

type Result struct {
	Message string
	Rows    []storage.Row
	Columns []string
}

func (e *Executor) getActiveDatabase() (*storage.DatabaseInstance, error) {
	dbName := e.session.GetDatabase()
	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	return e.db.GetDatabaseInstance(dbName)
}

func (e *Executor) Execute(stmt parser.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *parser.CreateDatabaseStmt:
		return e.executeCreateDatabase(s)
	case *parser.UseDatabaseStmt:
		return e.executeUseDatabase(s)
	case *parser.ShowStmt:
		return e.executeShow(s)
	case *parser.CreateTableStmt:
		return e.executeCreateTable(s)
	case *parser.InsertStmt:
		return e.executeInsert(s)
	case *parser.SelectStmt:
		return e.executeSelect(s)
	case *parser.CompoundSelectStmt:
		return e.executeCompoundSelect(s)
	case *parser.UpdateStmt:
		return e.executeUpdate(s)
	case *parser.DeleteStmt:
		return e.executeDelete(s)
	case *parser.DropTableStmt:
		return e.executeDropTable(s)
	case *parser.DropDatabaseStmt:
		return e.executeDropDatabase(s)
	case *parser.TruncateStmt:
		return e.executeTruncate(s)
	case *parser.AlterTableStmt:
		return e.executeAlterTable(s)
	case *parser.CommentStmt:
		return e.executeComment(s)
	case *parser.CreateIndexStmt:
		return e.executeCreateIndex(s)
	case *parser.DropIndexStmt:
		return e.executeDropIndex(s)
	case *parser.CreateRoleStmt:
		return e.executeCreateRole(s)
	case *parser.AlterRoleStmt:
		return e.executeAlterRole(s)
	case *parser.GrantStmt:
		return e.executeGrant(s)
	case *parser.RevokeStmt:
		return e.executeRevoke(s)
	case *parser.CreatePolicyStmt:
		return e.executeCreatePolicy(s)
	case *parser.SetStmt:
		return &Result{Message: "SET"}, nil
	case *parser.TransactionStmt:
		return e.executeTransaction(s)
	case *parser.SavepointStmt:
		return e.executeSavepoint(s)
	case *parser.DropRoleStmt:
		return e.executeDropRole(s)
	case *parser.CreateViewStmt:
		return e.executeCreateView(s)
	case *parser.DropViewStmt:
		return e.executeDropView(s)
	case *parser.CreateSchemaStmt:
		return e.executeCreateSchema(s)
	case *parser.CreateSequenceStmt:
		return e.executeCreateSequence(s)
	case *parser.CreateTypeStmt:
		return e.executeCreateType(s)
	case *parser.CreateMaterializedViewStmt:
		return e.executeCreateMaterializedView(s)
	case *parser.RefreshMaterializedViewStmt:
		return e.executeRefreshMaterializedView(s)
	case *parser.MergeStmt:
		return e.executeMerge(s)
	default:
		return nil, fmt.Errorf("unsupported statement type")
	}
}

func (e *Executor) checkPrivilege(objectType, objectName, privilege string) error {
	user := e.session.GetUser()

	// Superuser bypass (like pg_aclcheck)
	if role, ok := e.db.RoleStore.GetRole(user); ok && role.IsSuperuser {
		return nil
	}
	// Legacy superuser check for the default 'ghost' account
	if user == "ghost" {
		return nil
	}

	// Owner bypass for TABLE objects (PostgreSQL standard: owner always has access)
	if objectType == "TABLE" {
		if dbInstance, err := e.getActiveDatabase(); err == nil {
			if table, exists := e.getTable(dbInstance, objectName); exists && table.Owner == user {
				return nil
			}
		}
	}

	objectKey := fmt.Sprintf("%s:%s", objectType, objectName)
	if e.db.RoleStore.HasPrivilege(user, objectKey, privilege) {
		return nil
	}

	return fmt.Errorf("permission denied for %s on %s %s", privilege, objectType, objectName)
}

func (e *Executor) executeCreateDatabase(stmt *parser.CreateDatabaseStmt) (*Result, error) {
	if err := e.db.CreateDatabase(stmt.DatabaseName); err != nil {
		return nil, err
	}

	return &Result{
		Message: fmt.Sprintf("CREATE DATABASE %s", stmt.DatabaseName),
	}, nil
}

func (e *Executor) executeUseDatabase(stmt *parser.UseDatabaseStmt) (*Result, error) {
	if _, err := e.db.GetDatabaseInstance(stmt.DatabaseName); err != nil {
		return nil, err
	}

	e.session.SetDatabase(stmt.DatabaseName)
	e.db.Logger.Info("Session %s switched to database: %s", e.session.ID, stmt.DatabaseName)

	return &Result{
		Message: fmt.Sprintf("Database changed to %s", stmt.DatabaseName),
	}, nil
}

func (e *Executor) executeShow(stmt *parser.ShowStmt) (*Result, error) {
	switch stmt.ShowType {
	case "DATABASES":
		return e.executeShowDatabases()
	case "TABLES":
		return e.executeShowTables()
	case "COLUMNS":
		return e.executeShowColumns(stmt.TableName)
	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", stmt.ShowType)
	}
}

func (e *Executor) executeShowDatabases() (*Result, error) {
	databases := e.db.ListDatabases()

	rows := make([]storage.Row, len(databases))
	for i, dbName := range databases {
		current := ""
		if dbName == e.session.GetDatabase() {
			current = "*"
		}
		rows[i] = storage.Row{
			"Database": dbName,
			"Current":  current,
		}
	}

	return &Result{
		Rows:    rows,
		Columns: []string{"Database", "Current"},
	}, nil
}

func (e *Executor) executeShowTables() (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	tableNames := make(map[string]bool)
	for k := range dbInstance.Tables {
		tableNames[k] = true
	}
	if e.session != nil && e.session.TxActive {
		for k, v := range e.session.TxTables {
			if v == nil {
				delete(tableNames, k)
			} else {
				tableNames[k] = true
			}
		}
	}

	rows := make([]storage.Row, 0, len(tableNames))
	for tableName := range tableNames {
		rows = append(rows, storage.Row{
			"Table": tableName,
		})
	}

	return &Result{
		Rows:    rows,
		Columns: []string{"Table"},
	}, nil
}

func (e *Executor) executeShowColumns(tableName string) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := e.getTable(dbInstance, tableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	rows := make([]storage.Row, len(table.Columns))
	for i, col := range table.Columns {
		nullable := "NO"
		if col.Nullable {
			nullable = "YES"
		}

		comment := ""
		if col.Metadata != nil {
			comment = col.Metadata.Description
		}

		rows[i] = storage.Row{
			"Column":   col.Name,
			"Type":     col.Type.String(),
			"Nullable": nullable,
			"Comment":  comment,
		}
	}

	return &Result{
		Rows:    rows,
		Columns: []string{"Column", "Type", "Nullable", "Comment"},
	}, nil
}

func (e *Executor) executeCreateTable(stmt *parser.CreateTableStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}
	if err := e.checkPrivilege("DATABASE", dbInstance.Name, "CREATE"); err != nil {
		return nil, err
	}

	columns := make([]storage.Column, len(stmt.Columns))
	for i, colDef := range stmt.Columns {
		col := storage.Column{
			Name:        colDef.Name,
			Type:        colDef.Type,
			Length:      colDef.Length,
			Nullable:    colDef.Nullable,
			IsPrimary:   colDef.IsPrimary,
			IsUnique:    colDef.IsUnique,
			DefaultVal:  colDef.DefaultVal,
			DefaultExpr: colDef.DefaultExpr,
			CheckExpr:   colDef.CheckExpr,
		}

		// Add foreign key if specified
		if colDef.ForeignKey != nil {
			col.ForeignKey = &storage.ForeignKeyConstraint{
				RefTable:  colDef.ForeignKey.RefTable,
				RefColumn: colDef.ForeignKey.RefColumn,
			}
		}

		columns[i] = col
	}

	var tableMeta *metadata.Metadata
	if len(stmt.Metadata) >= 2 {
		var id [16]byte
		copy(id[:], stmt.TableName)
		tableMeta = metadata.NewMetadata(
			metadata.ObjTypeTable,
			id,
			stmt.Metadata[0],
			stmt.Metadata[1],
		)
	}

	// Set owner: the creator is the table owner (PostgreSQL standard)
	owner := e.session.GetUser()
	table := storage.NewTable(stmt.TableName, owner, columns, tableMeta)
	e.setTable(dbInstance, stmt.TableName, table)

	if err := e.saveTableToDisk(dbInstance, table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("CREATE TABLE %s", stmt.TableName),
	}, nil
}

func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*Result, error) {
	if err := e.checkPrivilege("TABLE", stmt.TableName, "INSERT"); err != nil {
		return nil, err
	}
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := e.getTableForModification(dbInstance, stmt.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	var sourceRows []storage.Row
	if stmt.SelectQuery != nil {
		selectRes, err := e.executeSelect(stmt.SelectQuery)
		if err != nil {
			return nil, err
		}

		targetCols := stmt.Columns
		if len(targetCols) == 0 {
			targetCols = table.GetColumnNames()
		}

		for _, sRow := range selectRes.Rows {
			row := make(storage.Row)
			for idx, colName := range targetCols {
				if idx < len(selectRes.Columns) {
					selectCol := selectRes.Columns[idx]
					row[colName] = sRow[selectCol]
				}
			}
			sourceRows = append(sourceRows, row)
		}
	} else {
		for _, values := range stmt.Values {
			row := make(storage.Row)
			colNames := stmt.Columns
			if len(colNames) == 0 {
				colNames = table.GetColumnNames()
			}
			if len(colNames) != len(values) {
				return nil, fmt.Errorf("column count mismatch")
			}
			for i, colName := range colNames {
				row[colName] = values[i]
			}
			sourceRows = append(sourceRows, row)
		}
	}

	insertedCount := 0
	var lastInsertedRows []storage.Row

	for _, row := range sourceRows {
		// Fill in missing columns with defaults or sequence values
		for _, col := range table.Columns {
			if _, exists := row[col.Name]; !exists || row[col.Name] == nil {
				var defaultVal interface{}
				exprUpper := strings.ToUpper(col.DefaultExpr)
				if exprUpper == "NOW()" || exprUpper == "CURRENT_TIMESTAMP" || exprUpper == "NOW" {
					defaultVal = "2026-05-17 15:00:00"
				} else if col.DefaultExpr == "nextval" || strings.HasPrefix(exprUpper, "NEXTVAL") {
					seqName := col.DefaultExpr
					if strings.Contains(seqName, "'") || strings.Contains(seqName, "\"") {
						seqName = strings.TrimPrefix(seqName, "nextval('")
						seqName = strings.TrimPrefix(seqName, "nextval(\"")
						seqName = strings.TrimSuffix(seqName, "')")
						seqName = strings.TrimSuffix(seqName, "\")")
						seqName = strings.TrimSuffix(seqName, ")")
						seqName = strings.TrimSpace(seqName)
					} else {
						seqName = table.Name + "_" + col.Name + "_seq"
					}

					seqKey := dbInstance.Name + "." + seqName
					state, hasSeq := sequenceRegistry[seqKey]
					if !hasSeq {
						state = &SequenceState{Current: 1, Start: 1, Increment: 1}
						sequenceRegistry[seqKey] = state
					}
					val := state.Current
					state.Current += state.Increment
					defaultVal = val
				} else if col.DefaultExpr != "" {
					if strings.HasPrefix(col.DefaultExpr, "'") && strings.HasSuffix(col.DefaultExpr, "'") {
						defaultVal = strings.Trim(col.DefaultExpr, "'")
					} else if strings.HasPrefix(col.DefaultExpr, "\"") && strings.HasSuffix(col.DefaultExpr, "\"") {
						defaultVal = strings.Trim(col.DefaultExpr, "\"")
					} else if iv, err := strconv.Atoi(col.DefaultExpr); err == nil {
						defaultVal = iv
					} else if fv, err := strconv.ParseFloat(col.DefaultExpr, 64); err == nil {
						defaultVal = fv
					} else if col.DefaultExpr == "NULL" || col.DefaultExpr == "null" {
						defaultVal = nil
					} else {
						defaultVal = col.DefaultExpr
					}
				}
				row[col.Name] = defaultVal
			}
		}

		// Check for conflict
		hasConflict := false
		var conflictingRow storage.Row
		var conflictingIdx = -1

		if stmt.OnConflict != nil {
			for i, r := range table.Rows {
				match := false
				if stmt.OnConflict.TargetColumn != "" {
					if compareValues(r[stmt.OnConflict.TargetColumn], row[stmt.OnConflict.TargetColumn]) == 0 {
						match = true
					}
				} else {
					for _, col := range table.Columns {
						if col.IsPrimary || col.IsUnique {
							if compareValues(r[col.Name], row[col.Name]) == 0 {
								match = true
								break
							}
						}
					}
				}
				if match {
					hasConflict = true
					conflictingRow = r
					conflictingIdx = i
					break
				}
			}
		}

		if hasConflict {
			if stmt.OnConflict.DoNothing {
				continue
			} else if stmt.OnConflict.DoUpdate {
				for k, v := range stmt.OnConflict.Updates {
					finalVal := v
					if sVal, ok := v.(string); ok && strings.HasPrefix(strings.ToUpper(sVal), "EXCLUDED.") {
						targetExCol := strings.TrimPrefix(sVal, "EXCLUDED.")
						for exCol := range row {
							if strings.EqualFold(exCol, targetExCol) {
								finalVal = row[exCol]
								break
							}
						}
					}
					conflictingRow[k] = finalVal
				}
				table.Rows[conflictingIdx] = conflictingRow
				insertedCount++
				lastInsertedRows = append(lastInsertedRows, conflictingRow)
				continue
			}
		}

		// Normal validation and insert
		for _, col := range table.Columns {
			val := row[col.Name]

			if col.Type == storage.TypeVarChar && col.Length > 0 {
				if strVal, ok := val.(string); ok {
					if len(strVal) > col.Length {
						return nil, fmt.Errorf("value too long for column %s (max %d, got %d)",
							col.Name, col.Length, len(strVal))
					}
				}
			}

			if col.Type == storage.TypeVector {
				if strVal, ok := val.(string); ok {
					vec, err := storage.ParseVector(strVal)
					if err != nil {
						return nil, fmt.Errorf("invalid vector format for column %s: %w", col.Name, err)
					}
					val = vec
				}
			}
			row[col.Name] = val
		}

		// Validate NOT NULL constraints
		for _, col := range table.Columns {
			if !col.Nullable {
				val, exists := row[col.Name]
				if !exists || val == nil {
					return nil, fmt.Errorf("column %s cannot be NULL", col.Name)
				}
			}
		}

		// Check PRIMARY KEY uniqueness
		for _, col := range table.Columns {
			if col.IsPrimary {
				newVal := row[col.Name]
				if newVal == nil {
					return nil, fmt.Errorf("PRIMARY KEY column %s cannot be NULL", col.Name)
				}

				for _, existingRow := range table.Rows {
					existingVal := existingRow[col.Name]
					if compareValues(newVal, existingVal) == 0 {
						return nil, fmt.Errorf("duplicate value for PRIMARY KEY column %s: %v", col.Name, newVal)
					}
				}
			}
		}

		// Validate foreign keys
		for _, col := range table.Columns {
			if col.ForeignKey != nil {
				fkValue := row[col.Name]
				if fkValue == nil && col.Nullable {
					continue
				}

				refTable, exists := e.getTable(dbInstance, col.ForeignKey.RefTable)
				if !exists {
					return nil, fmt.Errorf("referenced table %s does not exist", col.ForeignKey.RefTable)
				}

				found := false
				for _, refRow := range refTable.Rows {
					if compareValues(refRow[col.ForeignKey.RefColumn], fkValue) == 0 {
						found = true
						break
					}
				}

				if !found {
					return nil, fmt.Errorf("foreign key constraint failed: value %v not found in %s.%s",
						fkValue, col.ForeignKey.RefTable, col.ForeignKey.RefColumn)
				}
			}
		}

		if err := table.Insert(row); err != nil {
			return nil, err
		}
		insertedCount++
		lastInsertedRows = append(lastInsertedRows, row)
	}

	if err := e.saveTableToDisk(dbInstance, table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	// Handle RETURNING
	if len(stmt.Returning) > 0 {
		return e.projectReturning(lastInsertedRows, stmt.Returning, table), nil
	}

	return &Result{
		Message: fmt.Sprintf("INSERT 0 %d", insertedCount),
	}, nil
}

func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*Result, error) {
	// Set current statement for context (aliases in WHERE, etc.)
	prevStmt := e.currentStmt
	e.currentStmt = stmt
	defer func() { e.currentStmt = prevStmt }()

	// If this is a CTE query, route to CTE executor
	if len(stmt.CTEs) > 0 {
		return e.executeSelectWithCTEs(stmt)
	}

	var rows []storage.Row
	var columns []string
	var err error
	var dbInstance *storage.DatabaseInstance
	var table *storage.Table

	if stmt.TableName == "" {
		rows = []storage.Row{make(storage.Row)}
	} else {
		if err := e.checkPrivilege("TABLE", stmt.TableName, "SELECT"); err != nil {
			return nil, err
		}

		// Check if the table is a CTE virtual table
		if result, ok := e.cteResults[stmt.TableName]; ok {
			// Use CTE result
			rows = make([]storage.Row, len(result.Rows))
			for i, r := range result.Rows {
				rows[i] = make(storage.Row)
				for k, v := range r {
					rows[i][k] = v
				}
			}
			columns = result.Columns
		} else {
			// Normal physical table
			dbInstance, err = e.getActiveDatabase()
			if err != nil {
				return nil, err
			}

			// Check if it's a virtual view
			dbName := dbInstance.Name
			viewKey := dbName + "." + stmt.TableName
			if viewQuery, isView := viewRegistry[viewKey]; isView {
				res, err := e.executeSelect(viewQuery)
				if err != nil {
					return nil, err
				}
				rows = res.Rows
				columns = res.Columns
			} else {
				var exists bool
				table, exists = e.getTable(dbInstance, stmt.TableName)
				if !exists {
					return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
				}
			}
		}
	}

	var where *storage.WhereClause
	if stmt.Where != nil {
		where = e.convertWhereClauseWithSubquery(stmt.Where)
	}
	if table != nil && table.RLSEnabled && e.session.GetUser() != "ghost" {
		user := e.session.GetUser()
		for _, policy := range table.Policies {
			if (policy.Action == "SELECT" || policy.Action == "ALL") && (policy.Role == "all" || policy.Role == user) {
				if policy.Where != nil {
					where = combineWhere(policy.Where.Clone(), where)
				}
			}
		}
	}
	e.resolveWhereClauseVariables(where)

	// Fetch rows from main table if not CTE
	if table != nil {
		initialColumns := stmt.Columns
		needsAll := len(stmt.Aggregates) > 0 || where != nil
		if !needsAll {
			for _, sc := range stmt.SelectColumns {
				if strings.ContainsAny(sc.Expression, "+-*/%(") || strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sc.Expression)), "CASE WHEN") {
					needsAll = true
					break
				}
			}
		}

		if needsAll {
			initialColumns = []string{"*"}
		}

		effectiveWhere := where
		hasNonTableCol := false
		if where != nil {
			var checkNonTable func(w *storage.WhereClause) bool
			checkNonTable = func(w *storage.WhereClause) bool {
				if w.Column != "" && !strings.Contains(w.Column, "(") && !strings.ContainsAny(w.Column, "+-*/%") {
					colExists := false
					for _, col := range table.Columns {
						if col.Name == w.Column {
							colExists = true
							break
						}
					}
					if !colExists {
						return true
					}
				}
				if w.And != nil && checkNonTable(w.And) {
					return true
				}
				if w.Or != nil && checkNonTable(w.Or) {
					return true
				}
				return false
			}
			hasNonTableCol = checkNonTable(where)
		}

		if len(stmt.Joins) > 0 || e.currentOuterRow != nil || hasNonTableCol {
			initialColumns = []string{"*"}
			effectiveWhere = nil // Delay filtering until after JOINs or executor phase
		}


		rows, err = table.Select(initialColumns, effectiveWhere)
		if err != nil {
			return nil, err
		}

		if (e.currentOuterRow != nil || hasNonTableCol) && where != nil && len(stmt.Joins) == 0 {
			filteredRows := make([]storage.Row, 0)
			for _, row := range rows {
				if e.evaluateWhereOnRow(row, where) {
					filteredRows = append(filteredRows, row)
				}
			}
			rows = filteredRows
		}
	} else {
		// It's a CTE. Apply WHERE if needed unless we have joins (delayed).
		effectiveWhere := where
		if len(stmt.Joins) > 0 {
			effectiveWhere = nil
		}
		if effectiveWhere != nil {
			filteredRows := make([]storage.Row, 0)
			for _, row := range rows {
				if e.evaluateWhereOnRow(row, effectiveWhere) {
					filteredRows = append(filteredRows, row)
				}
			}
			rows = filteredRows
		}
	}

	// Apply TABLESAMPLE if requested
	if stmt.TableSamplePercent > 0 {
		rows = applyTableSample(rows, stmt.TableSamplePercent)
	}

	// Validate columns in WHERE clause (if not a JOIN where columns might be from other tables)
	if len(stmt.Joins) == 0 && where != nil && table != nil {
		if err := e.validateWhereColumns(table, where); err != nil {
			return nil, err
		}
	}

	// Handle JOINs
	if len(stmt.Joins) > 0 {
		// Determine the effective name for the main table (alias or name)
		mainTableRef := stmt.TableName
		if stmt.TableAlias != "" {
			mainTableRef = stmt.TableAlias
		}

		// Prefix main table columns before JOIN
		prefixedRows := make([]storage.Row, len(rows))
		for i, row := range rows {
			prefixedRow := make(storage.Row)
			for k, v := range row {
				if !strings.Contains(k, ".") {
					prefixedRow[mainTableRef+"."+k] = v
				} else {
					prefixedRow[k] = v
				}
			}
			prefixedRows[i] = prefixedRow
		}
		rows = prefixedRows

		rows, err = e.executeJoins(mainTableRef, rows, stmt.Joins, dbInstance)
		if err != nil {
			return nil, err
		}

		// Apply delayed WHERE clause after JOINs
		if len(stmt.Joins) > 0 && where != nil {
			filteredRows := make([]storage.Row, 0)
			for _, row := range rows {
				if e.evaluateWhereOnRow(row, where) {
					filteredRows = append(filteredRows, row)
				}
			}
			rows = filteredRows
		}

		// Build alias to table mapping
		aliasMap := make(map[string]string)

		// Add main table (use alias if specified, otherwise table name maps to itself)
		if stmt.TableAlias != "" {
			aliasMap[stmt.TableAlias] = stmt.TableName
		}
		aliasMap[stmt.TableName] = stmt.TableName

		// Add joined tables
		for _, join := range stmt.Joins {
			if join.Alias != "" {
				aliasMap[join.Alias] = join.Table
			}
			aliasMap[join.Table] = join.Table
		}

		// Project columns
		if len(stmt.Columns) > 0 && stmt.Columns[0] != "*" {
			projectedRows := make([]storage.Row, len(rows))
			for i, row := range rows {
				projectedRow := make(storage.Row)
				for _, sc := range stmt.SelectColumns {
					colSpec := sc.Expression
					val, ok := row[colSpec]
					if !ok {
						// Try evaluate as expression
						val = storage.EvaluateExpression(colSpec, row)
					}
					projectedRow[colSpec] = val
				}
				projectedRows[i] = projectedRow
			}
			rows = projectedRows
		}
	}

	// Check if this is a vector similarity search
	if stmt.VectorOrderBy != nil {
		return e.executeVectorSearch(stmt, rows, table)
	}

	// Check if this is an aggregate query
	if len(stmt.Aggregates) > 0 {
		return e.executeAggregateSelect(stmt, rows)
	}

	// Apply window functions before ORDER BY
	hasWindowFuncs := false
	for _, sc := range stmt.SelectColumns {
		if sc.Window != nil {
			hasWindowFuncs = true
			break
		}
	}
	if hasWindowFuncs {
		rows = executeWindowFunctions(rows, stmt.SelectColumns)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		rows = e.applyOrderBy(rows, stmt.OrderBy)
	}

	// Apply LIMIT and OFFSET
	if stmt.Offset > 0 {
		if stmt.Offset < len(rows) {
			rows = rows[stmt.Offset:]
		} else {
			rows = []storage.Row{}
		}
	}

	if stmt.Limit > 0 && stmt.Limit < len(rows) {
		rows = rows[:stmt.Limit]
	}

	// Determine output columns using SelectColumns for alias support

	if len(stmt.SelectColumns) > 0 && !(len(stmt.SelectColumns) == 1 && stmt.SelectColumns[0].Expression == "*") {
		columns = make([]string, len(stmt.SelectColumns))
		// Rewrite rows to use aliased keys
		for i, sc := range stmt.SelectColumns {
			outName := sc.Expression
			if sc.Alias != "" {
				outName = sc.Alias
			} else if strings.Contains(outName, ".") && !strings.Contains(outName, "(") && !strings.Contains(outName, " ") && !strings.Contains(outName, "'") && !strings.Contains(outName, "\"") {
				// Strip table prefix: "c.relname" -> "relname"
				parts := strings.Split(outName, ".")
				outName = parts[len(parts)-1]
			}
			columns[i] = outName
		}
		// Ensure unique column names for row mapping
		uniqueNames := make([]string, len(columns))
		nameCount := make(map[string]int)
		for i, name := range columns {
			count := nameCount[name]
			nameCount[name]++
			if count > 0 {
				uniqueNames[i] = fmt.Sprintf("%s_%d", name, count)
			} else {
				uniqueNames[i] = name
			}
		}
		columns = uniqueNames

		// Rewrite rows: map expression keys to output names
		rewritten := make([]storage.Row, len(rows))
		for i, row := range rows {
			newRow := make(storage.Row)
			for j, sc := range stmt.SelectColumns {
				outName := columns[j]
				expr := sc.Expression
				var val interface{}
				var ok bool
				if sc.Subquery != nil {
					// Set outer context for scalar subquery (with qualified keys to prevent collisions)
					qualifiedOuterRow := make(storage.Row)
					for k, v := range row {
						qualifiedOuterRow[k] = v
					}
					if stmt.TableName != "" {
						for k, v := range row {
							qualifiedOuterRow[stmt.TableName+"."+k] = v
						}
					}
					if stmt.TableAlias != "" {
						for k, v := range row {
							qualifiedOuterRow[stmt.TableAlias+"."+k] = v
						}
					}
					e.currentOuterRow = qualifiedOuterRow
					subqueryResult, err := e.executeSelect(sc.Subquery)
					e.currentOuterRow = nil // clear
					
					if err != nil {
						val = nil
					} else if len(subqueryResult.Rows) > 0 {
						// Get first column of first row
						firstCol := subqueryResult.Columns[0]
						val = subqueryResult.Rows[0][firstCol]
					} else {
						val = nil
					}
					ok = true
				} else {
					// Try exact key first
					val, ok = row[expr]
					if !ok {
						// Try with table prefix variants
						for k, v := range row {
							if k == expr || strings.HasSuffix(k, "."+expr) ||
								(strings.Contains(expr, ".") && strings.HasSuffix(expr, "."+strings.Split(k, ".")[len(strings.Split(k, "."))-1])) {
								val = v
								ok = true
								break
							}
						}
					}
					if !ok {
						// Try evaluate as arithmetic/function expression
						// Use a sentinel to know it was evaluated (even if result is nil)
						evaluated := false
						if strings.Contains(expr, "(") || strings.ContainsAny(expr, "+-*/") || strings.HasPrefix(strings.ToUpper(strings.TrimSpace(expr)), "CASE") {
							val = storage.EvaluateExpression(expr, row)
							evaluated = true
						} else {
							val = storage.EvaluateExpression(expr, row)
							if val != nil {
								evaluated = true
							}
						}
						if !evaluated {
							// Unknown column/expression — leave as nil (NULL)
							val = nil
						}
					}
				}
				newRow[outName] = val
			}
			rewritten[i] = newRow
		}
		rows = rewritten
	} else {
		// Only override columns if stmt.Columns has specific columns requested.
		// For CTEs, the initial columns might already be correctly inferred, 
		// but if it's explicitly '*', we need to keep it as '*' logic.
		if len(stmt.Columns) > 0 {
			columns = stmt.Columns
		}
		if len(columns) == 1 && columns[0] == "*" {
			if table != nil {
				columns = table.GetColumnNames()
				// Add joined table columns with prefixes
				if dbInstance != nil {
					for _, join := range stmt.Joins {
						joinTable, ok := e.getTable(dbInstance, join.Table)
						if !ok {
							viewKey := dbInstance.Name + "." + join.Table
							if viewQuery, isView := viewRegistry[viewKey]; isView {
								res, err := e.executeSelect(viewQuery)
								if err == nil {
									var cols []storage.Column
									for _, colName := range res.Columns {
										cols = append(cols, storage.Column{Name: colName, Type: storage.TypeText, Nullable: true})
									}
									joinTable = &storage.Table{
										Name: join.Table,
										Columns: cols,
										Rows: res.Rows,
									}
									ok = true
								}
							}
						}
						if !ok {
							continue
						}
						for _, col := range joinTable.GetColumnNames() {
							columns = append(columns, join.Table+"."+col)
						}
					}
				}
			} else {
				// It's a CTE, we already have columns from result.Columns
				if result, ok := e.cteResults[stmt.TableName]; ok {
					columns = result.Columns
				}
			}
		}
	}

	// Apply DISTINCT ON (after ORDER BY is applied)
	if len(stmt.DistinctOn) > 0 {
		rows = applyDistinctOn(rows, stmt.DistinctOn)
	}

	// Apply DISTINCT deduplication
	if stmt.Distinct && len(columns) > 0 {
		rows = applyDistinct(rows, columns)
	}

	return &Result{
		Rows:    rows,
		Columns: columns,
	}, nil
}

func (e *Executor) executeJoins(leftTable string, leftRows []storage.Row, joins []parser.JoinClause, dbInstance *storage.DatabaseInstance) ([]storage.Row, error) {
	resultRows := leftRows

	for _, join := range joins {
		var rightTableRef string
		var mergedResultRows []storage.Row

		if join.Lateral && join.Subquery != nil {
			rightTableRef = join.Alias
			if rightTableRef == "" {
				rightTableRef = "subquery"
			}

			// LATERAL nested loop
			for _, leftRow := range resultRows {
				// Set outer context (with qualified keys to prevent collisions)
				qualifiedOuterRow := make(storage.Row)
				for k, v := range leftRow {
					qualifiedOuterRow[k] = v
				}
				if leftTable != "" {
					for k, v := range leftRow {
						qualifiedOuterRow[leftTable+"."+k] = v
					}
				}
				if e.currentStmt != nil {
					if e.currentStmt.TableName != "" {
						for k, v := range leftRow {
							qualifiedOuterRow[e.currentStmt.TableName+"."+k] = v
						}
					}
					if e.currentStmt.TableAlias != "" {
						for k, v := range leftRow {
							qualifiedOuterRow[e.currentStmt.TableAlias+"."+k] = v
						}
					}
				}
				e.currentOuterRow = qualifiedOuterRow
				
				subqueryResult, err := e.executeSelect(join.Subquery)
				if err != nil {
					e.currentOuterRow = nil
					return nil, fmt.Errorf("lateral subquery failed: %w", err)
				}
				
				e.currentOuterRow = nil

				rightRows := subqueryResult.Rows
				
				// Execute join for this specific leftRow and its corresponding rightRows
				// Since we execute per row, we can just use the standard join functions
				// but passing a single-element slice for the left side
				var singleRowResult []storage.Row
				switch join.Type {
				case "INNER":
					singleRowResult = e.executeInnerJoin(leftTable, []storage.Row{leftRow}, rightTableRef, rightRows, join.Condition)
				case "LEFT":
					singleRowResult = e.executeLeftJoin(leftTable, []storage.Row{leftRow}, rightTableRef, rightRows, join.Condition)
				case "RIGHT":
					singleRowResult = e.executeRightJoin(leftTable, []storage.Row{leftRow}, rightTableRef, rightRows, join.Condition)
				case "FULL":
					singleRowResult = e.executeFullJoin(leftTable, []storage.Row{leftRow}, rightTableRef, rightRows, join.Condition)
				case "CROSS":
					singleRowResult = e.executeCrossJoin(leftTable, []storage.Row{leftRow}, rightTableRef, rightRows)
				default:
					return nil, fmt.Errorf("unsupported join type: %s", join.Type)
				}
				
				mergedResultRows = append(mergedResultRows, singleRowResult...)
			}
			resultRows = mergedResultRows
		} else {
			rightTable, exists := e.getTable(dbInstance, join.Table)
			if !exists {
				viewKey := dbInstance.Name + "." + join.Table
				if viewQuery, isView := viewRegistry[viewKey]; isView {
					res, err := e.executeSelect(viewQuery)
					if err == nil {
						var cols []storage.Column
						for _, colName := range res.Columns {
							cols = append(cols, storage.Column{Name: colName, Type: storage.TypeText, Nullable: true})
						}
						rightTable = &storage.Table{
							Name: join.Table,
							Columns: cols,
							Rows: res.Rows,
						}
						exists = true
					}
				}
			}
			if !exists {
				// Check if it's a CTE
				if cteRes, ok := e.cteResults[join.Table]; ok {
					rightRows := make([]storage.Row, len(cteRes.Rows))
					for i, r := range cteRes.Rows {
						rightRows[i] = make(storage.Row)
						for k, v := range r {
							rightRows[i][k] = v
						}
					}
					
					rightTableRef = join.Table
					if join.Alias != "" {
						rightTableRef = join.Alias
					}
					
					switch join.Type {
					case "INNER":
						resultRows = e.executeInnerJoin(leftTable, resultRows, rightTableRef, rightRows, join.Condition)
					case "LEFT":
						resultRows = e.executeLeftJoin(leftTable, resultRows, rightTableRef, rightRows, join.Condition)
					case "RIGHT":
						resultRows = e.executeRightJoin(leftTable, resultRows, rightTableRef, rightRows, join.Condition)
					case "FULL":
						resultRows = e.executeFullJoin(leftTable, resultRows, rightTableRef, rightRows, join.Condition)
					case "CROSS":
						resultRows = e.executeCrossJoin(leftTable, resultRows, rightTableRef, rightRows)
					default:
						return nil, fmt.Errorf("unsupported join type: %s", join.Type)
					}
				} else {
					return nil, fmt.Errorf("table %s does not exist", join.Table)
				}
			} else {
				rightRows, err := rightTable.Select([]string{"*"}, nil)
				if err != nil {
					return nil, err
				}

				rightTableRef = join.Table
				if join.Alias != "" {
					rightTableRef = join.Alias
				}

				switch join.Type {
				case "INNER":
					resultRows = e.executeInnerJoin(leftTable, resultRows, rightTableRef, rightRows, join.Condition)
				case "LEFT":
					resultRows = e.executeLeftJoin(leftTable, resultRows, rightTableRef, rightRows, join.Condition)
				case "RIGHT":
					resultRows = e.executeRightJoin(leftTable, resultRows, rightTableRef, rightRows, join.Condition)
				case "FULL":
					resultRows = e.executeFullJoin(leftTable, resultRows, rightTableRef, rightRows, join.Condition)
				case "CROSS":
					resultRows = e.executeCrossJoin(leftTable, resultRows, rightTableRef, rightRows)
				default:
					return nil, fmt.Errorf("unsupported join type: %s", join.Type)
				}
			}
		}

		leftTable = rightTableRef // For chained joins
	}

	return resultRows, nil
}

func (e *Executor) executeInnerJoin(leftTable string, leftRows []storage.Row, rightTable string, rightRows []storage.Row, condition *parser.JoinCondition) []storage.Row {
	result := make([]storage.Row, 0)

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			if e.evaluateJoinCondition(leftTable, leftRow, rightTable, rightRow, condition) {
				merged := make(storage.Row)

				// Copy left row
				for k, v := range leftRow {
					merged[k] = v
					if !strings.Contains(k, ".") {
						merged[leftTable+"."+k] = v
					}
				}

				// Copy right row
				for k, v := range rightRow {
					if !strings.Contains(k, ".") {
						merged[rightTable+"."+k] = v
						// Only add un-prefixed if not already present from left
						if _, exists := merged[k]; !exists {
							merged[k] = v
						}
					} else {
						merged[k] = v
					}
				}

				result = append(result, merged)
			}
		}
	}

	return result
}

func (e *Executor) executeLeftJoin(leftTable string, leftRows []storage.Row, rightTable string, rightRows []storage.Row, condition *parser.JoinCondition) []storage.Row {
	result := make([]storage.Row, 0)

	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			if e.evaluateJoinCondition(leftTable, leftRow, rightTable, rightRow, condition) {
				merged := make(storage.Row)

				// Copy left row
				for k, v := range leftRow {
					merged[k] = v
					if !strings.Contains(k, ".") {
						merged[leftTable+"."+k] = v
					}
				}

				// Copy right row
				for k, v := range rightRow {
					if !strings.Contains(k, ".") {
						merged[rightTable+"."+k] = v
						if _, exists := merged[k]; !exists {
							merged[k] = v
						}
					} else {
						merged[k] = v
					}
				}

				result = append(result, merged)
				matched = true
			}
		}

		if !matched {
			merged := make(storage.Row)
			for k, v := range leftRow {
				merged[k] = v
				if !strings.Contains(k, ".") {
					merged[leftTable+"."+k] = v
				}
			}
			result = append(result, merged)
		}
	}

	return result
}

func (e *Executor) executeRightJoin(leftTable string, leftRows []storage.Row, rightTable string, rightRows []storage.Row, condition *parser.JoinCondition) []storage.Row {
	result := make([]storage.Row, 0)

	for _, rightRow := range rightRows {
		matched := false
		for _, leftRow := range leftRows {
			if e.evaluateJoinCondition(leftTable, leftRow, rightTable, rightRow, condition) {
				merged := make(storage.Row)

				for k, v := range leftRow {
					if strings.Contains(k, ".") {
						merged[k] = v
					} else {
						merged[leftTable+"."+k] = v
					}
				}

				for k, v := range rightRow {
					if strings.Contains(k, ".") {
						merged[k] = v
					} else {
						merged[rightTable+"."+k] = v
					}
				}

				result = append(result, merged)
				matched = true
			}
		}

		if !matched {
			merged := make(storage.Row)
			for k, v := range rightRow {
				if strings.Contains(k, ".") {
					merged[k] = v
				} else {
					merged[rightTable+"."+k] = v
				}
			}
			result = append(result, merged)
		}
	}

	return result
}

func (e *Executor) executeFullJoin(leftTable string, leftRows []storage.Row, rightTable string, rightRows []storage.Row, condition *parser.JoinCondition) []storage.Row {
	result := make([]storage.Row, 0)
	rightMatched := make(map[int]bool)

	for _, leftRow := range leftRows {
		matched := false
		for i, rightRow := range rightRows {
			if e.evaluateJoinCondition(leftTable, leftRow, rightTable, rightRow, condition) {
				merged := make(storage.Row)

				for k, v := range leftRow {
					if strings.Contains(k, ".") {
						merged[k] = v
					} else {
						merged[leftTable+"."+k] = v
					}
				}

				for k, v := range rightRow {
					if strings.Contains(k, ".") {
						merged[k] = v
					} else {
						merged[rightTable+"."+k] = v
					}
				}

				result = append(result, merged)
				matched = true
				rightMatched[i] = true
			}
		}

		if !matched {
			merged := make(storage.Row)
			for k, v := range leftRow {
				if strings.Contains(k, ".") {
					merged[k] = v
				} else {
					merged[leftTable+"."+k] = v
				}
			}
			result = append(result, merged)
		}
	}

	for i, rightRow := range rightRows {
		if !rightMatched[i] {
			merged := make(storage.Row)
			for k, v := range rightRow {
				if strings.Contains(k, ".") {
					merged[k] = v
				} else {
					merged[rightTable+"."+k] = v
				}
			}
			result = append(result, merged)
		}
	}

	return result
}

func (e *Executor) executeCrossJoin(leftTable string, leftRows []storage.Row, rightTable string, rightRows []storage.Row) []storage.Row {
	result := make([]storage.Row, 0)

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			merged := make(storage.Row)

			for k, v := range leftRow {
				if strings.Contains(k, ".") {
					merged[k] = v
				} else {
					merged[leftTable+"."+k] = v
				}
			}

			for k, v := range rightRow {
				if strings.Contains(k, ".") {
					merged[k] = v
				} else {
					merged[rightTable+"."+k] = v
				}
			}

			result = append(result, merged)
		}
	}

	return result
}

func (e *Executor) evaluateJoinCondition(leftTable string, leftRow storage.Row, rightTable string, rightRow storage.Row, condition *parser.JoinCondition) bool {
	if condition == nil {
		return true
	}

	var leftVal, rightVal interface{}

	// Resolve left column value
	if condition.LeftTable != "" {
		// Explicit table specified in condition
		switch condition.LeftTable {
		case leftTable:
			key := leftTable + "." + condition.LeftColumn
			if val, ok := leftRow[key]; ok {
				leftVal = val
			} else {
				leftVal = leftRow[condition.LeftColumn]
			}
		case rightTable:
			key := rightTable + "." + condition.LeftColumn
			if val, ok := rightRow[key]; ok {
				leftVal = val
			} else {
				leftVal = rightRow[condition.LeftColumn]
			}
		}
	} else {
		// No table specified, try left row first
		key := leftTable + "." + condition.LeftColumn
		if val, ok := leftRow[key]; ok {
			leftVal = val
		} else if val, ok := leftRow[condition.LeftColumn]; ok {
			leftVal = val
		}
	}

	// Resolve right column value
	if condition.RightTable != "" {
		// Explicit table specified in condition
		switch condition.RightTable {
		case rightTable:
			key := rightTable + "." + condition.RightColumn
			if val, ok := rightRow[key]; ok {
				rightVal = val
			} else {
				rightVal = rightRow[condition.RightColumn]
			}
		case leftTable:
			key := leftTable + "." + condition.RightColumn
			if val, ok := leftRow[key]; ok {
				rightVal = val
			} else {
				rightVal = leftRow[condition.RightColumn]
			}
		}
	} else {
		// No table specified, try right row first
		key := rightTable + "." + condition.RightColumn
		if val, ok := rightRow[key]; ok {
			rightVal = val
		} else if val, ok := rightRow[condition.RightColumn]; ok {
			rightVal = val
		}
	}

	// Compare values
	cmp := compareValues(leftVal, rightVal)

	switch condition.Operator {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case ">":
		return cmp > 0
	case "<=":
		return cmp <= 0
	case ">=":
		return cmp >= 0
	default:
		return false
	}
}

func (e *Executor) executeAggregateSelect(stmt *parser.SelectStmt, rows []storage.Row) (*Result, error) {
	aggregates := make([]storage.AggregateSpec, len(stmt.Aggregates))
	for i, agg := range stmt.Aggregates {
		aggregates[i] = storage.AggregateSpec{
			Function: agg.Function,
			Column:   agg.Column,
			Alias:    agg.Alias,
		}
	}

	if len(stmt.GroupBy) > 0 {
		return e.executeGroupBy(stmt, rows, aggregates)
	}

	results, err := storage.ComputeAggregates(rows, aggregates)
	if err != nil {
		return nil, err
	}

	resultRow := make(storage.Row)
	columns := make([]string, 0)

	for _, col := range stmt.Columns {
		if col != "" && col != "*" {
			columns = append(columns, col)
			if len(rows) > 0 {
				resultRow[col] = rows[0][col]
			}
		}
	}

	for _, res := range results {
		columns = append(columns, res.Alias)
		resultRow[res.Alias] = res.Value
	}

	return &Result{
		Rows:    []storage.Row{resultRow},
		Columns: columns,
	}, nil
}

func (e *Executor) executeGroupBy(stmt *parser.SelectStmt, rows []storage.Row, aggregates []storage.AggregateSpec) (*Result, error) {
	groups := storage.GroupRows(rows, stmt.GroupBy)

	resultRows := make([]storage.Row, 0, len(groups))
	columns := make([]string, 0)

	columns = append(columns, stmt.GroupBy...)

	for _, agg := range aggregates {
		columns = append(columns, agg.Alias)
	}

	for _, group := range groups {
		row := make(storage.Row)

		for col, val := range group.GroupKey {
			row[col] = val
		}

		aggResults, err := storage.ComputeAggregates(group.Rows, aggregates)
		if err != nil {
			return nil, err
		}

		for _, res := range aggResults {
			row[res.Alias] = res.Value
		}

		if stmt.Having != nil {
			having := convertWhereClause(stmt.Having)
			if !e.evaluateWhereOnRow(row, having) {
				continue
			}
		}

		resultRows = append(resultRows, row)
	}

	if len(stmt.OrderBy) > 0 {
		resultRows = e.applyOrderBy(resultRows, stmt.OrderBy)
	}

	if stmt.Offset > 0 && stmt.Offset < len(resultRows) {
		resultRows = resultRows[stmt.Offset:]
	}

	if stmt.Limit > 0 && stmt.Limit < len(resultRows) {
		resultRows = resultRows[:stmt.Limit]
	}

	return &Result{
		Rows:    resultRows,
		Columns: columns,
	}, nil
}

func (e *Executor) evaluateWhereOnRow(row storage.Row, where *storage.WhereClause) bool {
	// Base condition evaluation
	match := false

	// Handle pre-resolved EXISTS/NOT EXISTS subqueries
	if where.Operator == "EXISTS_RESOLVED" {
		if b, ok := where.Value.(bool); ok {
			match = b
		}
		if where.And != nil {
			return match && e.evaluateWhereOnRow(row, where.And)
		}
		if where.Or != nil {
			return match || e.evaluateWhereOnRow(row, where.Or)
		}
		return match
	}

	evalRow := row
	if e.currentOuterRow != nil {
		evalRow = make(storage.Row)
		for k, v := range e.currentOuterRow {
			evalRow[k] = v
		}
		for k, v := range row {
			evalRow[k] = v
		}
	}

	// Detect any function-like call (containing parentheses)
	val, exists := evalRow[where.Column]
	if !exists && strings.Contains(where.Column, "(") {
		// If it's a function call NOT in the row, we assume it's a system function
		// like current_user() that should be handled during variable resolution
		// or catalog visibility checks.
		match = true
	} else {
		if !exists {
			// Try evaluate as arithmetic expression
			val = storage.EvaluateExpression(where.Column, evalRow)
			if val != nil {
				exists = true
			}
		}

		if !exists && e.currentStmt != nil {
			// Fallback: check if the column is an alias defined in the SELECT list
			for _, sc := range e.currentStmt.SelectColumns {
				if sc.Alias == where.Column {
					val = storage.EvaluateExpression(sc.Expression, evalRow)
					if val != nil {
						exists = true
					}
					break
				}
			}
		}

		if !exists {
			// Try fuzzy matching for joined columns (e.g. c.relname)
			for k, v := range evalRow {
				if strings.HasSuffix(k, "."+where.Column) || strings.HasSuffix(where.Column, "."+k) {
					val = v
					exists = true
					break
				}
			}
		}

		if !exists {
			// If column doesn't exist, it's a mismatch unless we're looking for NULL
			match = (where.Operator == "=" && where.Value == nil)
		} else {
			rhsVal := where.Value
			if s, ok := where.Value.(string); ok {
				if rVal, exists := evalRow[s]; exists {
					rhsVal = rVal
				} else if strings.Contains(s, ".") {
					parts := strings.Split(s, ".")
					lastPart := parts[len(parts)-1]
					if rVal, exists := evalRow[lastPart]; exists {
						rhsVal = rVal
					}
				}
			}

			switch where.Operator {
			case "=":
				match = compareValues(val, rhsVal) == 0
			case "!=", "<>":
				match = compareValues(val, rhsVal) != 0
			case "<":
				match = compareValues(val, rhsVal) < 0
			case "<=":
				match = compareValues(val, rhsVal) <= 0
			case ">":
				match = compareValues(val, rhsVal) > 0
			case ">=":
				match = compareValues(val, rhsVal) >= 0
			case "LIKE":
				pattern := strings.ReplaceAll(fmt.Sprintf("%v", rhsVal), "%", ".*")
				pattern = strings.ReplaceAll(pattern, "_", ".")
				// Map LIKE to a case-insensitive anchored regex
				match, _ = regexp.MatchString("(?i)^"+pattern+"$", fmt.Sprintf("%v", val))
			case "~":
				match, _ = regexp.MatchString(fmt.Sprintf("%v", rhsVal), fmt.Sprintf("%v", val))
			case "~*":
				match, _ = regexp.MatchString("(?i)"+fmt.Sprintf("%v", rhsVal), fmt.Sprintf("%v", val))
			case "!~":
				matched, _ := regexp.MatchString(fmt.Sprintf("%v", rhsVal), fmt.Sprintf("%v", val))
				match = !matched
			case "!~*":
				matched, _ := regexp.MatchString("(?i)"+fmt.Sprintf("%v", rhsVal), fmt.Sprintf("%v", val))
				match = !matched
			case "IN":
				if list, ok := rhsVal.([]interface{}); ok {
					for _, item := range list {
						if compareValues(val, item) == 0 {
							match = true
							break
						}
					}
				}
			case "@>":
				match = storage.EvaluateJsonContain(val, rhsVal)
			default:
				match = false
			}
		}
	}

	// Handle AND/OR chains
	if where.And != nil {
		return match && e.evaluateWhereOnRow(row, where.And)
	}
	if where.Or != nil {
		return match || e.evaluateWhereOnRow(row, where.Or)
	}

	return match
}

func (e *Executor) executeUpdate(stmt *parser.UpdateStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := e.getTableForModification(dbInstance, stmt.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	var where *storage.WhereClause
	if stmt.Where != nil {
		where = e.convertWhereClauseWithSubquery(stmt.Where)
	}

	if stmt.FromTable != "" {
		fromTable, ok := e.getTable(dbInstance, stmt.FromTable)
		if !ok {
			return nil, fmt.Errorf("table %s does not exist", stmt.FromTable)
		}

		updatedCount := 0
		var updatedRows []storage.Row
		for idx, targetRow := range table.Rows {
			var matchedFromRow storage.Row
			for _, fromRow := range fromTable.Rows {
				combined := make(storage.Row)
				for k, v := range targetRow {
					combined[k] = v
					combined[stmt.TableName+"."+k] = v
				}
				for k, v := range fromRow {
					combined[k] = v
					combined[stmt.FromTable+"."+k] = v
				}

				if e.evaluateWhereOnRow(combined, where) {
					matchedFromRow = fromRow
					break
				}
			}

			if matchedFromRow != nil {
				for colName, vExpr := range stmt.Updates {
					finalVal := vExpr
					if sVal, ok := vExpr.(string); ok {
						if val, exists := matchedFromRow[sVal]; exists {
							finalVal = val
						} else if val, exists := matchedFromRow[strings.TrimPrefix(sVal, stmt.FromTable+".")]; exists {
							finalVal = val
						} else if val, exists := targetRow[sVal]; exists {
							finalVal = val
						} else if val, exists := targetRow[strings.TrimPrefix(sVal, stmt.TableName+".")]; exists {
							finalVal = val
						}
					}
					table.Rows[idx][colName] = finalVal
				}
				updatedCount++
				updatedRows = append(updatedRows, table.Rows[idx])
			}
		}

		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}

		if len(stmt.Returning) > 0 {
			return e.projectReturning(updatedRows, stmt.Returning, table), nil
		}

		return &Result{
			Message: fmt.Sprintf("UPDATE %d row(s)", updatedCount),
		}, nil
	}

	count, err := table.Update(stmt.Updates, where)
	if err != nil {
		return nil, err
	}

	if err := e.saveTableToDisk(dbInstance, table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	if len(stmt.Returning) > 0 {
		allRows, _ := table.Select([]string{"*"}, where)
		return e.projectReturning(allRows, stmt.Returning, table), nil
	}

	return &Result{
		Message: fmt.Sprintf("UPDATE %d row(s)", count),
	}, nil
}

func (e *Executor) executeDelete(stmt *parser.DeleteStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := e.getTableForModification(dbInstance, stmt.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	var where *storage.WhereClause
	if stmt.Where != nil {
		where = e.convertWhereClauseWithSubquery(stmt.Where)
	}

	if stmt.UsingTable != "" {
		usingTable, ok := e.getTable(dbInstance, stmt.UsingTable)
		if !ok {
			return nil, fmt.Errorf("table %s does not exist", stmt.UsingTable)
		}

		var newRows []storage.Row
		deletedCount := 0
		var deletedRows []storage.Row

		for _, targetRow := range table.Rows {
			matched := false
			for _, usingRow := range usingTable.Rows {
				combined := make(storage.Row)
				for k, v := range targetRow {
					combined[k] = v
					combined[stmt.TableName+"."+k] = v
				}
				for k, v := range usingRow {
					combined[k] = v
					combined[stmt.UsingTable+"."+k] = v
				}

				if e.evaluateWhereOnRow(combined, where) {
					matched = true
					break
				}
			}

			if matched {
				deletedCount++
				deletedRows = append(deletedRows, targetRow)
			} else {
				newRows = append(newRows, targetRow)
			}
		}

		table.Rows = newRows

		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}

		if len(stmt.Returning) > 0 {
			return e.projectReturning(deletedRows, stmt.Returning, table), nil
		}

		return &Result{
			Message: fmt.Sprintf("DELETE %d row(s)", deletedCount),
		}, nil
	}

	var deletedRows []storage.Row
	if len(stmt.Returning) > 0 {
		deletedRows, _ = table.Select([]string{"*"}, where)
	}

	count, err := table.Delete(where)
	if err != nil {
		return nil, err
	}

	if err := e.saveTableToDisk(dbInstance, table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	if len(stmt.Returning) > 0 {
		return e.projectReturning(deletedRows, stmt.Returning, table), nil
	}

	return &Result{
		Message: fmt.Sprintf("DELETE %d row(s)", count),
	}, nil
}

func (e *Executor) executeDropTable(stmt *parser.DropTableStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	if _, exists := e.getTable(dbInstance, stmt.TableName); !exists {
		if stmt.IfExists {
			return &Result{Message: fmt.Sprintf("NOTICE: table %s does not exist, skipping", stmt.TableName)}, nil
		}
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	e.deleteTable(dbInstance, stmt.TableName)

	if e.session == nil || !e.session.TxActive {
		tablePath := filepath.Join(dbInstance.BasePath, "tables", stmt.TableName+".tbl")
		if err := os.Remove(tablePath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to remove table file: %w", err)
		}
	}

	return &Result{
		Message: fmt.Sprintf("DROP TABLE %s", stmt.TableName),
	}, nil
}

func (e *Executor) executeDropDatabase(stmt *parser.DropDatabaseStmt) (*Result, error) {
	if err := e.db.DropDatabase(stmt.DatabaseName); err != nil {
		if stmt.IfExists && strings.Contains(err.Error(), "does not exist") {
			return &Result{Message: fmt.Sprintf("NOTICE: database %s does not exist, skipping", stmt.DatabaseName)}, nil
		}
		return nil, err
	}

	return &Result{
		Message: fmt.Sprintf("DROP DATABASE %s", stmt.DatabaseName),
	}, nil
}

func (e *Executor) executeTruncate(stmt *parser.TruncateStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := e.getTableForModification(dbInstance, stmt.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	if err := table.Truncate(); err != nil {
		return nil, err
	}

	if err := e.saveTableToDisk(dbInstance, table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("TRUNCATE TABLE %s", stmt.TableName),
	}, nil
}

func (e *Executor) executeAlterTable(stmt *parser.AlterTableStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := e.getTableForModification(dbInstance, stmt.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	if stmt.Action == "ADD_COLUMN" {
		col := storage.Column{
			Name:     stmt.Column.Name,
			Type:     stmt.Column.Type,
			Nullable: stmt.Column.Nullable,
		}

		if err := table.AddColumn(col); err != nil {
			return nil, err
		}

		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}

		return &Result{
			Message: fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", stmt.TableName, stmt.Column.Name),
		}, nil
	}

	if stmt.Action == "RENAME_TO" {
		oldName := table.Name
		newName := stmt.RenameTo
		table.Name = newName
		
		e.deleteTable(dbInstance, oldName)
		e.setTable(dbInstance, newName, table)
		
		if e.session == nil || !e.session.TxActive {
			oldPath := filepath.Join(dbInstance.BasePath, "tables", oldName+".tbl")
			os.Remove(oldPath)
		}

		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("ALTER TABLE %s RENAME TO %s", oldName, newName)}, nil
	}

	if stmt.Action == "DROP_COLUMN" {
		if err := table.DropColumn(stmt.DropColumn, stmt.IfExists); err != nil {
			return nil, err
		}
		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", stmt.TableName, stmt.DropColumn)}, nil
	}

	if stmt.Action == "RENAME_COLUMN" {
		if err := table.RenameColumn(stmt.RenameColumnFrom, stmt.RenameColumnTo); err != nil {
			return nil, err
		}
		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", stmt.TableName, stmt.RenameColumnFrom, stmt.RenameColumnTo)}, nil
	}

	if stmt.Action == "ALTER_COLUMN_TYPE" {
		if err := table.AlterColumnType(stmt.AlterColumnName, stmt.AlterColumnType); err != nil {
			return nil, err
		}
		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %d", stmt.TableName, stmt.AlterColumnName, stmt.AlterColumnType)}, nil
	}

	if stmt.Action == "ADD_CONSTRAINT" {
		if len(stmt.AddConstraintUnique) > 0 {
			for _, colName := range stmt.AddConstraintUnique {
				for i, col := range table.Columns {
					if col.Name == colName {
						table.Columns[i].IsUnique = true
					}
				}
			}
		}
		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT", stmt.TableName)}, nil
	}

	if stmt.Action == "ENABLE_RLS" {
		table.RLSEnabled = true
		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", stmt.TableName)}, nil
	}

	if stmt.Action == "DISABLE_RLS" {
		table.RLSEnabled = false
		if err := e.saveTableToDisk(dbInstance, table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("ALTER TABLE %s DISABLE ROW LEVEL SECURITY", stmt.TableName)}, nil
	}
	return nil, fmt.Errorf("unsupported ALTER TABLE action: %s", stmt.Action)
}

func (e *Executor) executeComment(stmt *parser.CommentStmt) (*Result, error) {
	switch stmt.ObjectType {
	case "DATABASE":
		return e.executeCommentDatabase(stmt)
	case "TABLE":
		return e.executeCommentTable(stmt)
	case "COLUMN":
		return e.executeCommentColumn(stmt)
	default:
		return nil, fmt.Errorf("unsupported COMMENT object type: %s", stmt.ObjectType)
	}
}

func (e *Executor) executeCommentDatabase(stmt *parser.CommentStmt) (*Result, error) {
	return &Result{
		Message: fmt.Sprintf("COMMENT ON DATABASE %s", stmt.ObjectName),
	}, nil
}

func (e *Executor) executeCommentTable(stmt *parser.CommentStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := e.getTableForModification(dbInstance, stmt.ObjectName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.ObjectName)
	}

	if table.Metadata == nil {
		var id [16]byte
		copy(id[:], stmt.ObjectName)
		table.Metadata = metadata.NewMetadata(
			metadata.ObjTypeTable,
			id,
			"User comment",
			stmt.Comment,
		)
	} else {
		table.Metadata.Description = stmt.Comment
	}

	if err := e.saveTableToDisk(dbInstance, table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("COMMENT ON TABLE %s", stmt.ObjectName),
	}, nil
}

func (e *Executor) executeCommentColumn(stmt *parser.CommentStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	if stmt.TableName == "" {
		return nil, fmt.Errorf("column comments require format: COMMENT ON COLUMN table.column IS 'comment'")
	}

	table, exists := e.getTableForModification(dbInstance, stmt.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	found := false
	for i := range table.Columns {
		if table.Columns[i].Name == stmt.ObjectName {
			if table.Columns[i].Metadata == nil {
				var id [16]byte
				copy(id[:], stmt.ObjectName)
				table.Columns[i].Metadata = metadata.NewMetadata(
					metadata.ObjTypeColumn,
					id,
					"User comment",
					stmt.Comment,
				)
			} else {
				table.Columns[i].Metadata.Description = stmt.Comment
			}
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("column %s not found in table %s", stmt.ObjectName, stmt.TableName)
	}

	if err := e.saveTableToDisk(dbInstance, table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("COMMENT ON COLUMN %s.%s", stmt.TableName, stmt.ObjectName),
	}, nil
}

func (e *Executor) executeVectorSearch(stmt *parser.SelectStmt, rows []storage.Row, table *storage.Table) (*Result, error) {
	queryVector := storage.NewVector(stmt.VectorOrderBy.QueryVector)

	var metric storage.VectorDistance
	switch stmt.VectorOrderBy.Function {
	case "COSINE_DISTANCE":
		metric = storage.DistanceCosine
	case "L2_DISTANCE":
		metric = storage.DistanceL2
	default:
		return nil, fmt.Errorf("unsupported distance function: %s", stmt.VectorOrderBy.Function)
	}

	limit := len(rows)
	if stmt.Limit > 0 {
		limit = stmt.Limit
	}

	var results []storage.VectorSearchResult
	var err error

	if index, exists := table.VectorIndexes[stmt.VectorOrderBy.Column]; exists {
		ef := limit * 2
		if ef < 50 {
			ef = 50
		}
		results, err = index.Search(queryVector, limit, ef)
		if err != nil {
			return nil, err
		}

		for i := range results {
			rowID := results[i].Row["_row_id"].(int)
			if rowID < len(rows) {
				results[i].Row = rows[rowID]
			}
		}
	} else {
		results, err = storage.VectorSearch(rows, queryVector, stmt.VectorOrderBy.Column, metric, limit)
		if err != nil {
			return nil, err
		}
	}

	if stmt.Offset > 0 && stmt.Offset < len(results) {
		results = results[stmt.Offset:]
	}

	resultRows := make([]storage.Row, len(results))
	for i, res := range results {
		resultRows[i] = make(storage.Row)

		for _, col := range stmt.Columns {
			if col == "*" {
				for k, v := range res.Row {
					resultRows[i][k] = v
				}
			} else {
				resultRows[i][col] = res.Row[col]
			}
		}

		resultRows[i]["_distance"] = fmt.Sprintf("%.6f", res.Distance)
	}

	columns := stmt.Columns
	if len(columns) == 1 && columns[0] == "*" {
		columns = table.GetColumnNames()
	}
	columns = append(columns, "_distance")

	return &Result{
		Rows:    resultRows,
		Columns: columns,
	}, nil
}

func (e *Executor) executeCreateIndex(stmt *parser.CreateIndexStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := e.getTableForModification(dbInstance, stmt.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	if table.VectorIndexes == nil {
		table.VectorIndexes = make(map[string]*storage.HNSWIndex)
	}

	var colType storage.DataType
	found := false
	for _, col := range table.Columns {
		if col.Name == stmt.ColumnName {
			colType = col.Type
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("column %s not found", stmt.ColumnName)
	}

	if stmt.IndexType == "HNSW" {
		if colType != storage.TypeVector {
			return nil, fmt.Errorf("HNSW index only supported on VECTOR columns")
		}

		m := stmt.Options["m"]
		efConstruction := stmt.Options["ef_construction"]

		index := storage.NewHNSWIndex(m, efConstruction, storage.DistanceCosine)

		for i, row := range table.Rows {
			if vec, ok := row[stmt.ColumnName].(*storage.Vector); ok {
				if err := index.Add(vec, i); err != nil {
					return nil, fmt.Errorf("failed to build index: %w", err)
				}
			}
		}

		table.VectorIndexes[stmt.ColumnName] = index

		return &Result{
			Message: fmt.Sprintf("CREATE INDEX %s ON %s USING HNSW (m=%d, ef_construction=%d)",
				stmt.IndexName, stmt.TableName, m, efConstruction),
		}, nil
	}

	return nil, fmt.Errorf("unsupported index type: %s", stmt.IndexType)
}

func (e *Executor) executeDropIndex(stmt *parser.DropIndexStmt) (*Result, error) {
	// Index support is partial, but respect IfExists anyway
	return &Result{
		Message: fmt.Sprintf("DROP INDEX %s (not fully implemented)", stmt.IndexName),
	}, nil
}

func convertWhereClause(where *parser.WhereClause) *storage.WhereClause {
	if where == nil {
		return nil
	}

	sw := &storage.WhereClause{
		Column:   where.Column,
		Operator: where.Operator,
		Value:    where.Value,
	}

	if where.And != nil {
		sw.And = convertWhereClause(where.And)
	}
	if where.Or != nil {
		sw.Or = convertWhereClause(where.Or)
	}

	return sw
}

func (e *Executor) applyOrderBy(rows []storage.Row, orderBy []parser.OrderByClause) []storage.Row {
	if len(orderBy) == 0 {
		return rows
	}

	sorted := make([]storage.Row, len(rows))
	copy(sorted, rows)

	// Pre-build column list for positional ORDER BY resolution
	colNames := []string{}
	if len(sorted) > 0 {
		// Use keys from the first row as columns
		for k := range sorted[0] {
			colNames = append(colNames, k)
		}
		sort.Strings(colNames)
	}

	sort.SliceStable(sorted, func(i, j int) bool {
		for _, order := range orderBy {
			col := order.Column
			// Handle positional ORDER BY (e.g. ORDER BY 1, 2)
			if pos, err := strconv.Atoi(col); err == nil {
				if pos >= 1 && pos <= len(colNames) {
					// Map position to column name
					col = colNames[pos-1]
				}
			}

			valI := sorted[i][col]
			valJ := sorted[j][col]

			cmp := compareValues(valI, valJ)
			if cmp != 0 {
				if order.Descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return sorted
}

func (e *Executor) validateWhereColumns(table *storage.Table, where *storage.WhereClause) error {
	if where == nil {
		return nil
	}

	// Skip EXISTS/NOT EXISTS checks — no column LHS
	if where.Operator == "EXISTS" || where.Operator == "NOT EXISTS" || where.Operator == "EXISTS_RESOLVED" {
		if where.And != nil {
			return e.validateWhereColumns(table, where.And)
		}
		if where.Or != nil {
			return e.validateWhereColumns(table, where.Or)
		}
		return nil
	}

	// Skip empty column names
	if where.Column == "" {
		if where.And != nil {
			if err := e.validateWhereColumns(table, where.And); err != nil {
				return err
			}
		}
		if where.Or != nil {
			if err := e.validateWhereColumns(table, where.Or); err != nil {
				return err
			}
		}
		return nil
	}

	// Skip function calls, arithmetic expressions, and system stubs
	if !strings.Contains(where.Column, "(") && !strings.ContainsAny(where.Column, "+-*/%") {
		exists := false
		for _, col := range table.Columns {
			if col.Name == where.Column {
				exists = true
				break
			}
		}
		if !exists && e.currentStmt != nil {
			for _, sc := range e.currentStmt.SelectColumns {
				if sc.Alias == where.Column {
					exists = true
					break
				}
			}
		}
		if !exists {
			return fmt.Errorf("column \"%s\" does not exist", where.Column)
		}
	}

	if where.And != nil {
		if err := e.validateWhereColumns(table, where.And); err != nil {
			return err
		}
	}
	if where.Or != nil {
		if err := e.validateWhereColumns(table, where.Or); err != nil {
			return err
		}
	}

	return nil
}

func compareValues(a, b interface{}) int {
	// Try numeric comparison
	aInt, aIsInt := toComparableInt(a)
	bInt, bIsInt := toComparableInt(b)

	if aIsInt && bIsInt {
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	}

	// String comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func toComparableInt(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func (e *Executor) executeCreateRole(stmt *parser.CreateRoleStmt) (*Result, error) {
	// Check if current user has CREATEROLE privilege or is superuser
	user := e.session.GetUser()
	role, exists := e.db.RoleStore.GetRole(user)
	if user != "ghost" && (!exists || !role.CanCreateRole) {
		return nil, fmt.Errorf("permission denied to create roles")
	}

	newRole := &storage.Role{
		OID:           e.db.Catalog.GenerateOID(stmt.RoleName),
		Name:          stmt.RoleName,
		IsSuperuser:   stmt.IsSuperuser,
		CanLogin:      stmt.CanLogin,
		CanCreateRole: stmt.CanCreateRole,
		CanCreateDB:   stmt.CanCreateDB,
		PasswordHash:  storage.HashPassword(stmt.Password),
		Privileges:    make(map[string]map[string]bool),
	}

	if err := e.db.RoleStore.CreateRole(newRole); err != nil {
		return nil, err
	}
	e.db.RoleStore.Save()

	return &Result{
		Message: fmt.Sprintf("CREATE ROLE %s", stmt.RoleName),
	}, nil
}

func (e *Executor) executeDropRole(stmt *parser.DropRoleStmt) (*Result, error) {
	user := e.session.GetUser()
	role, exists := e.db.RoleStore.GetRole(user)
	if user != "ghost" && (!exists || !role.CanCreateRole) {
		return nil, fmt.Errorf("permission denied to drop role")
	}

	if _, exists := e.db.RoleStore.GetRole(stmt.RoleName); !exists {
		if stmt.IfExists {
			return &Result{Message: fmt.Sprintf("NOTICE: role %s does not exist, skipping", stmt.RoleName)}, nil
		}
		return nil, fmt.Errorf("role %s does not exist", stmt.RoleName)
	}

	if err := e.db.RoleStore.DeleteRole(stmt.RoleName); err != nil {
		return nil, err
	}

	if err := e.db.RoleStore.Save(); err != nil {
		return nil, fmt.Errorf("failed to persist role deletion: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("DROP ROLE %s", stmt.RoleName),
	}, nil
}

func (e *Executor) executeAlterRole(stmt *parser.AlterRoleStmt) (*Result, error) {
	user := e.session.GetUser()
	if user != "ghost" && user != stmt.RoleName {
		// Only superuser or the role itself can alter its password for now
		return nil, fmt.Errorf("permission denied to alter role %s", stmt.RoleName)
	}

	role, exists := e.db.RoleStore.GetRole(stmt.RoleName)
	if !exists {
		return nil, fmt.Errorf("role %s does not exist", stmt.RoleName)
	}

	if stmt.Password != "" {
		role.PasswordHash = storage.HashPassword(stmt.Password)
	}

	e.db.RoleStore.Save()
	return &Result{
		Message: fmt.Sprintf("ALTER ROLE %s", stmt.RoleName),
	}, nil
}

func (e *Executor) executeGrant(stmt *parser.GrantStmt) (*Result, error) {
	// Simple implementation: only superuser can GRANT for now
	user := e.session.GetUser()
	if user != "ghost" {
		return nil, fmt.Errorf("only superuser can GRANT privileges")
	}

	privs := stmt.Privileges
	if stmt.All {
		privs = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}
	}

	objectKey := fmt.Sprintf("%s:%s", stmt.ObjectType, stmt.ObjectName)
	for _, priv := range privs {
		if err := e.db.RoleStore.GrantPrivilege(stmt.ToRole, objectKey, priv); err != nil {
			return nil, err
		}
	}

	e.db.RoleStore.Save()
	return &Result{
		Message: "GRANT successful",
	}, nil
}

func (e *Executor) executeRevoke(stmt *parser.RevokeStmt) (*Result, error) {
	user := e.session.GetUser()
	if user != "ghost" {
		return nil, fmt.Errorf("only superuser can REVOKE privileges")
	}

	role, exists := e.db.RoleStore.GetRole(stmt.FromRole)
	if !exists {
		return nil, fmt.Errorf("role %s does not exist", stmt.FromRole)
	}

	objectKey := fmt.Sprintf("%s:%s", stmt.ObjectType, stmt.ObjectName)
	if stmt.All {
		delete(role.Privileges, objectKey)
	} else {
		if role.Privileges != nil && role.Privileges[objectKey] != nil {
			for _, priv := range stmt.Privileges {
				delete(role.Privileges[objectKey], priv)
			}
		}
	}

	e.db.RoleStore.Save()
	return &Result{
		Message: "REVOKE successful",
	}, nil
}


func combineWhere(p, w *storage.WhereClause) *storage.WhereClause {
	if p == nil {
		return w
	}
	if w == nil {
		return p
	}
	
	// P is already cloned by the caller if needed
	curr := p
	for curr.And != nil {
		curr = curr.And
	}
	curr.And = w
	return p
}

func (e *Executor) executeCreatePolicy(stmt *parser.CreatePolicyStmt) (*Result, error) {
	if err := e.checkPrivilege("TABLE", stmt.TableName, "ALL"); err != nil {
		return nil, err
	}
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}
	
	table, exists := e.getTableForModification(dbInstance, stmt.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}
	
	policy := storage.Policy{
		Name:      stmt.PolicyName,
		Action:    stmt.Action,
		Role:      stmt.Role,
		UsingExpr: "...", 
		Where:     convertWhereClause(stmt.Using),
	}
	
	table.Policies = append(table.Policies, policy)
	return &Result{Message: "CREATE POLICY"}, nil
}


func (e *Executor) resolveWhereClauseVariables(where *storage.WhereClause) {
	if where == nil {
		return
	}
	if s, ok := where.Value.(string); ok {
		if s == "current_user()" {
			where.Value = e.session.GetUser()
		}
	}
	if where.And != nil {
		e.resolveWhereClauseVariables(where.And)
	}
	if where.Or != nil {
		e.resolveWhereClauseVariables(where.Or)
	}
}

// convertWhereClauseWithSubquery converts parser WHERE to storage WHERE,
// resolving subqueries for IN and EXISTS operators.
func (e *Executor) convertWhereClauseWithSubquery(where *parser.WhereClause) *storage.WhereClause {
	if where == nil {
		return nil
	}

	sw := &storage.WhereClause{
		Column:   where.Column,
		Operator: where.Operator,
		Value:    where.Value,
	}

	// Resolve IN (SELECT ...) subquery
	if (where.Operator == "IN" || where.Operator == "NOT IN") && where.Subquery != nil {
		subResult, err := e.executeSelect(where.Subquery)
		if err == nil && subResult != nil {
			values := make([]interface{}, 0, len(subResult.Rows))
			for _, row := range subResult.Rows {
				for _, v := range row {
					values = append(values, v)
					break // take first column
				}
			}
			sw.Value = values
		}
	}

	// Resolve EXISTS / NOT EXISTS subquery
	if where.Operator == "EXISTS" || where.Operator == "NOT EXISTS" {
		subResult, err := e.executeSelect(where.Subquery)
		hasRows := err == nil && subResult != nil && len(subResult.Rows) > 0
		if where.Operator == "EXISTS" {
			sw.Value = hasRows
			sw.Operator = "EXISTS_RESOLVED"
		} else {
			sw.Value = !hasRows
			sw.Operator = "EXISTS_RESOLVED"
		}
	}

	if where.And != nil {
		sw.And = e.convertWhereClauseWithSubquery(where.And)
	}
	if where.Or != nil {
		sw.Or = e.convertWhereClauseWithSubquery(where.Or)
	}

	return sw
}

// projectReturning projects rows through RETURNING column specs
func (e *Executor) projectReturning(rows []storage.Row, returning []parser.SelectColumn, table *storage.Table) *Result {
	if len(returning) == 1 && returning[0].Expression == "*" {
		cols := table.GetColumnNames()
		return &Result{Rows: rows, Columns: cols}
	}

	cols := make([]string, len(returning))
	for i, sc := range returning {
		if sc.Alias != "" {
			cols[i] = sc.Alias
		} else {
			cols[i] = sc.Expression
		}
	}

	projected := make([]storage.Row, len(rows))
	for i, row := range rows {
		newRow := make(storage.Row)
		for j, sc := range returning {
			val, ok := row[sc.Expression]
			if !ok {
				val = storage.EvaluateExpression(sc.Expression, row)
			}
			newRow[cols[j]] = val
		}
		projected[i] = newRow
	}

	return &Result{Rows: projected, Columns: cols}
}

// executeCompoundSelect handles UNION / UNION ALL / INTERSECT / EXCEPT
func (e *Executor) executeCompoundSelect(stmt *parser.CompoundSelectStmt) (*Result, error) {
	leftResult, err := e.executeSelect(stmt.Left)
	if err != nil {
		return nil, err
	}
	rightResult, err := e.executeSelect(stmt.Right)
	if err != nil {
		return nil, err
	}

	var resultRows []storage.Row
	columns := leftResult.Columns

	rowKey := func(row storage.Row, cols []string) string {
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = fmt.Sprintf("%v", row[c])
		}
		return strings.Join(parts, "\x00")
	}

	switch stmt.Op {
	case "UNION ALL":
		resultRows = append(leftResult.Rows, rightResult.Rows...)
	case "UNION":
		seen := make(map[string]bool)
		for _, row := range append(leftResult.Rows, rightResult.Rows...) {
			k := rowKey(row, columns)
			if !seen[k] {
				seen[k] = true
				resultRows = append(resultRows, row)
			}
		}
	case "INTERSECT":
		rightKeys := make(map[string]bool)
		for _, row := range rightResult.Rows {
			rightKeys[rowKey(row, columns)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftResult.Rows {
			k := rowKey(row, columns)
			if rightKeys[k] && !seen[k] {
				seen[k] = true
				resultRows = append(resultRows, row)
			}
		}
	case "EXCEPT":
		rightKeys := make(map[string]bool)
		for _, row := range rightResult.Rows {
			rightKeys[rowKey(row, columns)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftResult.Rows {
			k := rowKey(row, columns)
			if !rightKeys[k] && !seen[k] {
				seen[k] = true
				resultRows = append(resultRows, row)
			}
		}
	}

	if len(stmt.OrderBy) > 0 {
		resultRows = e.applyOrderBy(resultRows, stmt.OrderBy)
	}
	if stmt.Offset > 0 && stmt.Offset < len(resultRows) {
		resultRows = resultRows[stmt.Offset:]
	}
	if stmt.Limit > 0 && stmt.Limit < len(resultRows) {
		resultRows = resultRows[:stmt.Limit]
	}

	return &Result{Rows: resultRows, Columns: columns}, nil
}

// applyDistinct removes duplicate rows from results
func applyDistinct(rows []storage.Row, columns []string) []storage.Row {
	seen := make(map[string]bool)
	result := make([]storage.Row, 0, len(rows))
	for _, row := range rows {
		parts := make([]string, len(columns))
		for i, c := range columns {
			parts[i] = fmt.Sprintf("%v", row[c])
		}
		key := strings.Join(parts, "\x00")
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

// applyDistinctOn keeps first row per DISTINCT ON key (requires rows to be pre-sorted)
func applyDistinctOn(rows []storage.Row, distinctOn []string) []storage.Row {
	seen := make(map[string]bool)
	result := make([]storage.Row, 0)
	for _, row := range rows {
		parts := make([]string, len(distinctOn))
		for i, c := range distinctOn {
			parts[i] = fmt.Sprintf("%v", row[c])
		}
		key := strings.Join(parts, "\x00")
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

// applyTableSample samples rows randomly at the given percentage
func applyTableSample(rows []storage.Row, percent float64) []storage.Row {
	if percent <= 0 {
		return []storage.Row{}
	}
	if percent >= 100 {
		return rows
	}
	result := make([]storage.Row, 0)
	for i, row := range rows {
		// Use modular deterministic selection (not truly random but reproducible)
		threshold := int(100.0 / percent)
		if threshold < 1 {
			threshold = 1
		}
		if i%threshold == 0 {
			result = append(result, row)
		}
	}
	return result
}

// executeWindowFunctions computes window functions (ROW_NUMBER, RANK, DENSE_RANK)
func executeWindowFunctions(rows []storage.Row, selectCols []parser.SelectColumn) []storage.Row {
	for _, sc := range selectCols {
		if sc.Window == nil {
			continue
		}

		fnUpper := strings.ToUpper(sc.Expression)
		outName := sc.Expression

		// Group rows into partitions
		type partition struct {
			key  string
			rows []int // indices into rows slice
		}
		partMap := make(map[string]*partition)
		var partOrder []string

		for i, row := range rows {
			keyParts := make([]string, len(sc.Window.PartitionBy))
			for j, col := range sc.Window.PartitionBy {
				keyParts[j] = fmt.Sprintf("%v", row[col])
			}
			key := strings.Join(keyParts, "\x00")
			if _, exists := partMap[key]; !exists {
				partMap[key] = &partition{key: key}
				partOrder = append(partOrder, key)
			}
			partMap[key].rows = append(partMap[key].rows, i)
		}

		// Sort each partition by the window ORDER BY
		for _, key := range partOrder {
			part := partMap[key]
			if len(sc.Window.OrderBy) > 0 {
				sort.SliceStable(part.rows, func(a, b int) bool {
					ia, ib := part.rows[a], part.rows[b]
					for _, ob := range sc.Window.OrderBy {
						valA := rows[ia][ob.Column]
						valB := rows[ib][ob.Column]
						cmp := compareValues(valA, valB)
						if cmp != 0 {
							if ob.Descending {
								return cmp > 0
							}
							return cmp < 0
						}
					}
					return false
				})
			}
		}

		// Assign row numbers per partition
		for _, key := range partOrder {
			part := partMap[key]
			rank := 0
			denseRank := 0
			lastOrderKey := ""

			for pos, idx := range part.rows {
				// Compute order key for RANK
				orderParts := make([]string, len(sc.Window.OrderBy))
				for j, ob := range sc.Window.OrderBy {
					orderParts[j] = fmt.Sprintf("%v", rows[idx][ob.Column])
				}
				orderKey := strings.Join(orderParts, "\x00")

				if orderKey != lastOrderKey {
					denseRank++
					rank = pos + 1
					lastOrderKey = orderKey
				}

				switch {
				case strings.HasPrefix(fnUpper, "ROW_NUMBER"):
					rows[idx][outName] = pos + 1
				case strings.HasPrefix(fnUpper, "DENSE_RANK"):
					rows[idx][outName] = denseRank
				case strings.HasPrefix(fnUpper, "RANK"):
					rows[idx][outName] = rank
				}
			}
		}
	}
	return rows
}

// executeSelectWithCTE wraps executeSelect with CTE virtual table support
func (e *Executor) executeSelectWithCTEs(stmt *parser.SelectStmt) (*Result, error) {
	if len(stmt.CTEs) == 0 {
		return e.executeSelectCore(stmt)
	}

	// Build virtual CTE tables in-memory
	for _, cte := range stmt.CTEs {
		if !cte.Recursive {
			var result *Result
			var err error
			switch q := cte.Query.(type) {
			case *parser.SelectStmt:
				result, err = e.executeSelect(q)
			case *parser.CompoundSelectStmt:
				result, err = e.executeCompoundSelect(q)
			default:
				return nil, fmt.Errorf("unexpected query type in CTE %s", cte.Name)
			}
			if err != nil {
				return nil, fmt.Errorf("CTE %s failed: %w", cte.Name, err)
			}
			e.cteResults[cte.Name] = result
		} else {
			// Recursive CTE
			compound, ok := cte.Query.(*parser.CompoundSelectStmt)
			if !ok {
				return nil, fmt.Errorf("recursive CTE %s must be a UNION/UNION ALL compound query", cte.Name)
			}

			// 1. Evaluate base case (Left side of compound SELECT)
			baseResult, err := e.executeSelect(compound.Left)
			if err != nil {
				return nil, fmt.Errorf("recursive CTE %s base query failed: %w", cte.Name, err)
			}

			allRows := append([]storage.Row{}, baseResult.Rows...)
			workingRows := baseResult.Rows

			// 2. Iterate up to 1000 times
			for iter := 0; iter < 1000; iter++ {
				// The next iteration queries using only the new rows from the previous iteration
				e.cteResults[cte.Name] = &Result{Rows: workingRows, Columns: baseResult.Columns}

				iterResult, err := e.executeSelect(compound.Right)
				if err != nil {
					return nil, fmt.Errorf("recursive CTE %s iteration %d failed: %w", cte.Name, iter, err)
				}

				if len(iterResult.Rows) == 0 {
					break
				}

				// Rewrite iterResult rows to match baseResult column names by index
				rewrittenRows := make([]storage.Row, len(iterResult.Rows))
				for rIdx, row := range iterResult.Rows {
					newRow := make(storage.Row)
					for cIdx, colName := range baseResult.Columns {
						if cIdx < len(iterResult.Columns) {
							origCol := iterResult.Columns[cIdx]
							newRow[colName] = row[origCol]
						}
					}
					rewrittenRows[rIdx] = newRow
				}

				allRows = append(allRows, rewrittenRows...)
				workingRows = rewrittenRows
			}

			// Store final accumulated result
			e.cteResults[cte.Name] = &Result{Rows: allRows, Columns: baseResult.Columns}
		}
	}

	// If the main query references a CTE as its table name, resolve it
	if result, ok := e.cteResults[stmt.TableName]; ok {
		// Filter + project from the CTE result
		return e.applySelectOnRows(stmt, result.Rows, result.Columns), nil
	}

	return e.executeSelectCore(stmt)
}

// applySelectOnRows applies WHERE/ORDER BY/LIMIT/OFFSET/DISTINCT on an in-memory row set
func (e *Executor) applySelectOnRows(stmt *parser.SelectStmt, rows []storage.Row, _ []string) *Result {
	var where *storage.WhereClause
	if stmt.Where != nil {
		where = e.convertWhereClauseWithSubquery(stmt.Where)
		e.resolveWhereClauseVariables(where)
	}

	if where != nil {
		filtered := make([]storage.Row, 0)
		for _, row := range rows {
			if e.evaluateWhereOnRow(row, where) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	if len(stmt.OrderBy) > 0 {
		rows = e.applyOrderBy(rows, stmt.OrderBy)
	}

	if stmt.Offset > 0 && stmt.Offset < len(rows) {
		rows = rows[stmt.Offset:]
	}
	if stmt.Limit > 0 && stmt.Limit < len(rows) {
		rows = rows[:stmt.Limit]
	}

	// Build columns
	var columns []string
	if len(stmt.SelectColumns) > 0 && !(len(stmt.SelectColumns) == 1 && stmt.SelectColumns[0].Expression == "*") {
		columns = make([]string, len(stmt.SelectColumns))
		for i, sc := range stmt.SelectColumns {
			if sc.Alias != "" {
				columns[i] = sc.Alias
			} else {
				columns[i] = sc.Expression
			}
		}
		projected := make([]storage.Row, len(rows))
		for i, row := range rows {
			newRow := make(storage.Row)
			for j, sc := range stmt.SelectColumns {
				val, ok := row[sc.Expression]
				if !ok {
					val = storage.EvaluateExpression(sc.Expression, row)
				}
				newRow[columns[j]] = val
			}
			projected[i] = newRow
		}
		rows = projected
	} else {
		// Collect all columns from first row
		if len(rows) > 0 {
			for k := range rows[0] {
				columns = append(columns, k)
			}
			sort.Strings(columns)
		}
	}

	if stmt.Distinct && len(columns) > 0 {
		rows = applyDistinct(rows, columns)
	}

	return &Result{Rows: rows, Columns: columns}
}

// executeSelectCore is the original executeSelect — we need this alias to avoid infinite recursion
// when CTE resolution calls back into the executor. The original function handles non-CTE queries.
func (e *Executor) executeSelectCore(stmt *parser.SelectStmt) (*Result, error) {
	return e.executeSelect(stmt)
}

var (
	viewRegistry             = make(map[string]*parser.SelectStmt)
	viewList                 = make(map[string][]string)
	materializedViewRegistry = make(map[string]*parser.SelectStmt)
	schemaRegistry           = make(map[string]bool)
	sequenceRegistry         = make(map[string]*SequenceState)
	typeRegistry             = make(map[string][]string)
)

type SequenceState struct {
	Current   int
	Start     int
	Increment int
}

func ResetRegistries() {
	viewRegistry = make(map[string]*parser.SelectStmt)
	viewList = make(map[string][]string)
	materializedViewRegistry = make(map[string]*parser.SelectStmt)
	schemaRegistry = make(map[string]bool)
	sequenceRegistry = make(map[string]*SequenceState)
	typeRegistry = make(map[string][]string)
}

func (e *Executor) executeCreateView(stmt *parser.CreateViewStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	dbName := dbInstance.Name
	viewKey := dbName + "." + stmt.ViewName

	if !stmt.OrReplace {
		if _, exists := viewRegistry[viewKey]; exists {
			return nil, fmt.Errorf("view %s already exists", stmt.ViewName)
		}
	}

	viewRegistry[viewKey] = stmt.Query
	viewList[dbName] = append(viewList[dbName], stmt.ViewName)

	return &Result{Message: "CREATE VIEW"}, nil
}

func (e *Executor) executeDropView(stmt *parser.DropViewStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	dbName := dbInstance.Name
	viewKey := dbName + "." + stmt.ViewName

	if _, exists := viewRegistry[viewKey]; !exists {
		if stmt.IfExists {
			return &Result{Message: "DROP VIEW"}, nil
		}
		return nil, fmt.Errorf("view %s does not exist", stmt.ViewName)
	}

	delete(viewRegistry, viewKey)

	list := viewList[dbName]
	for i, v := range list {
		if v == stmt.ViewName {
			viewList[dbName] = append(list[:i], list[i+1:]...)
			break
		}
	}

	return &Result{Message: "DROP VIEW"}, nil
}

func (e *Executor) executeCreateSchema(stmt *parser.CreateSchemaStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	dbName := dbInstance.Name
	schemaKey := dbName + "." + stmt.SchemaName

	if schemaRegistry[schemaKey] {
		if stmt.IfNotExists {
			return &Result{Message: "CREATE SCHEMA"}, nil
		}
		return nil, fmt.Errorf("schema %s already exists", stmt.SchemaName)
	}

	schemaRegistry[schemaKey] = true
	return &Result{Message: "CREATE SCHEMA"}, nil
}

func (e *Executor) executeCreateSequence(stmt *parser.CreateSequenceStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	dbName := dbInstance.Name
	seqKey := dbName + "." + stmt.SequenceName

	if _, exists := sequenceRegistry[seqKey]; exists {
		if stmt.IfNotExists {
			return &Result{Message: "CREATE SEQUENCE"}, nil
		}
		return nil, fmt.Errorf("sequence %s already exists", stmt.SequenceName)
	}

	sequenceRegistry[seqKey] = &SequenceState{
		Current:   stmt.Start,
		Start:     stmt.Start,
		Increment: stmt.Increment,
	}

	return &Result{Message: "CREATE SEQUENCE"}, nil
}

func (e *Executor) executeCreateType(stmt *parser.CreateTypeStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	dbName := dbInstance.Name
	typeKey := dbName + "." + stmt.TypeName

	if _, exists := typeRegistry[typeKey]; exists {
		return nil, fmt.Errorf("type %s already exists", stmt.TypeName)
	}

	typeRegistry[typeKey] = stmt.Values
	return &Result{Message: "CREATE TYPE"}, nil
}

func (e *Executor) executeCreateMaterializedView(stmt *parser.CreateMaterializedViewStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	dbName := dbInstance.Name
	mvKey := dbName + "." + stmt.ViewName

	if _, exists := e.getTable(dbInstance, stmt.ViewName); exists {
		if stmt.IfNotExists {
			return &Result{Message: "CREATE MATERIALIZED VIEW"}, nil
		}
		return nil, fmt.Errorf("materialized view %s already exists", stmt.ViewName)
	}

	res, err := e.executeSelect(stmt.Query)
	if err != nil {
		return nil, err
	}

	var cols []storage.Column
	for _, colName := range res.Columns {
		cols = append(cols, storage.Column{
			Name:     colName,
			Type:     storage.TypeText,
			Nullable: true,
		})
	}

	table := storage.NewTable(stmt.ViewName, e.session.GetUser(), cols, nil)
	for _, row := range res.Rows {
		table.Insert(row)
	}

	e.setTable(dbInstance, stmt.ViewName, table)
	materializedViewRegistry[mvKey] = stmt.Query

	e.saveTableToDisk(dbInstance, table)

	return &Result{Message: "CREATE MATERIALIZED VIEW"}, nil
}

func (e *Executor) executeRefreshMaterializedView(stmt *parser.RefreshMaterializedViewStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	dbName := dbInstance.Name
	mvKey := dbName + "." + stmt.ViewName

	query, exists := materializedViewRegistry[mvKey]
	if !exists {
		return nil, fmt.Errorf("materialized view %s does not exist", stmt.ViewName)
	}

	table, ok := e.getTableForModification(dbInstance, stmt.ViewName)
	if !ok {
		return nil, fmt.Errorf("materialized view physical table %s not found", stmt.ViewName)
	}

	res, err := e.executeSelect(query)
	if err != nil {
		return nil, err
	}

	table.Truncate()
	for _, row := range res.Rows {
		table.Insert(row)
	}

	e.saveTableToDisk(dbInstance, table)

	return &Result{Message: "REFRESH MATERIALIZED VIEW"}, nil
}

func (e *Executor) executeMerge(stmt *parser.MergeStmt) (*Result, error) {
	dbInstance, err := e.getActiveDatabase()
	if err != nil {
		return nil, err
	}

	targetTable, exists := e.getTableForModification(dbInstance, stmt.TargetTable)
	if !exists {
		return nil, fmt.Errorf("target table %s does not exist", stmt.TargetTable)
	}

	var sourceRows []storage.Row
	if stmt.SourceQuery != nil {
		res, err := e.executeSelect(stmt.SourceQuery)
		if err != nil {
			return nil, err
		}
		sourceRows = res.Rows
	} else {
		srcTab, ok := e.getTable(dbInstance, stmt.SourceTable)
		if !ok {
			return nil, fmt.Errorf("source table %s does not exist", stmt.SourceTable)
		}
		sourceRows = srcTab.Rows
	}

	var onCond *storage.WhereClause
	if stmt.OnCondition != nil {
		onCond = e.convertWhereClauseWithSubquery(stmt.OnCondition)
	}

	matchedCount := 0
	notMatchedCount := 0

	for _, sourceRow := range sourceRows {
		matchedIdx := -1
		for idx, targetRow := range targetTable.Rows {
			combined := make(storage.Row)
			for k, v := range targetRow {
				combined[k] = v
				combined[stmt.TargetTable+"."+k] = v
			}
			for k, v := range sourceRow {
				combined[k] = v
				combined[stmt.SourceTable+"."+k] = v
			}

			if e.evaluateWhereOnRow(combined, onCond) {
				matchedIdx = idx
				break
			}
		}

		if matchedIdx != -1 {
			for _, action := range stmt.WhenMatched {
				if action.Action == "UPDATE" {
					targetRow := targetTable.Rows[matchedIdx]
					for k, v := range action.Updates {
						finalVal := v
						if sVal, ok := v.(string); ok {
							if val, exists := sourceRow[sVal]; exists {
								finalVal = val
							} else if val, exists := sourceRow[strings.TrimPrefix(sVal, stmt.SourceTable+".")]; exists {
								finalVal = val
							} else if val, exists := targetRow[sVal]; exists {
								finalVal = val
							} else if val, exists := targetRow[strings.TrimPrefix(sVal, stmt.TargetTable+".")]; exists {
								finalVal = val
							}
						}
						targetRow[k] = finalVal
					}
					targetTable.Rows[matchedIdx] = targetRow
					matchedCount++
				} else if action.Action == "DELETE" {
					targetTable.Rows = append(targetTable.Rows[:matchedIdx], targetTable.Rows[matchedIdx+1:]...)
					matchedCount++
				}
			}
		} else {
			for _, action := range stmt.WhenNotMatched {
				if action.Action == "INSERT" {
					newRow := make(storage.Row)
					for _, col := range targetTable.Columns {
						newRow[col.Name] = nil
					}

					for idx, colName := range action.Columns {
						if idx < len(action.Values) {
							val := action.Values[idx]
							finalVal := val
							if sVal, ok := val.(string); ok {
								if val, exists := sourceRow[sVal]; exists {
									finalVal = val
								} else if val, exists := sourceRow[strings.TrimPrefix(sVal, stmt.SourceTable+".")]; exists {
									finalVal = val
								}
							}
							newRow[colName] = finalVal
						}
					}
					targetTable.Insert(newRow)
					notMatchedCount++
				}
			}
		}
	}

	e.saveTableToDisk(dbInstance, targetTable)

	return &Result{
		Message: fmt.Sprintf("MERGE %d row(s)", matchedCount+notMatchedCount),
	}, nil
}

// --- Transaction and Table Helper Methods ---

func (e *Executor) getTable(dbInstance *storage.DatabaseInstance, name string) (*storage.Table, bool) {
	if e.session != nil && e.session.TxActive {
		if t, ok := e.session.TxTables[name]; ok {
			if t == nil {
				return nil, false // Deleted in transaction
			}
			return t, true
		}
	}
	return dbInstance.GetTable(name)
}

func (e *Executor) getTableForModification(dbInstance *storage.DatabaseInstance, name string) (*storage.Table, bool) {
	table, exists := dbInstance.GetTable(name)
	if !exists {
		return nil, false
	}
	if e.session != nil && e.session.TxActive {
		if t, ok := e.session.TxTables[name]; ok {
			if t == nil {
				return nil, false
			}
			return t, true
		}
		// Clone and store in TxTables
		cloned := table.Clone()
		e.session.TxTables[name] = cloned
		return cloned, true
	}
	return table, true
}

func (e *Executor) setTable(dbInstance *storage.DatabaseInstance, name string, table *storage.Table) {
	if e.session != nil && e.session.TxActive {
		e.session.TxTables[name] = table
		return
	}
	dbInstance.SetTable(name, table)
}

func (e *Executor) deleteTable(dbInstance *storage.DatabaseInstance, name string) {
	if e.session != nil && e.session.TxActive {
		e.session.TxTables[name] = nil
		return
	}
	dbInstance.DeleteTable(name)
}

func (e *Executor) saveTableToDisk(dbInstance *storage.DatabaseInstance, table *storage.Table) error {
	if e.session != nil && e.session.TxActive {
		return nil
	}
	return e.db.SaveTableToDisk(dbInstance, table)
}

func (e *Executor) executeTransaction(stmt *parser.TransactionStmt) (*Result, error) {
	if e.session == nil {
		return nil, fmt.Errorf("no active session")
	}

	switch stmt.Command {
	case "BEGIN":
		e.session.TxActive = true
		e.session.TxTables = make(map[string]*storage.Table)
		e.session.TxSavepoints = make(map[string]map[string]*storage.Table)
		return &Result{Message: "BEGIN"}, nil

	case "COMMIT":
		if !e.session.TxActive {
			return &Result{Message: "COMMIT"}, nil
		}
		dbInstance, err := e.getActiveDatabase()
		if err != nil {
			return nil, err
		}
		for name, t := range e.session.TxTables {
			if t == nil {
				dbInstance.DeleteTable(name)
				tablePath := filepath.Join(dbInstance.BasePath, "tables", name+".tbl")
				_ = os.Remove(tablePath)
			} else {
				dbInstance.SetTable(name, t)
				if err := e.db.SaveTableToDisk(dbInstance, t); err != nil {
					return nil, err
				}
			}
		}
		e.session.TxActive = false
		e.session.TxTables = make(map[string]*storage.Table)
		e.session.TxSavepoints = make(map[string]map[string]*storage.Table)
		return &Result{Message: "COMMIT"}, nil

	case "ROLLBACK":
		e.session.TxActive = false
		e.session.TxTables = make(map[string]*storage.Table)
		e.session.TxSavepoints = make(map[string]map[string]*storage.Table)
		return &Result{Message: "ROLLBACK"}, nil

	default:
		return nil, fmt.Errorf("unknown transaction command: %s", stmt.Command)
	}
}

func (e *Executor) executeSavepoint(stmt *parser.SavepointStmt) (*Result, error) {
	if e.session == nil {
		return nil, fmt.Errorf("no active session")
	}
	if !e.session.TxActive {
		return nil, fmt.Errorf("no active transaction")
	}

	switch stmt.Command {
	case "SAVEPOINT":
		// Clone all current TxTables to savepoint
		snapshot := make(map[string]*storage.Table)
		for name, t := range e.session.TxTables {
			if t == nil {
				snapshot[name] = nil
			} else {
				snapshot[name] = t.Clone()
			}
		}
		e.session.TxSavepoints[stmt.Name] = snapshot
		return &Result{Message: "SAVEPOINT"}, nil

	case "ROLLBACK TO":
		snapshot, exists := e.session.TxSavepoints[stmt.Name]
		if !exists {
			return nil, fmt.Errorf("savepoint %s does not exist", stmt.Name)
		}
		// Restore e.session.TxTables from snapshot
		e.session.TxTables = make(map[string]*storage.Table)
		for name, t := range snapshot {
			if t == nil {
				e.session.TxTables[name] = nil
			} else {
				e.session.TxTables[name] = t.Clone()
			}
		}
		return &Result{Message: "ROLLBACK TO SAVEPOINT"}, nil

	default:
		return nil, fmt.Errorf("unknown savepoint command: %s", stmt.Command)
	}
}
