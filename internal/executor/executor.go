// internal/executor/executor.go
package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/ghosecorp/ghostsql/internal/metadata"
	"github.com/ghosecorp/ghostsql/internal/parser"
	"github.com/ghosecorp/ghostsql/internal/storage"
)

type Executor struct {
	db *storage.Database
}

func NewExecutor(db *storage.Database) *Executor {
	return &Executor{db: db}
}

type Result struct {
	Message string
	Rows    []storage.Row
	Columns []string
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
	default:
		return nil, fmt.Errorf("unsupported statement type")
	}
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
	if err := e.db.UseDatabase(stmt.DatabaseName); err != nil {
		return nil, err
	}

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
		if dbName == e.db.CurrentDatabase {
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
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	rows := make([]storage.Row, 0, len(dbInstance.Tables))
	for tableName := range dbInstance.Tables {
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
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[tableName]
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
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	columns := make([]storage.Column, len(stmt.Columns))
	for i, colDef := range stmt.Columns {
		col := storage.Column{
			Name:      colDef.Name,
			Type:      colDef.Type,
			Length:    colDef.Length,
			Nullable:  colDef.Nullable,
			IsPrimary: colDef.IsPrimary,
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

	table := storage.NewTable(stmt.TableName, columns, tableMeta)
	dbInstance.Tables[stmt.TableName] = table

	if err := e.db.SaveTableToDisk(table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("CREATE TABLE %s", stmt.TableName),
	}, nil
}

func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

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
			val := values[i]

			// Find column definition
			var colDef storage.Column
			for _, col := range table.Columns {
				if col.Name == colName {
					colDef = col
					break
				}
			}

			// Check VARCHAR length
			if colDef.Type == storage.TypeVarChar && colDef.Length > 0 {
				if strVal, ok := val.(string); ok {
					if len(strVal) > colDef.Length {
						return nil, fmt.Errorf("value too long for column %s (max %d, got %d)",
							colName, colDef.Length, len(strVal))
					}
				}
			}

			// Parse vector if needed
			if colDef.Type == storage.TypeVector {
				if strVal, ok := val.(string); ok {
					vec, err := storage.ParseVector(strVal)
					if err != nil {
						return nil, fmt.Errorf("invalid vector format for column %s: %w", colName, err)
					}
					val = vec
				}
			}

			row[colName] = val
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

				refTable, exists := dbInstance.Tables[col.ForeignKey.RefTable]
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
	}

	if err := e.db.SaveTableToDisk(table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("INSERT %d row(s)", len(stmt.Values)),
	}, nil
}

func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Get initial rows from the main table
	var where *storage.WhereClause
	if stmt.Where != nil {
		where = convertWhereClause(stmt.Where)
	}

	rows, err := table.Select([]string{"*"}, where)
	if err != nil {
		return nil, err
	}

	// Handle JOINs
	if len(stmt.Joins) > 0 {
		rows, err = e.executeJoins(stmt.TableName, rows, stmt.Joins, dbInstance)
		if err != nil {
			return nil, err
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

	// Build column list
	columns := stmt.Columns
	if len(columns) == 1 && columns[0] == "*" {
		if len(stmt.Joins) > 0 {
			// For JOINs, get all columns from all tables with prefixes
			columns = make([]string, 0)
			for _, col := range table.GetColumnNames() {
				columns = append(columns, stmt.TableName+"."+col)
			}
			for _, join := range stmt.Joins {
				joinTable := dbInstance.Tables[join.Table]
				for _, col := range joinTable.GetColumnNames() {
					columns = append(columns, join.Table+"."+col)
				}
			}
		} else {
			columns = table.GetColumnNames()
		}
	}

	// Filter rows to only include requested columns
	if len(stmt.Joins) > 0 && len(columns) > 0 {
		filteredRows := make([]storage.Row, len(rows))
		for i, row := range rows {
			filteredRow := make(storage.Row)
			for _, col := range columns {
				if val, exists := row[col]; exists {
					filteredRow[col] = val
				}
			}
			filteredRows[i] = filteredRow
		}
		rows = filteredRows
	}

	return &Result{
		Rows:    rows,
		Columns: columns,
	}, nil
}

func (e *Executor) executeJoins(leftTable string, leftRows []storage.Row, joins []parser.JoinClause, dbInstance *storage.DatabaseInstance) ([]storage.Row, error) {
	resultRows := leftRows

	for _, join := range joins {
		rightTable, exists := dbInstance.Tables[join.Table]
		if !exists {
			return nil, fmt.Errorf("table %s does not exist", join.Table)
		}

		rightRows, err := rightTable.Select([]string{"*"}, nil)
		if err != nil {
			return nil, err
		}

		switch join.Type {
		case "INNER":
			resultRows = e.executeInnerJoin(leftTable, resultRows, join.Table, rightRows, join.Condition)
		case "LEFT":
			resultRows = e.executeLeftJoin(leftTable, resultRows, join.Table, rightRows, join.Condition)
		case "RIGHT":
			resultRows = e.executeRightJoin(leftTable, resultRows, join.Table, rightRows, join.Condition)
		case "FULL":
			resultRows = e.executeFullJoin(leftTable, resultRows, join.Table, rightRows, join.Condition)
		case "CROSS":
			resultRows = e.executeCrossJoin(leftTable, resultRows, join.Table, rightRows)
		default:
			return nil, fmt.Errorf("unsupported join type: %s", join.Type)
		}

		leftTable = join.Table
	}

	return resultRows, nil
}

func (e *Executor) executeInnerJoin(leftTable string, leftRows []storage.Row, rightTable string, rightRows []storage.Row, condition *parser.JoinCondition) []storage.Row {
	result := make([]storage.Row, 0)

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			if e.evaluateJoinCondition(leftRow, rightRow, condition) {
				merged := make(storage.Row)
				for k, v := range leftRow {
					merged[leftTable+"."+k] = v
				}
				for k, v := range rightRow {
					merged[rightTable+"."+k] = v
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
			if e.evaluateJoinCondition(leftRow, rightRow, condition) {
				merged := make(storage.Row)
				for k, v := range leftRow {
					merged[leftTable+"."+k] = v
				}
				for k, v := range rightRow {
					merged[rightTable+"."+k] = v
				}
				result = append(result, merged)
				matched = true
			}
		}

		if !matched {
			merged := make(storage.Row)
			for k, v := range leftRow {
				merged[leftTable+"."+k] = v
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
			if e.evaluateJoinCondition(leftRow, rightRow, condition) {
				merged := make(storage.Row)
				for k, v := range leftRow {
					merged[leftTable+"."+k] = v
				}
				for k, v := range rightRow {
					merged[rightTable+"."+k] = v
				}
				result = append(result, merged)
				matched = true
			}
		}

		if !matched {
			merged := make(storage.Row)
			for k, v := range rightRow {
				merged[rightTable+"."+k] = v
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
			if e.evaluateJoinCondition(leftRow, rightRow, condition) {
				merged := make(storage.Row)
				for k, v := range leftRow {
					merged[leftTable+"."+k] = v
				}
				for k, v := range rightRow {
					merged[rightTable+"."+k] = v
				}
				result = append(result, merged)
				matched = true
				rightMatched[i] = true
			}
		}

		if !matched {
			merged := make(storage.Row)
			for k, v := range leftRow {
				merged[leftTable+"."+k] = v
			}
			result = append(result, merged)
		}
	}

	for i, rightRow := range rightRows {
		if !rightMatched[i] {
			merged := make(storage.Row)
			for k, v := range rightRow {
				merged[rightTable+"."+k] = v
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
				merged[leftTable+"."+k] = v
			}
			for k, v := range rightRow {
				merged[rightTable+"."+k] = v
			}
			result = append(result, merged)
		}
	}

	return result
}

func (e *Executor) evaluateJoinCondition(leftRow storage.Row, rightRow storage.Row, condition *parser.JoinCondition) bool {
	if condition == nil {
		return true
	}

	// Get values directly from rows by column name (no table prefix in row keys)
	leftVal := leftRow[condition.LeftColumn]
	rightVal := rightRow[condition.RightColumn]

	// Handle NULL values
	if leftVal == nil || rightVal == nil {
		return false
	}

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
			if !evaluateWhereOnRow(row, having) {
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

func evaluateWhereOnRow(row storage.Row, where *storage.WhereClause) bool {
	val, exists := row[where.Column]
	if !exists {
		return false
	}

	switch where.Operator {
	case "=":
		return compareValues(val, where.Value) == 0
	case "!=", "<>":
		return compareValues(val, where.Value) != 0
	case "<":
		return compareValues(val, where.Value) < 0
	case "<=":
		return compareValues(val, where.Value) <= 0
	case ">":
		return compareValues(val, where.Value) > 0
	case ">=":
		return compareValues(val, where.Value) >= 0
	default:
		return false
	}
}

func (e *Executor) executeUpdate(stmt *parser.UpdateStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	var where *storage.WhereClause
	if stmt.Where != nil {
		where = convertWhereClause(stmt.Where)
	}

	count, err := table.Update(stmt.Updates, where)
	if err != nil {
		return nil, err
	}

	if err := e.db.SaveTableToDisk(table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("UPDATE %d row(s)", count),
	}, nil
}

func (e *Executor) executeDelete(stmt *parser.DeleteStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	var where *storage.WhereClause
	if stmt.Where != nil {
		where = convertWhereClause(stmt.Where)
	}

	count, err := table.Delete(where)
	if err != nil {
		return nil, err
	}

	if err := e.db.SaveTableToDisk(table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("DELETE %d row(s)", count),
	}, nil
}

func (e *Executor) executeDropTable(stmt *parser.DropTableStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	if _, exists := dbInstance.Tables[stmt.TableName]; !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	delete(dbInstance.Tables, stmt.TableName)

	tablePath := filepath.Join(dbInstance.BasePath, "tables", stmt.TableName+".tbl")
	if err := os.Remove(tablePath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove table file: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("DROP TABLE %s", stmt.TableName),
	}, nil
}

func (e *Executor) executeDropDatabase(stmt *parser.DropDatabaseStmt) (*Result, error) {
	if err := e.db.DropDatabase(stmt.DatabaseName); err != nil {
		return nil, err
	}

	return &Result{
		Message: fmt.Sprintf("DROP DATABASE %s", stmt.DatabaseName),
	}, nil
}

func (e *Executor) executeTruncate(stmt *parser.TruncateStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	if err := table.Truncate(); err != nil {
		return nil, err
	}

	if err := e.db.SaveTableToDisk(table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("TRUNCATE TABLE %s", stmt.TableName),
	}, nil
}

func (e *Executor) executeAlterTable(stmt *parser.AlterTableStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[stmt.TableName]
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

		if err := e.db.SaveTableToDisk(table); err != nil {
			return nil, fmt.Errorf("failed to persist table: %w", err)
		}

		return &Result{
			Message: fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", stmt.TableName, stmt.Column.Name),
		}, nil
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
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[stmt.ObjectName]
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

	if err := e.db.SaveTableToDisk(table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("COMMENT ON TABLE %s", stmt.ObjectName),
	}, nil
}

func (e *Executor) executeCommentColumn(stmt *parser.CommentStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	if stmt.TableName == "" {
		return nil, fmt.Errorf("column comments require format: COMMENT ON COLUMN table.column IS 'comment'")
	}

	table, exists := dbInstance.Tables[stmt.TableName]
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

	if err := e.db.SaveTableToDisk(table); err != nil {
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
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	table, exists := dbInstance.Tables[stmt.TableName]
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
	return &Result{
		Message: fmt.Sprintf("DROP INDEX %s (not fully implemented)", stmt.IndexName),
	}, nil
}

func convertWhereClause(where *parser.WhereClause) *storage.WhereClause {
	if where == nil {
		return nil
	}

	return &storage.WhereClause{
		Column:   where.Column,
		Operator: where.Operator,
		Value:    where.Value,
	}
}

func (e *Executor) applyOrderBy(rows []storage.Row, orderBy []parser.OrderByClause) []storage.Row {
	if len(orderBy) == 0 {
		return rows
	}

	sorted := make([]storage.Row, len(rows))
	copy(sorted, rows)

	sort.SliceStable(sorted, func(i, j int) bool {
		for _, order := range orderBy {
			valI := sorted[i][order.Column]
			valJ := sorted[j][order.Column]

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

func compareValues(a, b interface{}) int {
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
