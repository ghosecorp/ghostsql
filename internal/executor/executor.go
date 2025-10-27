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
	case *parser.CommentStmt: // ADD THIS LINE
		return e.executeComment(s) // ADD THIS LINE
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
		columns[i] = storage.Column{
			Name:     colDef.Name,
			Type:     colDef.Type,
			Nullable: colDef.Nullable,
		}
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

			// Check if this column is a VECTOR type
			var colType storage.DataType
			for _, col := range table.Columns {
				if col.Name == colName {
					colType = col.Type
					break
				}
			}

			// Parse vector if needed
			if colType == storage.TypeVector {
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

	var where *storage.WhereClause
	if stmt.Where != nil {
		where = convertWhereClause(stmt.Where)
	}

	rows, err := table.Select(stmt.Columns, where)
	if err != nil {
		return nil, err
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

	columns := stmt.Columns
	if len(columns) == 1 && columns[0] == "*" {
		columns = table.GetColumnNames()
	}

	return &Result{
		Rows:    rows,
		Columns: columns,
	}, nil
}

func (e *Executor) executeAggregateSelect(stmt *parser.SelectStmt, rows []storage.Row) (*Result, error) {
	// Convert parser aggregates to storage aggregates
	aggregates := make([]storage.AggregateSpec, len(stmt.Aggregates))
	for i, agg := range stmt.Aggregates {
		aggregates[i] = storage.AggregateSpec{
			Function: agg.Function,
			Column:   agg.Column,
			Alias:    agg.Alias,
		}
	}

	// Handle GROUP BY
	if len(stmt.GroupBy) > 0 {
		return e.executeGroupBy(stmt, rows, aggregates)
	}

	// Simple aggregation without GROUP BY
	results, err := storage.ComputeAggregates(rows, aggregates)
	if err != nil {
		return nil, err
	}

	// Build result row
	resultRow := make(storage.Row)
	columns := make([]string, 0)

	// Add regular columns (if any)
	for _, col := range stmt.Columns {
		if col != "" && col != "*" {
			columns = append(columns, col)
			if len(rows) > 0 {
				resultRow[col] = rows[0][col]
			}
		}
	}

	// Add aggregate results
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
	// Group rows
	groups := storage.GroupRows(rows, stmt.GroupBy)

	// Compute aggregates for each group
	resultRows := make([]storage.Row, 0, len(groups))
	columns := make([]string, 0)

	// Add GROUP BY columns
	columns = append(columns, stmt.GroupBy...)

	// Add aggregate column names
	for _, agg := range aggregates {
		columns = append(columns, agg.Alias)
	}

	for _, group := range groups {
		row := make(storage.Row)

		// Add group key values
		for col, val := range group.GroupKey {
			row[col] = val
		}

		// Compute and add aggregates
		aggResults, err := storage.ComputeAggregates(group.Rows, aggregates)
		if err != nil {
			return nil, err
		}

		for _, res := range aggResults {
			row[res.Alias] = res.Value
		}

		// Apply HAVING filter if present
		if stmt.Having != nil {
			having := convertWhereClause(stmt.Having)
			if !evaluateWhereOnRow(row, having) {
				continue
			}
		}

		resultRows = append(resultRows, row)
	}

	// Apply ORDER BY on grouped results
	if len(stmt.OrderBy) > 0 {
		resultRows = e.applyOrderBy(resultRows, stmt.OrderBy)
	}

	// Apply LIMIT and OFFSET
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

// Helper function to evaluate WHERE on a single row
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

	// Delete from memory
	delete(dbInstance.Tables, stmt.TableName)

	// Delete from disk
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

// Helper functions

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

	// Make a copy to avoid modifying original
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
	// Store database metadata
	// For now, we'll store it in a simple way
	// In production, this would go to the metadata store

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

	// Update table metadata
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

	// Save to disk
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

	// Determine table name
	if stmt.TableName == "" {
		// Need to parse column name differently
		return nil, fmt.Errorf("column comments require format: COMMENT ON COLUMN table.column IS 'comment'")
	}

	table, exists := dbInstance.Tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Find and update column metadata
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

	// Save to disk
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

	// Check if HNSW index exists for this column
	if index, exists := table.VectorIndexes[stmt.VectorOrderBy.Column]; exists {
		// Use HNSW index - O(log n)
		ef := limit * 2 // ef_search parameter
		if ef < 50 {
			ef = 50
		}
		results, err = index.Search(queryVector, limit, ef)
		if err != nil {
			return nil, err
		}

		// Map row IDs back to actual rows
		for i := range results {
			rowID := results[i].Row["_row_id"].(int)
			if rowID < len(rows) {
				results[i].Row = rows[rowID]
			}
		}
	} else {
		// Use brute-force - O(n)
		results, err = storage.VectorSearch(rows, queryVector, stmt.VectorOrderBy.Column, metric, limit)
		if err != nil {
			return nil, err
		}
	}

	// Rest of the function stays the same...
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

	// Initialize VectorIndexes map if nil
	if table.VectorIndexes == nil {
		table.VectorIndexes = make(map[string]*storage.HNSWIndex)
	}

	// Find column
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

	// Only HNSW for VECTOR columns
	if stmt.IndexType == "HNSW" {
		if colType != storage.TypeVector {
			return nil, fmt.Errorf("HNSW index only supported on VECTOR columns")
		}

		// Create HNSW index
		m := stmt.Options["m"]
		efConstruction := stmt.Options["ef_construction"]

		index := storage.NewHNSWIndex(m, efConstruction, storage.DistanceCosine)

		// Build index from existing data
		for i, row := range table.Rows {
			if vec, ok := row[stmt.ColumnName].(*storage.Vector); ok {
				if err := index.Add(vec, i); err != nil {
					return nil, fmt.Errorf("failed to build index: %w", err)
				}
			}
		}

		// Store index
		table.VectorIndexes[stmt.ColumnName] = index

		return &Result{
			Message: fmt.Sprintf("CREATE INDEX %s ON %s USING HNSW (m=%d, ef_construction=%d)",
				stmt.IndexName, stmt.TableName, m, efConstruction),
		}, nil
	}

	return nil, fmt.Errorf("unsupported index type: %s", stmt.IndexType)
}

func (e *Executor) executeDropIndex(stmt *parser.DropIndexStmt) (*Result, error) {
	// For now, we store indexes by column name, not index name
	// In production, you'd maintain an index registry
	return &Result{
		Message: fmt.Sprintf("DROP INDEX %s (not fully implemented)", stmt.IndexName),
	}, nil
}
