// internal/executor/executor.go << 'EOF'
package executor

import (
	"fmt"

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
		rows[i] = storage.Row{
			"Column":   col.Name,
			"Type":     col.Type.String(),
			"Nullable": nullable,
		}
	}

	return &Result{
		Rows:    rows,
		Columns: []string{"Column", "Type", "Nullable"},
	}, nil
}

func (e *Executor) executeCreateTable(stmt *parser.CreateTableStmt) (*Result, error) {
	dbInstance, err := e.db.GetCurrentDatabase()
	if err != nil {
		return nil, err
	}

	// Convert column definitions
	columns := make([]storage.Column, len(stmt.Columns))
	for i, colDef := range stmt.Columns {
		columns[i] = storage.Column{
			Name:     colDef.Name,
			Type:     colDef.Type,
			Nullable: colDef.Nullable,
		}
	}

	// Create table metadata
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

	// Create table
	table := storage.NewTable(stmt.TableName, columns, tableMeta)
	dbInstance.Tables[stmt.TableName] = table

	// Persist to disk
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
			row[colName] = values[i]
		}

		if err := table.Insert(row); err != nil {
			return nil, err
		}
	}

	// Persist to disk
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
		where = &storage.WhereClause{
			Column:   stmt.Where.Column,
			Operator: stmt.Where.Operator,
			Value:    stmt.Where.Value,
		}
	}

	rows, err := table.Select(stmt.Columns, where)
	if err != nil {
		return nil, err
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
