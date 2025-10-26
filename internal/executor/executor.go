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

func (e *Executor) executeCreateTable(stmt *parser.CreateTableStmt) (*Result, error) {
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

	// Store in database
	if e.db.Tables == nil {
		e.db.Tables = make(map[string]*storage.Table)
	}
	e.db.Tables[stmt.TableName] = table

	// Persist to disk
	if err := e.db.SaveTableToDisk(table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("CREATE TABLE %s", stmt.TableName),
	}, nil
}

func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*Result, error) {
	table, exists := e.db.Tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	for _, values := range stmt.Values {
		row := make(storage.Row)

		// If columns specified, use them; otherwise use table order
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

	// Persist to disk after insert
	if err := e.db.SaveTableToDisk(table); err != nil {
		return nil, fmt.Errorf("failed to persist table: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("INSERT %d row(s)", len(stmt.Values)),
	}, nil
}

func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*Result, error) {
	table, exists := e.db.Tables[stmt.TableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", stmt.TableName)
	}

	// Convert parser WhereClause to storage WhereClause
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
