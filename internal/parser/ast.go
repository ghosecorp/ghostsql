// internal/parser/ast.go << 'EOF'
package parser

import "github.com/ghosecorp/ghostsql/internal/storage"

// Statement is the interface for all SQL statements
type Statement interface {
	StatementNode()
}

// CreateDatabaseStmt represents CREATE DATABASE
type CreateDatabaseStmt struct {
	DatabaseName string
	Metadata     []string
}

func (s *CreateDatabaseStmt) StatementNode() {}

// UseDatabaseStmt represents USE database
type UseDatabaseStmt struct {
	DatabaseName string
}

func (s *UseDatabaseStmt) StatementNode() {}

// ShowStmt represents SHOW commands
type ShowStmt struct {
	ShowType  string // "DATABASES", "TABLES", "COLUMNS"
	TableName string // For SHOW COLUMNS
}

func (s *ShowStmt) StatementNode() {}

// CreateTableStmt represents CREATE TABLE statement
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
	Metadata  []string
}

func (s *CreateTableStmt) StatementNode() {}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name     string
	Type     storage.DataType
	Nullable bool
	Metadata []string
}

// InsertStmt represents INSERT INTO statement
type InsertStmt struct {
	TableName string
	Columns   []string
	Values    [][]interface{}
}

func (s *InsertStmt) StatementNode() {}

// SelectStmt represents SELECT statement
type SelectStmt struct {
	Columns   []string
	TableName string
	Where     *WhereClause
}

func (s *SelectStmt) StatementNode() {}

// WhereClause represents WHERE condition
type WhereClause struct {
	Column   string
	Operator string
	Value    interface{}
}
