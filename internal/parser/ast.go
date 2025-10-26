package parser

import "github.com/ghosecorp/ghostsql/internal/storage"

// Statement is the interface for all SQL statements
type Statement interface {
	StatementNode()
}

// CreateTableStmt represents CREATE TABLE statement
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
	Metadata  []string // [0]=purpose, [1]=description
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

// WhereClause represents WHERE condition (simplified)
type WhereClause struct {
	Column   string
	Operator string
	Value    interface{}
}
