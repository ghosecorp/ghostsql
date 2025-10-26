// internal/parser/ast.go
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

// CreateTableStmt represents CREATE TABLE
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

// InsertStmt represents INSERT INTO
type InsertStmt struct {
	TableName string
	Columns   []string
	Values    [][]interface{}
}

func (s *InsertStmt) StatementNode() {}

// SelectStmt represents SELECT
type SelectStmt struct {
	Columns   []string
	TableName string
	Where     *WhereClause
	OrderBy   []OrderByClause
	Limit     int
	Offset    int
}

func (s *SelectStmt) StatementNode() {}

// UpdateStmt represents UPDATE
type UpdateStmt struct {
	TableName string
	Updates   map[string]interface{}
	Where     *WhereClause
}

func (s *UpdateStmt) StatementNode() {}

// DeleteStmt represents DELETE
type DeleteStmt struct {
	TableName string
	Where     *WhereClause
}

func (s *DeleteStmt) StatementNode() {}

// DropTableStmt represents DROP TABLE
type DropTableStmt struct {
	TableName string
}

func (s *DropTableStmt) StatementNode() {}

// DropDatabaseStmt represents DROP DATABASE
type DropDatabaseStmt struct {
	DatabaseName string
}

func (s *DropDatabaseStmt) StatementNode() {}

// TruncateStmt represents TRUNCATE TABLE
type TruncateStmt struct {
	TableName string
}

func (s *TruncateStmt) StatementNode() {}

// AlterTableStmt represents ALTER TABLE
type AlterTableStmt struct {
	TableName string
	Action    string // "ADD_COLUMN", "DROP_COLUMN"
	Column    *ColumnDef
}

func (s *AlterTableStmt) StatementNode() {}

// WhereClause represents WHERE condition
type WhereClause struct {
	Column   string
	Operator string
	Value    interface{}
	And      *WhereClause // For AND conditions
	Or       *WhereClause // For OR conditions
}

// OrderByClause represents ORDER BY
type OrderByClause struct {
	Column     string
	Descending bool
}
