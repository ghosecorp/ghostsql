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
	Length   int // For VARCHAR(n) or VECTOR(n)
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
	Columns       []string
	Aggregates    []AggregateFunc
	TableName     string
	Where         *WhereClause
	GroupBy       []string
	Having        *WhereClause
	OrderBy       []OrderByClause
	VectorOrderBy *VectorOrderBy // Add this
	Limit         int
	Offset        int
}

// VectorOrderBy represents ORDER BY with vector distance
type VectorOrderBy struct {
	Function    string // "COSINE_DISTANCE", "L2_DISTANCE"
	Column      string
	QueryVector []float32
	Descending  bool
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

// CommentStmt represents COMMENT ON for setting metadata
type CommentStmt struct {
	ObjectType string // "DATABASE", "TABLE", "COLUMN"
	ObjectName string
	TableName  string // For columns
	Comment    string
}

func (s *CommentStmt) StatementNode() {}

// ShowMetadataStmt represents showing metadata
type ShowMetadataStmt struct {
	ObjectType string // "DATABASE", "TABLE", "COLUMN"
	ObjectName string
	TableName  string // For columns
}

func (s *ShowMetadataStmt) StatementNode() {}

// AggregateFunc represents aggregate functions
type AggregateFunc struct {
	Function string // COUNT, SUM, AVG, MAX, MIN
	Column   string
	Alias    string
}

// CreateIndexStmt represents CREATE INDEX
type CreateIndexStmt struct {
	IndexName  string
	TableName  string
	ColumnName string
	IndexType  string         // "HNSW", "BTREE", etc.
	Options    map[string]int // m, ef_construction, etc.
}

func (s *CreateIndexStmt) StatementNode() {}

// DropIndexStmt represents DROP INDEX
type DropIndexStmt struct {
	IndexName string
}

func (s *DropIndexStmt) StatementNode() {}
