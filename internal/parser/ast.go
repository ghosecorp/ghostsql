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
	Name       string
	Type       storage.DataType
	Length     int
	Nullable   bool
	IsPrimary  bool           // PRIMARY KEY
	IsUnique   bool           // UNIQUE
	ForeignKey *ForeignKeyDef // Add this
	DefaultVal interface{}
}

// InsertStmt represents INSERT INTO
type InsertStmt struct {
	TableName string
	Columns   []string
	Values    [][]interface{}
}

// ForeignKeyDef represents FOREIGN KEY constraint
type ForeignKeyDef struct {
	RefTable  string
	RefColumn string
}

func (s *InsertStmt) StatementNode() {}

// SelectColumn represents a SELECT list entry with optional alias
type SelectColumn struct {
	Expression string // raw expression: "c.relname", "computed_column", etc.
	Alias      string // AS alias, empty if none
}

// SelectStmt represents a SELECT query
type SelectStmt struct {
	Columns       []string
	SelectColumns []SelectColumn
	Aggregates    []AggregateFunc
	TableName     string
	TableAlias    string       // ADD THIS LINE
	Joins         []JoinClause // Add this
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
	IfExists  bool
}

func (s *DropTableStmt) StatementNode() {}

// DropDatabaseStmt represents DROP DATABASE
type DropDatabaseStmt struct {
	DatabaseName string
	IfExists     bool
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
	Action    string // "ADD_COLUMN", "DROP_COLUMN", "ENABLE_RLS", "DISABLE_RLS"
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
	IfExists  bool
}

func (s *DropIndexStmt) StatementNode() {}

// JoinClause represents a JOIN operation
type JoinClause struct {
	Type      string // "INNER", "LEFT", "RIGHT", "FULL", "CROSS"
	Table     string // Table to join
	Alias     string // Optional table alias
	Condition *JoinCondition
}

// JoinCondition represents ON condition
type JoinCondition struct {
	LeftTable   string
	LeftColumn  string
	Operator    string // "=", "!=", etc.
	RightTable  string
	RightColumn string
}

// CreateRoleStmt represents CREATE ROLE
type CreateRoleStmt struct {
	RoleName      string
	IsSuperuser   bool
	CanLogin      bool
	CanCreateRole bool
	CanCreateDB   bool
	Password      string
}

func (s *CreateRoleStmt) StatementNode() {}

// AlterRoleStmt represents ALTER ROLE
type AlterRoleStmt struct {
	RoleName string
	Password string // New password if specified
}

func (s *AlterRoleStmt) StatementNode() {}

// GrantStmt represents GRANT privileges
type GrantStmt struct {
	Privileges []string // SELECT, INSERT, etc.
	All        bool
	ObjectType string   // TABLE, DATABASE, etc.
	ObjectName string   // employees, my_app, etc.
	ToRole     string
}

func (s *GrantStmt) StatementNode() {}

// RevokeStmt represents REVOKE privileges
type RevokeStmt struct {
	Privileges []string
	All        bool
	ObjectType string
	ObjectName string
	FromRole   string
}

func (s *RevokeStmt) StatementNode() {}

// CreatePolicyStmt represents CREATE POLICY
type CreatePolicyStmt struct {
	PolicyName string
	TableName  string
	Action     string // "SELECT", "INSERT", "UPDATE", "DELETE"
	Role       string // "all", or specific role
	Using      *WhereClause
}

func (s *CreatePolicyStmt) StatementNode() {}

// SetStmt represents SET commands
type SetStmt struct {
	Name  string
	Value string
}

func (s *SetStmt) StatementNode() {}

// TransactionStmt represents BEGIN, COMMIT, ROLLBACK
type TransactionStmt struct {
	Command string // "BEGIN", "COMMIT", "ROLLBACK"
}

func (s *TransactionStmt) StatementNode() {}

// DropRoleStmt represents DROP ROLE
type DropRoleStmt struct {
	RoleName string
	IfExists bool
}

func (s *DropRoleStmt) StatementNode() {}
