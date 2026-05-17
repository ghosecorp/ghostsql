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
	TableName   string
	Columns     []ColumnDef
	Metadata    []string
	IfNotExists bool
}

func (s *CreateTableStmt) StatementNode() {}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name        string
	Type        storage.DataType
	Length      int
	Nullable    bool
	IsPrimary   bool           // PRIMARY KEY
	IsUnique    bool           // UNIQUE
	ForeignKey  *ForeignKeyDef // Add this
	DefaultVal  interface{}
	DefaultExpr string         // Expression string like NOW()
	CheckExpr   string         // CHECK constraint expression
}

// OnConflictDef represents ON CONFLICT target DO UPDATE / NOTHING
type OnConflictDef struct {
	TargetColumn string
	DoNothing    bool
	DoUpdate     bool
	Updates      map[string]interface{}
}

// InsertStmt represents INSERT INTO
type InsertStmt struct {
	TableName   string
	Columns     []string
	Values      [][]interface{}
	Returning   []SelectColumn
	SelectQuery *SelectStmt    // For INSERT ... SELECT
	OnConflict  *OnConflictDef // For ON CONFLICT
}

// ForeignKeyDef represents FOREIGN KEY constraint
type ForeignKeyDef struct {
	RefTable  string
	RefColumn string
}

func (s *InsertStmt) StatementNode() {}

// SelectColumn represents a SELECT list entry with optional alias
type SelectColumn struct {
	Expression string     // raw expression
	Alias      string     // AS alias, empty if none
	Subquery   *SelectStmt // for scalar subqueries like (SELECT ...)
	Window     *WindowDef  // for window functions like ROW_NUMBER() OVER (...)
}

// SelectStmt represents a SELECT query
type SelectStmt struct {
	Columns       []string
	SelectColumns []SelectColumn
	Aggregates    []AggregateFunc
	TableName     string
	TableAlias    string
	Joins         []JoinClause
	Where         *WhereClause
	GroupBy       []string
	Having        *WhereClause
	OrderBy       []OrderByClause
	VectorOrderBy *VectorOrderBy
	Limit         int
	Offset        int
	// New fields
	Distinct            bool
	DistinctOn          []string
	CTEs                []CTEDefinition
	TableSampleMethod   string  // "BERNOULLI", "SYSTEM"
	TableSamplePercent  float64 // 0-100
}

// CTEDefinition represents a WITH clause CTE
type CTEDefinition struct {
	Name      string
	Recursive bool
	Query     Statement
}

// WindowDef represents OVER (PARTITION BY ... ORDER BY ...) for window functions
type WindowDef struct {
	PartitionBy []string
	OrderBy     []OrderByClause
}

// CompoundSelectStmt represents UNION / INTERSECT / EXCEPT
type CompoundSelectStmt struct {
	Left    *SelectStmt
	Op      string // "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
	Right   *SelectStmt
	OrderBy []OrderByClause
	Limit   int
	Offset  int
}

func (s *CompoundSelectStmt) StatementNode() {}

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
	Returning []SelectColumn
	FromTable string // For join-based UPDATE ... FROM
}

func (s *UpdateStmt) StatementNode() {}

// DeleteStmt represents DELETE
type DeleteStmt struct {
	TableName  string
	Where      *WhereClause
	Returning  []SelectColumn
	UsingTable string // For join-based DELETE ... USING
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
	TableName            string
	Action               string // "ADD_COLUMN", "DROP_COLUMN", "ENABLE_RLS", "DISABLE_RLS", "RENAME_TO", "RENAME_COLUMN", "ALTER_COLUMN_TYPE", "ADD_CONSTRAINT"
	Column               *ColumnDef
	DropColumn           string
	RenameTo             string
	RenameColumnFrom     string
	RenameColumnTo       string
	AlterColumnName      string
	AlterColumnType      storage.DataType
	AddConstraintName    string
	AddConstraintUnique  []string
	IfExists             bool
}

func (s *AlterTableStmt) StatementNode() {}

// WhereClause represents WHERE condition
type WhereClause struct {
	Column   string
	Operator string
	Value    interface{}
	And      *WhereClause
	Or       *WhereClause
	// For EXISTS / NOT EXISTS subqueries
	Subquery *SelectStmt
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
	Lateral   bool        // True if LATERAL join
	Subquery  *SelectStmt // For LATERAL (SELECT ...) joins
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

// CreateViewStmt represents CREATE VIEW
type CreateViewStmt struct {
	ViewName  string
	Query     *SelectStmt
	OrReplace bool
}

func (s *CreateViewStmt) StatementNode() {}

// DropViewStmt represents DROP VIEW
type DropViewStmt struct {
	ViewName string
	IfExists bool
}

func (s *DropViewStmt) StatementNode() {}

// CreateSchemaStmt represents CREATE SCHEMA
type CreateSchemaStmt struct {
	SchemaName  string
	IfNotExists bool
}

func (s *CreateSchemaStmt) StatementNode() {}

// CreateSequenceStmt represents CREATE SEQUENCE
type CreateSequenceStmt struct {
	SequenceName string
	Start        int
	Increment    int
	IfNotExists  bool
}

func (s *CreateSequenceStmt) StatementNode() {}

// CreateTypeStmt represents CREATE TYPE ... AS ENUM (...)
type CreateTypeStmt struct {
	TypeName string
	Values   []string
}

func (s *CreateTypeStmt) StatementNode() {}

// CreateMaterializedViewStmt represents CREATE MATERIALIZED VIEW
type CreateMaterializedViewStmt struct {
	ViewName    string
	Query       *SelectStmt
	IfNotExists bool
}

func (s *CreateMaterializedViewStmt) StatementNode() {}

// RefreshMaterializedViewStmt represents REFRESH MATERIALIZED VIEW
type RefreshMaterializedViewStmt struct {
	ViewName string
}

func (s *RefreshMaterializedViewStmt) StatementNode() {}

// MergeStmt represents MERGE INTO
type MergeStmt struct {
	TargetTable    string
	SourceTable    string
	SourceQuery    *SelectStmt
	OnCondition    *WhereClause
	WhenMatched    []MergeAction
	WhenNotMatched []MergeAction
}

func (s *MergeStmt) StatementNode() {}

type MergeAction struct {
	Action  string // "UPDATE", "INSERT", "DELETE", "DO NOTHING"
	Updates map[string]interface{}
	Columns []string
	Values  []interface{}
}

// SavepointStmt represents SAVEPOINT & ROLLBACK TO savepoint
type SavepointStmt struct {
	Command string // "SAVEPOINT" or "ROLLBACK TO"
	Name    string
}

func (s *SavepointStmt) StatementNode() {}

// ShowVarStmt represents SHOW <var>
type ShowVarStmt struct {
	Name string
}

func (s *ShowVarStmt) StatementNode() {}

// ResetStmt represents RESET <var>
type ResetStmt struct {
	Name string
}

func (s *ResetStmt) StatementNode() {}

// SetRoleStmt represents SET ROLE <role>
type SetRoleStmt struct {
	Role string
}

func (s *SetRoleStmt) StatementNode() {}

// SetSessionAuthorizationStmt represents SET SESSION AUTHORIZATION <user>
type SetSessionAuthorizationStmt struct {
	User string
}

func (s *SetSessionAuthorizationStmt) StatementNode() {}

// LockTableStmt represents LOCK TABLE <name>
type LockTableStmt struct {
	TableName string
	Mode      string
}

func (s *LockTableStmt) StatementNode() {}

// DeclareCursorStmt represents DECLARE cur CURSOR FOR
type DeclareCursorStmt struct {
	Name  string
	Query *SelectStmt
}

func (s *DeclareCursorStmt) StatementNode() {}

// FetchCursorStmt represents FETCH FROM cur
type FetchCursorStmt struct {
	Name  string
	Count int
}

func (s *FetchCursorStmt) StatementNode() {}

// MoveCursorStmt represents MOVE IN cur
type MoveCursorStmt struct {
	Name  string
	Count int
}

func (s *MoveCursorStmt) StatementNode() {}

// CloseCursorStmt represents CLOSE cur
type CloseCursorStmt struct {
	Name string
}

func (s *CloseCursorStmt) StatementNode() {}


