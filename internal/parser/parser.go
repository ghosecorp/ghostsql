// internal/parser/parser.go
package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ghosecorp/ghostsql/internal/storage"
)

type Parser struct {
	lexer   *Lexer
	current Token
	peek    Token
}

func NewParser(input string) *Parser {
	lexer := NewLexer(input)
	p := &Parser{lexer: lexer}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.current = p.peek
	p.peek = p.lexer.NextToken()
}

func (p *Parser) Parse() (Statement, error) {
	var stmt Statement
	var err error

	switch p.current.Type {
	case TOKEN_SELECT:
		stmt, err = p.parseSelectOrCompound()
	case TOKEN_WITH:
		stmt, err = p.parseWithCTE()
	case TOKEN_INSERT:
		stmt, err = p.parseInsert()
	case TOKEN_CREATE:
		stmt, err = p.parseCreate()
	case TOKEN_USE:
		stmt, err = p.parseUse()
	case TOKEN_SHOW:
		stmt, err = p.parseShow()
	case TOKEN_UPDATE:
		stmt, err = p.parseUpdate()
	case TOKEN_DELETE:
		stmt, err = p.parseDelete()
	case TOKEN_DROP:
		stmt, err = p.parseDrop()
	case TOKEN_ALTER:
		stmt, err = p.parseAlter()
	case TOKEN_TRUNCATE:
		stmt, err = p.parseTruncate()
	case TOKEN_COMMENT:
		stmt, err = p.parseComment()
	case TOKEN_GRANT:
		stmt, err = p.parseGrant()
	case TOKEN_REVOKE:
		stmt, err = p.parseRevoke()
	case TOKEN_SET:
		stmt, err = p.parseSet()
	case TOKEN_REFRESH:
		stmt, err = p.parseRefresh()
	case TOKEN_MERGE:
		stmt, err = p.parseMerge()
	case TOKEN_BEGIN:
		p.nextToken()
		stmt = &TransactionStmt{Command: "BEGIN"}
	case TOKEN_COMMIT:
		p.nextToken()
		stmt = &TransactionStmt{Command: "COMMIT"}
	case TOKEN_ROLLBACK:
		p.nextToken()
		if p.current.Type == TOKEN_TO || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "TO") {
			p.nextToken()
			if p.current.Type == TOKEN_SAVEPOINT || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "SAVEPOINT") {
				p.nextToken()
			}
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected savepoint name after ROLLBACK TO")
			}
			spName := p.current.Literal
			p.nextToken()
			stmt = &SavepointStmt{Command: "ROLLBACK TO", Name: spName}
		} else {
			stmt = &TransactionStmt{Command: "ROLLBACK"}
		}
	case TOKEN_SAVEPOINT:
		stmt, err = p.parseSavepoint()
	case TOKEN_RESET:
		stmt, err = p.parseReset()
	case TOKEN_LOCK:
		stmt, err = p.parseLock()
	case TOKEN_DECLARE:
		stmt, err = p.parseDeclareCursor()
	case TOKEN_FETCH:
		stmt, err = p.parseFetchCursor()
	case TOKEN_MOVE:
		stmt, err = p.parseMoveCursor()
	case TOKEN_CLOSE:
		stmt, err = p.parseCloseCursor()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.current.Type)
	}

	if err != nil {
		return nil, err
	}

	// Ensure no trailing junk
	if p.current.Type != TOKEN_EOF && p.current.Type != TOKEN_SEMICOLON {
		return nil, fmt.Errorf("unexpected token after statement: %s (literal: '%s')", p.current.Type, p.current.Literal)
	}

	return stmt, nil
}

// parseSelectOrCompound parses SELECT or SELECT ... UNION/INTERSECT/EXCEPT SELECT
func (p *Parser) parseSelectOrCompound() (Statement, error) {
	left, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	if p.current.Type == TOKEN_UNION || p.current.Type == TOKEN_INTERSECT || p.current.Type == TOKEN_EXCEPT {
		op := strings.ToUpper(p.current.Literal)
		p.nextToken()
		// Check for ALL after UNION
		if op == "UNION" && p.current.Type == TOKEN_ALL {
			op = "UNION ALL"
			p.nextToken()
		}
		if p.current.Type != TOKEN_SELECT {
			return nil, fmt.Errorf("expected SELECT after %s", op)
		}
		right, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		compound := &CompoundSelectStmt{Left: left, Op: op, Right: right}
		// Parse optional trailing ORDER BY, LIMIT, OFFSET
		if p.current.Type == TOKEN_ORDER {
			p.nextToken()
			if p.current.Type == TOKEN_BY {
				p.nextToken()
			}
			for p.current.Type == TOKEN_IDENT {
				col := p.current.Literal
				p.nextToken()
				desc := false
				if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "DESC" {
					desc = true
					p.nextToken()
				} else if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ASC" {
					p.nextToken()
				}
				compound.OrderBy = append(compound.OrderBy, OrderByClause{Column: col, Descending: desc})
				if p.current.Type == TOKEN_COMMA {
					p.nextToken()
				} else {
					break
				}
			}
		}
		if p.current.Type == TOKEN_LIMIT {
			p.nextToken()
			if p.current.Type == TOKEN_NUMBER {
				compound.Limit, _ = strconv.Atoi(p.current.Literal)
				p.nextToken()
			}
		}
		if p.current.Type == TOKEN_OFFSET {
			p.nextToken()
			if p.current.Type == TOKEN_NUMBER {
				compound.Offset, _ = strconv.Atoi(p.current.Literal)
				p.nextToken()
			}
		}
		return compound, nil
	}

	return left, nil
}

// parseWithCTE parses WITH [RECURSIVE] name AS (SELECT ...) [, ...] SELECT ...
func (p *Parser) parseWithCTE() (Statement, error) {
	p.nextToken() // consume WITH

	recursive := false
	if p.current.Type == TOKEN_RECURSIVE {
		recursive = true
		p.nextToken()
	}

	var ctes []CTEDefinition
	for {
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected CTE name")
		}
		cteName := p.current.Literal
		p.nextToken()

		if p.current.Type != TOKEN_AS {
			return nil, fmt.Errorf("expected AS after CTE name")
		}
		p.nextToken()

		if p.current.Type != TOKEN_LPAREN {
			return nil, fmt.Errorf("expected ( after AS in CTE")
		}
		p.nextToken()

		if p.current.Type != TOKEN_SELECT {
			return nil, fmt.Errorf("expected SELECT in CTE body")
		}
		cteQuery, err := p.parseSelectOrCompound()
		if err != nil {
			return nil, err
		}

		if p.current.Type != TOKEN_RPAREN {
			return nil, fmt.Errorf("expected ) after CTE body")
		}
		p.nextToken()

		ctes = append(ctes, CTEDefinition{Name: cteName, Recursive: recursive, Query: cteQuery})

		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
			continue
		}
		break
	}

	if p.current.Type != TOKEN_SELECT {
		return nil, fmt.Errorf("expected SELECT after WITH clause")
	}
	main, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	main.CTEs = ctes
	return main, nil
}

func (p *Parser) parseCreate() (Statement, error) {
	p.nextToken() // consume CREATE

	orReplace := false
	if p.current.Type == TOKEN_OR {
		p.nextToken() // consume OR
		if p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "REPLACE" {
			return nil, fmt.Errorf("expected REPLACE after CREATE OR")
		}
		p.nextToken() // consume REPLACE
		orReplace = true
	}

	isMaterialized := false
	if p.current.Type == TOKEN_MATERIALIZED {
		p.nextToken() // consume MATERIALIZED
		isMaterialized = true
	}

	switch p.current.Type {
	case TOKEN_DATABASE:
		return p.parseCreateDatabase()
	case TOKEN_TABLE:
		return p.parseCreateTable()
	case TOKEN_VIEW:
		if isMaterialized {
			return p.parseCreateMaterializedView()
		}
		return p.parseCreateView(orReplace)
	case TOKEN_SCHEMA:
		return p.parseCreateSchema()
	case TOKEN_SEQUENCE:
		return p.parseCreateSequence()
	case TOKEN_TYPE:
		return p.parseCreateType()
	case TOKEN_INDEX:
		return p.parseCreateIndex()
	case TOKEN_ROLE:
		return p.parseCreateRole()
	case TOKEN_POLICY:
		return p.parseCreatePolicy()
	default:
		return nil, fmt.Errorf("expected DATABASE, TABLE, VIEW, SCHEMA, SEQUENCE, TYPE, INDEX, ROLE, or POLICY after CREATE")
	}
}

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{
		Updates: make(map[string]interface{}),
	}

	p.nextToken() // consume UPDATE

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_SET {
		return nil, fmt.Errorf("expected SET")
	}
	p.nextToken()

	// Parse SET column = value, column = value
	for {
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected column name")
		}
		colName := p.current.Literal
		p.nextToken()

		if p.current.Type != TOKEN_EQUALS {
			return nil, fmt.Errorf("expected =")
		}
		p.nextToken()

		var value interface{}
		switch p.current.Type {
		case TOKEN_NUMBER:
			if strings.Contains(p.current.Literal, ".") {
				num, _ := strconv.ParseFloat(p.current.Literal, 64)
				value = num
			} else {
				num, _ := strconv.Atoi(p.current.Literal)
				value = num
			}
			p.nextToken()
		case TOKEN_STRING:
			value = p.current.Literal
			p.nextToken()
		case TOKEN_IDENT:
			identVal := p.current.Literal
			p.nextToken()
			if p.current.Type == TOKEN_DOT {
				p.nextToken()
				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected column name after .")
				}
				identVal += "." + p.current.Literal
				p.nextToken()
			}
			value = identVal
		default:
			return nil, fmt.Errorf("expected value")
		}
		stmt.Updates[colName] = value

		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
			continue
		}
		break
	}

	// Parse optional FROM clause for join updates
	if p.current.Type == TOKEN_FROM {
		p.nextToken() // consume FROM
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected table name after FROM in UPDATE")
		}
		stmt.FromTable = p.current.Literal
		p.nextToken()
		// Consume optional table alias
		if p.current.Type != TOKEN_WHERE && p.current.Type != TOKEN_RETURNING && p.current.Type != TOKEN_EOF {
			p.nextToken()
		}
	}

	// Parse WHERE clause
	if p.current.Type == TOKEN_WHERE {
		p.nextToken()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Parse RETURNING clause
	if p.current.Type == TOKEN_RETURNING {
		p.nextToken()
		returning, err := p.parseReturningColumns()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	return stmt, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}

	p.nextToken() // consume DELETE

	if p.current.Type != TOKEN_FROM {
		return nil, fmt.Errorf("expected FROM")
	}
	p.nextToken()

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.current.Literal
	p.nextToken()

	// Parse optional USING clause for join deletes
	if p.current.Type == TOKEN_USING || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "USING") {
		p.nextToken() // consume USING
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected table name after USING")
		}
		stmt.UsingTable = p.current.Literal
		p.nextToken()
		// Consume optional table alias
		if p.current.Type != TOKEN_WHERE && p.current.Type != TOKEN_RETURNING && p.current.Type != TOKEN_EOF {
			p.nextToken()
		}
	}

	// Parse WHERE clause
	if p.current.Type == TOKEN_WHERE {
		p.nextToken()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Parse RETURNING clause
	if p.current.Type == TOKEN_RETURNING {
		p.nextToken()
		returning, err := p.parseReturningColumns()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	return stmt, nil
}

// parseReturningColumns parses RETURNING col1, col2 or RETURNING *
func (p *Parser) parseReturningColumns() ([]SelectColumn, error) {
	var cols []SelectColumn
	for p.current.Type != TOKEN_EOF && p.current.Type != TOKEN_SEMICOLON {
		if p.current.Type == TOKEN_ASTERISK {
			cols = append(cols, SelectColumn{Expression: "*"})
			p.nextToken()
			break
		}
		if p.current.Type != TOKEN_IDENT {
			break
		}
		name := p.current.Literal
		p.nextToken()
		alias := ""
		if p.current.Type == TOKEN_AS {
			p.nextToken()
			if p.current.Type == TOKEN_IDENT {
				alias = p.current.Literal
				p.nextToken()
			}
		}
		cols = append(cols, SelectColumn{Expression: name, Alias: alias})
		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
		} else {
			break
		}
	}
	return cols, nil
}

func (p *Parser) parseDrop() (Statement, error) {
	p.nextToken() // consume DROP

	// Support DROP IF EXISTS TABLE / DATABASE / etc.
	ifExists := false
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IF" {
		p.nextToken()
		if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "EXISTS" {
			ifExists = true
			p.nextToken()
		} else if p.current.Type == TOKEN_EXISTS {
			ifExists = true
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected EXISTS after DROP IF")
		}
	}

	targetType := p.current.Type
	targetTypeLiteral := strings.ToUpper(p.current.Literal)
	p.nextToken() // consume TABLE, DATABASE, INDEX, ROLE, VIEW

	// Support DROP TABLE IF EXISTS
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IF" {
		p.nextToken()
		if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "EXISTS" {
			ifExists = true
			p.nextToken()
		} else if p.current.Type == TOKEN_EXISTS {
			ifExists = true
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected EXISTS after DROP ... IF")
		}
	} else if p.current.Type == TOKEN_EXISTS {
		ifExists = true
		p.nextToken()
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected name after DROP %s", targetTypeLiteral)
	}
	name := p.current.Literal
	p.nextToken()

	switch targetType {
	case TOKEN_TABLE:
		return &DropTableStmt{TableName: name, IfExists: ifExists}, nil
	case TOKEN_DATABASE:
		return &DropDatabaseStmt{DatabaseName: name, IfExists: ifExists}, nil
	case TOKEN_INDEX:
		return &DropIndexStmt{IndexName: name, IfExists: ifExists}, nil
	case TOKEN_ROLE:
		return &DropRoleStmt{RoleName: name, IfExists: ifExists}, nil
	case TOKEN_VIEW:
		return &DropViewStmt{ViewName: name, IfExists: ifExists}, nil
	default:
		return nil, fmt.Errorf("expected TABLE, DATABASE, INDEX, ROLE, or VIEW after DROP")
	}
}

func (p *Parser) parseDropRole() (*DropRoleStmt, error) {
	p.nextToken() // consume ROLE
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected role name after DROP ROLE")
	}
	stmt := &DropRoleStmt{RoleName: p.current.Literal}
	p.nextToken()
	return stmt, nil
}

func (p *Parser) parseDropTable() (*DropTableStmt, error) {
	stmt := &DropTableStmt{}

	p.nextToken() // consume TABLE

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.current.Literal
	p.nextToken()

	return stmt, nil
}

func (p *Parser) parseDropDatabase() (*DropDatabaseStmt, error) {
	stmt := &DropDatabaseStmt{}

	p.nextToken() // consume DATABASE

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected database name")
	}
	stmt.DatabaseName = p.current.Literal
	p.nextToken()

	return stmt, nil
}

func (p *Parser) parseTruncate() (*TruncateStmt, error) {
	stmt := &TruncateStmt{}

	p.nextToken() // consume TRUNCATE

	if p.current.Type == TOKEN_TABLE {
		p.nextToken() // consume TABLE (optional)
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.current.Literal
	p.nextToken()

	return stmt, nil
}

func (p *Parser) parseAlter() (Statement, error) {
	p.nextToken() // consume ALTER

	switch p.current.Type {
	case TOKEN_TABLE:
		return p.parseAlterTable()
	case TOKEN_ROLE:
		return p.parseAlterRole()
	case TOKEN_DEFAULT:
		return p.parseAlterDefaultPrivileges()
	default:
		return nil, fmt.Errorf("expected TABLE, ROLE or DEFAULT after ALTER")
	}
}

func (p *Parser) parseAlterTable() (*AlterTableStmt, error) {
	stmt := &AlterTableStmt{}

	p.nextToken() // consume TABLE

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.current.Literal
	p.nextToken()

	switch p.current.Type {
	case TOKEN_ADD:
		p.nextToken()
		// Check for CONSTRAINT: ADD CONSTRAINT constraint_name UNIQUE (column)
		if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "CONSTRAINT" {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected constraint name")
			}
			stmt.AddConstraintName = p.current.Literal
			p.nextToken()
		}

		// If ADD UNIQUE (column)
		if p.current.Type == TOKEN_UNIQUE {
			p.nextToken()
			if p.current.Type != TOKEN_LPAREN {
				return nil, fmt.Errorf("expected ( after UNIQUE")
			}
			p.nextToken()
			for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected column name")
				}
				stmt.AddConstraintUnique = append(stmt.AddConstraintUnique, p.current.Literal)
				p.nextToken()
				if p.current.Type == TOKEN_COMMA {
					p.nextToken()
				}
			}
			if p.current.Type != TOKEN_RPAREN {
				return nil, fmt.Errorf("expected )")
			}
			p.nextToken()
			stmt.Action = "ADD_CONSTRAINT"
			return stmt, nil
		}

		if p.current.Type == TOKEN_COLUMN {
			p.nextToken()
		}
		stmt.Action = "ADD_COLUMN"
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Column = &col

	case TOKEN_DROP:
		p.nextToken()
		if p.current.Type == TOKEN_COLUMN {
			p.nextToken()
		}
		ifExists := false
		if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IF" {
			p.nextToken()
			if p.current.Type == TOKEN_EXISTS || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "EXISTS") {
				ifExists = true
				p.nextToken()
			} else {
				return nil, fmt.Errorf("expected EXISTS after IF")
			}
		} else if p.current.Type == TOKEN_EXISTS {
			ifExists = true
			p.nextToken()
		}

		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected column name after DROP COLUMN")
		}
		stmt.DropColumn = p.current.Literal
		p.nextToken()
		stmt.Action = "DROP_COLUMN"
		stmt.IfExists = ifExists

	case TOKEN_IDENT, TOKEN_ALTER:
		keyword := strings.ToUpper(p.current.Literal)
		if p.current.Type == TOKEN_ALTER {
			keyword = "ALTER"
		}
		switch keyword {
		case "RENAME":
			p.nextToken() // consume RENAME
			if p.current.Type == TOKEN_TO {
				p.nextToken() // consume TO
				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected new table name after RENAME TO")
				}
				stmt.RenameTo = p.current.Literal
				p.nextToken()
				stmt.Action = "RENAME_TO"
			} else {
				if p.current.Type == TOKEN_COLUMN {
					p.nextToken()
				}
				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected column name to rename")
				}
				stmt.RenameColumnFrom = p.current.Literal
				p.nextToken()

				if p.current.Type != TOKEN_TO {
					return nil, fmt.Errorf("expected TO after column name in RENAME COLUMN")
				}
				p.nextToken()

				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected new column name")
				}
				stmt.RenameColumnTo = p.current.Literal
				p.nextToken()
				stmt.Action = "RENAME_COLUMN"
			}
		case "ALTER":
			p.nextToken() // consume ALTER
			if p.current.Type == TOKEN_COLUMN {
				p.nextToken()
			}
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected column name after ALTER COLUMN")
			}
			stmt.AlterColumnName = p.current.Literal
			p.nextToken()

			actionType := strings.ToUpper(p.current.Literal)
			p.nextToken() // consume keyword

			if actionType == "TYPE" {
				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected new type after ALTER COLUMN TYPE")
				}
				typeName := strings.ToUpper(p.current.Literal)
				var dataType storage.DataType
				switch typeName {
				case "INT":
					dataType = storage.TypeInt
				case "BIGINT":
					dataType = storage.TypeBigInt
				case "TEXT":
					dataType = storage.TypeText
				case "VARCHAR":
					dataType = storage.TypeVarChar
				case "FLOAT":
					dataType = storage.TypeFloat
				case "BOOLEAN":
					dataType = storage.TypeBoolean
				case "DATE", "TIMESTAMP":
					dataType = storage.TypeText
				default:
					return nil, fmt.Errorf("unknown type: %s", typeName)
				}
				stmt.AlterColumnType = dataType
				p.nextToken()
				stmt.Action = "ALTER_COLUMN_TYPE"
			} else {
				return nil, fmt.Errorf("expected TYPE after ALTER COLUMN col")
			}
		default:
			return nil, fmt.Errorf("expected ADD, DROP, RENAME, ALTER, ENABLE, or DISABLE after ALTER TABLE")
		}

	case TOKEN_ENABLE, TOKEN_DISABLE:
		action := "ENABLE_RLS"
		if p.current.Type == TOKEN_DISABLE {
			action = "DISABLE_RLS"
		}
		p.nextToken()
		if p.current.Type == TOKEN_ROW {
			p.nextToken()
			if p.current.Type == TOKEN_LEVEL {
				p.nextToken()
			}
		}
		if p.current.Type != TOKEN_SECURITY {
			return nil, fmt.Errorf("expected SECURITY after ENABLE/DISABLE [ROW LEVEL]")
		}
		p.nextToken()
		stmt.Action = action

	default:
		return nil, fmt.Errorf("expected ADD, DROP, RENAME, ALTER, ENABLE, or DISABLE after ALTER TABLE")
	}
	return stmt, nil
}

func (p *Parser) parseCreateDatabase() (*CreateDatabaseStmt, error) {
	stmt := &CreateDatabaseStmt{}

	p.nextToken() // consume DATABASE

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected database name")
	}

	stmt.DatabaseName = p.current.Literal
	p.nextToken()

	// Check for METADATA
	if p.current.Type == TOKEN_METADATA {
		p.nextToken()
		if p.current.Type != TOKEN_LBRACKET {
			return nil, fmt.Errorf("expected [ after METADATA")
		}
		p.nextToken()

		if p.current.Type != TOKEN_STRING {
			return nil, fmt.Errorf("expected string for purpose")
		}
		stmt.Metadata = append(stmt.Metadata, p.current.Literal)
		p.nextToken()

		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
			if p.current.Type != TOKEN_STRING {
				return nil, fmt.Errorf("expected string for description")
			}
			stmt.Metadata = append(stmt.Metadata, p.current.Literal)
			p.nextToken()
		}

		if p.current.Type != TOKEN_RBRACKET {
			return nil, fmt.Errorf("expected ]")
		}
		p.nextToken()
	}

	return stmt, nil
}

func (p *Parser) parseUse() (*UseDatabaseStmt, error) {
	stmt := &UseDatabaseStmt{}

	p.nextToken() // consume USE

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected database name")
	}

	stmt.DatabaseName = p.current.Literal
	p.nextToken()

	return stmt, nil
}

func (p *Parser) parseShow() (Statement, error) {
	p.nextToken() // consume SHOW

	switch p.current.Type {
	case TOKEN_DATABASES:
		p.nextToken()
		return &ShowStmt{ShowType: "DATABASES"}, nil

	case TOKEN_TABLES:
		p.nextToken()
		return &ShowStmt{ShowType: "TABLES"}, nil

	case TOKEN_COLUMNS:
		p.nextToken()

		if p.current.Type != TOKEN_FROM {
			return nil, fmt.Errorf("expected FROM after SHOW COLUMNS")
		}
		p.nextToken()

		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected table name")
		}
		tableName := p.current.Literal
		p.nextToken()
		return &ShowStmt{ShowType: "COLUMNS", TableName: tableName}, nil

	case TOKEN_IDENT:
		varName := p.current.Literal
		p.nextToken()
		return &ShowVarStmt{Name: varName}, nil

	default:
		return nil, fmt.Errorf("expected DATABASES, TABLES, COLUMNS, or variable name after SHOW")
	}
}

func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}

	p.nextToken() // consume TABLE

	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IF" {
		p.nextToken() // consume IF
		if p.current.Type == TOKEN_NOT || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "NOT") {
			p.nextToken() // consume NOT
			if p.current.Type == TOKEN_EXISTS || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "EXISTS") {
				p.nextToken() // consume EXISTS
				stmt.IfNotExists = true
			} else {
				return nil, fmt.Errorf("expected EXISTS after IF NOT")
			}
		} else {
			return nil, fmt.Errorf("expected NOT after IF")
		}
	} else if p.current.Type == TOKEN_EXISTS {
		// Just in case of keyword token tokenization:
		stmt.IfNotExists = true
		p.nextToken()
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name, got %s", p.current.Type)
	}

	stmt.TableName = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected (, got %s", p.current.Type)
	}
	p.nextToken()

	// Parse columns
	for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)

		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
		}
	}

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected ), got %s", p.current.Type)
	}
	p.nextToken()

	// Check for METADATA
	if p.current.Type == TOKEN_METADATA {
		p.nextToken()
		if p.current.Type != TOKEN_LBRACKET {
			return nil, fmt.Errorf("expected [ after METADATA")
		}
		p.nextToken()

		if p.current.Type != TOKEN_STRING {
			return nil, fmt.Errorf("expected string for purpose")
		}
		stmt.Metadata = append(stmt.Metadata, p.current.Literal)
		p.nextToken()

		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
			if p.current.Type != TOKEN_STRING {
				return nil, fmt.Errorf("expected string for description")
			}
			stmt.Metadata = append(stmt.Metadata, p.current.Literal)
			p.nextToken()
		}

		if p.current.Type != TOKEN_RBRACKET {
			return nil, fmt.Errorf("expected ]")
		}
		p.nextToken()
	}

	return stmt, nil
}

func (p *Parser) parseColumnDef() (ColumnDef, error) {
	col := ColumnDef{Nullable: true}

	if p.current.Type != TOKEN_IDENT {
		return col, fmt.Errorf("expected column name, got %s (literal: '%s')", p.current.Type, p.current.Literal)
	}

	col.Name = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_SERIAL && p.current.Type != TOKEN_BIGSERIAL && p.current.Type != TOKEN_JSONB {
		return col, fmt.Errorf("expected column type after %s, got %s (literal: '%s')", col.Name, p.current.Type, p.current.Literal)
	}

	typeName := strings.ToUpper(p.current.Literal)

	switch typeName {
	case "INT":
		col.Type = storage.TypeInt
		p.nextToken()

	case "SERIAL", "BIGSERIAL":
		col.Type = storage.TypeInt
		col.Nullable = false
		col.DefaultExpr = "nextval"
		p.nextToken()

	case "BIGINT":
		col.Type = storage.TypeBigInt
		p.nextToken()

	case "TEXT":
		col.Type = storage.TypeText
		p.nextToken()

	case "VARCHAR":
		col.Type = storage.TypeVarChar
		col.Length = 255 // default
		p.nextToken()

		// Parse optional length VARCHAR(n)
		if p.current.Type == TOKEN_LPAREN {
			p.nextToken()
			if p.current.Type != TOKEN_NUMBER {
				return col, fmt.Errorf("expected number for VARCHAR length")
			}
			length, _ := strconv.Atoi(p.current.Literal)
			col.Length = length
			p.nextToken()

			if p.current.Type != TOKEN_RPAREN {
				return col, fmt.Errorf("expected )")
			}
			p.nextToken()
		}

	case "FLOAT":
		col.Type = storage.TypeFloat
		p.nextToken()

	case "BOOLEAN":
		col.Type = storage.TypeBoolean
		p.nextToken()

	case "DATE", "TIMESTAMP":
		col.Type = storage.TypeText
		p.nextToken()

	case "VECTOR":
		col.Type = storage.TypeVector
		col.Length = 0 // dimensions
		p.nextToken()

		// Parse optional dimensions VECTOR(384)
		if p.current.Type == TOKEN_LPAREN {
			p.nextToken()
			if p.current.Type == TOKEN_NUMBER {
				length, _ := strconv.Atoi(p.current.Literal)
				col.Length = length
				p.nextToken()
			}
			if p.current.Type != TOKEN_RPAREN {
				return col, fmt.Errorf("expected )")
			}
			p.nextToken()
		}

	case "JSONB":
		col.Type = storage.TypeJSONB
		p.nextToken()

	default:
		return col, fmt.Errorf("unknown type: %s", typeName)
	}

	// After parsing the type, parse constraints
	for {
		if p.current.Type == TOKEN_COMMA || p.current.Type == TOKEN_RPAREN {
			break
		}

		if p.current.Type == TOKEN_EOF {
			break
		}

		// Handle TOKEN_DEFAULT directly
		if p.current.Type == TOKEN_DEFAULT {
			p.nextToken() // consume DEFAULT
			exprStr := ""
			if p.current.Type == TOKEN_IDENT {
				exprStr = p.current.Literal
				p.nextToken()
				if p.current.Type == TOKEN_LPAREN {
					exprStr += "("
					p.nextToken()
					if p.current.Type == TOKEN_STRING || p.current.Type == TOKEN_IDENT {
						exprStr += "'" + p.current.Literal + "'"
						p.nextToken()
					}
					if p.current.Type == TOKEN_RPAREN {
						exprStr += ")"
						p.nextToken()
					}
				}
			} else if p.current.Type == TOKEN_NUMBER || p.current.Type == TOKEN_STRING || p.current.Type == TOKEN_NULL {
				exprStr = p.current.Literal
				p.nextToken()
			} else {
				return col, fmt.Errorf("unexpected token in DEFAULT expression: %s", p.current.Type)
			}
			col.DefaultExpr = exprStr
			continue
		}

		// Handle TOKEN_CHECK directly
		if p.current.Type == TOKEN_CHECK {
			p.nextToken() // consume CHECK
			if p.current.Type != TOKEN_LPAREN {
				return col, fmt.Errorf("expected ( after CHECK")
			}
			p.nextToken()
			checkExpr := ""
			parenDepth := 1
			for parenDepth > 0 && p.current.Type != TOKEN_EOF {
				if p.current.Type == TOKEN_LPAREN {
					parenDepth++
				} else if p.current.Type == TOKEN_RPAREN {
					parenDepth--
					if parenDepth == 0 {
						p.nextToken()
						break
					}
				}
				checkExpr += p.current.Literal + " "
				p.nextToken()
			}
			col.CheckExpr = strings.TrimSpace(checkExpr)
			continue
		}

		// Handle TOKEN_UNIQUE directly
		if p.current.Type == TOKEN_UNIQUE {
			col.IsUnique = true
			p.nextToken()
			continue
		}

		// Handle TOKEN_NULL directly
		if p.current.Type == TOKEN_NULL {
			col.Nullable = true
			p.nextToken()
			continue
		}

		// Handle TOKEN_REFERENCES directly
		if p.current.Type == TOKEN_REFERENCES {
			// Parse FOREIGN KEY: REFERENCES table(column)
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return col, fmt.Errorf("expected table name after REFERENCES")
			}
			refTable := p.current.Literal
			p.nextToken()

			if p.current.Type != TOKEN_LPAREN {
				return col, fmt.Errorf("expected ( after table name")
			}
			p.nextToken()

			if p.current.Type != TOKEN_IDENT {
				return col, fmt.Errorf("expected column name")
			}
			refColumn := p.current.Literal
			p.nextToken()

			if p.current.Type != TOKEN_RPAREN {
				return col, fmt.Errorf("expected )")
			}
			p.nextToken()

			col.ForeignKey = &ForeignKeyDef{
				RefTable:  refTable,
				RefColumn: refColumn,
			}
			continue
		}

		if p.current.Type == TOKEN_IDENT {
			keyword := strings.ToUpper(p.current.Literal)
			switch keyword {
			case "NOT":
				p.nextToken()
				// Handle both TOKEN_NULL and "NULL" as identifier
				if p.current.Type == TOKEN_NULL || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "NULL") {
					col.Nullable = false
					p.nextToken()
				} else {
					return col, fmt.Errorf("expected NULL after NOT")
				}
			case "NULL":
				col.Nullable = true
				p.nextToken()
			case "PRIMARY":
				p.nextToken()
				if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "KEY" {
					col.IsPrimary = true
					col.Nullable = false
					p.nextToken()
				}
			case "REFERENCES":
				// Parse FOREIGN KEY: REFERENCES table(column)
				p.nextToken()
				if p.current.Type != TOKEN_IDENT {
					return col, fmt.Errorf("expected table name after REFERENCES")
				}
				refTable := p.current.Literal
				p.nextToken()

				if p.current.Type != TOKEN_LPAREN {
					return col, fmt.Errorf("expected ( after table name")
				}
				p.nextToken()

				if p.current.Type != TOKEN_IDENT {
					return col, fmt.Errorf("expected column name")
				}
				refColumn := p.current.Literal
				p.nextToken()

				if p.current.Type != TOKEN_RPAREN {
					return col, fmt.Errorf("expected )")
				}
				p.nextToken()

				col.ForeignKey = &ForeignKeyDef{
					RefTable:  refTable,
					RefColumn: refColumn,
				}
			default:
				// Skip unknown keywords
				p.nextToken()
			}
		} else {
			break
		}
	}

	return col, nil
}

func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{}

	p.nextToken() // consume INSERT
	if p.current.Type != TOKEN_INTO {
		return nil, fmt.Errorf("expected INTO, got %s", p.current.Type)
	}

	p.nextToken() // consume INTO
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	stmt.TableName = p.current.Literal
	p.nextToken()

	// Optional column list
	if p.current.Type == TOKEN_LPAREN {
		p.nextToken()
		for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected column name")
			}
			stmt.Columns = append(stmt.Columns, p.current.Literal)
			p.nextToken()
			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
			}
		}
		if p.current.Type != TOKEN_RPAREN {
			return nil, fmt.Errorf("expected )")
		}
		p.nextToken()
	}

	if p.current.Type == TOKEN_SELECT || p.current.Type == TOKEN_WITH {
		selectQuery, err := p.parseSelectOrCompound()
		if err != nil {
			return nil, err
		}
		stmt.SelectQuery = selectQuery.(*SelectStmt)
	} else if p.current.Type == TOKEN_VALUES {
		p.nextToken() // consume VALUES

		// Parse multiple value sets: VALUES (1, 'a'), (2, 'b'), (3, 'c')
		for {
			if p.current.Type != TOKEN_LPAREN {
				return nil, fmt.Errorf("expected (")
			}
			p.nextToken()

			// Parse values for one row
			values := []interface{}{}
			for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
				var val interface{}

				if p.current.Type == TOKEN_LBRACKET {
					// Parse vector array
					vectorStr := "["
					p.nextToken()
					for p.current.Type != TOKEN_RBRACKET && p.current.Type != TOKEN_EOF {
						vectorStr += p.current.Literal
						p.nextToken()
						if p.current.Type == TOKEN_COMMA {
							vectorStr += ","
							p.nextToken()
						}
					}
					if p.current.Type != TOKEN_RBRACKET {
						return nil, fmt.Errorf("expected ]")
					}
					vectorStr += "]"
					val = vectorStr
					p.nextToken()
				} else {
					switch p.current.Type {
					case TOKEN_NUMBER:
						if strings.Contains(p.current.Literal, ".") {
							num, _ := strconv.ParseFloat(p.current.Literal, 64)
							val = num
						} else {
							num, _ := strconv.Atoi(p.current.Literal)
							val = num
						}
					case TOKEN_STRING:
						val = p.current.Literal
					case TOKEN_NULL:
						val = nil
					case TOKEN_IDENT:
						if strings.ToUpper(p.current.Literal) == "NULL" {
							val = nil
						} else {
							return nil, fmt.Errorf("unexpected identifier: %s", p.current.Literal)
						}
					default:
						return nil, fmt.Errorf("unexpected value type: %s", p.current.Type)
					}
					p.nextToken()
				}

				values = append(values, val)

				if p.current.Type == TOKEN_COMMA {
					p.nextToken()
				}
			}

			if p.current.Type != TOKEN_RPAREN {
				return nil, fmt.Errorf("expected )")
			}
			p.nextToken()

			stmt.Values = append(stmt.Values, values)

			// Check for more rows
			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
				continue
			} else {
				break
			}
		}
	} else {
		return nil, fmt.Errorf("expected VALUES or SELECT statement")
	}

	// Parse ON CONFLICT
	if p.current.Type == TOKEN_CONFLICT || p.current.Type == TOKEN_ON || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ON") {
		hasConflict := false
		if p.current.Type == TOKEN_CONFLICT {
			hasConflict = true
			p.nextToken()
		} else if p.current.Type == TOKEN_ON || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ON") {
			p.nextToken()
			if p.current.Type == TOKEN_CONFLICT || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "CONFLICT") {
				hasConflict = true
				p.nextToken()
			}
		}

		if hasConflict {
			onConflict := &OnConflictDef{Updates: make(map[string]interface{})}
			if p.current.Type == TOKEN_LPAREN {
				p.nextToken()
				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected column name inside ON CONFLICT (...)")
				}
				onConflict.TargetColumn = p.current.Literal
				p.nextToken()
				if p.current.Type != TOKEN_RPAREN {
					return nil, fmt.Errorf("expected )")
				}
				p.nextToken()
			}

			if p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "DO" {
				return nil, fmt.Errorf("expected DO after ON CONFLICT")
			}
			p.nextToken()

			action := strings.ToUpper(p.current.Literal)
			p.nextToken()
			if action == "NOTHING" || p.current.Type == TOKEN_NOTHING {
				onConflict.DoNothing = true
				if p.current.Type == TOKEN_NOTHING {
					p.nextToken()
				}
			} else if action == "UPDATE" {
				onConflict.DoUpdate = true
				if p.current.Type != TOKEN_SET {
					return nil, fmt.Errorf("expected SET after DO UPDATE")
				}
				p.nextToken()

				for {
					if p.current.Type != TOKEN_IDENT {
						return nil, fmt.Errorf("expected column name")
					}
					colName := p.current.Literal
					p.nextToken()

					if p.current.Type != TOKEN_EQUALS {
						return nil, fmt.Errorf("expected =")
					}
					p.nextToken()

					var val interface{}
					if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "EXCLUDED" {
						p.nextToken()
						if p.current.Type != TOKEN_DOT {
							return nil, fmt.Errorf("expected . after EXCLUDED")
						}
						p.nextToken()
						if p.current.Type != TOKEN_IDENT {
							return nil, fmt.Errorf("expected column name after EXCLUDED.")
						}
						val = "EXCLUDED." + p.current.Literal
						p.nextToken()
					} else {
						switch p.current.Type {
						case TOKEN_NUMBER:
							if strings.Contains(p.current.Literal, ".") {
								num, _ := strconv.ParseFloat(p.current.Literal, 64)
								val = num
							} else {
								num, _ := strconv.Atoi(p.current.Literal)
								val = num
							}
						case TOKEN_STRING:
							val = p.current.Literal
						case TOKEN_NULL:
							val = nil
						case TOKEN_IDENT:
							if strings.ToUpper(p.current.Literal) == "NULL" {
								val = nil
							} else {
								val = p.current.Literal
							}
						default:
							return nil, fmt.Errorf("unexpected value type in DO UPDATE SET: %s", p.current.Type)
						}
						p.nextToken()
					}

					onConflict.Updates[colName] = val

					if p.current.Type == TOKEN_COMMA {
						p.nextToken()
						continue
					}
					break
				}
			} else {
				return nil, fmt.Errorf("expected NOTHING or UPDATE after DO")
			}
			stmt.OnConflict = onConflict
		}
	}

	// Parse RETURNING clause
	if p.current.Type == TOKEN_RETURNING {
		p.nextToken()
		returning, err := p.parseReturningColumns()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	return stmt, nil
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	p.nextToken() // consume SELECT

	// Handle DISTINCT / DISTINCT ON
	if p.current.Type == TOKEN_DISTINCT {
		p.nextToken()
		if p.current.Type == TOKEN_ON {
			p.nextToken()
			if p.current.Type != TOKEN_LPAREN {
				return nil, fmt.Errorf("expected ( after DISTINCT ON")
			}
			p.nextToken()
			for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
				if p.current.Type == TOKEN_IDENT {
					stmt.DistinctOn = append(stmt.DistinctOn, p.current.Literal)
				}
				p.nextToken()
				if p.current.Type == TOKEN_COMMA {
					p.nextToken()
				}
			}
			if p.current.Type == TOKEN_RPAREN {
				p.nextToken()
			}
		} else {
			stmt.Distinct = true
		}
	}

	// Parse columns and aggregates
	for p.current.Type != TOKEN_FROM && p.current.Type != TOKEN_EOF && p.current.Type != TOKEN_UNION && p.current.Type != TOKEN_INTERSECT && p.current.Type != TOKEN_EXCEPT && p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_SEMICOLON {
		// Check for aggregate functions
		if p.isAggregateFunction(p.current.Type) {
			agg, err := p.parseAggregate()
			if err != nil {
				return nil, err
			}
			stmt.Aggregates = append(stmt.Aggregates, agg)
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: agg.Function + "(" + agg.Column + ")", Alias: agg.Alias})
		} else if p.current.Type == TOKEN_ASTERISK {
			stmt.Columns = append(stmt.Columns, "*")
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: "*"})
			p.nextToken()
		} else if p.current.Type == TOKEN_LPAREN {
			// Could be a scalar subquery or parenthesized expression
			p.nextToken()
			if p.current.Type == TOKEN_SELECT {
				subquery, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				if p.current.Type != TOKEN_RPAREN {
					return nil, fmt.Errorf("expected ) after scalar subquery")
				}
				p.nextToken() // consume )

				alias := ""
				if p.current.Type == TOKEN_AS {
					p.nextToken()
					if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
						alias = p.current.Literal
						p.nextToken()
					}
				} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_FROM && p.current.Type != TOKEN_COMMA {
					alias = p.current.Literal
					p.nextToken()
				}

				stmt.Columns = append(stmt.Columns, alias)
				stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Subquery: subquery, Alias: alias})
			} else {
				// We don't support arbitrary parenthesized expressions here yet, so error out for now
				return nil, fmt.Errorf("expected SELECT inside parenthesized expression in SELECT list")
			}
		} else if p.current.Type == TOKEN_CASE {
			// Capture full CASE ... END expression
			expr := "CASE "
			p.nextToken() // consume CASE
			depth := 1
			for depth > 0 && p.current.Type != TOKEN_EOF {
				if p.current.Type == TOKEN_CASE {
					depth++
				} else if p.current.Type == TOKEN_END {
					depth--
				}
				if p.current.Type == TOKEN_STRING {
					expr += "'" + p.current.Literal + "' "
				} else {
					expr += p.current.Literal + " "
				}
				p.nextToken()
			}
			expr = strings.TrimSpace(expr)

			alias := ""
			if p.current.Type == TOKEN_AS {
				p.nextToken()
				if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
					alias = p.current.Literal
					p.nextToken()
				}
			} else if p.current.Type == TOKEN_IDENT {
				// Implicit alias
				alias = p.current.Literal
				p.nextToken()
			}
			stmt.Columns = append(stmt.Columns, expr)
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: expr, Alias: alias})
		} else if p.current.Type == TOKEN_IDENT {
			// Parse column name (may be table.column or function_call())
			name := p.current.Literal
			p.nextToken()

			// Check for function call or dot
			if p.current.Type == TOKEN_LPAREN {
				// Function call — capture full args including nested parens
				args := "("
				p.nextToken() // consume (
				depth := 1
				for depth > 0 && p.current.Type != TOKEN_EOF {
					if p.current.Type == TOKEN_LPAREN {
						depth++
					} else if p.current.Type == TOKEN_RPAREN {
						depth--
					}
					if depth > 0 {
						if p.current.Type == TOKEN_STRING {
							args += "'" + p.current.Literal + "'"
						} else if p.current.Type == TOKEN_COMMA {
							args += ","
						} else if p.current.Type == TOKEN_FROM {
							// If it's EXTRACT(YEAR FROM date), replace FROM with a comma
							if strings.ToUpper(name) == "EXTRACT" {
								args += ","
							} else {
								args += " " + p.current.Literal + " "
							}
						} else if p.current.Type == TOKEN_LPAREN || p.current.Type == TOKEN_RPAREN {
							args += p.current.Literal
						} else {
							args += p.current.Literal + " "
						}
					}
					p.nextToken()
				}
				args += ")"
				exprFull := name + args
				// The loop already consumed ) when depth became 0; no need for nextToken()
				// Check for OVER (window function)
				var winDef *WindowDef
				if p.current.Type == TOKEN_OVER {
					p.nextToken()
					w, err := p.parseWindowDef()
					if err != nil {
						return nil, err
					}
					winDef = w
				}

				// Handle potential cast: EXTRACT(YEAR FROM date)::INT
				for p.current.Type == TOKEN_CAST {
					p.nextToken() // consume ::
					exprFull += "::"
					for p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_DOT {
						exprFull += p.current.Literal
						p.nextToken()
					}
				}

				alias := ""
				if p.current.Type == TOKEN_AS {
					p.nextToken()
					if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
						alias = p.current.Literal
						p.nextToken()
					}
				}
				if winDef != nil && alias == "" {
					alias = exprFull
				}
				stmt.Columns = append(stmt.Columns, name)
				stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: exprFull, Alias: alias, Window: winDef})
			} else if p.current.Type == TOKEN_DOT {
				// Handle table.column or schema.table.column
				for p.current.Type == TOKEN_DOT {
					p.nextToken()
					if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_ASTERISK {
						return nil, fmt.Errorf("expected identifier after dot")
					}
					name += "." + p.current.Literal
					p.nextToken()
				}
				// Check for function call after dotted name
				if p.current.Type == TOKEN_LPAREN {
					p.nextToken()
					depth := 1
					for depth > 0 && p.current.Type != TOKEN_EOF {
						if p.current.Type == TOKEN_LPAREN {
							depth++
						} else if p.current.Type == TOKEN_RPAREN {
							depth--
						}
						p.nextToken()
					}
				}
				// Handle potential cast: dotted_name::type
				for p.current.Type == TOKEN_CAST {
					p.nextToken() // consume ::
					name += "::"
					for p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_DOT {
						name += p.current.Literal
						p.nextToken()
					}
				}
				alias := ""
				if p.current.Type == TOKEN_AS {
					p.nextToken()
					if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
						alias = p.current.Literal
						p.nextToken()
					}
				} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM {
					// Implicit alias
					alias = p.current.Literal
					p.nextToken()
				}
				stmt.Columns = append(stmt.Columns, name)
				stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: name, Alias: alias})
			} else if p.current.Type == TOKEN_PLUS || p.current.Type == TOKEN_MINUS || p.current.Type == TOKEN_ASTERISK || p.current.Type == TOKEN_SLASH || p.current.Type == TOKEN_JSON_ARROW || p.current.Type == TOKEN_JSON_TEXT_ARROW || p.current.Type == TOKEN_JSON_CONTAIN {
				// Arithmetic or JSON expression starting with identifier
				expr := name
				for p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM && p.current.Type != TOKEN_EOF && p.current.Type != TOKEN_AS {
					if p.current.Type == TOKEN_STRING {
						expr += "'" + p.current.Literal + "'"
					} else {
						expr += p.current.Literal
					}
					p.nextToken()
				}
				expr = strings.TrimSpace(expr)
				alias := expr
				if p.current.Type == TOKEN_AS {
					p.nextToken()
					if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
						alias = p.current.Literal
						p.nextToken()
					}
				} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM {
					alias = p.current.Literal
					p.nextToken()
				}
				stmt.Columns = append(stmt.Columns, alias)
				stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: expr, Alias: alias})
			} else {
				// Handle potential cast: plain_ident::type
				for p.current.Type == TOKEN_CAST {
					p.nextToken() // consume ::
					name += "::"
					for p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_DOT {
						name += p.current.Literal
						p.nextToken()
					}
				}
				alias := ""
				if p.current.Type == TOKEN_AS {
					p.nextToken()
					if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
						alias = p.current.Literal
						p.nextToken()
					}
				} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM {
					// Implicit alias
					alias = p.current.Literal
					p.nextToken()
				}
				stmt.Columns = append(stmt.Columns, name)
				stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: name, Alias: alias})
			}
		} else if p.current.Type == TOKEN_STRING {
			// Bare string literal e.g. ''
			expr := "'" + p.current.Literal + "'"
			p.nextToken()

			// Handle potential cast: '2023-01-01'::DATE
			for p.current.Type == TOKEN_CAST {
				p.nextToken() // consume ::
				expr += "::"
				for p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_DOT {
					expr += p.current.Literal
					p.nextToken()
				}
			}

			alias := expr
			if p.current.Type == TOKEN_AS {
				p.nextToken()
				if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
					alias = p.current.Literal
					p.nextToken()
				}
			} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM {
				alias = p.current.Literal
				p.nextToken()
			}
			stmt.Columns = append(stmt.Columns, alias)
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: expr, Alias: alias})
		} else if p.current.Type == TOKEN_LPAREN {
			// Standalone subquery or expression in parens
			p.nextToken() // consume (
			depth := 1
			expr := "("
			isSubquery := false
			if p.current.Type == TOKEN_SELECT {
				isSubquery = true
			}

			for depth > 0 && p.current.Type != TOKEN_EOF {
				if p.current.Type == TOKEN_LPAREN {
					depth++
				} else if p.current.Type == TOKEN_RPAREN {
					depth--
				}
				expr += p.current.Literal + " "
				p.nextToken()
			}
			expr = strings.TrimSpace(expr)

			alias := "computed_column"
			if isSubquery {
				alias = "subquery"
			}

			if p.current.Type == TOKEN_AS {
				p.nextToken()
				if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
					alias = p.current.Literal
					p.nextToken()
				}
			} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM {
				alias = p.current.Literal
				p.nextToken()
			}
			stmt.Columns = append(stmt.Columns, alias)
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: expr, Alias: alias})
		} else if p.current.Type == TOKEN_NUMBER || p.current.Type == TOKEN_PLUS || p.current.Type == TOKEN_MINUS || p.current.Type == TOKEN_ASTERISK || p.current.Type == TOKEN_SLASH {
			// Arithmetic expression
			expr := ""
			for p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM && p.current.Type != TOKEN_EOF && p.current.Type != TOKEN_AS {
				expr += p.current.Literal
				p.nextToken()
			}
			expr = strings.TrimSpace(expr)
			alias := expr
			if p.current.Type == TOKEN_AS {
				p.nextToken()
				if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
					alias = p.current.Literal
					p.nextToken()
				}
			} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM {
				alias = p.current.Literal
				p.nextToken()
			}
			stmt.Columns = append(stmt.Columns, alias)
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: expr, Alias: alias})
		} else {
			// Other literal (e.g. false, true)
			expr := p.current.Literal
			p.nextToken()

			// Handle potential cast: false::pg_catalog.bool
			for p.current.Type == TOKEN_CAST {
				p.nextToken() // consume ::
				expr += "::"
				// Consume type name (may be dotted)
				for p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_DOT {
					expr += p.current.Literal
					p.nextToken()
				}
			}

			alias := expr
			if p.current.Type == TOKEN_AS {
				p.nextToken()
				if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
					alias = p.current.Literal
					p.nextToken()
				}
			} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_COMMA && p.current.Type != TOKEN_FROM {
				alias = p.current.Literal
				p.nextToken()
			}
			stmt.Columns = append(stmt.Columns, alias)
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: expr, Alias: alias})
		}
		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
		}
	}
	if p.current.Type == TOKEN_FROM {
		p.nextToken()

		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected table name")
		}

		stmt.TableName = p.current.Literal
		p.nextToken()

		// Handle schema-qualified table names (e.g. pg_catalog.pg_class)
		for p.current.Type == TOKEN_DOT {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected table name after dot")
			}
			stmt.TableName += "." + p.current.Literal
			p.nextToken()
		}

		// Check for table alias (AS alias or just alias)
		if p.current.Type == TOKEN_AS {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected alias after AS")
			}
			stmt.TableAlias = p.current.Literal
			p.nextToken()
		} else if p.current.Type == TOKEN_IDENT && p.current.Type != TOKEN_WHERE && p.current.Type != TOKEN_GROUP && p.current.Type != TOKEN_ORDER && p.current.Type != TOKEN_LIMIT && p.current.Type != TOKEN_OFFSET && p.current.Type != TOKEN_UNION && p.current.Type != TOKEN_INTERSECT && p.current.Type != TOKEN_EXCEPT && p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_SEMICOLON {
			stmt.TableAlias = p.current.Literal
			p.nextToken()
		}

		// Parse table sampling: TABLESAMPLE BERNOULLI (10)
		if p.current.Type == TOKEN_TABLESAMPLE {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected tablesample method")
			}
			stmt.TableSampleMethod = strings.ToUpper(p.current.Literal)
			p.nextToken()
			if p.current.Type != TOKEN_LPAREN {
				return nil, fmt.Errorf("expected ( after tablesample method")
			}
			p.nextToken()
			if p.current.Type != TOKEN_NUMBER {
				return nil, fmt.Errorf("expected percentage number in tablesample")
			}
			percent, _ := strconv.ParseFloat(p.current.Literal, 64)
			stmt.TableSamplePercent = percent
			p.nextToken()
			if p.current.Type != TOKEN_RPAREN {
				return nil, fmt.Errorf("expected ) after tablesample percent")
			}
			p.nextToken()
		}

		// Parse JOIN clauses
		for {
			joinType := ""
			if p.current.Type == TOKEN_LEFT || p.current.Type == TOKEN_RIGHT || p.current.Type == TOKEN_INNER || p.current.Type == TOKEN_CROSS || p.current.Type == TOKEN_FULL {
				joinType = strings.ToUpper(p.current.Literal)
				p.nextToken()
				if p.current.Type == TOKEN_OUTER {
					joinType += " OUTER"
					p.nextToken()
				}
			}

			// Check for JOIN keyword
			if p.current.Type == TOKEN_JOIN {
				if joinType == "" {
					joinType = "INNER" // Default to INNER JOIN
				}
				p.nextToken()

				join, err := p.parseJoin(joinType)
				if err != nil {
					return nil, err
				}
				stmt.Joins = append(stmt.Joins, join)
			} else {
				break
			}
		}
	}

	// Parse WHERE clause
	if p.current.Type == TOKEN_WHERE {
		p.nextToken()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Parse GROUP BY
	if p.current.Type == TOKEN_GROUP {
		p.nextToken()
		if p.current.Type != TOKEN_BY {
			return nil, fmt.Errorf("expected BY after GROUP")
		}
		p.nextToken()

		for {
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected column name in GROUP BY")
			}
			stmt.GroupBy = append(stmt.GroupBy, p.current.Literal)
			p.nextToken()

			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
				continue
			}
			break
		}
	}

	// Parse HAVING
	if p.current.Type == TOKEN_HAVING {
		p.nextToken()
		having, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Having = having
	}

	// Parse ORDER BY
	if p.current.Type == TOKEN_ORDER {
		p.nextToken()
		if p.current.Type != TOKEN_BY {
			return nil, fmt.Errorf("expected BY after ORDER")
		}
		p.nextToken()

		// Check for vector distance function
		if p.isVectorDistanceFunc(p.current.Type) {
			vectorOrder, err := p.parseVectorOrderBy()
			if err != nil {
				return nil, err
			}
			stmt.VectorOrderBy = vectorOrder
		} else {
			// Regular ORDER BY
			for {
				col := p.current.Literal
				p.nextToken()

				// Check for operator syntax: vec <-> '[...]'
				if p.current.Type == TOKEN_L2_DISTANCE || p.current.Type == TOKEN_COSINE_DISTANCE {
					opType := p.current.Type
					p.nextToken()

					// Parse vector
					vec, err := p.parseLiteralValue()
					if err != nil {
						return nil, err
					}

					funcName := "L2_DISTANCE"
					if opType == TOKEN_COSINE_DISTANCE {
						funcName = "COSINE_DISTANCE"
					}

					// Convert to vector string for executor
					vecStr := ""
					if s, ok := vec.(string); ok {
						vecStr = s
					}

					stmt.VectorOrderBy = &VectorOrderBy{
						Function:    funcName,
						Column:      col,
						QueryVector: nil, // Will be parsed by executor if string
					}
					// Special case: if it's a string, we can parse it here
					if vecStr != "" {
						v, err := storage.ParseVector(vecStr)
						if err == nil {
							stmt.VectorOrderBy.QueryVector = v.Values
						}
					}
					break // Vector order by is usually standalone
				}

				orderBy := OrderByClause{
					Column:     col,
					Descending: false,
				}

				if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "DESC" {
					orderBy.Descending = true
					p.nextToken()
				} else if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ASC" {
					p.nextToken()
				}

				stmt.OrderBy = append(stmt.OrderBy, orderBy)

				if p.current.Type == TOKEN_COMMA {
					p.nextToken()
					continue
				}
				break
			}
		}
	}

	// Parse LIMIT
	if p.current.Type == TOKEN_LIMIT {
		p.nextToken()
		if p.current.Type != TOKEN_NUMBER {
			return nil, fmt.Errorf("expected number after LIMIT")
		}
		limit, _ := strconv.Atoi(p.current.Literal)
		stmt.Limit = limit
		p.nextToken()
	}

	// Parse OFFSET
	if p.current.Type == TOKEN_OFFSET {
		p.nextToken()
		if p.current.Type != TOKEN_NUMBER {
			return nil, fmt.Errorf("expected number after OFFSET")
		}
		offset, _ := strconv.Atoi(p.current.Literal)
		stmt.Offset = offset
		p.nextToken()
	}

	// Parse FOR UPDATE
	if p.current.Type == TOKEN_FOR || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "FOR") {
		p.nextToken()
		if p.current.Type == TOKEN_UPDATE || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "UPDATE") {
			stmt.ForUpdate = true
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected UPDATE after FOR")
		}
	}

	return stmt, nil
}

func (p *Parser) isAggregateFunction(t TokenType) bool {
	return t == TOKEN_COUNT || t == TOKEN_SUM || t == TOKEN_AVG || t == TOKEN_MAX || t == TOKEN_MIN
}

func (p *Parser) parseAggregate() (AggregateFunc, error) {
	agg := AggregateFunc{}

	// Get function name
	agg.Function = strings.ToUpper(p.current.Literal)
	p.nextToken()

	if p.current.Type != TOKEN_LPAREN {
		return agg, fmt.Errorf("expected ( after aggregate function")
	}
	p.nextToken()

	// Parse column or *
	switch p.current.Type {
	case TOKEN_ASTERISK:
		agg.Column = "*"
	case TOKEN_IDENT:
		agg.Column = p.current.Literal
	default:
		return agg, fmt.Errorf("expected column name or * in aggregate function")
	}
	p.nextToken()

	if p.current.Type != TOKEN_RPAREN {
		return agg, fmt.Errorf("expected ) after aggregate function")
	}
	p.nextToken()

	// Optional AS alias
	if p.current.Type == TOKEN_AS {
		p.nextToken()
		if p.current.Type != TOKEN_IDENT {
			return agg, fmt.Errorf("expected alias after AS")
		}
		agg.Alias = p.current.Literal
		p.nextToken()
	} else {
		// Default alias
		agg.Alias = agg.Function + "(" + agg.Column + ")"
	}

	return agg, nil
}

func (p *Parser) parseWhere() (*WhereClause, error) {
	where := &WhereClause{}

	// Handle EXISTS / NOT EXISTS subqueries
	if p.current.Type == TOKEN_EXISTS {
		p.nextToken()
		if p.current.Type != TOKEN_LPAREN {
			return nil, fmt.Errorf("expected ( after EXISTS")
		}
		p.nextToken()
		if p.current.Type != TOKEN_SELECT {
			return nil, fmt.Errorf("expected SELECT in EXISTS subquery")
		}
		subq, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if p.current.Type != TOKEN_RPAREN {
			return nil, fmt.Errorf("expected ) after EXISTS subquery")
		}
		p.nextToken()
		where.Operator = "EXISTS"
		where.Subquery = subq
		goto parseChain
	}

	if p.current.Type == TOKEN_NOT {
		// Peek for NOT EXISTS
		p.nextToken()
		if p.current.Type == TOKEN_EXISTS {
			p.nextToken()
			if p.current.Type != TOKEN_LPAREN {
				return nil, fmt.Errorf("expected ( after NOT EXISTS")
			}
			p.nextToken()
			if p.current.Type != TOKEN_SELECT {
				return nil, fmt.Errorf("expected SELECT in NOT EXISTS subquery")
			}
			subq, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if p.current.Type != TOKEN_RPAREN {
				return nil, fmt.Errorf("expected ) after NOT EXISTS subquery")
			}
			p.nextToken()
			where.Operator = "NOT EXISTS"
			where.Subquery = subq
			goto parseChain
		}
		// If NOT followed by something else, we put it back by treating as error
		return nil, fmt.Errorf("unexpected NOT in WHERE clause (expected EXISTS)")
	}

	// Parse LHS (may be column, table.column, function_call(), or CURRENT_USER)
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_CURRENT_USER && !p.isAggregateFunction(p.current.Type) {
		return nil, fmt.Errorf("expected identifier in WHERE, got %s", p.current.Literal)
	}

	{
		lhs := p.current.Literal
		if p.current.Type == TOKEN_CURRENT_USER {
			lhs = "current_user()"
		}
		p.nextToken()

		// Handle dots, function calls, arithmetic operators, and JSON operators
		for p.current.Type == TOKEN_DOT || p.current.Type == TOKEN_LPAREN || p.current.Type == TOKEN_PLUS || p.current.Type == TOKEN_MINUS || p.current.Type == TOKEN_ASTERISK || p.current.Type == TOKEN_SLASH || p.current.Type == TOKEN_JSON_ARROW || p.current.Type == TOKEN_JSON_TEXT_ARROW {
			if p.current.Type == TOKEN_DOT {
				p.nextToken()
				lhs += "." + p.current.Literal
				p.nextToken()
			} else if p.current.Type == TOKEN_PLUS || p.current.Type == TOKEN_MINUS || p.current.Type == TOKEN_ASTERISK || p.current.Type == TOKEN_SLASH {
				lhs += p.current.Literal
				p.nextToken()
				if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_NUMBER {
					lhs += p.current.Literal
					p.nextToken()
				}
			} else if p.current.Type == TOKEN_JSON_ARROW || p.current.Type == TOKEN_JSON_TEXT_ARROW {
				lhs += p.current.Literal
				p.nextToken()
				if p.current.Type == TOKEN_STRING || p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_NUMBER {
					lhs += "'" + p.current.Literal + "'"
					p.nextToken()
				}
			} else if p.current.Type == TOKEN_LPAREN {
				// Preserve function arguments
				lhs += "("
				p.nextToken()
				depth := 1
				for depth > 0 && p.current.Type != TOKEN_EOF {
					if p.current.Type == TOKEN_LPAREN {
						depth++
					} else if p.current.Type == TOKEN_RPAREN {
						depth--
					}
					if depth > 0 {
						lhs += p.current.Literal
						p.nextToken()
					}
				}
				lhs += ")"
				p.nextToken() // consume )
			}
		}

		where.Column = lhs

		// Parse operator
		switch p.current.Type {
		case TOKEN_EQUALS:
			where.Operator = "="
		case TOKEN_JSON_CONTAIN:
			where.Operator = "@>"
		case TOKEN_LT:
			where.Operator = "<"
		case TOKEN_GT:
			where.Operator = ">"
		case TOKEN_LE:
			where.Operator = "<="
		case TOKEN_GE:
			where.Operator = ">="
		case TOKEN_NE:
			where.Operator = "!="
		case TOKEN_LIKE:
			where.Operator = "LIKE"
		case TOKEN_ILIKE:
			where.Operator = "ILIKE"
		case TOKEN_NOT_MATCH:
			where.Operator = "!~"
		case TOKEN_NOT_MATCH_CI:
			where.Operator = "!~*"
		case TOKEN_IN:
			where.Operator = "IN"
		case TOKEN_BETWEEN:
			where.Operator = "BETWEEN"
		case TOKEN_IS:
			p.nextToken()
			if p.current.Type == TOKEN_NOT {
				p.nextToken()
				if p.current.Type != TOKEN_NULL {
					return nil, fmt.Errorf("expected NULL after IS NOT")
				}
				where.Operator = "IS NOT NULL"
				p.nextToken()
				where.Value = nil
				goto parseChain
			}
			if p.current.Type != TOKEN_NULL {
				return nil, fmt.Errorf("expected NULL after IS")
			}
			where.Operator = "IS NULL"
			p.nextToken()
			where.Value = nil
			goto parseChain
		case TOKEN_NOT:
			// NOT IN, NOT LIKE, NOT ILIKE, NOT BETWEEN
			p.nextToken()
			switch p.current.Type {
			case TOKEN_IN:
				where.Operator = "NOT IN"
			case TOKEN_LIKE:
				where.Operator = "NOT LIKE"
			case TOKEN_ILIKE:
				where.Operator = "NOT ILIKE"
			case TOKEN_BETWEEN:
				where.Operator = "NOT BETWEEN"
			default:
				return nil, fmt.Errorf("unexpected token after NOT: %s", p.current.Literal)
			}
		case TOKEN_OPERATOR:
			// Handle OPERATOR(schema.~)
			p.nextToken() // consume OPERATOR
			if p.current.Type == TOKEN_LPAREN {
				p.nextToken()
				op := ""
				for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
					op += p.current.Literal
					p.nextToken()
				}
				p.nextToken() // consume )
				where.Operator = op
				if strings.Contains(op, "~") {
					where.Operator = "~"
				}
				val, err := p.parseLiteralValue()
				if err != nil {
					return nil, err
				}
				where.Value = val
				goto parseChain
			}
		default:
			// If no operator, maybe it's a boolean function call
			where.Operator = "="
			where.Value = "true"
			goto parseChain
		}
		p.nextToken()

		// Parse value
		if where.Operator == "IN" || where.Operator == "NOT IN" {
			if p.current.Type != TOKEN_LPAREN {
				return nil, fmt.Errorf("expected ( after IN/NOT IN")
			}
			p.nextToken()
			// Check for subquery
			if p.current.Type == TOKEN_SELECT {
				subq, err := p.parseSelect()
				if err != nil {
					return nil, err
				}
				where.Subquery = subq
				if p.current.Type != TOKEN_RPAREN {
					return nil, fmt.Errorf("expected ) after IN subquery")
				}
				p.nextToken()
			} else {
				values := make([]interface{}, 0)
				for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
					switch p.current.Type {
					case TOKEN_STRING:
						values = append(values, p.current.Literal)
					case TOKEN_NUMBER:
						if strings.Contains(p.current.Literal, ".") {
							num, _ := strconv.ParseFloat(p.current.Literal, 64)
							values = append(values, num)
						} else {
							num, _ := strconv.Atoi(p.current.Literal)
							values = append(values, num)
						}
					case TOKEN_NULL:
						values = append(values, nil)
					}
					p.nextToken()
					if p.current.Type == TOKEN_COMMA {
						p.nextToken()
					}
				}
				where.Value = values
				p.nextToken() // consume )
			}
		} else if where.Operator == "BETWEEN" || where.Operator == "NOT BETWEEN" {
			// BETWEEN low AND high
			low, err := p.parseLiteralValue()
			if err != nil {
				return nil, err
			}
			// expect AND
			if p.current.Type != TOKEN_AND {
				return nil, fmt.Errorf("expected AND in BETWEEN expression")
			}
			p.nextToken()
			high, err := p.parseLiteralValue()
			if err != nil {
				return nil, err
			}
			where.Value = []interface{}{low, high}
		} else {
			val, err := p.parseLiteralValue()
			if err != nil {
				return nil, err
			}
			where.Value = val
		}
	}

parseChain:
	// Parse AND/OR recursively
	if p.current.Type == TOKEN_AND {
		p.nextToken()
		next, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		where.And = next
	} else if p.current.Type == TOKEN_OR {
		p.nextToken()
		next, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		where.Or = next
	}

	return where, nil
}

// parseWindowDef parses OVER (PARTITION BY ... ORDER BY ...)
func (p *Parser) parseWindowDef() (*WindowDef, error) {
	win := &WindowDef{}

	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected ( after OVER")
	}
	p.nextToken()

	// Optional PARTITION BY
	if p.current.Type == TOKEN_PARTITION {
		p.nextToken()
		if p.current.Type == TOKEN_BY {
			p.nextToken()
		}
		for p.current.Type == TOKEN_IDENT {
			win.PartitionBy = append(win.PartitionBy, p.current.Literal)
			p.nextToken()
			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
			} else {
				break
			}
		}
	}

	// Optional ORDER BY
	if p.current.Type == TOKEN_ORDER {
		p.nextToken()
		if p.current.Type == TOKEN_BY {
			p.nextToken()
		}
		for p.current.Type == TOKEN_IDENT {
			col := p.current.Literal
			p.nextToken()
			desc := false
			if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "DESC" {
				desc = true
				p.nextToken()
			} else if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ASC" {
				p.nextToken()
			}
			win.OrderBy = append(win.OrderBy, OrderByClause{Column: col, Descending: desc})
			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
			} else {
				break
			}
		}
	}

	if p.current.Type == TOKEN_RPAREN {
		p.nextToken()
	}

	return win, nil
}
func (p *Parser) parseLiteralValue() (interface{}, error) {
	var val interface{}
	switch p.current.Type {
	case TOKEN_NUMBER:
		if strings.Contains(p.current.Literal, ".") {
			num, _ := strconv.ParseFloat(p.current.Literal, 64)
			val = num
		} else {
			num, _ := strconv.Atoi(p.current.Literal)
			val = num
		}
	case TOKEN_STRING:
		val = p.current.Literal
	case TOKEN_IDENT, TOKEN_CURRENT_USER:
		// Treat as string literal for variable resolution later
		val = p.current.Literal
		if p.current.Type == TOKEN_CURRENT_USER {
			val = "current_user()"
			p.nextToken()
			if p.current.Type == TOKEN_LPAREN {
				p.nextToken()
				if p.current.Type == TOKEN_RPAREN {
					p.nextToken()
				} else {
					return nil, fmt.Errorf("expected ) after current_user(")
				}
			}
			return val, nil
		}
		p.nextToken()
		// Support dotted identifiers: table.col or schema.table.col
		for p.current.Type == TOKEN_DOT {
			p.nextToken() // consume dot
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected identifier after dot in literal value")
			}
			val = val.(string) + "." + p.current.Literal
			p.nextToken()
		}
		return val, nil
	case TOKEN_NULL:
		val = nil
	default:
		return nil, fmt.Errorf("unexpected value type: %s", p.current.Type)
	}
	p.nextToken()
	return val, nil
}
func (p *Parser) parseComment() (*CommentStmt, error) {
	stmt := &CommentStmt{}

	p.nextToken() // consume COMMENT

	if p.current.Type != TOKEN_ON {
		return nil, fmt.Errorf("expected ON after COMMENT")
	}
	p.nextToken()

	// Parse object type
	switch p.current.Type {
	case TOKEN_DATABASE:
		stmt.ObjectType = "DATABASE"
		p.nextToken()

		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected database name")
		}
		stmt.ObjectName = p.current.Literal
		p.nextToken()

	case TOKEN_TABLE:
		stmt.ObjectType = "TABLE"
		p.nextToken()

		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected table name")
		}
		stmt.ObjectName = p.current.Literal
		p.nextToken()

	case TOKEN_COLUMN:
		stmt.ObjectType = "COLUMN"
		p.nextToken()

		// Format: COMMENT ON COLUMN table.column IS 'comment'
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected table name")
		}
		stmt.TableName = p.current.Literal
		p.nextToken()

		// Expect a dot
		if p.current.Type != TOKEN_IDENT || p.current.Literal != "." {
			// Try without dot for now
			stmt.ObjectName = stmt.TableName
			stmt.TableName = ""
		} else {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected column name")
			}
			stmt.ObjectName = p.current.Literal
			p.nextToken()
		}

	default:
		return nil, fmt.Errorf("expected DATABASE, TABLE, or COLUMN after ON")
	}

	if p.current.Type != TOKEN_IS {
		return nil, fmt.Errorf("expected IS")
	}
	p.nextToken()

	if p.current.Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected comment string")
	}
	stmt.Comment = p.current.Literal
	p.nextToken()

	return stmt, nil
}

func (p *Parser) isVectorDistanceFunc(t TokenType) bool {
	return t == TOKEN_COSINE_DISTANCE || t == TOKEN_L2_DISTANCE
}

func (p *Parser) parseVectorOrderBy() (*VectorOrderBy, error) {
	vo := &VectorOrderBy{}

	// Get function name
	vo.Function = strings.ToUpper(p.current.Literal)
	p.nextToken()

	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected ( after %s", vo.Function)
	}
	p.nextToken()

	// Parse column name
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected column name")
	}
	vo.Column = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_COMMA {
		return nil, fmt.Errorf("expected , after column name")
	}
	p.nextToken()

	// Parse query vector [0.1, 0.2, 0.3]
	switch p.current.Type {
	case TOKEN_LBRACKET:
		values := make([]float32, 0)
		p.nextToken()

		for p.current.Type != TOKEN_RBRACKET && p.current.Type != TOKEN_EOF {
			if p.current.Type == TOKEN_NUMBER {
				var val float64
				if strings.Contains(p.current.Literal, ".") {
					val, _ = strconv.ParseFloat(p.current.Literal, 64)
				} else {
					intVal, _ := strconv.Atoi(p.current.Literal)
					val = float64(intVal)
				}
				values = append(values, float32(val))
			}
			p.nextToken()

			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
			}
		}

		if p.current.Type != TOKEN_RBRACKET {
			return nil, fmt.Errorf("expected ]")
		}
		p.nextToken()

		vo.QueryVector = values
	case TOKEN_STRING:
		// Parse from string '[0.1, 0.2, 0.3]'
		vec, err := storage.ParseVector(p.current.Literal)
		if err != nil {
			return nil, fmt.Errorf("invalid vector: %w", err)
		}
		vo.QueryVector = vec.Values
		p.nextToken()
	default:
		return nil, fmt.Errorf("expected vector array or string")
	}

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected ) after vector")
	}
	p.nextToken()

	// Check for DESC/ASC
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "DESC" {
		vo.Descending = true
		p.nextToken()
	}

	return vo, nil
}

func (p *Parser) parseCreateIndex() (*CreateIndexStmt, error) {
	stmt := &CreateIndexStmt{
		Options: make(map[string]int),
	}

	p.nextToken() // consume INDEX

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected index name")
	}
	stmt.IndexName = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_ON {
		return nil, fmt.Errorf("expected ON")
	}
	p.nextToken()

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.current.Literal
	p.nextToken()

	// Optional USING
	if p.current.Type == TOKEN_USING {
		p.nextToken()
		switch p.current.Type {
		case TOKEN_HNSW:
			stmt.IndexType = "HNSW"
		case TOKEN_BTREE:
			stmt.IndexType = "BTREE"
		default:
			return nil, fmt.Errorf("expected HNSW or BTREE")
		}
		p.nextToken()
	} else {
		stmt.IndexType = "BTREE" // default
	}

	// Parse column
	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected (")
	}
	p.nextToken()

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected column name")
	}
	stmt.ColumnName = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected )")
	}
	p.nextToken()

	// Parse WITH options for HNSW
	if p.current.Type == TOKEN_WITH {
		p.nextToken()
		if p.current.Type != TOKEN_LPAREN {
			return nil, fmt.Errorf("expected (")
		}
		p.nextToken()

		// Parse m=16, ef_construction=200
		for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected option name")
			}
			optionName := p.current.Literal
			p.nextToken()

			if p.current.Type != TOKEN_EQUALS {
				return nil, fmt.Errorf("expected =")
			}
			p.nextToken()

			if p.current.Type != TOKEN_NUMBER {
				return nil, fmt.Errorf("expected number")
			}
			value, _ := strconv.Atoi(p.current.Literal)
			stmt.Options[optionName] = value
			p.nextToken()

			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
			}
		}

		if p.current.Type != TOKEN_RPAREN {
			return nil, fmt.Errorf("expected )")
		}
		p.nextToken()
	}

	// Set defaults if not specified
	if stmt.IndexType == "HNSW" {
		if _, exists := stmt.Options["m"]; !exists {
			stmt.Options["m"] = 16
		}
		if _, exists := stmt.Options["ef_construction"]; !exists {
			stmt.Options["ef_construction"] = 200
		}
	}

	return stmt, nil
}

func (p *Parser) parseDropIndex() (*DropIndexStmt, error) {
	stmt := &DropIndexStmt{}

	p.nextToken() // consume INDEX

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected index name")
	}
	stmt.IndexName = p.current.Literal
	p.nextToken()

	return stmt, nil
}

func (p *Parser) parseJoin(joinType string) (JoinClause, error) {
	join := JoinClause{Type: joinType}

	if p.current.Type == TOKEN_LATERAL {
		join.Lateral = true
		p.nextToken()
	}

	if p.current.Type == TOKEN_LPAREN {
		// Subquery join
		p.nextToken()
		if p.current.Type != TOKEN_SELECT {
			return join, fmt.Errorf("expected SELECT inside join subquery")
		}
		subquery, err := p.parseSelect()
		if err != nil {
			return join, err
		}
		if p.current.Type != TOKEN_RPAREN {
			return join, fmt.Errorf("expected ) after join subquery")
		}
		p.nextToken() // consume )
		
		join.Subquery = subquery
	} else {
		// Parse table name
		if p.current.Type != TOKEN_IDENT {
			return join, fmt.Errorf("expected table name")
		}
		join.Table = p.current.Literal
		p.nextToken()

		// Handle schema-qualified table names (e.g. pg_catalog.pg_namespace)
		for p.current.Type == TOKEN_DOT {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return join, fmt.Errorf("expected table name after dot in JOIN")
			}
			join.Table += "." + p.current.Literal
			p.nextToken()
		}
	}

	// Optional alias
	switch p.current.Type {
	case TOKEN_AS:
		p.nextToken()
		if p.current.Type != TOKEN_IDENT {
			return join, fmt.Errorf("expected alias after AS")
		}
		join.Alias = p.current.Literal
		p.nextToken()
	case TOKEN_IDENT:
		keyword := strings.ToUpper(p.current.Literal)

		if keyword != "ON" && keyword != "WHERE" && keyword != "INNER" &&
			keyword != "LEFT" && keyword != "RIGHT" && keyword != "FULL" &&
			keyword != "CROSS" && keyword != "JOIN" && keyword != "ORDER" &&
			keyword != "GROUP" && keyword != "LIMIT" && keyword != "OFFSET" {
			join.Alias = p.current.Literal
			p.nextToken()
		}
	}

	// CROSS JOIN doesn't have ON condition
	if joinType == "CROSS" {
		return join, nil
	}

	// Parse ON condition
	if p.current.Type != TOKEN_ON {
		return join, fmt.Errorf("expected ON after JOIN table, got %s (literal: '%s')",
			p.current.Type, p.current.Literal)
	}
	p.nextToken()

	// Handle optional parentheses around ON condition: ON (a.x = b.y)
	hasParen := false
	if p.current.Type == TOKEN_LPAREN {
		hasParen = true
		p.nextToken()
	}

	// Parse join condition
	condition := &JoinCondition{}

	// Parse left side
	if p.current.Type != TOKEN_IDENT {
		return join, fmt.Errorf("expected table or column name")
	}
	leftPart := p.current.Literal
	p.nextToken()

	if p.current.Type == TOKEN_DOT {
		p.nextToken()
		if p.current.Type != TOKEN_IDENT {
			return join, fmt.Errorf("expected column name after dot")
		}
		condition.LeftTable = leftPart
		condition.LeftColumn = p.current.Literal
		p.nextToken()
	} else {
		condition.LeftColumn = leftPart
	}

	// Parse operator
	switch p.current.Type {
	case TOKEN_EQUALS:
		condition.Operator = "="
	case TOKEN_NE:
		condition.Operator = "!="
	case TOKEN_LT:
		condition.Operator = "<"
	case TOKEN_GT:
		condition.Operator = ">"
	case TOKEN_LE:
		condition.Operator = "<="
	case TOKEN_GE:
		condition.Operator = ">="
	default:
		return join, fmt.Errorf("expected comparison operator, got %s", p.current.Type)
	}
	p.nextToken()

	// Parse right side
	if p.current.Type != TOKEN_IDENT {
		return join, fmt.Errorf("expected table or column name")
	}
	rightPart := p.current.Literal
	p.nextToken()

	if p.current.Type == TOKEN_DOT {
		p.nextToken()
		if p.current.Type != TOKEN_IDENT {
			return join, fmt.Errorf("expected column name after dot")
		}
		condition.RightTable = rightPart
		condition.RightColumn = p.current.Literal
		p.nextToken()
	} else {
		condition.RightColumn = rightPart
	}

	join.Condition = condition

	// Consume closing paren if ON condition was parenthesized
	if hasParen && p.current.Type == TOKEN_RPAREN {
		p.nextToken()
	}

	return join, nil
}

func (p *Parser) parseCreateRole() (*CreateRoleStmt, error) {
	stmt := &CreateRoleStmt{CanLogin: true}
	p.nextToken() // consume ROLE
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_ALL {
		return nil, fmt.Errorf("expected role name")
	}
	stmt.RoleName = p.current.Literal
	p.nextToken()
	if p.current.Type == TOKEN_WITH {
		p.nextToken()
	}
	for {
		switch p.current.Type {
		case TOKEN_LOGIN:
			stmt.CanLogin = true
			p.nextToken()
		case TOKEN_SUPERUSER:
			stmt.IsSuperuser = true
			p.nextToken()
		case TOKEN_PASSWORD:
			p.nextToken()
			if p.current.Type != TOKEN_STRING {
				return nil, fmt.Errorf("expected password string")
			}
			stmt.Password = p.current.Literal
			p.nextToken()
		case TOKEN_SEMICOLON, TOKEN_EOF:
			return stmt, nil
		default:
			return stmt, nil
		}
	}
}

func (p *Parser) parseAlterRole() (*AlterRoleStmt, error) {
	stmt := &AlterRoleStmt{}
	p.nextToken() // consume ROLE
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_ALL {
		return nil, fmt.Errorf("expected role name")
	}
	stmt.RoleName = p.current.Literal
	p.nextToken()
	if p.current.Type == TOKEN_WITH {
		p.nextToken()
	}
	if p.current.Type == TOKEN_PASSWORD {
		p.nextToken()
		if p.current.Type != TOKEN_STRING {
			return nil, fmt.Errorf("expected password string")
		}
		stmt.Password = p.current.Literal
		p.nextToken()
	}
	return stmt, nil
}

func (p *Parser) parseAlterDefaultPrivileges() (Statement, error) {
	p.nextToken() // consume DEFAULT

	if p.current.Type != TOKEN_PRIVILEGES {
		return nil, fmt.Errorf("expected PRIVILEGES after ALTER DEFAULT")
	}
	p.nextToken() // consume PRIVILEGES

	stmt := &AlterDefaultPrivilegesStmt{}

	// Optional: FOR ROLE target_role
	if p.current.Type == TOKEN_FOR || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "FOR") {
		p.nextToken()
		if p.current.Type == TOKEN_ROLE || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ROLE") {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected role name after FOR ROLE")
			}
			stmt.ForRole = p.current.Literal
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected ROLE after FOR")
		}
	}

	// Optional: IN SCHEMA schema_name
	if p.current.Type == TOKEN_IN || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IN") {
		p.nextToken()
		if p.current.Type == TOKEN_SCHEMA || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "SCHEMA") {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected schema name after IN SCHEMA")
			}
			stmt.InSchema = p.current.Literal
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected SCHEMA after IN")
		}
	}

	// Expect GRANT or REVOKE
	if p.current.Type == TOKEN_GRANT {
		stmt.IsGrant = true
		p.nextToken() // consume GRANT
	} else if p.current.Type == TOKEN_REVOKE {
		stmt.IsGrant = false
		p.nextToken() // consume REVOKE
	} else {
		return nil, fmt.Errorf("expected GRANT or REVOKE in ALTER DEFAULT PRIVILEGES")
	}

	// Parse privileges: e.g. SELECT, INSERT, etc. or ALL [PRIVILEGES]
	if p.current.Type == TOKEN_ALL {
		p.nextToken()
		if p.current.Type == TOKEN_PRIVILEGES {
			p.nextToken()
		}
		stmt.Privileges = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}
	} else {
		for {
			if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_SELECT && p.current.Type != TOKEN_INSERT && p.current.Type != TOKEN_UPDATE && p.current.Type != TOKEN_DELETE && p.current.Type != TOKEN_TRUNCATE && p.current.Type != TOKEN_REFERENCES {
				break
			}
			stmt.Privileges = append(stmt.Privileges, strings.ToUpper(p.current.Literal))
			p.nextToken()
			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
			} else {
				break
			}
		}
	}

	// Expect ON TABLES
	if p.current.Type == TOKEN_ON || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ON") {
		p.nextToken()
		if p.current.Type != TOKEN_TABLES && (p.current.Type != TOKEN_IDENT || (strings.ToUpper(p.current.Literal) != "TABLES" && strings.ToUpper(p.current.Literal) != "SEQUENCES" && strings.ToUpper(p.current.Literal) != "TYPES" && strings.ToUpper(p.current.Literal) != "SCHEMAS")) {
			return nil, fmt.Errorf("expected TABLES, SEQUENCES, or TYPES after ON")
		}
		stmt.ObjectType = strings.ToUpper(p.current.Literal)
		p.nextToken()
	} else {
		return nil, fmt.Errorf("expected ON after privileges")
	}

	// Expect TO role (if Grant) or FROM role (if Revoke)
	if stmt.IsGrant {
		if p.current.Type != TOKEN_TO {
			return nil, fmt.Errorf("expected TO after ON <object_type>")
		}
		p.nextToken()
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected role name after TO")
		}
		stmt.ToFromRole = p.current.Literal
		p.nextToken()
	} else {
		if p.current.Type == TOKEN_FROM || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "FROM") {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected role name after FROM")
			}
			stmt.ToFromRole = p.current.Literal
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected FROM after ON <object_type>")
		}
	}

	return stmt, nil
}

func (p *Parser) parseGrant() (*GrantStmt, error) {
	stmt := &GrantStmt{}
	p.nextToken() // consume GRANT

	// Check if this is a role-to-role grant: GRANT role TO role
	if p.current.Type == TOKEN_IDENT && (p.peek.Type == TOKEN_TO || strings.ToUpper(p.peek.Literal) == "TO") {
		stmt.ObjectType = "ROLE"
		stmt.ObjectName = p.current.Literal
		p.nextToken() // consume role name
		if p.current.Type != TOKEN_TO && strings.ToUpper(p.current.Literal) != "TO" {
			return nil, fmt.Errorf("expected TO in role grant")
		}
		p.nextToken() // consume TO
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected target role name")
		}
		stmt.ToRole = p.current.Literal
		p.nextToken() // consume target role name

		// Parse optional WITH ADMIN OPTION / WITH GRANT OPTION
		if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "WITH" {
			p.nextToken()
			if p.current.Type == TOKEN_IDENT && (strings.ToUpper(p.current.Literal) == "ADMIN" || strings.ToUpper(p.current.Literal) == "GRANT") {
				p.nextToken()
				if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "OPTION" {
					p.nextToken()
					stmt.WithGrantOption = true
				}
			}
		}
		return stmt, nil
	}

	if p.current.Type == TOKEN_ALL {
		stmt.All = true
		p.nextToken()
		if p.current.Type == TOKEN_PRIVILEGES {
			p.nextToken()
		}
	} else {
		for {
			if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_SELECT && p.current.Type != TOKEN_INSERT && p.current.Type != TOKEN_UPDATE && p.current.Type != TOKEN_DELETE && p.current.Type != TOKEN_CREATE {
				break
			}
			stmt.Privileges = append(stmt.Privileges, strings.ToUpper(p.current.Literal))
			p.nextToken()

			// Check for column-level grant: SELECT (col1, col2)
			if p.current.Type == TOKEN_LPAREN {
				p.nextToken() // consume LPAREN
				for {
					if p.current.Type != TOKEN_IDENT {
						return nil, fmt.Errorf("expected column name inside parenthesis")
					}
					stmt.Columns = append(stmt.Columns, p.current.Literal)
					p.nextToken()
					if p.current.Type == TOKEN_COMMA {
						p.nextToken()
						continue
					}
					if p.current.Type == TOKEN_RPAREN {
						p.nextToken()
						break
					}
					return nil, fmt.Errorf("expected COMMA or RPAREN inside columns list")
				}
			}

			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
				continue
			}
			break
		}
	}
	if p.current.Type != TOKEN_ON {
		return nil, fmt.Errorf("expected ON after privileges")
	}
	p.nextToken()
	switch p.current.Type {
	case TOKEN_TABLE:
		stmt.ObjectType = "TABLE"
		p.nextToken()
	case TOKEN_DATABASE:
		stmt.ObjectType = "DATABASE"
		p.nextToken()
	case TOKEN_SCHEMA:
		stmt.ObjectType = "SCHEMA"
		p.nextToken()
	default:
		stmt.ObjectType = "TABLE"
	}
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected object name")
	}
	stmt.ObjectName = p.current.Literal
	p.nextToken()
	if p.current.Type != TOKEN_TO {
		return nil, fmt.Errorf("expected TO after object name")
	}
	p.nextToken()
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_ALL {
		return nil, fmt.Errorf("expected role name")
	}
	stmt.ToRole = p.current.Literal
	p.nextToken()

	// Parse optional WITH GRANT OPTION
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "WITH" {
		p.nextToken() // consume WITH
		if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "GRANT" {
			p.nextToken() // consume GRANT
			if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "OPTION" {
				p.nextToken() // consume OPTION
				stmt.WithGrantOption = true
			}
		}
	}

	return stmt, nil
}

func (p *Parser) parseRevoke() (*RevokeStmt, error) {
	stmt := &RevokeStmt{}
	p.nextToken() // consume REVOKE

	// Check if this is a role-to-role revoke: REVOKE role FROM role
	if p.current.Type == TOKEN_IDENT && (p.peek.Type == TOKEN_FROM || strings.ToUpper(p.peek.Literal) == "FROM") {
		stmt.ObjectType = "ROLE"
		stmt.ObjectName = p.current.Literal
		p.nextToken() // consume role name
		if p.current.Type != TOKEN_FROM && strings.ToUpper(p.current.Literal) != "FROM" {
			return nil, fmt.Errorf("expected FROM in role revoke")
		}
		p.nextToken() // consume FROM
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected target role name")
		}
		stmt.FromRole = p.current.Literal
		p.nextToken() // consume target role name
		return stmt, nil
	}

	// Check if we are revoking GRANT OPTION FOR: REVOKE GRANT OPTION FOR ...
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "GRANT" && p.peek.Type == TOKEN_IDENT && strings.ToUpper(p.peek.Literal) == "OPTION" {
		p.nextToken() // consume GRANT
		p.nextToken() // consume OPTION
		if p.current.Type == TOKEN_FOR || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "FOR") {
			p.nextToken() // consume FOR
		}
		stmt.GrantOptionOnly = true
	}

	if p.current.Type == TOKEN_ALL {
		stmt.All = true
		p.nextToken()
		if p.current.Type == TOKEN_PRIVILEGES {
			p.nextToken()
		}
	} else {
		for {
			if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_SELECT && p.current.Type != TOKEN_INSERT && p.current.Type != TOKEN_UPDATE && p.current.Type != TOKEN_DELETE && p.current.Type != TOKEN_CREATE {
				break
			}
			stmt.Privileges = append(stmt.Privileges, strings.ToUpper(p.current.Literal))
			p.nextToken()

			// Check for column-level revoke: SELECT (col1, col2)
			if p.current.Type == TOKEN_LPAREN {
				p.nextToken() // consume LPAREN
				for {
					if p.current.Type != TOKEN_IDENT {
						return nil, fmt.Errorf("expected column name inside parenthesis")
					}
					stmt.Columns = append(stmt.Columns, p.current.Literal)
					p.nextToken()
					if p.current.Type == TOKEN_COMMA {
						p.nextToken()
						continue
					}
					if p.current.Type == TOKEN_RPAREN {
						p.nextToken()
						break
					}
					return nil, fmt.Errorf("expected COMMA or RPAREN inside columns list")
				}
			}

			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
				continue
			}
			break
		}
	}
	if p.current.Type != TOKEN_ON {
		return nil, fmt.Errorf("expected ON after privileges")
	}
	p.nextToken()
	switch p.current.Type {
	case TOKEN_TABLE:
		stmt.ObjectType = "TABLE"
		p.nextToken()
	case TOKEN_DATABASE:
		stmt.ObjectType = "DATABASE"
		p.nextToken()
	case TOKEN_SCHEMA:
		stmt.ObjectType = "SCHEMA"
		p.nextToken()
	default:
		stmt.ObjectType = "TABLE"
	}
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected object name")
	}
	stmt.ObjectName = p.current.Literal
	p.nextToken()
	if p.current.Type != TOKEN_FROM {
		return nil, fmt.Errorf("expected FROM after object name")
	}
	p.nextToken()
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_ALL {
		return nil, fmt.Errorf("expected role name")
	}
	stmt.FromRole = p.current.Literal
	p.nextToken()
	return stmt, nil
}

func (p *Parser) parseCreatePolicy() (*CreatePolicyStmt, error) {
	stmt := &CreatePolicyStmt{}
	p.nextToken() // consume POLICY

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected policy name")
	}
	stmt.PolicyName = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_ON {
		return nil, fmt.Errorf("expected ON after policy name")
	}
	p.nextToken()

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.current.Literal
	p.nextToken()

	if p.current.Type == TOKEN_FOR {
		p.nextToken()
		// Action: SELECT, INSERT, etc.
		if p.current.Type == TOKEN_SELECT || p.current.Type == TOKEN_INSERT || p.current.Type == TOKEN_UPDATE || p.current.Type == TOKEN_DELETE {
			stmt.Action = strings.ToUpper(p.current.Literal)
			p.nextToken()
		} else {
			stmt.Action = "ALL"
		}
	} else {
		stmt.Action = "ALL"
	}

	if p.current.Type == TOKEN_TO {
		p.nextToken()
		if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_ALL {
			return nil, fmt.Errorf("expected role name or ALL")
		}
		stmt.Role = strings.ToLower(p.current.Literal)
		p.nextToken()
	} else {
		stmt.Role = "all"
	}

	if p.current.Type == TOKEN_USING {
		p.nextToken()
		if p.current.Type != TOKEN_LPAREN {
			return nil, fmt.Errorf("expected ( after USING")
		}
		p.nextToken()

		// Parse expression as WhereClause
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Using = where

		if p.current.Type != TOKEN_RPAREN {
			return nil, fmt.Errorf("expected ) after USING expression")
		}
		p.nextToken()
	}

	return stmt, nil
}

func (p *Parser) parseSet() (Statement, error) {
	p.nextToken() // consume SET

	isLocal := false
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "LOCAL" {
		isLocal = true
		p.nextToken()
	}

	// Check SET ROLE
	if p.current.Type == TOKEN_ROLE || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ROLE") {
		p.nextToken()
		if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_STRING {
			return nil, fmt.Errorf("expected role name after SET ROLE")
		}
		roleName := p.current.Literal
		p.nextToken()
		return &SetRoleStmt{Role: roleName}, nil
	}

	// Check SET TRANSACTION ISOLATION LEVEL ...
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "TRANSACTION" {
		p.nextToken() // consume TRANSACTION
		if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ISOLATION" {
			p.nextToken() // consume ISOLATION
			if p.current.Type == TOKEN_LEVEL || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "LEVEL") {
				p.nextToken() // consume LEVEL
				level := ""
				for p.current.Type == TOKEN_IDENT {
					if level != "" {
						level += " "
					}
					level += strings.ToUpper(p.current.Literal)
					p.nextToken()
				}
				return &SetTransactionIsolationStmt{Level: level, IsLocal: isLocal}, nil
			}
		}
	}

	// Check SET SESSION AUTHORIZATION or SET SESSION CHARACTERISTICS AS TRANSACTION
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "SESSION" {
		if p.peek.Type == TOKEN_IDENT && strings.ToUpper(p.peek.Literal) == "CHARACTERISTICS" {
			p.nextToken() // consume SESSION
			p.nextToken() // consume CHARACTERISTICS
			if p.current.Type == TOKEN_AS || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "AS") {
				p.nextToken() // consume AS
				if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "TRANSACTION" {
					p.nextToken() // consume TRANSACTION
					if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "ISOLATION" {
						p.nextToken() // consume ISOLATION
						if p.current.Type == TOKEN_LEVEL || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "LEVEL") {
							p.nextToken() // consume LEVEL
							level := ""
							for p.current.Type == TOKEN_IDENT {
								if level != "" {
									level += " "
								}
								level += strings.ToUpper(p.current.Literal)
								p.nextToken()
							}
							return &SetTransactionIsolationStmt{Level: level, IsLocal: false}, nil
						}
					}
				}
			}
			return nil, fmt.Errorf("expected AS TRANSACTION after SESSION CHARACTERISTICS")
		}

		p.nextToken()
		if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "AUTHORIZATION" {
			p.nextToken()
			if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_STRING {
				return nil, fmt.Errorf("expected user name after SET SESSION AUTHORIZATION")
			}
			userName := p.current.Literal
			p.nextToken()
			return &SetSessionAuthorizationStmt{User: userName}, nil
		}
		return nil, fmt.Errorf("expected AUTHORIZATION after SET SESSION")
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected variable name after SET")
	}
	name := p.current.Literal
	p.nextToken()

	if p.current.Type == TOKEN_TO || p.current.Type == TOKEN_EQUALS {
		p.nextToken()
	}

	var value string
	for {
		if p.current.Type == TOKEN_STRING || p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_NUMBER {
			if value != "" {
				value += ", "
			}
			value += p.current.Literal
			p.nextToken()
		} else {
			break
		}
		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return &SetStmt{Name: name, Value: value, IsLocal: isLocal}, nil
}

func (p *Parser) parseCreateView(orReplace bool) (*CreateViewStmt, error) {
	p.nextToken() // consume VIEW
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected view name after CREATE VIEW")
	}
	name := p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_AS {
		return nil, fmt.Errorf("expected AS after view name")
	}
	p.nextToken() // consume AS

	query, err := p.parseSelectOrCompound()
	if err != nil {
		return nil, err
	}

	return &CreateViewStmt{
		ViewName:  name,
		Query:     query.(*SelectStmt),
		OrReplace: orReplace,
	}, nil
}

func (p *Parser) parseCreateSchema() (*CreateSchemaStmt, error) {
	p.nextToken() // consume SCHEMA

	ifNotExists := false
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IF" {
		p.nextToken()
		if p.current.Type == TOKEN_NOT || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "NOT") {
			p.nextToken()
			if p.current.Type == TOKEN_EXISTS || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "EXISTS") {
				ifNotExists = true
				p.nextToken()
			} else {
				return nil, fmt.Errorf("expected EXISTS after IF NOT")
			}
		} else {
			return nil, fmt.Errorf("expected NOT after IF")
		}
	} else if p.current.Type == TOKEN_EXISTS {
		ifNotExists = true
		p.nextToken()
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected schema name")
	}
	name := p.current.Literal
	p.nextToken()

	return &CreateSchemaStmt{SchemaName: name, IfNotExists: ifNotExists}, nil
}

func (p *Parser) parseCreateSequence() (*CreateSequenceStmt, error) {
	p.nextToken() // consume SEQUENCE

	ifNotExists := false
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IF" {
		p.nextToken()
		if p.current.Type == TOKEN_NOT || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "NOT") {
			p.nextToken()
			if p.current.Type == TOKEN_EXISTS || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "EXISTS") {
				ifNotExists = true
				p.nextToken()
			} else {
				return nil, fmt.Errorf("expected EXISTS after IF NOT")
			}
		} else {
			return nil, fmt.Errorf("expected NOT after IF")
		}
	} else if p.current.Type == TOKEN_EXISTS {
		ifNotExists = true
		p.nextToken()
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected sequence name")
	}
	name := p.current.Literal
	p.nextToken()

	startVal := 1
	incVal := 1

	for {
		if p.current.Type == TOKEN_EOF {
			break
		}
		keyword := strings.ToUpper(p.current.Literal)
		if keyword == "START" {
			p.nextToken()
			if strings.ToUpper(p.current.Literal) == "WITH" {
				p.nextToken()
			}
			if p.current.Type != TOKEN_NUMBER {
				return nil, fmt.Errorf("expected sequence start number")
			}
			startVal, _ = strconv.Atoi(p.current.Literal)
			p.nextToken()
		} else if keyword == "INCREMENT" {
			p.nextToken()
			if strings.ToUpper(p.current.Literal) == "BY" {
				p.nextToken()
			}
			if p.current.Type != TOKEN_NUMBER {
				return nil, fmt.Errorf("expected sequence increment number")
			}
			incVal, _ = strconv.Atoi(p.current.Literal)
			p.nextToken()
		} else {
			break
		}
	}

	return &CreateSequenceStmt{
		SequenceName: name,
		Start:        startVal,
		Increment:    incVal,
		IfNotExists:  ifNotExists,
	}, nil
}

func (p *Parser) parseCreateType() (*CreateTypeStmt, error) {
	p.nextToken() // consume TYPE

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected type name after CREATE TYPE")
	}
	name := p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_AS {
		return nil, fmt.Errorf("expected AS after type name")
	}
	p.nextToken()

	if p.current.Type != TOKEN_ENUM {
		return nil, fmt.Errorf("expected ENUM after AS")
	}
	p.nextToken()

	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected ( after ENUM")
	}
	p.nextToken()

	var values []string
	for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
		if p.current.Type != TOKEN_STRING {
			return nil, fmt.Errorf("expected string literal in ENUM values")
		}
		values = append(values, p.current.Literal)
		p.nextToken()
		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
		}
	}

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected )")
	}
	p.nextToken()

	return &CreateTypeStmt{TypeName: name, Values: values}, nil
}

func (p *Parser) parseCreateMaterializedView() (*CreateMaterializedViewStmt, error) {
	p.nextToken() // consume VIEW

	ifNotExists := false
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IF" {
		p.nextToken()
		if p.current.Type == TOKEN_NOT || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "NOT") {
			p.nextToken()
			if p.current.Type == TOKEN_EXISTS || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "EXISTS") {
				ifNotExists = true
				p.nextToken()
			} else {
				return nil, fmt.Errorf("expected EXISTS after IF NOT")
			}
		} else {
			return nil, fmt.Errorf("expected NOT after IF")
		}
	} else if p.current.Type == TOKEN_EXISTS {
		ifNotExists = true
		p.nextToken()
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected view name after CREATE MATERIALIZED VIEW")
	}
	name := p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_AS {
		return nil, fmt.Errorf("expected AS after view name")
	}
	p.nextToken()

	query, err := p.parseSelectOrCompound()
	if err != nil {
		return nil, err
	}

	return &CreateMaterializedViewStmt{
		ViewName:    name,
		Query:       query.(*SelectStmt),
		IfNotExists: ifNotExists,
	}, nil
}

func (p *Parser) parseRefresh() (Statement, error) {
	p.nextToken() // consume REFRESH
	if p.current.Type != TOKEN_MATERIALIZED {
		return nil, fmt.Errorf("expected MATERIALIZED after REFRESH")
	}
	p.nextToken()
	if p.current.Type != TOKEN_VIEW {
		return nil, fmt.Errorf("expected VIEW after REFRESH MATERIALIZED")
	}
	p.nextToken()
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected view name to refresh")
	}
	name := p.current.Literal
	p.nextToken()
	return &RefreshMaterializedViewStmt{ViewName: name}, nil
}

func (p *Parser) parseMerge() (*MergeStmt, error) {
	p.nextToken() // consume MERGE

	if p.current.Type != TOKEN_INTO {
		return nil, fmt.Errorf("expected INTO after MERGE")
	}
	p.nextToken()

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected target table name")
	}
	targetTable := p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_USING && (p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "USING") {
		return nil, fmt.Errorf("expected USING after target table")
	}
	p.nextToken()

	var sourceTable string
	var sourceQuery *SelectStmt

	if p.current.Type == TOKEN_LPAREN {
		p.nextToken() // consume (
		subquery, err := p.parseSelectOrCompound()
		if err != nil {
			return nil, err
		}
		sourceQuery = subquery.(*SelectStmt)

		if p.current.Type != TOKEN_RPAREN {
			return nil, fmt.Errorf("expected ) after subquery in MERGE USING")
		}
		p.nextToken()

		if p.current.Type == TOKEN_AS {
			p.nextToken()
		}
		if p.current.Type == TOKEN_IDENT {
			sourceTable = p.current.Literal
			p.nextToken()
		}
	} else {
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected source table name")
		}
		sourceTable = p.current.Literal
		p.nextToken()
	}

	if p.current.Type != TOKEN_ON && (p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "ON") {
		return nil, fmt.Errorf("expected ON condition in MERGE")
	}
	p.nextToken()

	if p.current.Type == TOKEN_LPAREN {
		p.nextToken()
	}
	onCondition, err := p.parseWhere()
	if err != nil {
		return nil, err
	}
	if p.current.Type == TOKEN_RPAREN {
		p.nextToken()
	}

	stmt := &MergeStmt{
		TargetTable: targetTable,
		SourceTable: sourceTable,
		SourceQuery: sourceQuery,
		OnCondition: onCondition,
	}

	for {
		if p.current.Type != TOKEN_WHEN && (p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "WHEN") {
			break
		}
		p.nextToken() // consume WHEN

		isNotMatched := false
		if p.current.Type == TOKEN_NOT || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "NOT") {
			p.nextToken()
			isNotMatched = true
		}

		if p.current.Type != TOKEN_MATCHED && (p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "MATCHED") {
			return nil, fmt.Errorf("expected MATCHED after WHEN [NOT]")
		}
		p.nextToken()

		if p.current.Type != TOKEN_THEN && (p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "THEN") {
			return nil, fmt.Errorf("expected THEN after WHEN [NOT] MATCHED")
		}
		p.nextToken()

		action := strings.ToUpper(p.current.Literal)
		p.nextToken() // consume UPDATE / INSERT / DELETE / DO

		mergeAction := MergeAction{Action: action}

		if action == "UPDATE" {
			if p.current.Type != TOKEN_SET {
				return nil, fmt.Errorf("expected SET after UPDATE in MERGE action")
			}
			p.nextToken()

			mergeAction.Updates = make(map[string]interface{})
			for {
				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected column name")
				}
				colName := p.current.Literal
				p.nextToken()

				if p.current.Type != TOKEN_EQUALS {
					return nil, fmt.Errorf("expected =")
				}
				p.nextToken()

				var val interface{}
				if p.current.Type == TOKEN_NUMBER {
					if strings.Contains(p.current.Literal, ".") {
						num, _ := strconv.ParseFloat(p.current.Literal, 64)
						val = num
					} else {
						num, _ := strconv.Atoi(p.current.Literal)
						val = num
					}
					p.nextToken()
				} else if p.current.Type == TOKEN_STRING {
					val = p.current.Literal
					p.nextToken()
				} else if p.current.Type == TOKEN_IDENT {
					identVal := p.current.Literal
					p.nextToken()
					if p.current.Type == TOKEN_DOT {
						p.nextToken()
						if p.current.Type != TOKEN_IDENT {
							return nil, fmt.Errorf("expected column after .")
						}
						identVal += "." + p.current.Literal
						p.nextToken()
					}
					val = identVal
				} else {
					return nil, fmt.Errorf("unexpected value in MERGE UPDATE action")
				}

				mergeAction.Updates[colName] = val

				if p.current.Type == TOKEN_COMMA {
					p.nextToken()
					continue
				}
				break
			}
		} else if action == "INSERT" {
			if p.current.Type == TOKEN_LPAREN {
				p.nextToken()
				for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
					if p.current.Type != TOKEN_IDENT {
						return nil, fmt.Errorf("expected column name")
					}
					mergeAction.Columns = append(mergeAction.Columns, p.current.Literal)
					p.nextToken()
					if p.current.Type == TOKEN_COMMA {
						p.nextToken()
					}
				}
				if p.current.Type != TOKEN_RPAREN {
					return nil, fmt.Errorf("expected )")
				}
				p.nextToken()
			}

			if p.current.Type != TOKEN_VALUES {
				return nil, fmt.Errorf("expected VALUES in MERGE INSERT action")
			}
			p.nextToken()

			if p.current.Type != TOKEN_LPAREN {
				return nil, fmt.Errorf("expected ( after VALUES")
			}
			p.nextToken()

			for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
				var val interface{}
				if p.current.Type == TOKEN_NUMBER {
					if strings.Contains(p.current.Literal, ".") {
						num, _ := strconv.ParseFloat(p.current.Literal, 64)
						val = num
					} else {
						num, _ := strconv.Atoi(p.current.Literal)
						val = num
					}
					p.nextToken()
				} else if p.current.Type == TOKEN_STRING {
					val = p.current.Literal
					p.nextToken()
				} else if p.current.Type == TOKEN_IDENT {
					identVal := p.current.Literal
					p.nextToken()
					if p.current.Type == TOKEN_DOT {
						p.nextToken()
						if p.current.Type != TOKEN_IDENT {
							return nil, fmt.Errorf("expected column after .")
						}
						identVal += "." + p.current.Literal
						p.nextToken()
					}
					val = identVal
				} else {
					return nil, fmt.Errorf("unexpected value in MERGE INSERT action")
				}
				mergeAction.Values = append(mergeAction.Values, val)
				if p.current.Type == TOKEN_COMMA {
					p.nextToken()
				}
			}
			if p.current.Type != TOKEN_RPAREN {
				return nil, fmt.Errorf("expected )")
			}
			p.nextToken()
		} else if action == "DO" {
			if p.current.Type != TOKEN_NOTHING && (p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "NOTHING") {
				return nil, fmt.Errorf("expected NOTHING after DO in MERGE action")
			}
			p.nextToken()
			mergeAction.Action = "DO NOTHING"
		}

		if isNotMatched {
			stmt.WhenNotMatched = append(stmt.WhenNotMatched, mergeAction)
		} else {
			stmt.WhenMatched = append(stmt.WhenMatched, mergeAction)
		}
	}

	return stmt, nil
}

func (p *Parser) parseSavepoint() (*SavepointStmt, error) {
	p.nextToken() // consume SAVEPOINT
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected savepoint name")
	}
	name := p.current.Literal
	p.nextToken()
	return &SavepointStmt{Command: "SAVEPOINT", Name: name}, nil
}

func (p *Parser) parseReset() (*ResetStmt, error) {
	p.nextToken() // consume RESET
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected variable name after RESET")
	}
	name := p.current.Literal
	p.nextToken()
	return &ResetStmt{Name: name}, nil
}

func (p *Parser) parseLock() (*LockTableStmt, error) {
	p.nextToken() // consume LOCK
	if p.current.Type == TOKEN_TABLE || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "TABLE") {
		p.nextToken()
	}
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name to lock")
	}
	tableName := p.current.Literal
	p.nextToken()

	mode := "EXCLUSIVE"
	if p.current.Type == TOKEN_IN || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IN") {
		p.nextToken()
		var modeParts []string
		for p.current.Type == TOKEN_IDENT {
			modeParts = append(modeParts, strings.ToUpper(p.current.Literal))
			p.nextToken()
		}
		if len(modeParts) > 0 {
			mode = strings.Join(modeParts, " ")
		}
	}
	return &LockTableStmt{TableName: tableName, Mode: mode}, nil
}

func (p *Parser) parseDeclareCursor() (*DeclareCursorStmt, error) {
	p.nextToken() // consume DECLARE
	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected cursor name")
	}
	cursorName := p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_CURSOR && (p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "CURSOR") {
		return nil, fmt.Errorf("expected CURSOR after cursor name")
	}
	p.nextToken()

	if p.current.Type != TOKEN_FOR && (p.current.Type != TOKEN_IDENT || strings.ToUpper(p.current.Literal) != "FOR") {
		return nil, fmt.Errorf("expected FOR after CURSOR")
	}
	p.nextToken()

	query, err := p.parseSelectOrCompound()
	if err != nil {
		return nil, err
	}
	selectQuery, ok := query.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("CURSOR declaration only supports SELECT queries")
	}
	return &DeclareCursorStmt{Name: cursorName, Query: selectQuery}, nil
}

func (p *Parser) parseFetchCursor() (*FetchCursorStmt, error) {
	p.nextToken() // consume FETCH

	count := 1
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "NEXT" {
		p.nextToken()
	} else if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "FORWARD" {
		p.nextToken()
		if p.current.Type == TOKEN_NUMBER {
			count, _ = strconv.Atoi(p.current.Literal)
			p.nextToken()
		}
	}

	if p.current.Type == TOKEN_FROM || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "FROM") {
		p.nextToken()
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected cursor name in FETCH")
	}
	cursorName := p.current.Literal
	p.nextToken()

	return &FetchCursorStmt{Name: cursorName, Count: count}, nil
}

func (p *Parser) parseMoveCursor() (*MoveCursorStmt, error) {
	p.nextToken() // consume MOVE

	count := 1
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "FORWARD" {
		p.nextToken()
		if p.current.Type == TOKEN_NUMBER {
			count, _ = strconv.Atoi(p.current.Literal)
			p.nextToken()
		}
	}

	if p.current.Type == TOKEN_IN || (p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Literal) == "IN") {
		p.nextToken()
	}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected cursor name in MOVE")
	}
	cursorName := p.current.Literal
	p.nextToken()

	return &MoveCursorStmt{Name: cursorName, Count: count}, nil
}

func (p *Parser) parseCloseCursor() (*CloseCursorStmt, error) {
	p.nextToken() // consume CLOSE

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected cursor name in CLOSE")
	}
	cursorName := p.current.Literal
	p.nextToken()

	return &CloseCursorStmt{Name: cursorName}, nil
}

