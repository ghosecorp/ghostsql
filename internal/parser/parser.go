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
		stmt, err = p.parseSelect()
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
	case TOKEN_BEGIN:
		p.nextToken()
		stmt = &TransactionStmt{Command: "BEGIN"}
	case TOKEN_COMMIT:
		p.nextToken()
		stmt = &TransactionStmt{Command: "COMMIT"}
	case TOKEN_ROLLBACK:
		p.nextToken()
		stmt = &TransactionStmt{Command: "ROLLBACK"}
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

func (p *Parser) parseCreate() (Statement, error) {
	p.nextToken() // consume CREATE

	switch p.current.Type {
	case TOKEN_DATABASE:
		return p.parseCreateDatabase()
	case TOKEN_TABLE:
		return p.parseCreateTable()
	case TOKEN_INDEX:
		return p.parseCreateIndex()
	case TOKEN_ROLE:
		return p.parseCreateRole()
	case TOKEN_POLICY:
		return p.parseCreatePolicy()
	default:
		return nil, fmt.Errorf("expected DATABASE, TABLE, INDEX, ROLE, or POLICY after CREATE")
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
			// Check if it's a float or int
			if strings.Contains(p.current.Literal, ".") {
				num, _ := strconv.ParseFloat(p.current.Literal, 64)
				value = num
			} else {
				num, _ := strconv.Atoi(p.current.Literal)
				value = num
			}
		case TOKEN_STRING:
			value = p.current.Literal
		default:
			return nil, fmt.Errorf("expected value")
		}
		stmt.Updates[colName] = value
		p.nextToken()

		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
			continue
		}
		break
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

	// Parse WHERE clause
	if p.current.Type == TOKEN_WHERE {
		p.nextToken()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseDrop() (Statement, error) {
	p.nextToken() // consume DROP

	switch p.current.Type {
	case TOKEN_TABLE:
		return p.parseDropTable()
	case TOKEN_DATABASE:
		return p.parseDropDatabase()
	case TOKEN_INDEX:
		return p.parseDropIndex()
	case TOKEN_ROLE:
		return p.parseDropRole()
	default:
		return nil, fmt.Errorf("expected TABLE, DATABASE, INDEX, or ROLE after DROP")
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
	default:
		return nil, fmt.Errorf("expected TABLE or ROLE after ALTER")
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
		if p.current.Type == TOKEN_COLUMN {
			p.nextToken()
		}
		stmt.Action = "ADD_COLUMN"
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Column = &col
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
		return nil, fmt.Errorf("expected ADD, ENABLE, or DISABLE after ALTER TABLE")
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

func (p *Parser) parseShow() (*ShowStmt, error) {
	stmt := &ShowStmt{}

	p.nextToken() // consume SHOW

	switch p.current.Type {
	case TOKEN_DATABASES:
		stmt.ShowType = "DATABASES"
		p.nextToken()

	case TOKEN_TABLES:
		stmt.ShowType = "TABLES"
		p.nextToken()

	case TOKEN_COLUMNS:
		stmt.ShowType = "COLUMNS"
		p.nextToken()

		if p.current.Type != TOKEN_FROM {
			return nil, fmt.Errorf("expected FROM after SHOW COLUMNS")
		}
		p.nextToken()

		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected table name")
		}
		stmt.TableName = p.current.Literal
		p.nextToken()

	default:
		return nil, fmt.Errorf("expected DATABASES, TABLES, or COLUMNS after SHOW")
	}

	return stmt, nil
}

func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}

	p.nextToken() // consume TABLE

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

	if p.current.Type != TOKEN_IDENT {
		return col, fmt.Errorf("expected column type after %s, got %s (literal: '%s')", col.Name, p.current.Type, p.current.Literal)
	}

	typeName := strings.ToUpper(p.current.Literal)

	switch typeName {
	case "INT":
		col.Type = storage.TypeInt
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

	if p.current.Type != TOKEN_VALUES {
		return nil, fmt.Errorf("expected VALUES")
	}
	p.nextToken()

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

	return stmt, nil
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	p.nextToken() // consume SELECT

	// Parse columns and aggregates
	for p.current.Type != TOKEN_FROM && p.current.Type != TOKEN_EOF {
		// Check for aggregate functions
		if p.isAggregateFunction(p.current.Type) {
			agg, err := p.parseAggregate()
			if err != nil {
				return nil, err
			}
			stmt.Aggregates = append(stmt.Aggregates, agg)
		} else if p.current.Type == TOKEN_ASTERISK {
			stmt.Columns = append(stmt.Columns, "*")
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: "*"})
			p.nextToken()
		} else if p.current.Type == TOKEN_CASE {
			// Skip CASE ... END, capture alias
			p.nextToken() // consume CASE
			depth := 1
			for depth > 0 && p.current.Type != TOKEN_EOF {
				if p.current.Type == TOKEN_CASE {
					depth++
				} else if p.current.Type == TOKEN_END {
					depth--
				}
				p.nextToken()
			}
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
			stmt.Columns = append(stmt.Columns, "computed_column")
			stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: "computed_column", Alias: alias})
		} else if p.current.Type == TOKEN_IDENT {
			// Parse column name (may be table.column or function_call())
			name := p.current.Literal
			p.nextToken()

			// Check for function call or dot
			if p.current.Type == TOKEN_LPAREN {
				// Function call — skip args
				p.nextToken() // consume (
				depth := 1
				for depth > 0 && p.current.Type != TOKEN_EOF {
					if p.current.Type == TOKEN_LPAREN {
						depth++
					} else if p.current.Type == TOKEN_RPAREN {
						depth--
					}
					p.nextToken()
				}
				alias := ""
				if p.current.Type == TOKEN_AS {
					p.nextToken()
					if p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_STRING {
						alias = p.current.Literal
						p.nextToken()
					}
				}
				stmt.Columns = append(stmt.Columns, name)
				stmt.SelectColumns = append(stmt.SelectColumns, SelectColumn{Expression: name + "()", Alias: alias})
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
			} else {
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
		} else {
			// Other literal (e.g. false, true)
			expr := p.current.Literal
			p.nextToken()

			// Handle potential cast: false::pg_catalog.bool
			for p.current.Type == TOKEN_CAST {
				p.nextToken() // consume ::
				// Consume type name (may be dotted)
				for p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_DOT {
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

	if p.current.Type != TOKEN_FROM {
		return nil, fmt.Errorf("expected FROM")
	}
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
	} else if p.current.Type == TOKEN_IDENT &&
		p.current.Type != TOKEN_INNER &&
		p.current.Type != TOKEN_LEFT &&
		p.current.Type != TOKEN_RIGHT &&
		p.current.Type != TOKEN_FULL &&
		p.current.Type != TOKEN_CROSS &&
		p.current.Type != TOKEN_WHERE &&
		p.current.Type != TOKEN_ORDER &&
		p.current.Type != TOKEN_GROUP &&
		p.current.Type != TOKEN_LIMIT {
		// Implicit alias (no AS keyword)
		stmt.TableAlias = p.current.Literal
		p.nextToken()
	}

	// Parse optional table alias: FROM documents d or FROM documents AS d
	switch p.current.Type {
	case TOKEN_AS:
		p.nextToken()
		if p.current.Type != TOKEN_IDENT {
			return nil, fmt.Errorf("expected alias after AS")
		}
		stmt.TableAlias = p.current.Literal
		p.nextToken()
	case TOKEN_IDENT:
		// Check if this is an implicit alias (not a keyword)
		keyword := strings.ToUpper(p.current.Literal)
		if keyword != "INNER" && keyword != "LEFT" && keyword != "RIGHT" &&
			keyword != "FULL" && keyword != "CROSS" && keyword != "JOIN" &&
			keyword != "WHERE" && keyword != "GROUP" && keyword != "ORDER" &&
			keyword != "LIMIT" && keyword != "OFFSET" && keyword != "HAVING" {
			stmt.TableAlias = p.current.Literal
			p.nextToken()
		}
	}

	// Parse JOINs
	for {
		joinType := ""

		// Check for join type keywords
		if p.current.Type == TOKEN_INNER || p.current.Type == TOKEN_LEFT ||
			p.current.Type == TOKEN_RIGHT || p.current.Type == TOKEN_FULL ||
			p.current.Type == TOKEN_CROSS {
			joinType = strings.ToUpper(p.current.Literal)
			p.nextToken()

			// Handle FULL OUTER JOIN
			if joinType == "FULL" && p.current.Type == TOKEN_OUTER {
				p.nextToken()
			}
			// Handle LEFT OUTER JOIN, RIGHT OUTER JOIN
			if (joinType == "LEFT" || joinType == "RIGHT") && p.current.Type == TOKEN_OUTER {
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
		agg.Alias = strings.ToLower(agg.Function) + "_" + agg.Column
	}

	return agg, nil
}

func (p *Parser) parseWhere() (*WhereClause, error) {
	where := &WhereClause{}

	// Parse LHS (may be column, table.column, function_call(), or CURRENT_USER)
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_CURRENT_USER {
		return nil, fmt.Errorf("expected identifier in WHERE, got %s", p.current.Type)
	}

	lhs := p.current.Literal
	if p.current.Type == TOKEN_CURRENT_USER {
		lhs = "current_user()"
	}
	p.nextToken()

	// Handle dots and function calls
	for p.current.Type == TOKEN_DOT || p.current.Type == TOKEN_LPAREN {
		if p.current.Type == TOKEN_DOT {
			p.nextToken()
			lhs += "." + p.current.Literal
			p.nextToken()
		} else if p.current.Type == TOKEN_LPAREN {
			// Skip function arguments for now
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
			// If LHS was current_user, it already has parens if we want them normalized
			if !strings.HasSuffix(lhs, "()") {
				lhs += "()"
			}
		}
	}

	where.Column = lhs

	// Parse operator
	switch p.current.Type {
	case TOKEN_EQUALS:
		where.Operator = "="
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
	case TOKEN_NOT_MATCH:
		where.Operator = "!~"
	case TOKEN_NOT_MATCH_CI:
		where.Operator = "!~*"
	case TOKEN_IN:
		where.Operator = "IN"
	case TOKEN_OPERATOR:
		// Handle OPERATOR(schema.~)
		p.nextToken() // consume OPERATOR
		if p.current.Type == TOKEN_LPAREN {
			p.nextToken()
			// Consume until )
			op := ""
			for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
				op += p.current.Literal
				p.nextToken()
			}
			p.nextToken() // consume )
			where.Operator = op
			if strings.Contains(op, "~") {
				where.Operator = "~" // Map to regex match
			}

			// Parse value after OPERATOR
			val, err := p.parseLiteralValue()
			if err != nil {
				return nil, err
			}
			where.Value = val
			goto parseChain
		}
	default:
		// If no operator, maybe it's a boolean function call (like is_visible)
		// We'll treat it as Column = "true" to skip filtering
		where.Operator = "="
		where.Value = "true"
		goto parseChain
	}
	p.nextToken()

	// Parse value
	if where.Operator == "IN" {
		if p.current.Type != TOKEN_LPAREN {
			return nil, fmt.Errorf("expected ( after IN")
		}
		p.nextToken()
		values := make([]interface{}, 0)
		for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
			switch p.current.Type {
			case TOKEN_STRING:
				values = append(values, p.current.Literal)
			case TOKEN_NUMBER:
				num, _ := strconv.Atoi(p.current.Literal)
				values = append(values, num)
			}
			p.nextToken()
			if p.current.Type == TOKEN_COMMA {
				p.nextToken()
			}
		}
		where.Value = values
		p.nextToken() // consume )
	} else {
		val, err := p.parseLiteralValue()
		if err != nil {
			return nil, err
		}
		where.Value = val
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

func (p *Parser) parseGrant() (*GrantStmt, error) {
	stmt := &GrantStmt{}
	p.nextToken() // consume GRANT
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
	return stmt, nil
}

func (p *Parser) parseRevoke() (*RevokeStmt, error) {
	stmt := &RevokeStmt{}
	p.nextToken() // consume REVOKE
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

func (p *Parser) parseSet() (*SetStmt, error) {
	p.nextToken() // consume SET

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected variable name after SET")
	}
	name := p.current.Literal
	p.nextToken()

	if p.current.Type == TOKEN_TO || p.current.Type == TOKEN_EQUALS {
		p.nextToken()
	}

	var value string
	if p.current.Type == TOKEN_STRING || p.current.Type == TOKEN_IDENT || p.current.Type == TOKEN_NUMBER {
		value = p.current.Literal
		p.nextToken()
	}

	return &SetStmt{Name: name, Value: value}, nil
}
