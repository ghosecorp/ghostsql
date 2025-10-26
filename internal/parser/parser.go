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
	switch p.current.Type {
	case TOKEN_CREATE:
		return p.parseCreateTable()
	case TOKEN_INSERT:
		return p.parseInsert()
	case TOKEN_SELECT:
		return p.parseSelect()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.current.Type)
	}
}

func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}

	p.nextToken() // consume CREATE
	if p.current.Type != TOKEN_TABLE {
		return nil, fmt.Errorf("expected TABLE, got %s", p.current.Type)
	}

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

		// Read purpose
		if p.current.Type != TOKEN_STRING {
			return nil, fmt.Errorf("expected string for purpose")
		}
		stmt.Metadata = append(stmt.Metadata, p.current.Literal)
		p.nextToken()

		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
			// Read description
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
		return col, fmt.Errorf("expected column name")
	}

	col.Name = p.current.Literal
	p.nextToken()

	// Parse type
	if p.current.Type != TOKEN_IDENT {
		return col, fmt.Errorf("expected column type")
	}

	typeName := strings.ToUpper(p.current.Literal)
	switch typeName {
	case "INT":
		col.Type = storage.TypeInt
	case "BIGINT":
		col.Type = storage.TypeBigInt
	case "TEXT":
		col.Type = storage.TypeText
	case "VARCHAR":
		col.Type = storage.TypeVarChar
	case "FLOAT":
		col.Type = storage.TypeFloat
	case "BOOLEAN":
		col.Type = storage.TypeBoolean
	case "VECTOR":
		col.Type = storage.TypeVector
	default:
		return col, fmt.Errorf("unknown type: %s", typeName)
	}

	p.nextToken()
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

	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected (")
	}
	p.nextToken()

	// Parse values
	values := []interface{}{}
	for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
		var val interface{}
		switch p.current.Type {
		case TOKEN_NUMBER:
			num, _ := strconv.Atoi(p.current.Literal)
			val = num
		case TOKEN_STRING:
			val = p.current.Literal
		default:
			return nil, fmt.Errorf("unexpected value type")
		}
		values = append(values, val)
		p.nextToken()
		if p.current.Type == TOKEN_COMMA {
			p.nextToken()
		}
	}

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected )")
	}

	stmt.Values = append(stmt.Values, values)
	return stmt, nil
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	p.nextToken() // consume SELECT

	// Parse columns
	for p.current.Type != TOKEN_FROM && p.current.Type != TOKEN_EOF {
		switch p.current.Type {
		case TOKEN_ASTERISK:
			stmt.Columns = append(stmt.Columns, "*")
			p.nextToken()
		case TOKEN_IDENT:
			stmt.Columns = append(stmt.Columns, p.current.Literal)
			p.nextToken()
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

func (p *Parser) parseWhere() (*WhereClause, error) {
	where := &WhereClause{}

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected column name in WHERE")
	}

	where.Column = p.current.Literal
	p.nextToken()

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
	default:
		return nil, fmt.Errorf("expected comparison operator, got %s", p.current.Type)
	}
	p.nextToken()

	// Parse value
	switch p.current.Type {
	case TOKEN_NUMBER:
		num, _ := strconv.Atoi(p.current.Literal)
		where.Value = num
	case TOKEN_STRING:
		where.Value = p.current.Literal
	default:
		return nil, fmt.Errorf("expected value in WHERE")
	}
	p.nextToken()

	return where, nil
}
