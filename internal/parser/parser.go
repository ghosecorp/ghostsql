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
		return p.parseCreate()
	case TOKEN_INSERT:
		return p.parseInsert()
	case TOKEN_SELECT:
		return p.parseSelect()
	case TOKEN_USE:
		return p.parseUse()
	case TOKEN_SHOW:
		return p.parseShow()
	case TOKEN_UPDATE:
		return p.parseUpdate()
	case TOKEN_DELETE:
		return p.parseDelete()
	case TOKEN_DROP:
		return p.parseDrop()
	case TOKEN_TRUNCATE:
		return p.parseTruncate()
	case TOKEN_ALTER:
		return p.parseAlter()
	case TOKEN_COMMENT:
		return p.parseComment()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.current.Type)
	}
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
	default:
		return nil, fmt.Errorf("expected DATABASE, TABLE, or INDEX after CREATE")
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
	default:
		return nil, fmt.Errorf("expected TABLE, DATABASE, or INDEX after DROP")
	}
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

func (p *Parser) parseAlter() (*AlterTableStmt, error) {
	stmt := &AlterTableStmt{}

	p.nextToken() // consume ALTER

	if p.current.Type != TOKEN_TABLE {
		return nil, fmt.Errorf("expected TABLE after ALTER")
	}
	p.nextToken()

	if p.current.Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_ADD {
		return nil, fmt.Errorf("expected ADD (only ADD COLUMN supported for now)")
	}
	p.nextToken()

	if p.current.Type == TOKEN_COLUMN {
		p.nextToken() // consume COLUMN (optional)
	}

	stmt.Action = "ADD_COLUMN"
	col, err := p.parseColumnDef()
	if err != nil {
		return nil, err
	}
	stmt.Column = &col

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
		return col, fmt.Errorf("expected column name")
	}

	col.Name = p.current.Literal
	p.nextToken()

	if p.current.Type != TOKEN_IDENT {
		return col, fmt.Errorf("expected column type")
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

	// Parse optional column constraints (NOT NULL, PRIMARY KEY, etc.)
	// For now, we'll skip any additional keywords we don't recognize
	// This allows the parser to continue to the next column or end of column list
	for {
		// Check if we've hit a comma (next column) or right paren (end of columns)
		if p.current.Type == TOKEN_COMMA || p.current.Type == TOKEN_RPAREN {
			break
		}

		// Check for EOF
		if p.current.Type == TOKEN_EOF {
			break
		}

		// Skip constraint keywords we don't yet support
		// This includes NOT, NULL, PRIMARY, KEY, DEFAULT, AUTO_INCREMENT, etc.
		if p.current.Type == TOKEN_IDENT {
			keyword := strings.ToUpper(p.current.Literal)
			switch keyword {
			case "NOT", "NULL", "PRIMARY", "KEY", "DEFAULT", "AUTO_INCREMENT",
				"UNIQUE", "CHECK", "REFERENCES", "FOREIGN":
				// Skip these constraint keywords
				p.nextToken()
			default:
				// Unknown token - might be an error, but let's be lenient
				return col, fmt.Errorf("unexpected token after column type: %s", p.current.Literal)
			}
		} else {
			// Some other token type we don't expect
			// Could be an error, but let's break and let the caller handle it
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

	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected (")
	}
	p.nextToken()

	// Parse values
	values := []interface{}{}
	for p.current.Type != TOKEN_RPAREN && p.current.Type != TOKEN_EOF {
		var val interface{}

		if p.current.Type == TOKEN_LBRACKET {
			// Parse vector array [0.1, 0.2, 0.3]
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
			val = vectorStr // Store as string, will be parsed by executor
			p.nextToken()   // consume ]
		} else {
			// Regular value parsing
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
			default:
				return nil, fmt.Errorf("unexpected value type: %s (literal: %s)", p.current.Type, p.current.Literal)
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

	stmt.Values = append(stmt.Values, values)
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
			p.nextToken()
		} else if p.current.Type == TOKEN_IDENT {
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
				if p.current.Type != TOKEN_IDENT {
					return nil, fmt.Errorf("expected column name in ORDER BY")
				}

				orderBy := OrderByClause{
					Column:     p.current.Literal,
					Descending: false,
				}
				p.nextToken()

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
	case TOKEN_LIKE:
		where.Operator = "LIKE"
	default:
		return nil, fmt.Errorf("expected comparison operator, got %s", p.current.Type)
	}
	p.nextToken()

	// Parse value
	switch p.current.Type {
	case TOKEN_NUMBER:
		// Check if it's a float or int
		if strings.Contains(p.current.Literal, ".") {
			num, _ := strconv.ParseFloat(p.current.Literal, 64)
			where.Value = num
		} else {
			num, _ := strconv.Atoi(p.current.Literal)
			where.Value = num
		}
	case TOKEN_STRING:
		where.Value = p.current.Literal
	default:
		return nil, fmt.Errorf("expected value in WHERE")
	}
	p.nextToken()

	// Parse AND/OR (simplified - single level only for now)
	switch p.current.Type {
	case TOKEN_AND:
		p.nextToken()
		and, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		where.And = and
	case TOKEN_OR:
		p.nextToken()
		or, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		where.Or = or
	}

	return where, nil
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
