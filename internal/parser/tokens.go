// internal/parser/tokens.go
package parser

type TokenType int

const (
	TOKEN_ILLEGAL TokenType = iota
	TOKEN_EOF
	TOKEN_WHITESPACE

	// Keywords
	TOKEN_SELECT
	TOKEN_INSERT
	TOKEN_INTO
	TOKEN_VALUES
	TOKEN_CREATE
	TOKEN_TABLE
	TOKEN_DATABASE
	TOKEN_USE
	TOKEN_SHOW
	TOKEN_DATABASES
	TOKEN_TABLES
	TOKEN_COLUMNS
	TOKEN_FROM
	TOKEN_WHERE
	TOKEN_METADATA
	TOKEN_UPDATE
	TOKEN_SET
	TOKEN_DELETE
	TOKEN_DROP
	TOKEN_ALTER
	TOKEN_ADD
	TOKEN_COLUMN
	TOKEN_TRUNCATE
	TOKEN_ORDER
	TOKEN_BY
	TOKEN_LIMIT
	TOKEN_OFFSET
	TOKEN_AND
	TOKEN_OR
	TOKEN_LIKE
	TOKEN_COMMENT
	TOKEN_ON
	TOKEN_IS
	TOKEN_GROUP
	TOKEN_HAVING
	TOKEN_COUNT
	TOKEN_SUM
	TOKEN_AVG
	TOKEN_MAX
	TOKEN_MIN
	TOKEN_AS
	TOKEN_INDEX
	TOKEN_USING
	TOKEN_HNSW
	TOKEN_BTREE
	TOKEN_WITH

	// Literals
	TOKEN_IDENT
	TOKEN_NUMBER
	TOKEN_STRING

	// Symbols
	TOKEN_COMMA
	TOKEN_SEMICOLON
	TOKEN_LPAREN
	TOKEN_RPAREN
	TOKEN_LBRACKET
	TOKEN_RBRACKET
	TOKEN_ASTERISK
	TOKEN_EQUALS
	TOKEN_LT
	TOKEN_GT
	TOKEN_LE
	TOKEN_GE
	TOKEN_NE
	TOKEN_PERCENT

	// Vector Tokens
	TOKEN_VECTOR_SEARCH
	TOKEN_COSINE_DISTANCE
	TOKEN_L2_DISTANCE
	TOKEN_DISTANCE

	TOKEN_NULL

	// Foreign Key
	TOKEN_FOREIGN
	TOKEN_REFERENCES

	TOKEN_JOIN
	TOKEN_INNER
	TOKEN_LEFT
	TOKEN_RIGHT
	TOKEN_FULL
	TOKEN_OUTER
	TOKEN_CROSS

	TOKEN_DOT
)

type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

func (t TokenType) String() string {
	names := map[TokenType]string{
		TOKEN_ILLEGAL:         "ILLEGAL",
		TOKEN_EOF:             "EOF",
		TOKEN_SELECT:          "SELECT",
		TOKEN_INSERT:          "INSERT",
		TOKEN_INTO:            "INTO",
		TOKEN_VALUES:          "VALUES",
		TOKEN_CREATE:          "CREATE",
		TOKEN_TABLE:           "TABLE",
		TOKEN_DATABASE:        "DATABASE",
		TOKEN_USE:             "USE",
		TOKEN_SHOW:            "SHOW",
		TOKEN_DATABASES:       "DATABASES",
		TOKEN_TABLES:          "TABLES",
		TOKEN_COLUMNS:         "COLUMNS",
		TOKEN_FROM:            "FROM",
		TOKEN_WHERE:           "WHERE",
		TOKEN_METADATA:        "METADATA",
		TOKEN_UPDATE:          "UPDATE",
		TOKEN_SET:             "SET",
		TOKEN_DELETE:          "DELETE",
		TOKEN_DROP:            "DROP",
		TOKEN_ALTER:           "ALTER",
		TOKEN_ADD:             "ADD",
		TOKEN_COLUMN:          "COLUMN",
		TOKEN_TRUNCATE:        "TRUNCATE",
		TOKEN_ORDER:           "ORDER",
		TOKEN_BY:              "BY",
		TOKEN_LIMIT:           "LIMIT",
		TOKEN_OFFSET:          "OFFSET",
		TOKEN_AND:             "AND",
		TOKEN_OR:              "OR",
		TOKEN_LIKE:            "LIKE",
		TOKEN_COMMENT:         "COMMENT",
		TOKEN_ON:              "ON",
		TOKEN_IS:              "IS",
		TOKEN_GROUP:           "GROUP",
		TOKEN_HAVING:          "HAVING",
		TOKEN_COUNT:           "COUNT",
		TOKEN_SUM:             "SUM",
		TOKEN_AVG:             "AVG",
		TOKEN_MAX:             "MAX",
		TOKEN_MIN:             "MIN",
		TOKEN_AS:              "AS",
		TOKEN_INDEX:           "INDEX",
		TOKEN_USING:           "USING",
		TOKEN_HNSW:            "HNSW",
		TOKEN_BTREE:           "BTREE",
		TOKEN_WITH:            "WITH",
		TOKEN_IDENT:           "IDENT",
		TOKEN_NUMBER:          "NUMBER",
		TOKEN_STRING:          "STRING",
		TOKEN_COMMA:           "COMMA",
		TOKEN_SEMICOLON:       "SEMICOLON",
		TOKEN_LPAREN:          "LPAREN",
		TOKEN_RPAREN:          "RPAREN",
		TOKEN_LBRACKET:        "LBRACKET",
		TOKEN_RBRACKET:        "RBRACKET",
		TOKEN_ASTERISK:        "ASTERISK",
		TOKEN_EQUALS:          "EQUALS",
		TOKEN_LT:              "LT",
		TOKEN_GT:              "GT",
		TOKEN_LE:              "LE",
		TOKEN_GE:              "GE",
		TOKEN_NE:              "NE",
		TOKEN_PERCENT:         "PERCENT",
		TOKEN_VECTOR_SEARCH:   "VECTOR_SEARCH",
		TOKEN_COSINE_DISTANCE: "COSINE_DISTANCE",
		TOKEN_L2_DISTANCE:     "L2_DISTANCE",
		TOKEN_DISTANCE:        "DISTANCE",

		TOKEN_NULL:       "NULL",    // Add this
		TOKEN_FOREIGN:    "FOREIGN", // Add this
		TOKEN_REFERENCES: "REFERENCES",

		TOKEN_JOIN:  "JOIN",
		TOKEN_INNER: "INNER",
		TOKEN_LEFT:  "LEFT",
		TOKEN_RIGHT: "RIGHT",
		TOKEN_FULL:  "FULL",
		TOKEN_OUTER: "OUTER",
		TOKEN_CROSS: "CROSS",

		TOKEN_DOT: "DOT",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return "UNKNOWN"
}

var keywords = map[string]TokenType{
	"SELECT":          TOKEN_SELECT,
	"INSERT":          TOKEN_INSERT,
	"INTO":            TOKEN_INTO,
	"VALUES":          TOKEN_VALUES,
	"CREATE":          TOKEN_CREATE,
	"TABLE":           TOKEN_TABLE,
	"DATABASE":        TOKEN_DATABASE,
	"USE":             TOKEN_USE,
	"SHOW":            TOKEN_SHOW,
	"DATABASES":       TOKEN_DATABASES,
	"TABLES":          TOKEN_TABLES,
	"COLUMNS":         TOKEN_COLUMNS,
	"FROM":            TOKEN_FROM,
	"WHERE":           TOKEN_WHERE,
	"METADATA":        TOKEN_METADATA,
	"UPDATE":          TOKEN_UPDATE,
	"SET":             TOKEN_SET,
	"DELETE":          TOKEN_DELETE,
	"DROP":            TOKEN_DROP,
	"ALTER":           TOKEN_ALTER,
	"ADD":             TOKEN_ADD,
	"COLUMN":          TOKEN_COLUMN,
	"TRUNCATE":        TOKEN_TRUNCATE,
	"ORDER":           TOKEN_ORDER,
	"BY":              TOKEN_BY,
	"LIMIT":           TOKEN_LIMIT,
	"OFFSET":          TOKEN_OFFSET,
	"AND":             TOKEN_AND,
	"OR":              TOKEN_OR,
	"LIKE":            TOKEN_LIKE,
	"COMMENT":         TOKEN_COMMENT,
	"ON":              TOKEN_ON,
	"IS":              TOKEN_IS,
	"GROUP":           TOKEN_GROUP,
	"HAVING":          TOKEN_HAVING,
	"COUNT":           TOKEN_COUNT,
	"SUM":             TOKEN_SUM,
	"AVG":             TOKEN_AVG,
	"MAX":             TOKEN_MAX,
	"MIN":             TOKEN_MIN,
	"AS":              TOKEN_AS,
	"INDEX":           TOKEN_INDEX,
	"USING":           TOKEN_USING,
	"HNSW":            TOKEN_HNSW,
	"BTREE":           TOKEN_BTREE,
	"WITH":            TOKEN_WITH,
	"VECTOR_SEARCH":   TOKEN_VECTOR_SEARCH,
	"COSINE_DISTANCE": TOKEN_COSINE_DISTANCE,
	"L2_DISTANCE":     TOKEN_L2_DISTANCE,
	"DISTANCE":        TOKEN_DISTANCE,
	"NULL":            TOKEN_NULL,
	"FOREIGN":         TOKEN_FOREIGN,
	"REFERENCES":      TOKEN_REFERENCES,
	"JOIN":            TOKEN_JOIN,
	"INNER":           TOKEN_INNER,
	"LEFT":            TOKEN_LEFT,
	"RIGHT":           TOKEN_RIGHT,
	"FULL":            TOKEN_FULL,
	"OUTER":           TOKEN_OUTER,
	"CROSS":           TOKEN_CROSS,
}

func LookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TOKEN_IDENT
}
