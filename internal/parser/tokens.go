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
	TOKEN_FROM
	TOKEN_WHERE
	TOKEN_METADATA

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
	TOKEN_LT // <
	TOKEN_GT // >
	TOKEN_LE // <=
	TOKEN_GE // >=
	TOKEN_NE // !=
)

type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

func (t TokenType) String() string {
	names := map[TokenType]string{
		TOKEN_ILLEGAL:   "ILLEGAL",
		TOKEN_EOF:       "EOF",
		TOKEN_SELECT:    "SELECT",
		TOKEN_INSERT:    "INSERT",
		TOKEN_INTO:      "INTO",
		TOKEN_VALUES:    "VALUES",
		TOKEN_CREATE:    "CREATE",
		TOKEN_TABLE:     "TABLE",
		TOKEN_FROM:      "FROM",
		TOKEN_WHERE:     "WHERE",
		TOKEN_METADATA:  "METADATA",
		TOKEN_IDENT:     "IDENT",
		TOKEN_NUMBER:    "NUMBER",
		TOKEN_STRING:    "STRING",
		TOKEN_COMMA:     "COMMA",
		TOKEN_SEMICOLON: "SEMICOLON",
		TOKEN_LPAREN:    "LPAREN",
		TOKEN_RPAREN:    "RPAREN",
		TOKEN_LBRACKET:  "LBRACKET",
		TOKEN_RBRACKET:  "RBRACKET",
		TOKEN_ASTERISK:  "ASTERISK",
		TOKEN_EQUALS:    "EQUALS",
		TOKEN_LT:        "LT",
		TOKEN_GT:        "GT",
		TOKEN_LE:        "LE",
		TOKEN_GE:        "GE",
		TOKEN_NE:        "NE",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return "UNKNOWN"
}

var keywords = map[string]TokenType{
	"SELECT":   TOKEN_SELECT,
	"INSERT":   TOKEN_INSERT,
	"INTO":     TOKEN_INTO,
	"VALUES":   TOKEN_VALUES,
	"CREATE":   TOKEN_CREATE,
	"TABLE":    TOKEN_TABLE,
	"FROM":     TOKEN_FROM,
	"WHERE":    TOKEN_WHERE,
	"METADATA": TOKEN_METADATA,
}

func LookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TOKEN_IDENT
}
