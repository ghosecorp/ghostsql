// internal/parser/lexer.go
package parser

import (
	"strings"
	"unicode"
)

type Lexer struct {
	input  string
	pos    int
	line   int
	column int
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		pos:    0,
		line:   1,
		column: 1,
	}
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TOKEN_EOF, Line: l.line, Column: l.column}
	}

	ch := l.input[l.pos]
	token := Token{Line: l.line, Column: l.column}

	switch ch {
	case ',':
		token.Type = TOKEN_COMMA
		token.Literal = ","
		l.advance()
	case ';':
		token.Type = TOKEN_SEMICOLON
		token.Literal = ";"
		l.advance()
	case '(':
		token.Type = TOKEN_LPAREN
		token.Literal = "("
		l.advance()
	case ')':
		token.Type = TOKEN_RPAREN
		token.Literal = ")"
		l.advance()
	case '[':
		token.Type = TOKEN_LBRACKET
		token.Literal = "["
		l.advance()
	case ']':
		token.Type = TOKEN_RBRACKET
		token.Literal = "]"
		l.advance()
	case '*':
		token.Type = TOKEN_ASTERISK
		token.Literal = "*"
		l.advance()
	case '=':
		token.Type = TOKEN_EQUALS
		token.Literal = "="
		l.advance()
	case '<':
		if l.peek() == '=' {
			token.Type = TOKEN_LE
			token.Literal = "<="
			l.advance()
			l.advance()
		} else if l.peek() == '>' {
			token.Type = TOKEN_NE
			token.Literal = "<>"
			l.advance()
			l.advance()
		} else {
			token.Type = TOKEN_LT
			token.Literal = "<"
			l.advance()
		}
	case '>':
		if l.peek() == '=' {
			token.Type = TOKEN_GE
			token.Literal = ">="
			l.advance()
			l.advance()
		} else {
			token.Type = TOKEN_GT
			token.Literal = ">"
			l.advance()
		}
	case '!':
		if l.peek() == '=' {
			token.Type = TOKEN_NE
			token.Literal = "!="
			l.advance()
			l.advance()
		} else {
			token.Type = TOKEN_ILLEGAL
			token.Literal = "!"
			l.advance()
		}
	case '\'', '"':
		token.Type = TOKEN_STRING
		token.Literal = l.readString(ch)
	default:
		if unicode.IsLetter(rune(ch)) {
			token.Literal = l.readIdentifier()
			token.Type = LookupKeyword(strings.ToUpper(token.Literal))
		} else if unicode.IsDigit(rune(ch)) {
			token.Type = TOKEN_NUMBER
			token.Literal = l.readNumber()
		} else {
			token.Type = TOKEN_ILLEGAL
			token.Literal = string(ch)
			l.advance()
		}
	}
	return token
}

func (l *Lexer) peek() byte {
	if l.pos+1 >= len(l.input) {
		return 0
	}
	return l.input[l.pos+1]
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.column = 1
		} else {
			l.column++
		}
		l.pos++
	}
}

func (l *Lexer) advance() {
	l.pos++
	l.column++
}

func (l *Lexer) readIdentifier() string {
	start := l.pos
	for l.pos < len(l.input) && (unicode.IsLetter(rune(l.input[l.pos])) || unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '_') {
		l.advance()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readNumber() string {
	start := l.pos
	for l.pos < len(l.input) && unicode.IsDigit(rune(l.input[l.pos])) {
		l.advance()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readString(quote byte) string {
	l.advance() // skip opening quote
	start := l.pos
	for l.pos < len(l.input) && l.input[l.pos] != quote {
		l.advance()
	}
	str := l.input[start:l.pos]
	if l.pos < len(l.input) {
		l.advance() // skip closing quote
	}
	return str
}
