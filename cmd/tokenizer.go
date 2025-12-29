package cmd

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType represents the type of token parsed from a command string
type TokenType int

const (
	TokenWord           TokenType = iota // Regular words/arguments
	TokenPipe                            // |
	TokenRedirectIn                      // <
	TokenRedirectOut                     // >
	TokenRedirectAppend                  // >>
	TokenRedirectErr                     // 2>
	TokenRedirectErrOut                  // 2>&1
	TokenBackground                      // & (future)
	TokenSemicolon                       // ; (future)
	TokenAnd                             // && (future)
	TokenOr                              // || (future)
)

// String returns a string representation of the token type
func (t TokenType) String() string {
	switch t {
	case TokenWord:
		return "Word"
	case TokenPipe:
		return "Pipe"
	case TokenRedirectIn:
		return "RedirectIn"
	case TokenRedirectOut:
		return "RedirectOut"
	case TokenRedirectAppend:
		return "RedirectAppend"
	case TokenRedirectErr:
		return "RedirectErr"
	case TokenRedirectErrOut:
		return "RedirectErrOut"
	case TokenBackground:
		return "Background"
	case TokenSemicolon:
		return "Semicolon"
	case TokenAnd:
		return "And"
	case TokenOr:
		return "Or"
	default:
		return "Unknown"
	}
}

// Token represents a single token in a command string
type Token struct {
	Type  TokenType
	Value string
}

// String returns a string representation of the token
func (t Token) String() string {
	if t.Type == TokenWord {
		return fmt.Sprintf("%s(%q)", t.Type, t.Value)
	}
	return t.Type.String()
}

// Tokenizer parses command strings into tokens
type Tokenizer struct {
	input []rune
	pos   int
}

// NewTokenizer creates a new tokenizer for the given input string
func NewTokenizer(input string) *Tokenizer {
	return &Tokenizer{
		input: []rune(input),
		pos:   0,
	}
}

// Tokenize parses the input string and returns a slice of tokens
func (t *Tokenizer) Tokenize() ([]Token, error) {
	var tokens []Token

	for t.pos < len(t.input) {
		// Skip whitespace
		if unicode.IsSpace(t.current()) {
			t.advance()
			continue
		}

		// Check for operators
		if token, ok := t.tryReadOperator(); ok {
			tokens = append(tokens, token)
			continue
		}

		// Read word (handles quotes and escapes)
		word, err := t.readWord()
		if err != nil {
			return nil, err
		}

		tokens = append(tokens, Token{
			Type:  TokenWord,
			Value: word,
		})
	}

	return tokens, nil
}

// current returns the current rune without advancing
func (t *Tokenizer) current() rune {
	if t.pos >= len(t.input) {
		return 0
	}
	return t.input[t.pos]
}

// peek returns the next rune without advancing
func (t *Tokenizer) peek() rune {
	if t.pos+1 >= len(t.input) {
		return 0
	}
	return t.input[t.pos+1]
}

// advance moves to the next rune
func (t *Tokenizer) advance() {
	t.pos++
}

// tryReadOperator attempts to read an operator token
func (t *Tokenizer) tryReadOperator() (Token, bool) {
	ch := t.current()
	next := t.peek()

	// Two-character operators
	switch {
	case ch == '>' && next == '>':
		t.advance()
		t.advance()
		return Token{Type: TokenRedirectAppend}, true

	case ch == '2' && next == '>':
		// Check if it's 2>&1
		if t.pos+2 < len(t.input) && t.input[t.pos+2] == '&' && t.pos+3 < len(t.input) && t.input[t.pos+3] == '1' {
			t.pos += 4
			return Token{Type: TokenRedirectErrOut}, true
		}
		// Just 2>
		t.advance()
		t.advance()
		return Token{Type: TokenRedirectErr}, true

	case ch == '&' && next == '&':
		t.advance()
		t.advance()
		return Token{Type: TokenAnd}, true

	case ch == '|' && next == '|':
		t.advance()
		t.advance()
		return Token{Type: TokenOr}, true
	}

	// Single-character operators
	switch ch {
	case '|':
		t.advance()
		return Token{Type: TokenPipe}, true
	case '<':
		t.advance()
		return Token{Type: TokenRedirectIn}, true
	case '>':
		t.advance()
		return Token{Type: TokenRedirectOut}, true
	case '&':
		t.advance()
		return Token{Type: TokenBackground}, true
	case ';':
		t.advance()
		return Token{Type: TokenSemicolon}, true
	}

	return Token{}, false
}

// readWord reads a word token, handling quotes and escapes
func (t *Tokenizer) readWord() (string, error) {
	var builder strings.Builder

	for t.pos < len(t.input) {
		ch := t.current()

		// Stop at whitespace or operators (unless quoted)
		if unicode.IsSpace(ch) {
			break
		}

		// Check for operators that terminate the word
		if t.isOperatorStart(ch) {
			break
		}

		// Handle quoted strings
		if ch == '"' || ch == '\'' {
			quoted, err := t.readQuoted(ch)
			if err != nil {
				return "", err
			}
			builder.WriteString(quoted)
			continue
		}

		// Handle escape sequences
		if ch == '\\' {
			escaped, err := t.readEscape()
			if err != nil {
				return "", err
			}
			builder.WriteRune(escaped)
			continue
		}

		// Regular character
		builder.WriteRune(ch)
		t.advance()
	}

	return builder.String(), nil
}

// readQuoted reads a quoted string
func (t *Tokenizer) readQuoted(quote rune) (string, error) {
	var builder strings.Builder
	t.advance() // Skip opening quote

	for t.pos < len(t.input) {
		ch := t.current()

		// Found closing quote
		if ch == quote {
			t.advance()
			return builder.String(), nil
		}

		// Handle escapes inside double quotes
		if quote == '"' && ch == '\\' {
			escaped, err := t.readEscape()
			if err != nil {
				return "", err
			}
			builder.WriteRune(escaped)
			continue
		}

		// Regular character
		builder.WriteRune(ch)
		t.advance()
	}

	return "", fmt.Errorf("unclosed quote at position %d", t.pos)
}

// readEscape reads an escape sequence
func (t *Tokenizer) readEscape() (rune, error) {
	t.advance() // Skip backslash

	if t.pos >= len(t.input) {
		return 0, fmt.Errorf("incomplete escape sequence at end of input")
	}

	ch := t.current()
	t.advance()

	switch ch {
	case 'n':
		return '\n', nil
	case 't':
		return '\t', nil
	case 'r':
		return '\r', nil
	case '\\':
		return '\\', nil
	case '"':
		return '"', nil
	case '\'':
		return '\'', nil
	case '$':
		return '$', nil
	default:
		// Unknown escape - just return the character as-is
		return ch, nil
	}
}

// isOperatorStart checks if a character can start an operator
func (t *Tokenizer) isOperatorStart(ch rune) bool {
	switch ch {
	case '|', '<', '>', '&', ';':
		return true
	case '2':
		// Check if it's 2> or 2>&1
		return t.peek() == '>'
	default:
		return false
	}
}
