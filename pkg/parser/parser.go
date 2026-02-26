// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// HandParser is a hand-written recursive descent SQL parser.
// It produces the same AST nodes as the original parser.
// If a statement is not supported, it returns nil.
type HandParser struct {
	lexer     *LexerBridge
	arena     *Arena
	src       string
	sqlMode   mysql.SQLMode
	charset   string
	collation string

	// connectionEncoding is the charset encoding of the client connection.
	// Used for SetText on AST nodes so that Text() can transform raw bytes
	// to UTF-8 while OriginalText() preserves the raw bytes.
	connectionEncoding charset.Encoding

	// supportWindowFunc controls whether window function keywords are recognized.
	supportWindowFunc bool

	// strictDoubleFieldType controls if DOUBLE(len) is allowed (it should be rejected if strict).
	strictDoubleFieldType bool

	// lastFieldTypeExplicitCollate is set to true by parseStringOptions when it
	// consumes an explicit COLLATE clause. parseColumnDef uses this to detect
	// duplicate COLLATE in column options. Reset at the start of parseFieldType.
	lastFieldTypeExplicitCollate bool

	// hintParser parses optimizer hint comments (/*+ ... */) directly.
	hintParser *hintParser

	// Errors accumulated during parsing.
	errs  []error
	warns []error

	// result accumulates parsed statements.
	result []ast.StmtNode
}

// NewHandParser creates a new hand-written parser instance.
func NewHandParser() *HandParser {
	return &HandParser{
		arena: NewArena(),
	}
}

// Reset prepares the parser for reuse. Must be called before each parse.
func (p *HandParser) Reset() {
	// Reset the arena so typed slabs can reuse their backing arrays.
	// The old slab batches remain alive (GC-tracked) as long as any AST node
	// from a previous parse still references them.
	p.arena.Reset()

	p.lexer = nil
	p.src = ""
	p.errs = p.errs[:0]
	p.warns = p.warns[:0]
	p.result = p.result[:0]
}

// SetSQLMode sets the SQL mode.
func (p *HandParser) SetSQLMode(mode mysql.SQLMode) {
	p.sqlMode = mode
}

// parseOptHints consumes an optional hintComment token and parses it into
// optimizer hints using the embedded hint parser.
func (p *HandParser) parseOptHints() []*ast.TableOptimizerHint {
	if tok, ok := p.accept(hintComment); ok {
		if p.hintParser == nil {
			p.hintParser = newHintParser()
		}
		hints, warns := p.hintParser.parse(tok.Lit, p.sqlMode, p.offsetToPos(tok.Offset))
		p.warns = append(p.warns, warns...)
		return hints
	}
	return nil
}

// EnableWindowFunc sets window function keyword support.
func (p *HandParser) EnableWindowFunc(val bool) {
	p.supportWindowFunc = val
}

// SetStrictDoubleFieldTypeCheck sets strict double type check.
func (p *HandParser) SetStrictDoubleFieldTypeCheck(val bool) {
	p.strictDoubleFieldType = val
}

// Init initializes the parser with a lexer and source SQL.
func (p *HandParser) Init(lexer Lexer, src string) {
	p.lexer = NewLexerBridge(lexer, src)
	p.src = src
}

// SetCharsetCollation sets the default charset and collation for string literals.
func (p *HandParser) SetCharsetCollation(cs, collation string) {
	p.charset = cs
	p.collation = collation
	p.connectionEncoding = charset.FindEncoding(cs)
}

// Parse acts as a complete parser entry point, handling initialization and execution.
func (p *HandParser) Parse(lexer Lexer, src string, charset, collation string) ([]ast.StmtNode, []error, error) {
	p.Reset()
	p.Init(lexer, src)
	p.SetCharsetCollation(charset, collation)
	return p.ParseSQL()
}

// calcLineCol computes the 1-based line number and 0-based column number
// by scanning source bytes up to (but not including) limit.
func (p *HandParser) calcLineCol(limit int) (line int, col int) {
	line = 1
	col = 0
	for i := 0; i < limit && i < len(p.src); i++ {
		// Match reader.inc() semantics: check newline FIRST, then increment.
		if p.src[i] == '\n' {
			line++
			col = 0
		}
		col++
	}
	return line, col
}

// offsetToPos converts a byte offset in the source to a Pos{Line, Col, Offset}.
// Used to supply the hint parser with the correct initial position.
func (p *HandParser) offsetToPos(offset int) Pos {
	line, col := p.calcLineCol(offset)
	return Pos{Line: line, Col: col, Offset: offset}
}

// error records a TiDB-specific validation error with diagnostic text.
// Use this for SEMANTIC validation errors where the yacc grammar would NOT have
// reported the same message (e.g., "VALUES clause is not allowed for HASH partitions").
//
// DO NOT USE for pure syntax errors where yacc would report `near "..."` only —
// use errorNear or syntaxErrorAt instead.
func (p *HandParser) error(offset int, format string, args ...interface{}) {
	// The yacc parser reports column based on the scanner's current position
	// (after consuming the token). For non-EOF tokens, this means iterating
	// up to and including the offset byte; for EOF, offset == len(src)
	// and we stop before it.
	limit := offset
	if limit < len(p.src) {
		limit++ // include the byte at offset for non-EOF (matches yacc post-scan position)
	}
	line, col := p.calcLineCol(limit)
	// Build context string: remaining source from offset.
	near := ""
	if offset < len(p.src) {
		near = p.src[offset:]
		if len(near) > 80 {
			near = near[:80]
		}
	}
	msg := fmt.Sprintf(format, args...)
	p.errs = append(p.errs, fmt.Errorf("line %d column %d near \"%s\"%s ", line, col, near, msg))
}

// errorNear records a parse error in yacc-compatible format: `near "..."` only.
// Use this when matching the yacc parser's error output exactly.
//   - colOffset: byte position for calculating column number (typically end-of-token)
//   - nearOffset: byte position for near text (typically start-of-token)
func (p *HandParser) errorNear(colOffset, nearOffset int) {
	line, col := p.calcLineCol(colOffset)
	near := ""
	if nearOffset < len(p.src) {
		near = p.src[nearOffset:]
		if len(near) > 2048 {
			near = near[:2048]
		}
	}
	p.errs = append(p.errs, fmt.Errorf("line %d column %d near \"%s\" ", line, col, near))
}

// syntaxErrorAt records a yacc-compatible syntax error for the given token.
// This is the preferred method for reporting unexpected tokens during parsing.
// It uses tok.EndOffset for column (matching yacc scanner position after consuming
// token bytes) and tok.Offset for near-text display.
func (p *HandParser) syntaxErrorAt(tok Token) {
	p.errorNear(tok.EndOffset, tok.Offset)
}

// errSyntax is the [parser:1149] error instance.
var errSyntax = terror.ClassParser.NewStd(mysql.ErrSyntax)

// syntaxError records a MySQL-standard syntax error with code 1149.
// This produces the [parser:1149] prefix expected by test assertions.
func (p *HandParser) syntaxError(_ int) {
	p.errs = append(p.errs, errSyntax)
}

// warn records a parse warning.
func (p *HandParser) warn(format string, args ...interface{}) {
	p.warns = append(p.warns, fmt.Errorf(format, args...))
}

// warnNear records a position-aware warning in yacc-compatible format.
// It mirrors the yacc Scanner.Errorf layout so that SyntaxWarn produces
// matching "line N column N near ..." output.
func (p *HandParser) warnNear(offset int, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	// Use calcLineCol with offset directly (no +1 adjustment) to match yacc
	// Scanner reader Col which is 0-based-then-incremented (Col==offset after
	// reading offset bytes on a single line).
	line, col := p.calcLineCol(offset)
	near := ""
	if offset < len(p.src) {
		near = p.src[offset:]
		if len(near) > 2048 {
			near = near[:2048]
		}
	}
	p.warns = append(p.warns, fmt.Errorf("line %d column %d near \"%s\"%s ", line, col, near, msg))
}

// peek returns the next token without consuming it.
func (p *HandParser) peek() Token {
	return p.lexer.Peek()
}

// peekN returns the token at offset n from the current position without consuming it.
// peekN(0) is equivalent to peek().
func (p *HandParser) peekN(n int) Token {
	return p.lexer.PeekN(n)
}

// next consumes and returns the next token.
func (p *HandParser) next() Token {
	return p.lexer.Next()
}

// expect consumes and returns a token of the expected type.
// If the token does not match, an error is recorded in yacc-compatible format.
func (p *HandParser) expect(expected int) (Token, bool) {
	tok := p.next()
	if tok.Tp != expected {
		// Use errorNear for yacc-compatible error format:
		// - colOffset = tok.EndOffset (= scanner position after consuming token bytes,
		//   matching yacc's s.r.p.Col at the time Errorf is called)
		// - nearOffset = tok.Offset (= start of error context, matching yacc's lastScanOffset)
		p.errorNear(tok.EndOffset, tok.Offset)
		return tok, false
	}
	return tok, true
}

// expectAny consumes and returns a token if it matches any of the expected types.
// If not, an error is recorded in yacc-compatible format.
func (p *HandParser) expectAny(expected ...int) (Token, bool) {
	tok := p.next()
	for _, e := range expected {
		if tok.Tp == e {
			return tok, true
		}
	}
	// Use errorNear for yacc-compatible error format:
	// colOffset = tok.EndOffset (matches yacc scanner position after consuming token).
	p.errorNear(tok.EndOffset, tok.Offset)
	return tok, false
}

// expectIdentLike consumes and returns a token if it can be used as an identifier
// (unreserved keywords, identifier, or stringLit). Reports an error if not.
// This replaces expectAny(identifier, stringLit) which misses unreserved keywords.
func (p *HandParser) expectIdentLike() (Token, bool) {
	tok := p.next()
	if isIdentLike(tok.Tp) {
		return tok, true
	}
	p.errorNear(tok.EndOffset, tok.Offset)
	return tok, false
}

// accept consumes the next token if it matches, returning whether it matched.
func (p *HandParser) accept(expected int) (Token, bool) {
	return p.lexer.Accept(expected)
}

// acceptAny consumes the next token if it matches any of the expected types.
func (p *HandParser) acceptAny(expected ...int) (Token, bool) {
	return p.lexer.AcceptAny(expected...)
}

// acceptKeyword consumes the next token if it matches the given token type OR is
// an identifier with the given keyword string (case-insensitive). This consolidates
// the common pattern: tok.Tp == tokFoo || (tok.Tp == identifier && strings.EqualFold(tok.Lit, "FOO"))
func (p *HandParser) acceptKeyword(tokKeyword int, keyword string) (Token, bool) {
	tok := p.peek()
	if tok.Tp == tokKeyword {
		p.next()
		return tok, true
	}
	if tok.Tp == identifier && strings.EqualFold(tok.Lit, keyword) {
		p.next()
		return tok, true
	}
	return tok, false
}

// peekKeyword returns true if the next token matches the given token type OR is
// an identifier with the given keyword string (case-insensitive). Does not consume.
func (p *HandParser) peekKeyword(tokKeyword int, keyword string) bool {
	tok := p.peek()
	return tok.Tp == tokKeyword || (tok.Tp == identifier && strings.EqualFold(tok.Lit, keyword))
}

// acceptIfNotExists consumes IF NOT EXISTS if present, returning true if found.
// Consolidates the 3-line pattern: accept(ifKwd) → expect(not) → expect(exists).
func (p *HandParser) acceptIfNotExists() bool {
	if _, ok := p.accept(ifKwd); ok {
		p.expect(not)
		p.expect(exists)
		return true
	}
	return false
}

// acceptIfExists consumes IF EXISTS if present, returning true if found.
// Consolidates the 2-line pattern: accept(ifKwd) → expect(exists).
func (p *HandParser) acceptIfExists() bool {
	if _, ok := p.accept(ifKwd); ok {
		p.expect(exists)
		return true
	}
	return false
}

// isConstraintToken returns true if the token type represents a constraint keyword.
// Consolidates the 10-way disjunction used in ADD COLUMN/CONSTRAINT parsing.
func isConstraintToken(tp int) bool {
	switch tp {
	case constraint, primary, key, index,
		unique, foreign, fulltext, check,
		vectorType, columnar:
		return true
	}
	return false
}

// tryBuiltinFunc dispatches to parseFn if the next token is '(', otherwise
// falls back to parseIdentOrFuncCall. This consolidates the common pattern
// used by builtin function keywords like CAST, TRIM, EXTRACT, etc.
func (p *HandParser) tryBuiltinFunc(parseFn func() ast.ExprNode) ast.ExprNode {
	if p.peekN(1).Tp == '(' {
		return parseFn()
	}
	return p.parseIdentOrFuncCall()
}

// Errors returns parsing errors and warnings.
func (p *HandParser) Errors() (warns []error, errs []error) {
	return p.warns, p.errs
}

// speculate runs a parser function speculatively. It saves the lexer position
// before calling fn. If fn returns nil (indicating parse failure), the lexer
// is restored to the saved position so the parser can try alternative parses.
func (p *HandParser) speculate(fn func() ast.StmtNode) ast.StmtNode {
	mark := p.lexer.Mark()
	result := fn()
	if result == nil {
		p.lexer.Restore(mark)
	}
	return result
}

// mark returns a position marker that can be used to restore the lexer state.
func (p *HandParser) mark() LexerMark {
	return p.lexer.Mark()
}

// restore resets the lexer to a previously saved mark.
func (p *HandParser) restore(m LexerMark) {
	p.lexer.Restore(m)
}
