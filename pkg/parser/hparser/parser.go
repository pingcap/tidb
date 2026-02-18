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

package hparser

import (
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// HintParseFn is a callback that parses optimizer hint comments (/*+ ... */).
// Injected by yy_parser.go since the hint parser lives in the parent package.
type HintParseFn func(input string) ([]*ast.TableOptimizerHint, []error)

// HandParser is a hand-written recursive descent SQL parser.
// It produces the same AST nodes as the goyacc-generated parser.
// DML is parsed natively via recursive descent. DDL/admin statements
// return nil, causing the caller (yy_parser.go) to use goyacc.
type HandParser struct {
	lexer     *LexerBridge
	arena     *Arena
	src       string
	sqlMode   mysql.SQLMode
	charset   string
	collation string

	// supportWindowFunc controls whether window function keywords are recognized.
	supportWindowFunc bool

	// strictDoubleFieldType controls if DOUBLE(len) is allowed (it should be rejected if strict).
	strictDoubleFieldType bool

	// lastFieldTypeExplicitCollate is set to true by parseStringOptions when it
	// consumes an explicit COLLATE clause. parseColumnDef uses this to detect
	// duplicate COLLATE in column options. Reset at the start of parseFieldType.
	lastFieldTypeExplicitCollate bool

	// hintParse parses optimizer hint comments (/*+ ... */).
	hintParse HintParseFn

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
	// NOTE: Alloc/AllocSlice currently use heap allocation (new/make) instead
	// of the arena's bump-pointer. The arena is still created for API compat
	// but its memory is not used. See arena.go for rationale.
	p.arena = NewArena()

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

// SetHintParse sets the optimizer hint parse callback.
func (p *HandParser) SetHintParse(fn HintParseFn) {
	p.hintParse = fn
}

// parseOptHints consumes an optional hintComment token and parses it into
// optimizer hints using the injected HintParseFn. Returns nil if no hint
// token is present or no callback is set.
func (p *HandParser) parseOptHints() []*ast.TableOptimizerHint {
	if tok, ok := p.accept(tokHintComment); ok {
		if p.hintParse != nil {
			hints, warns := p.hintParse(tok.Lit)
			p.warns = append(p.warns, warns...)
			return hints
		}
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

// Init initializes the parser with a lexer function and source SQL.
func (p *HandParser) Init(lexFunc LexFunc, src string) {
	p.lexer = NewLexerBridge(lexFunc, src)
	p.src = src
}

// SetCharsetCollation sets the default charset and collation for string literals.
func (p *HandParser) SetCharsetCollation(charset, collation string) {
	p.charset = charset
	p.collation = collation
}

// Parse acts as a complete parser entry point, handling initialization and execution.
func (p *HandParser) Parse(lexFunc LexFunc, src string, charset, collation string) ([]ast.StmtNode, []error, error) {
	p.Reset()
	p.Init(lexFunc, src)
	p.SetCharsetCollation(charset, collation)
	return p.ParseSQL()
}

// calcLineCol computes the 1-based line number and 0-based column number
// by scanning source bytes up to (but not including) limit.
func (p *HandParser) calcLineCol(limit int) (int, int) {
	line := 1
	col := 0
	for i := 0; i < limit && i < len(p.src); i++ {
		col++
		if p.src[i] == '\n' {
			line++
			col = 0
		}
	}
	return line, col
}

// error records a parse error with position info (line X column Y near "...").
func (p *HandParser) error(offset int, format string, args ...any) {
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

// errorNear records a parse error in yacc-compatible format.
// colOffset is the byte position for column (typically end-of-token),
// nearOffset is the byte position for near text (typically start-of-token).
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

// errSyntax is the [parser:1149] error instance, matching yy_parser.go's ErrSyntax.
var errSyntax = terror.ClassParser.NewStd(mysql.ErrSyntax)

// syntaxError records a MySQL-standard syntax error with code 1149.
// This produces the [parser:1149] prefix expected by test assertions.
func (p *HandParser) syntaxError(offset int) {
	p.errs = append(p.errs, errSyntax)
}

// warn records a parse warning.
func (p *HandParser) warn(format string, args ...any) {
	p.warns = append(p.warns, fmt.Errorf(format, args...))
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
		// - colOffset = next token's offset (= scanner position after consumed token)
		// - nearOffset = consumed token's offset (= start of error context)
		p.errorNear(p.peek().Offset, tok.Offset)
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
	// Use errorNear for yacc-compatible error format.
	p.errorNear(p.peek().Offset, tok.Offset)
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
// the common pattern: tok.Tp == tokFoo || (tok.Tp == tokIdentifier && strings.EqualFold(tok.Lit, "FOO"))
func (p *HandParser) acceptKeyword(tokType int, keyword string) (Token, bool) {
	tok := p.peek()
	if tok.Tp == tokType {
		p.next()
		return tok, true
	}
	if tok.Tp == tokIdentifier && strings.EqualFold(tok.Lit, keyword) {
		p.next()
		return tok, true
	}
	return tok, false
}

// peekKeyword returns true if the next token matches the given token type OR is
// an identifier with the given keyword string (case-insensitive). Does not consume.
func (p *HandParser) peekKeyword(tokType int, keyword string) bool {
	tok := p.peek()
	return tok.Tp == tokType || (tok.Tp == tokIdentifier && strings.EqualFold(tok.Lit, keyword))
}

// acceptIfNotExists consumes IF NOT EXISTS if present, returning true if found.
// Consolidates the 3-line pattern: accept(tokIf) → expect(tokNot) → expect(tokExists).
func (p *HandParser) acceptIfNotExists() bool {
	if _, ok := p.accept(tokIf); ok {
		p.expect(tokNot)
		p.expect(tokExists)
		return true
	}
	return false
}

// acceptIfExists consumes IF EXISTS if present, returning true if found.
// Consolidates the 2-line pattern: accept(tokIf) → expect(tokExists).
func (p *HandParser) acceptIfExists() bool {
	if _, ok := p.accept(tokIf); ok {
		p.expect(tokExists)
		return true
	}
	return false
}

// isConstraintToken returns true if the token type represents a constraint keyword.
// Consolidates the 10-way disjunction used in ADD COLUMN/CONSTRAINT parsing.
func isConstraintToken(tp int) bool {
	switch tp {
	case tokConstraint, tokPrimary, tokKey, tokIndex,
		tokUnique, tokForeign, tokFulltext, tokCheck,
		tokVector, tokColumnar:
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

// parseUint64 parses and returns an unsigned 64-bit integer literal.
func (p *HandParser) parseUint64() uint64 {
	tok := p.next()
	if tok.Tp != tokIntLit {
		p.error(tok.Offset, "expected integer literal, got %d", tok.Tp)
		return 0
	}
	return tokenItemToUint64(tok.Item)
}

// tokenItemToUint64 converts a token's Item (which may be int64 or uint64) to uint64.
// This is a common pattern throughout the parser when handling integer literal tokens.
func tokenItemToUint64(item any) uint64 {
	if v, ok := item.(uint64); ok {
		return v
	}
	if v, ok := item.(int64); ok {
		return uint64(v)
	}
	return 0
}

// tokenItemToInt64 converts a token's Item (which may be int64 or uint64) to int64.
func tokenItemToInt64(item any) (int64, bool) {
	if v, ok := item.(int64); ok {
		return v, true
	}
	if v, ok := item.(uint64); ok {
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	}
	return 0, true
}

// parseInt64 parses the next token as a signed int64, reporting an error if out of range.
func (p *HandParser) parseInt64() (int64, bool) {
	tok := p.next()
	if tok.Tp != tokIntLit {
		p.error(tok.Offset, "expected integer literal")
		return 0, false
	}
	v, ok := tokenItemToInt64(tok.Item)
	if !ok {
		p.error(tok.Offset, "integer value is out of range")
		return 0, false
	}
	return v, true
}

// isIdentLike returns true if the token can be used as an identifier.
// In MySQL, most keywords are non-reserved and can appear as identifiers.
// Tokens with types > 0xFF are keyword tokens from the lexer.
func isIdentLike(tp int) bool {
	// Exclude literals
	if tp == tokIntLit || tp == tokFloatLit || tp == tokDecLit || tp == tokHexLit || tp == tokBitLit || tp == tokBitAnd {
		return false
	}
	// Exclude builtin function tokens (58125-58152 in goyacc).
	// These can only appear as function names (e.g. BIT_AND, COUNT, MAX, SUM),
	// never as identifiers.
	if tp >= 58125 && tp <= 58152 {
		return false
	}
	if tp == tokIdentifier || tp == tokStringLit {
		return true
	}
	if tp > 0xFF {
		return !IsReserved(tp)
	}
	return false
}

// parseColumnName parses a column name identifier, potentially qualified (col, tbl.col, db.tbl.col).
func (p *HandParser) parseColumnName() *ast.ColumnName {
	tok := p.next()
	if !isIdentLike(tok.Tp) {
		return nil
	}
	name := ast.NewCIStr(tok.Lit)

	col := Alloc[ast.ColumnName](p.arena)
	col.Name = name

	// Check for qualifications
	if p.peek().Tp == '.' {
		p.next() // consume '.'
		tok2 := p.next()
		if !isIdentLike(tok2.Tp) && tok2.Tp != '*' {
			p.error(tok2.Offset, "expected identifier after .")
			return nil
		}

		if tok2.Tp == '*' {
			// tbl.*
			col.Table = col.Name
			col.Name = ast.NewCIStr("*")
			return col
		}

		name2 := ast.NewCIStr(tok2.Lit)

		if p.peek().Tp == '.' {
			p.next() // consume second '.'
			tok3 := p.next()
			if !isIdentLike(tok3.Tp) && tok3.Tp != '*' {
				p.error(tok3.Offset, "expected identifier after .")
				return nil
			}

			if tok3.Tp == '*' {
				// db.tbl.*
				col.Schema = col.Name
				col.Table = name2
				col.Name = ast.NewCIStr("*")
				return col
			}

			// db.tbl.col
			col.Schema = col.Name
			col.Table = name2
			col.Name = ast.NewCIStr(tok3.Lit)
		} else {
			// tbl.col
			col.Table = col.Name
			col.Name = name2
		}
	}
	return col
}

// wrap string to pointer for AST fields
func sptr(s string) *string {
	return &s
}

// wrap uint64 to pointer for AST fields
func uptr(u uint64) *uint64 {
	return &u
}

// parseStringValue parses a string, bit or hex literal and returns its string value.
func (p *HandParser) parseStringValue() string {
	tok := p.next()
	switch tok.Tp {
	case tokStringLit:
		return tok.Lit
	case tokBitLit, tokHexLit:
		if bl, ok := tok.Item.(ast.BinaryLiteral); ok {
			return bl.ToString()
		}
		return tok.Lit
	default:
		return tok.Lit
	}
}

// parseHostname parses the @host portion of user@host or role@host identifiers.
// Handles three cases:
//  1. tokAt followed by identifier/string → user @ host as separate tokens
//  2. tokSingleAtIdent → lexer combined @host into one token
//  3. no @ → default to '%'
func (p *HandParser) parseHostname() string {
	if _, ok := p.accept(tokAt); ok {
		hostTok := p.next()
		if hostTok.Tp == '%' {
			return "%"
		}
		return strings.ToLower(hostTok.Lit)
	}
	if tok, ok := p.accept(tokSingleAtIdentifier); ok {
		host := tok.Lit
		if len(host) > 0 && host[0] == '@' {
			host = host[1:]
		}

		// Strip quotes if present (e.g. 'localhost' -> localhost)
		if len(host) >= 2 && (host[0] == '\'' || host[0] == '"' || host[0] == '`') {
			if host[0] == host[len(host)-1] {
				host = host[1 : len(host)-1]
			}
		}
		return strings.ToLower(host)
	}
	return "%"
}

// parseUserIdentity parses user@host or CURRENT_USER or USER()
func (p *HandParser) parseUserIdentity() *auth.UserIdentity {
	user := Alloc[auth.UserIdentity](p.arena)

	if _, ok := p.accept(tokCurrentUser); ok {
		user.CurrentUser = true
		if _, ok := p.accept('('); ok {
			p.accept(')')
		}
		return user
	} else if p.peek().Tp == tokUser && p.peekN(1).Tp == '(' {
		p.next() // consume USER
		p.next() // consume (
		p.expect(')')
		user.CurrentUser = true
		return user
	}

	// Username: identifier or string literal
	if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
		user.Username = tok.Lit
	} else {
		return nil
	}

	user.Hostname = p.parseHostname()
	return user
}

// parseRoleIdentity parses a role identifier
func (p *HandParser) parseRoleIdentity() *auth.RoleIdentity {
	role := Alloc[auth.RoleIdentity](p.arena)
	if tok, ok := p.expectAny(tokStringLit, tokIdentifier); ok {
		role.Username = tok.Lit
	} else {
		return nil
	}

	role.Hostname = p.parseHostname()
	return role
}

// parseLinesClause parses LINES [STARTING BY 'str'] [TERMINATED BY 'str'].
// Shared between LOAD DATA and SELECT INTO OUTFILE.
func (p *HandParser) parseLinesClause() *ast.LinesClause {
	lines := Alloc[ast.LinesClause](p.arena)
	for {
		if _, ok := p.accept(tokStarting); ok {
			p.expect(tokBy)
			lines.Starting = sptr(p.parseStringValue())
		} else if _, ok := p.accept(tokTerminated); ok {
			p.expect(tokBy)
			lines.Terminated = sptr(p.parseStringValue())
		} else {
			break
		}
	}
	return lines
}

// parseFieldsClause parses FIELDS/COLUMNS [TERMINATED BY ...] [[OPTIONALLY] ENCLOSED BY ...]
// [ESCAPED BY ...] and, in loadDataMode, [DEFINED NULL BY ... [OPTIONALLY ENCLOSED]].
// In loadDataMode, separator values are validated to be single-character (or "\\"),
// producing a [parser:1083] error on violation.
// Shared between LOAD DATA and SELECT INTO OUTFILE.
func (p *HandParser) parseFieldsClause(loadDataMode bool) *ast.FieldsClause {
	fields := Alloc[ast.FieldsClause](p.arena)
	for {
		if _, ok := p.accept(tokTerminated); ok {
			p.expect(tokBy)
			fields.Terminated = sptr(p.parseStringValue())
		} else if _, ok := p.accept(tokOptionallyEnclosedBy); ok {
			// Scanner fuses OPTIONALLY ENCLOSED BY into one token.
			val := p.parseStringValue()
			if loadDataMode && !p.isValidFieldSep(val) {
				return nil
			}
			fields.Enclosed = sptr(val)
			fields.OptEnclosed = true
		} else if _, ok := p.accept(tokOptionally); ok {
			p.expect(tokEnclosed)
			p.expect(tokBy)
			val := p.parseStringValue()
			if loadDataMode && !p.isValidFieldSep(val) {
				return nil
			}
			fields.Enclosed = sptr(val)
			fields.OptEnclosed = true
		} else if _, ok := p.accept(tokEnclosed); ok {
			p.expect(tokBy)
			val := p.parseStringValue()
			if loadDataMode && !p.isValidFieldSep(val) {
				return nil
			}
			fields.Enclosed = sptr(val)
		} else if _, ok := p.accept(tokEscaped); ok {
			p.expect(tokBy)
			val := p.parseStringValue()
			if loadDataMode && !p.isValidFieldSep(val) {
				return nil
			}
			fields.Escaped = sptr(val)
		} else if loadDataMode {
			if _, ok := p.accept(tokDefined); ok {
				// DEFINED NULL BY 'val' [OPTIONALLY ENCLOSED]
				p.expect(tokNull)
				p.expect(tokBy)
				fields.DefinedNullBy = sptr(p.parseStringValue())
				if _, ok := p.accept(tokOptionallyEnclosedBy); ok {
					fields.NullValueOptEnclosed = true
				} else if _, ok := p.accept(tokOptionally); ok {
					p.accept(tokEnclosed) // OPTIONALLY ENCLOSED
					fields.NullValueOptEnclosed = true
				}
			} else {
				break
			}
		} else {
			break
		}
	}
	return fields
}

// isValidFieldSep validates a LOAD DATA field separator value.
// Returns true if valid (single-char or "\\"), false and records [parser:1083] error otherwise.
func (p *HandParser) isValidFieldSep(val string) bool {
	if val != "\\" && len(val) > 1 {
		p.errs = append(p.errs, fmt.Errorf("[parser:1083]Field separator argument is not what is expected; check the manual"))
		return false
	}
	return true
}

// parseStatsTablesAndPartitions parses table list with optional PARTITION clause,
// shared between LOCK STATS and UNLOCK STATS.
func (p *HandParser) parseStatsTablesAndPartitions() []*ast.TableName {
	var tables []*ast.TableName
	for {
		tbl := p.parseTableName()
		tables = append(tables, tbl)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	// Optional PARTITION clause
	if _, ok := p.accept(tokPartition); ok {
		p.expect('(')
		for {
			partTok := p.next()
			tbl := tables[len(tables)-1]
			tbl.PartitionNames = append(tbl.PartitionNames, ast.NewCIStr(partTok.Lit))
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}
	return tables
}

// parseStringOrUserVarList parses a comma-separated list of string literals and/or @user_variables,
// shared between CREATE BINDING (PlanDigests) and DROP BINDING (SQLDigests).
func (p *HandParser) parseStringOrUserVarList() []*ast.StringOrUserVar {
	var list []*ast.StringOrUserVar
	for {
		souv := &ast.StringOrUserVar{}
		if p.peek().Tp == tokStringLit {
			souv.StringLit = p.next().Lit
		} else if p.peek().Tp == tokSingleAtIdent {
			tok := p.next()
			name := tok.Lit
			if len(name) > 0 && name[0] == '@' {
				name = name[1:]
			}
			ve := &ast.VariableExpr{Name: name, IsGlobal: false, IsSystem: false}
			souv.UserVar = ve
		} else {
			break
		}
		list = append(list, souv)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return list
}

// expectFromOrIn consumes FROM or IN (mandatory). Used for SHOW ... FROM|IN patterns.
func (p *HandParser) expectFromOrIn() {
	if _, ok := p.accept(tokFrom); !ok {
		p.expect(tokIn)
	}
}

// acceptFromOrIn optionally consumes FROM or IN. Returns (dbName, true) if found.
func (p *HandParser) acceptFromOrIn() (string, bool) {
	if _, ok := p.accept(tokFrom); ok {
		return p.next().Lit, true
	}
	if _, ok := p.accept(tokIn); ok {
		return p.next().Lit, true
	}
	return "", false
}

// isRoleStatement peeks ahead through a comma-separated identifier list to detect
// whether this is a role statement (terminated by `terminator`, e.g. tokTo/tokFrom)
// vs a privilege statement (terminated by tokOn).
// Used to distinguish GRANT role vs GRANT privilege, and REVOKE role vs REVOKE privilege.
func (p *HandParser) isRoleStatement(terminator int) bool {
	for i := 0; ; i++ {
		tok := p.peekN(i)
		if tok.Tp == terminator {
			return true
		}
		if tok.Tp == tokOn || tok.Tp == 0 || tok.Tp == ';' {
			return false
		}
	}
}

// parseObjectType parses an optional object type: TABLE | FUNCTION | PROCEDURE.
// Shared between GRANT and REVOKE.
func (p *HandParser) parseObjectType() ast.ObjectTypeType {
	switch p.peek().Tp {
	case tokTable:
		p.next()
		return ast.ObjectTypeTable
	case tokFunction:
		p.next()
		return ast.ObjectTypeFunction
	case tokProcedure:
		p.next()
		return ast.ObjectTypeProcedure
	default:
		return ast.ObjectTypeNone
	}
}

// parseProxyLevel checks if the privilege list contains PROXY, and if so,
// parses the user spec as a grant level. Returns (level, true) if PROXY was handled.
// Shared between GRANT and REVOKE.
func (p *HandParser) parseProxyLevel(privs []*ast.PrivElem, stmtName string) (*ast.GrantLevel, bool) {
	for _, priv := range privs {
		if priv.Priv == mysql.ExtendedPriv && strings.ToUpper(priv.Name) == "PROXY" {
			spec := p.parseUserSpec()
			if spec == nil {
				p.error(0, "Expected user specification for %s PROXY", stmtName)
				return nil, true
			}
			level := Alloc[ast.GrantLevel](p.arena)
			level.Level = ast.GrantLevelUser
			level.DBName = spec.User.Username
			level.TableName = spec.User.Hostname
			return level, true
		}
	}
	return nil, false
}

// parseUserSpecList parses a comma-separated list of user specifications.
// Shared between GRANT ... TO and REVOKE ... FROM.
func (p *HandParser) parseUserSpecList() []*ast.UserSpec {
	var list []*ast.UserSpec
	for {
		spec := p.parseUserSpec()
		if spec == nil {
			return nil
		}
		list = append(list, spec)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return list
}

// parseIdentList parses a comma-separated list of identifiers (or string literals)
// and returns them as []ast.CIStr. Used for partition names, index names, etc.
func (p *HandParser) parseIdentList() []ast.CIStr {
	var list []ast.CIStr
	for {
		tok, ok := p.expectAny(tokIdentifier, tokStringLit)
		if !ok {
			break
		}
		list = append(list, ast.NewCIStr(tok.Lit))
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return list
}

// acceptNoWriteToBinlog accepts the optional NO_WRITE_TO_BINLOG or LOCAL keyword.
// Returns true if either was consumed.
func (p *HandParser) acceptNoWriteToBinlog() bool {
	if _, ok := p.accept(tokNoWriteToBinLog); ok {
		return true
	}
	if _, ok := p.accept(tokLocal); ok {
		return true
	}
	return false
}

// parsePartitionLessThanExpr parses: PARTITION LESS THAN '(' expr ')'
// and sets spec.Partition to a new PartitionOptions with the parsed expression.
func (p *HandParser) parsePartitionLessThanExpr(spec *ast.AlterTableSpec) {
	p.expect(tokPartition)
	p.expect(tokLess)
	p.expect(tokThan)
	p.expect('(')
	expr := p.parseExpression(0)
	p.expect(')')
	spec.Partition = Alloc[ast.PartitionOptions](p.arena)
	spec.Partition.Expr = expr
}

// parseSimpleColumnNameList parses a comma-separated list of simple (non-dotted) column names.
// Returns (list, true) on success; returns (nil, false) if a dotted name like t.c1 is encountered.
func (p *HandParser) parseSimpleColumnNameList() ([]ast.CIStr, bool) {
	var list []ast.CIStr
	for {
		tok := p.next()
		if p.peek().Tp == '.' {
			return nil, false
		}
		list = append(list, ast.NewCIStr(tok.Lit))
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return list, true
}

// acceptRestrictOrCascade consumes optional RESTRICT or CASCADE keywords.
func (p *HandParser) acceptRestrictOrCascade() {
	p.accept(tokRestrict)
	p.accept(tokCascade)
}
