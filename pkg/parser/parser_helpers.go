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

// parser_helpers.go contains shared utility functions and literal/identifier
// parsing helpers used across the hand-written parser files.

import (
	"math"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// parseUint64 parses and returns an unsigned 64-bit integer literal.
func (p *HandParser) parseUint64() uint64 {
	tok := p.next()
	if tok.Tp != intLit {
		p.syntaxErrorAt(tok)
		return 0
	}
	return tokenItemToUint64(tok.Item)
}

// tokenItemToUint64 converts a token's Item (which may be int64 or uint64) to uint64.
// This is a common pattern throughout the parser when handling integer literal tokens.
func tokenItemToUint64(item interface{}) uint64 {
	if v, ok := item.(uint64); ok {
		return v
	}
	if v, ok := item.(int64); ok {
		return uint64(v)
	}
	return 0
}

// tokenItemToInt64 converts a token's Item (which may be int64 or uint64) to int64.
func tokenItemToInt64(item interface{}) (int64, bool) {
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
	if tok.Tp != intLit {
		p.syntaxErrorAt(tok)
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
	if tp == intLit || tp == floatLit || tp == decLit || tp == hexLit || tp == bitLit || tp == builtinBitAnd {
		return false
	}
	// Exclude builtin function tokens (builtinApproxCountDistinct-builtinVarSamp).
	// These can only appear as function names (e.g. BIT_AND, COUNT, MAX, SUM),
	// never as identifiers.
	if tp >= builtinApproxCountDistinct && tp <= builtinVarSamp {
		return false
	}
	if tp == identifier || tp == stringLit {
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
	if !isIdentLike(tok.Tp) || tok.Tp == stringLit {
		p.syntaxErrorAt(tok)
		return nil
	}
	name := ast.NewCIStr(tok.Lit)

	col := p.arena.AllocColumnName()
	col.Name = name

	// Check for qualifications
	if p.peek().Tp == '.' {
		p.next() // consume '.'
		tok2 := p.next()
		if !isIdentLike(tok2.Tp) && tok2.Tp != '*' {
			p.syntaxErrorAt(tok2)
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
				p.syntaxErrorAt(tok3)
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
	case stringLit:
		return tok.Lit
	case bitLit, hexLit:
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
//  1. optional followed by identifier/string → user @ host as separate tokens
//  2. singleAtIdentifier → lexer combined @host into one token
//  3. no @ → default to '%'
func (p *HandParser) parseHostname() string {
	if _, ok := p.accept(optional); ok {
		hostTok := p.next()
		if hostTok.Tp == '%' {
			return "%"
		}
		return strings.ToLower(hostTok.Lit)
	}
	if tok, ok := p.accept(singleAtIdentifier); ok {
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
	authUser := p.arena.AllocUserIdentity()

	if _, ok := p.accept(currentUser); ok {
		authUser.CurrentUser = true
		if _, ok := p.accept('('); ok {
			p.accept(')')
		}
		return authUser
	} else if p.peek().Tp == user && p.peekN(1).Tp == '(' {
		p.next() // consume USER
		p.next() // consume (
		p.expect(')')
		authUser.CurrentUser = true
		return authUser
	}

	// Username: identifier or string literal (restricted — bare unreserved keywords not accepted).
	tok, ok := p.expectAny(identifier, stringLit)
	if !ok {
		return nil
	}
	authUser.Username = tok.Lit

	authUser.Hostname = p.parseHostname()
	return authUser
}

// parseRoleIdentity parses a role identifier
func (p *HandParser) parseRoleIdentity() *auth.RoleIdentity {
	role := Alloc[auth.RoleIdentity](p.arena)
	tok, ok := p.expectAny(stringLit, identifier)
	if !ok {
		return nil
	}
	role.Username = tok.Lit

	role.Hostname = p.parseHostname()
	return role
}

// parseLinesClause parses LINES [STARTING BY 'str'] [TERMINATED BY 'str'].
// Shared between LOAD DATA and SELECT INTO OUTFILE.
func (p *HandParser) parseLinesClause() *ast.LinesClause {
	lines := Alloc[ast.LinesClause](p.arena)
	for {
		if _, ok := p.accept(starting); ok {
			p.expect(by)
			lines.Starting = sptr(p.parseStringValue())
		} else if _, ok := p.accept(terminated); ok {
			p.expect(by)
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
		if _, ok := p.accept(terminated); ok {
			p.expect(by)
			fields.Terminated = sptr(p.parseStringValue())
		} else if _, ok := p.accept(optionallyEnclosedBy); ok {
			// Scanner fuses OPTIONALLY ENCLOSED BY into one token.
			val := p.parseStringValue()
			if loadDataMode && !p.isValidFieldSep(val) {
				return nil
			}
			fields.Enclosed = sptr(val)
			fields.OptEnclosed = true
		} else if _, ok := p.accept(optionally); ok {
			p.expect(enclosed)
			p.expect(by)
			val := p.parseStringValue()
			if loadDataMode && !p.isValidFieldSep(val) {
				return nil
			}
			fields.Enclosed = sptr(val)
			fields.OptEnclosed = true
		} else if _, ok := p.accept(enclosed); ok {
			p.expect(by)
			val := p.parseStringValue()
			if loadDataMode && !p.isValidFieldSep(val) {
				return nil
			}
			fields.Enclosed = sptr(val)
		} else if _, ok := p.accept(escaped); ok {
			p.expect(by)
			val := p.parseStringValue()
			if loadDataMode && !p.isValidFieldSep(val) {
				return nil
			}
			fields.Escaped = sptr(val)
		} else if loadDataMode {
			if _, ok := p.accept(defined); !ok {
				break
			}
			// DEFINED NULL BY 'val' [OPTIONALLY ENCLOSED]
			p.expect(null)
			p.expect(by)
			fields.DefinedNullBy = sptr(p.parseStringValue())
			if _, ok := p.accept(optionallyEnclosedBy); ok {
				fields.NullValueOptEnclosed = true
			} else if _, ok := p.accept(optionally); ok {
				p.accept(enclosed) // OPTIONALLY ENCLOSED
				fields.NullValueOptEnclosed = true
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
		p.errs = append(p.errs, ErrWrongFieldTerminators.GenWithStackByArgs())
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
	if _, ok := p.accept(partition); ok {
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
		if p.peek().Tp == stringLit {
			souv.StringLit = p.next().Lit
		} else if p.peek().Tp == singleAtIdentifier {
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
	if _, ok := p.accept(from); !ok {
		p.expect(in)
	}
}

// acceptFromOrIn optionally consumes FROM or IN. Returns (dbName, true) if found.
func (p *HandParser) acceptFromOrIn() (string, bool) {
	if _, ok := p.accept(from); ok {
		return p.next().Lit, true
	}
	if _, ok := p.accept(in); ok {
		return p.next().Lit, true
	}
	return "", false
}

// isRoleStatement peeks ahead through a comma-separated identifier list to detect
// whether this is a role statement (terminated by `terminator`, e.g. to/from)
// vs a privilege statement (terminated by on).
// Used to distinguish GRANT role vs GRANT privilege, and REVOKE role vs REVOKE privilege.
func (p *HandParser) isRoleStatement(terminator int) bool {
	for i := 0; ; i++ {
		tok := p.peekN(i)
		if tok.Tp == terminator {
			return true
		}
		if tok.Tp == on || tok.Tp == 0 || tok.Tp == ';' {
			return false
		}
	}
}

// parseObjectType parses an optional object type: TABLE | FUNCTION | PROCEDURE.
// Shared between GRANT and REVOKE.
func (p *HandParser) parseObjectType() ast.ObjectTypeType {
	switch p.peek().Tp {
	case tableKwd:
		p.next()
		return ast.ObjectTypeTable
	case function:
		p.next()
		return ast.ObjectTypeFunction
	case procedure:
		p.next()
		return ast.ObjectTypeProcedure
	default:
		return ast.ObjectTypeNone
	}
}

// parseProxyLevel checks if the privilege list contains PROXY, and if so,
// parses the user spec as a grant level. Returns (level, true) if PROXY was handled.
// Shared between GRANT and REVOKE.
func (p *HandParser) parseProxyLevel(privs []*ast.PrivElem, _ string) (*ast.GrantLevel, bool) {
	for _, priv := range privs {
		if priv.Priv == mysql.ExtendedPriv && strings.ToUpper(priv.Name) == "PROXY" {
			spec := p.parseUserSpec()
			if spec == nil {
				p.syntaxErrorAt(p.peek())
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
		tok, ok := p.expectIdentLike()
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
	if _, ok := p.accept(noWriteToBinLog); ok {
		return true
	}
	if _, ok := p.accept(local); ok {
		return true
	}
	return false
}

// parsePartitionLessThanExpr parses: PARTITION LESS THAN '(' expr ')'
// and sets spec.Partition to a new PartitionOptions with the parsed expression.
// Also captures the OriginalText on the embedded PartitionMethod so that the
// DDL executor can locate and replace the syntactic sugar in the query string.
// preStartOff, if >= 0, overrides the default start offset for OriginalText
// (used to include preceding keywords like FIRST/LAST/MERGE in the captured text).
func (p *HandParser) parsePartitionLessThanExpr(spec *ast.AlterTableSpec) {
	p.parsePartitionLessThanExprFrom(spec, p.peek().Offset)
}

func (p *HandParser) parsePartitionLessThanExprFrom(spec *ast.AlterTableSpec, startOff int) {
	p.expect(partition)
	p.expect(less)
	p.expect(than)
	p.expect('(')
	expr := p.parseExpression(0)
	p.expect(')')
	endOff := p.peek().Offset
	spec.Partition = Alloc[ast.PartitionOptions](p.arena)
	spec.Partition.Expr = expr
	// Set OriginalText on the PartitionMethod so DDL executor can find it
	// in the query string for syntactic sugar replacement.
	if endOff > startOff && endOff <= len(p.src) {
		spec.Partition.PartitionMethod.SetText(p.connectionEncoding, p.src[startOff:endOff])
	}
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
	p.accept(restrict)
	p.accept(cascade)
}

// acceptStringName implements the 'StringName' rule logic from parser.y:
// it accepts either an identifier or a stringLiteral.
func (p *HandParser) acceptStringName() (Token, bool) {
	tok := p.peek()
	if tok.Tp == stringLit || isIdentLike(tok.Tp) {
		return p.next(), true
	}
	return Token{}, false
}
