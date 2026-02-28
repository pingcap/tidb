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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
)

// ---------------------------------------------------------------------------
// SET statement
// ---------------------------------------------------------------------------

// parseSetStmt parses SET statements.
func (p *HandParser) parseSetStmt() ast.StmtNode {
	p.expect(set)

	// Check for One-Shot Statements first

	// SET [GLOBAL|SESSION] TRANSACTION ...
	if p.peek().Tp == transaction {
		return p.parseSetTransaction(false, false)
	}
	if p.peek().Tp == global || p.peek().Tp == session {
		mark := p.lexer.Mark()
		p.next()
		if p.peek().Tp == transaction {
			p.lexer.Restore(mark)
			scope := p.next()
			return p.parseSetTransaction(scope.Tp == global, true)
		}
		p.lexer.Restore(mark)
	}

	// SET ROLE ...
	if p.peek().Tp == role {
		return p.parseSetRole()
	}

	// SET DEFAULT ROLE ...
	if p.peek().Tp == defaultKwd {
		mark := p.lexer.Mark()
		p.next() // consume DEFAULT
		if p.peek().Tp == role {
			return p.parseSetDefaultRole()
		}
		p.lexer.Restore(mark)
	}

	// SET BINDING {ENABLED|DISABLED} FOR ...
	if p.peekKeyword(binding, "BINDING") {
		p.next() // consume BINDING
		return p.parseSetBindingStmt()
	}

	// SET SESSION_STATES 'xxx'
	if p.peek().IsKeyword("SESSION_STATES") {
		p.next() // consume SESSION_STATES
		if p.peek().Tp != stringLit {
			p.syntaxErrorAt(p.peek())
			return nil
		}
		sess := Alloc[ast.SetSessionStatesStmt](p.arena)
		sess.SessionStates = p.next().Lit
		return sess
	}

	// SET RESOURCE GROUP <name>
	if p.peek().Tp == resource {
		p.next() // consume RESOURCE (which is resource)
		p.expect(group)
		stmt := Alloc[ast.SetResourceGroupStmt](p.arena)
		stmt.Name = ast.NewCIStr(p.next().Lit)
		return stmt
	}

	// SET PASSWORD ... → fall back
	if p.peek().Tp == password {
		p.next() // consume PASSWORD
		pwdStmt := Alloc[ast.SetPwdStmt](p.arena)
		if _, ok := p.accept(forKwd); ok {
			pwdStmt.User = p.parseUserIdentity()
		}
		p.acceptEqOrAssign()
		// Accept PASSWORD('string') form or bare string literal
		if _, ok := p.accept(password); ok {
			p.expect('(')
			if tok, ok := p.expect(stringLit); ok {
				pwdStmt.Password = tok.Lit
			}
			p.expect(')')
		} else {
			pwdStmt.Password = p.next().Lit
		}
		return pwdStmt
	}

	// SET CONFIG ...
	if p.peek().Tp == config {
		return p.parseSetConfig()
	}

	// Generic SET Loop (Variable Assignments, NAMES, CHARSET)
	stmt := Alloc[ast.SetStmt](p.arena)
	var variables []*ast.VariableAssignment

	for {
		// SET NAMES ...
		if _, ok := p.accept(names); ok {
			variables = append(variables, p.parseSetNamesAssignment()...)
		} else if p.peek().Tp == character || p.peek().Tp == charType || p.peek().Tp == charsetKwd {
			// SET CHARACTER SET / SET CHAR SET / SET CHARSET
			if p.next().Tp != charsetKwd {
				p.expect(set) // CHARACTER and CHAR require SET after them
			}
			variables = append(variables, p.parseSetCharsetAssignment()...)
		} else {
			// standard variable assignment
			va := p.parseVariableAssignment()
			if va == nil {
				return nil
			}
			variables = append(variables, va)
		}

		if _, ok := p.accept(','); !ok {
			break
		}
	}

	stmt.Variables = variables
	return stmt
}

// parseVariableAssignment parses a single variable assignment.
func (p *HandParser) parseVariableAssignment() *ast.VariableAssignment {
	va := p.arena.AllocVariableAssignment()

	switch p.peek().Tp {
	case singleAtIdentifier:
		// @user_var = expr
		tok := p.next()
		// The lexer stores @var literals in Item, not Lit.
		va.Name = strings.TrimPrefix(tok.Item.(string), "@")
		if !p.acceptEqOrAssign() {
			return nil
		}
		va.Value = p.parseExpression(precNone)
		if va.Value == nil {
			return nil
		}
		return va

	case doubleAtIdentifier:
		// @@[global.|session.|local.|instance.]sysvar = expr
		tok := p.next()
		// The lexer stores @@var literals in Item, not Lit.
		v := strings.ToLower(tok.Item.(string))
		if strings.HasPrefix(v, "@@global.") {
			va.IsGlobal = true
			v = strings.TrimPrefix(v, "@@global.")
		} else if strings.HasPrefix(v, "@@instance.") {
			va.IsInstance = true
			v = strings.TrimPrefix(v, "@@instance.")
		} else if strings.HasPrefix(v, "@@session.") {
			v = strings.TrimPrefix(v, "@@session.")
		} else if strings.HasPrefix(v, "@@local.") {
			v = strings.TrimPrefix(v, "@@local.")
		} else {
			v = strings.TrimPrefix(v, "@@")
		}
		va.Name = v
		va.IsSystem = true
		if !p.acceptEqOrAssign() {
			return nil
		}
		va.Value = p.parseSetExpr()
		return va

	case global:
		p.next()
		return p.parseSystemVariableAssignment(true, false)

	case session, local:
		p.next()
		return p.parseSystemVariableAssignment(false, false)

	case instance:
		p.next()
		return p.parseSystemVariableAssignment(false, true)

	default:
		// Bare identifier = system variable
		return p.parseSystemVariableAssignment(false, false)
	}
}

func (p *HandParser) parseSystemVariableAssignment(isGlobal, isInstance bool) *ast.VariableAssignment {
	va := p.arena.AllocVariableAssignment()
	va.IsSystem = true
	va.IsGlobal = isGlobal
	va.IsInstance = isInstance

	va.Name = p.parseVariableName()
	if va.Name == "" {
		return nil
	}
	if !p.acceptEqOrAssign() {
		return nil
	}
	va.Value = p.parseSetExpr()
	return va
}

// parseVariableName parses: Identifier ['.' Identifier]
// Variable names can be keywords (e.g., SESSION_STATES, ISOLATION).
func (p *HandParser) parseVariableName() string {
	tok := p.peek()
	if !isIdentLike(tok.Tp) {
		return ""
	}
	p.next()
	name := tok.Lit
	if _, ok := p.accept('.'); ok {
		tok2 := p.next()
		name = name + "." + tok2.Lit
	}
	return name
}

// acceptEqOrAssign accepts '=' or ':='.
func (p *HandParser) acceptEqOrAssign() bool {
	if _, ok := p.accept(eq); ok {
		return true
	}
	if _, ok := p.accept(assignmentEq); ok {
		return true
	}
	return false
}

// parseSetExpr parses the RHS of a SET variable assignment.
// Handles ON, OFF, DEFAULT, BINARY (as charset string), and general expressions.
func (p *HandParser) parseSetExpr() ast.ExprNode {
	if _, ok := p.accept(defaultKwd); ok {
		return p.arena.AllocDefaultExpr()
	}
	// BINARY as a SET value means the charset name 'BINARY' (string)
	if p.peek().Tp == binaryType {
		p.next()
		return p.newValueExpr("BINARY")
	}
	// ON and BINARY are special-cased as string ValueExpr (matching yacc).
	// OFF and ALL are NOT special-cased: they fall through to parseExpression
	// which produces ColumnNameExpr, matching yacc's ExprOrDefault path.
	if p.peek().Tp == on {
		p.next()
		return p.newValueExpr("ON")
	}
	return p.parseExpression(precNone)
}

// parseSetNamesAssignment returns assignments for: NAMES charset [COLLATE collation | DEFAULT]
func (p *HandParser) parseSetNamesAssignment() []*ast.VariableAssignment {
	va := p.arena.AllocVariableAssignment()
	va.Name = ast.SetNames

	if _, ok := p.accept(defaultKwd); ok {
		va.Value = p.arena.AllocDefaultExpr()
	} else {
		tok := p.next()
		// Validate charset name at parse time (yacc CharsetName rule).
		cs, err := charset.GetCharsetInfo(tok.Lit)
		if err != nil {
			p.errs = append(p.errs, ast.ErrUnknownCharacterSet.GenWithStackByArgs(tok.Lit))
			return nil
		}
		va.Value = ast.NewValueExpr(cs.Name, "", "")
		if _, ok := p.accept(collate); ok {
			if _, ok := p.accept(defaultKwd); !ok {
				colTok := p.next()
				va.ExtendValue = ast.NewValueExpr(colTok.Lit, "", "")
			}
		}
	}
	return []*ast.VariableAssignment{va}
}

// parseSetCharsetAssignment returns assignments for: {charset_name | DEFAULT}
// Used for both SET CHARACTER SET and SET CHARSET — caller already consumed the keyword tokens.
func (p *HandParser) parseSetCharsetAssignment() []*ast.VariableAssignment {
	va := p.arena.AllocVariableAssignment()
	va.Name = ast.SetCharset

	if _, ok := p.accept(defaultKwd); ok {
		va.Value = p.arena.AllocDefaultExpr()
	} else {
		tok := p.next()
		// Validate charset name at parse time (yacc CharsetName rule).
		cs, err := charset.GetCharsetInfo(tok.Lit)
		if err != nil {
			p.errs = append(p.errs, ast.ErrUnknownCharacterSet.GenWithStackByArgs(tok.Lit))
			return nil
		}
		va.Value = ast.NewValueExpr(cs.Name, "", "")
	}
	return []*ast.VariableAssignment{va}
}

// parseSetRole parses: SET ROLE { DEFAULT | NONE | ALL [EXCEPT ...] | role_list }
func (p *HandParser) parseSetRole() ast.StmtNode {
	p.next() // consume ROLE
	stmt := Alloc[ast.SetRoleStmt](p.arena)

	if _, ok := p.accept(defaultKwd); ok {
		stmt.SetRoleOpt = ast.SetRoleDefault
	} else if _, ok := p.accept(none); ok {
		stmt.SetRoleOpt = ast.SetRoleNone
	} else if _, ok := p.accept(all); ok {
		stmt.SetRoleOpt = ast.SetRoleAll
		if _, ok := p.accept(except); ok {
			stmt.SetRoleOpt = ast.SetRoleAllExcept
			stmt.RoleList = p.parseRoleList()
		}
	} else {
		stmt.SetRoleOpt = ast.SetRoleRegular
		stmt.RoleList = p.parseRoleList()
	}
	return stmt
}

func (p *HandParser) parseRoleList() []*auth.RoleIdentity {
	var list []*auth.RoleIdentity
	for {
		// Use parseUserIdentity which parses 'user'@'host'
		user := p.parseUserIdentity()
		if user == nil {
			p.syntaxErrorAt(p.peek())
			return nil
		}
		role := &auth.RoleIdentity{Username: user.Username, Hostname: user.Hostname}
		list = append(list, role)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return list
}

// parseSetDefaultRole parses: SET DEFAULT ROLE { NONE | ALL | role_list } TO user_list
func (p *HandParser) parseSetDefaultRole() ast.StmtNode {
	p.next() // consume ROLE (DEFAULT was consumed by caller)
	stmt := Alloc[ast.SetDefaultRoleStmt](p.arena)

	if _, ok := p.accept(none); ok {
		stmt.SetRoleOpt = ast.SetRoleNone
	} else if _, ok := p.accept(all); ok {
		stmt.SetRoleOpt = ast.SetRoleAll
	} else {
		stmt.SetRoleOpt = ast.SetRoleRegular
		stmt.RoleList = p.parseRoleList()
	}

	p.expect(to)

	// Parse user list (similar to role list but returning *auth.UserIdentity)
	var users []*auth.UserIdentity
	for {
		user := p.parseUserIdentity()
		if user == nil {
			p.syntaxErrorAt(p.peek())
			return nil
		}
		users = append(users, user)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	stmt.UserList = users

	return stmt
}

// parseSetConfig parses: SET CONFIG {Type | "Instance"} Name = Value
func (p *HandParser) parseSetConfig() ast.StmtNode {
	p.next() // consume CONFIG (SET consumed by caller)
	stmt := Alloc[ast.SetConfigStmt](p.arena)

	// Parse Type or Instance
	tok := p.peek()
	if tok.Tp == stringLit {
		stmt.Instance = tok.Lit
		p.next()
	} else if isIdentLike(tok.Tp) {
		stmt.Type = strings.ToLower(p.parseVariableName())
	} else {
		p.syntaxErrorAt(tok)
		return nil
	}

	// Parse Name (e.g. log.level, auto-compaction-mode)
	stmt.Name = p.parseConfigName()
	if stmt.Name == "" {
		p.syntaxErrorAt(p.peek())
		return nil
	}

	p.expectAny(eq, assignmentEq)

	// Parse Value — yacc uses SetExpr which handles ON/BINARY as string values.
	stmt.Value = p.parseSetExpr()
	if stmt.Value == nil {
		return nil
	}

	return stmt
}

// parseConfigName parses configuration names which can contain identifiers, dots, and dashes.
// Uses explicit token type checks to avoid consuming operators like '=' which isIdentLike
// incorrectly treats as identifiers (since eq > 0xFF and is not reserved).
func (p *HandParser) parseConfigName() string {
	var sb strings.Builder
	for {
		tok := p.peek()
		// Accept identifiers, unreserved keywords, and string literals.
		// Exclude operator tokens (assignmentEq=assignmentEq and above) which have
		// Tp > 0xFF but are NOT identifiers/keywords.
		if tok.Tp == stringLit || (tok.Tp >= identifier && tok.Tp < assignmentEq && tok.Lit != "") {
			sb.WriteString(tok.Lit)
			p.next()
		} else if tok.Tp == '.' {
			sb.WriteByte('.')
			p.next()
		} else if tok.Tp == '-' {
			sb.WriteByte('-')
			p.next()
		} else {
			break
		}
	}
	return sb.String()
}

// parseSetTransaction parses:
//
//	[GLOBAL|SESSION already consumed] TRANSACTION { ISOLATION LEVEL level | READ { ONLY | WRITE } } [, ...]
//
// isGlobal: whether GLOBAL scope was specified
// hasScope: whether an explicit scope (GLOBAL/SESSION) was specified
//
// Maps to SET @@[GLOBAL.|SESSION.]tx_isolation / tx_read_only variable assignments.
func (p *HandParser) parseSetTransaction(isGlobal bool, hasScope bool) ast.StmtNode {
	p.expect(transaction)

	stmt := Alloc[ast.SetStmt](p.arena)

	for {
		va := p.arena.AllocVariableAssignment()
		va.IsSystem = true
		va.IsGlobal = isGlobal

		if _, ok := p.accept(isolation); ok {
			// ISOLATION LEVEL { READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE }
			p.expect(level)
			var level string
			if _, ok := p.accept(read); ok {
				if _, ok := p.accept(uncommitted); ok {
					level = "READ-UNCOMMITTED"
				} else {
					p.expect(committed)
					level = "READ-COMMITTED"
				}
			} else if _, ok := p.accept(repeatable); ok {
				p.expect(read)
				level = "REPEATABLE-READ"
			} else if _, ok := p.accept(serializable); ok {
				level = "SERIALIZABLE"
			} else {
				p.syntaxErrorAt(p.peek())
				return nil
			}
			if hasScope {
				va.Name = "tx_isolation"
			} else {
				va.Name = "tx_isolation_one_shot"
			}
			va.Value = p.newValueExpr(level)
		} else {
			// READ { ONLY | WRITE }
			p.expect(read)
			if _, ok := p.accept(write); ok {
				va.Name = "tx_read_only"
				va.Value = p.newValueExpr("0")
			} else {
				p.expect(only)
				// Check for AS OF clause
				if p.peek().Tp == asof {
					p.next()
					p.expect(timestampType)
					va.Name = "tx_read_ts"
					va.Value = p.parseExpression(precNone)
					if va.Value == nil {
						return nil
					}
				} else if p.peek().Tp == as {
					p.next()
					if p.peek().Tp == of {
						p.next()
						p.expect(timestampType)
						va.Name = "tx_read_ts"
						va.Value = p.parseExpression(precNone)
						if va.Value == nil {
							return nil
						}
					} else {
						// Just 'AS' without 'OF'? Should not happen here in valid SQL
						p.syntaxErrorAt(p.peek())
					}
				} else {
					va.Name = "tx_read_only"
					va.Value = p.newValueExpr("1")
				}
			}
		}
		stmt.Variables = append(stmt.Variables, va)
		if _, ok := p.accept(','); !ok {
			break
		}
	}

	return stmt
}

// parseSetBindingStmt parses: BINDING {ENABLED|DISABLED} FOR <stmt> | SQL DIGEST <digest>
// SET and BINDING already consumed.
func (p *HandParser) parseSetBindingStmt() ast.StmtNode {
	stmt := Alloc[ast.SetBindingStmt](p.arena)

	// ENABLED or DISABLED
	tok := p.next()
	if tok.IsKeyword("ENABLED") {
		stmt.BindingStatusType = ast.BindingStatusTypeEnabled
	} else if tok.IsKeyword("DISABLED") {
		stmt.BindingStatusType = ast.BindingStatusTypeDisabled
	} else {
		p.syntaxErrorAt(tok)
		return nil
	}

	// FOR
	p.expect(forKwd)

	// SQL DIGEST 'digest_string' or <statement>
	if p.peekKeyword(sql, "SQL") {
		m := p.mark()
		p.next() // consume SQL
		if p.peek().IsKeyword("DIGEST") {
			p.next() // consume DIGEST
			if tok, ok := p.expect(stringLit); ok {
				stmt.SQLDigest = tok.Lit
			}
			return stmt
		}
		p.restore(m)
	}

	// Parse the origin statement
	originStmt := p.parseAndSetText()
	if originStmt == nil {
		return nil
	}
	stmt.OriginNode = originStmt

	// Optional: USING <hinted_stmt>
	if _, ok := p.accept(using); ok {
		hintedStmt := p.parseAndSetText()
		if hintedStmt == nil {
			return nil
		}
		stmt.HintedNode = hintedStmt
	}

	return stmt
}

// ---------------------------------------------------------------------------
// EXPLAIN / DESCRIBE
// ---------------------------------------------------------------------------

// parseExplainStmt parses EXPLAIN/DESCRIBE/DESC.
func (p *HandParser) parseExplainStmt() ast.StmtNode {
	p.next() // consume EXPLAIN/DESCRIBE/DESC

	stmt := Alloc[ast.ExplainStmt](p.arena)
	stmt.Format = "row"

	if p.peek().Tp == explore {
		p.next()
		stmt.Explore = true
		stmt.Format = "" // Format is empty for EXPLORE
		if tok, ok := p.accept(stringLit); ok {
			stmt.SQLDigest = tok.Lit
			return stmt
		}
	}

	// [ANALYZE]
	if _, ok := p.accept(analyze); ok {
		stmt.Analyze = true
	}

	// [FORMAT = 'format']
	if p.peek().Tp == format {
		p.next()
		p.expectAny(eq, assignmentEq)
		fmt := p.next()
		stmt.Format = fmt.Lit
	}

	// EXPLAIN [FORMAT = ...] FOR CONNECTION N
	if _, ok := p.accept(forKwd); ok {
		// FOR CONNECTION n
		p.expect(connection)
		forStmt := Alloc[ast.ExplainForStmt](p.arena)
		forStmt.Format = stmt.Format
		forStmt.ConnectionID = p.parseUint64()
		return forStmt
	}

	// EXPLAIN [ANALYZE] [FORMAT = ...] 'plan_digest' — string literal = plan digest
	if p.peek().Tp == stringLit {
		stmt.PlanDigest = p.next().Lit
		return stmt
	}

	// EXPLAIN SELECT|INSERT|UPDATE|DELETE|... → parse sub-statement
	subStartOff := p.peek().Offset
	var sub ast.StmtNode
	switch p.peek().Tp {
	case selectKwd, insert, replace, update, deleteKwd,
		alter, create, drop, set, show, with, truncate,
		rename, analyze, load, grant, revoke, tableKwd, values,
		importKwd, '(':
		sub = p.parseStatement()
	default:
		// EXPLAIN|DESCRIBE tablename → maps to SHOW COLUMNS
		if p.peek().Tp == identifier || p.peek().Tp > 0xFF {
			tn := p.parseTableName()
			if tn == nil {
				return nil
			}
			showStmt := p.arena.AllocShowStmt()
			showStmt.Tp = ast.ShowColumns
			showStmt.Table = tn
			// Check for column specifier after table name
			if isIdentLike(p.peek().Tp) {
				showStmt.Column = p.parseColumnName()
			}
			stmt.Stmt = showStmt
			stmt.Format = "" // DESCRIBE form uses empty format, not "row"
			return stmt
		}
		return nil
	}

	if sub == nil {
		return nil
	}
	// For EXPLAIN EXPLORE, set Text on the inner statement (the yacc parser does this).
	// For regular EXPLAIN, do NOT set Text — the binding system relies on empty Text().
	if stmt.Explore {
		endOff := p.peek().Offset
		if p.peek().Tp == EOF {
			endOff = len(p.src)
		}
		sub.SetText(p.connectionEncoding, strings.TrimSpace(p.src[subStartOff:endOff]))
	}
	stmt.Stmt = sub
	return stmt
}

// ---------------------------------------------------------------------------
// LOCK TABLES
// ---------------------------------------------------------------------------

// parseLockTablesStmt parses: LOCK TABLE[S] t1 READ|WRITE [, t2 ...]
func (p *HandParser) parseLockTablesStmt() ast.StmtNode {
	stmt := Alloc[ast.LockTablesStmt](p.arena)
	p.expect(lock)
	// Accept TABLE or TABLES
	if _, ok := p.accept(tables); !ok {
		p.expect(tableKwd)
	}

	// Parse table lock list
	first, ok := p.parseTableLock()
	if !ok {
		return nil
	}
	locks := []ast.TableLock{first}
	for {
		if _, ok := p.accept(','); !ok {
			break
		}
		tl, ok := p.parseTableLock()
		if !ok {
			return nil
		}
		locks = append(locks, tl)
	}
	stmt.TableLocks = locks
	return stmt
}

// parseTableLock parses: tablename READ [LOCAL] | WRITE [LOCAL]
func (p *HandParser) parseTableLock() (ast.TableLock, bool) {
	tn := p.parseTableName()
	if tn == nil {
		return ast.TableLock{}, false
	}
	tl := ast.TableLock{
		Table: tn,
	}
	switch p.peek().Tp {
	case read:
		p.next()
		if _, ok := p.accept(local); ok {
			tl.Type = ast.TableLockReadLocal
		} else {
			tl.Type = ast.TableLockRead
		}
	case write:
		p.next()
		if _, ok := p.accept(local); ok {
			tl.Type = ast.TableLockWriteLocal
		} else {
			tl.Type = ast.TableLockWrite
		}
	}
	return tl, true
}
