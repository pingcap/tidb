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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
)

// ---------------------------------------------------------------------------
// SET statement
// ---------------------------------------------------------------------------

// parseSetStmt parses SET statements.
func (p *HandParser) parseSetStmt() ast.StmtNode {
	p.expect(tokSet)

	// Check for One-Shot Statements first

	// SET [GLOBAL|SESSION] TRANSACTION ...
	if p.peek().Tp == tokTransaction {
		return p.parseSetTransaction(false, false)
	}
	if p.peek().Tp == tokGlobal || p.peek().Tp == tokSession {
		mark := p.lexer.Mark()
		p.next()
		if p.peek().Tp == tokTransaction {
			p.lexer.Restore(mark)
			scope := p.next()
			return p.parseSetTransaction(scope.Tp == tokGlobal, true)
		}
		p.lexer.Restore(mark)
	}

	// SET ROLE ...
	if p.peek().Tp == tokRole {
		return p.parseSetRole()
	}

	// SET DEFAULT ROLE ...
	if p.peek().Tp == tokDefault {
		mark := p.lexer.Mark()
		p.next() // consume DEFAULT
		if p.peek().Tp == tokRole {
			return p.parseSetDefaultRole()
		}
		p.lexer.Restore(mark)
	}

	// SET BINDING {ENABLED|DISABLED} FOR ...
	if p.peekKeyword(tokBinding, "BINDING") {
		p.next() // consume BINDING
		return p.parseSetBindingStmt()
	}

	// SET SESSION_STATES 'xxx'
	if p.peek().IsKeyword("SESSION_STATES") {
		p.next() // consume SESSION_STATES
		if p.peek().Tp != tokStringLit {
			p.error(p.peek().Offset, "SET SESSION_STATES requires a string literal")
			return nil
		}
		sess := Alloc[ast.SetSessionStatesStmt](p.arena)
		sess.SessionStates = p.next().Lit
		return sess
	}

	// SET RESOURCE GROUP <name>
	if p.peek().Tp == tokResourceGroup {
		p.next() // consume RESOURCE (which is tokResourceGroup)
		p.expect(tokGroup)
		stmt := Alloc[ast.SetResourceGroupStmt](p.arena)
		stmt.Name = ast.NewCIStr(p.next().Lit)
		return stmt
	}

	// SET PASSWORD ... → fall back
	if p.peek().Tp == tokPassword {
		p.next() // consume PASSWORD
		pwdStmt := Alloc[ast.SetPwdStmt](p.arena)
		if _, ok := p.accept(tokFor); ok {
			pwdStmt.User = p.parseUserIdentity()
		}
		p.expect(tokEq)
		pwdStmt.Password = p.next().Lit
		return pwdStmt
	}

	// SET CONFIG ...
	if p.peek().Tp == tokConfig {
		return p.parseSetConfig()
	}

	// Generic SET Loop (Variable Assignments, NAMES, CHARSET)
	stmt := Alloc[ast.SetStmt](p.arena)
	var variables []*ast.VariableAssignment

	for {
		// SET NAMES ...
		if _, ok := p.accept(tokNames); ok {
			variables = append(variables, p.parseSetNamesAssignment()...)
		} else if p.peek().Tp == tokCharacter || p.peek().Tp == tokChar || p.peek().Tp == tokCharsetKwd {
			// SET CHARACTER SET / SET CHAR SET / SET CHARSET
			if p.next().Tp != tokCharsetKwd {
				p.expect(tokSet) // CHARACTER and CHAR require SET after them
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
	va := Alloc[ast.VariableAssignment](p.arena)

	switch p.peek().Tp {
	case tokSingleAtIdentifier:
		// @user_var = expr
		tok := p.next()
		// The lexer stores @var literals in Item, not Lit.
		va.Name = strings.TrimPrefix(tok.Item.(string), "@")
		if !p.acceptEqOrAssign() {
			return nil
		}
		va.Value = p.parseExpression(precNone)
		return va

	case tokDoubleAtIdentifier:
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

	case tokGlobal:
		p.next()
		return p.parseSystemVariableAssignment(true, false)

	case tokSession, tokLocal:
		p.next()
		return p.parseSystemVariableAssignment(false, false)

	case tokInstance:
		p.next()
		return p.parseSystemVariableAssignment(false, true)

	default:
		// Bare identifier = system variable
		return p.parseSystemVariableAssignment(false, false)
	}
}

func (p *HandParser) parseSystemVariableAssignment(isGlobal, isInstance bool) *ast.VariableAssignment {
	va := Alloc[ast.VariableAssignment](p.arena)
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
	if _, ok := p.accept(tokEq); ok {
		return true
	}
	if _, ok := p.accept(tokAssignmentEq); ok {
		return true
	}
	return false
}

// parseSetExpr parses the RHS of a SET variable assignment.
// Handles ON, OFF, DEFAULT, BINARY (as charset string), and general expressions.
func (p *HandParser) parseSetExpr() ast.ExprNode {
	if _, ok := p.accept(tokDefault); ok {
		return Alloc[ast.DefaultExpr](p.arena)
	}
	// BINARY as a SET value means the charset name 'BINARY' (string)
	if p.peek().Tp == tokBinary {
		p.next()
		return p.newValueExpr("BINARY")
	}
	// ON/OFF are reserved keywords but valid SET values (e.g., SET @@var = ON).
	// The parser treats them as string literals in this context.
	if p.peek().Tp == tokOn {
		p.next()
		return p.newValueExpr("ON")
	}
	if p.peek().IsKeyword("OFF") {
		p.next()
		return p.newValueExpr("OFF")
	}
	// ALL is valid for some SET variables (e.g., sql_mode)
	if p.peek().Tp == tokAll {
		p.next()
		return p.newValueExpr("ALL")
	}
	return p.parseExpression(precNone)
}

// parseSetNamesAssignment returns assignments for: NAMES charset [COLLATE collation | DEFAULT]
func (p *HandParser) parseSetNamesAssignment() []*ast.VariableAssignment {
	va := Alloc[ast.VariableAssignment](p.arena)
	va.Name = ast.SetNames

	if _, ok := p.accept(tokDefault); ok {
		va.Value = Alloc[ast.DefaultExpr](p.arena)
	} else {
		tok := p.next()
		va.Value = ast.NewValueExpr(tok.Lit, "", "")
		if _, ok := p.accept(tokCollate); ok {
			if _, ok := p.accept(tokDefault); !ok {
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
	va := Alloc[ast.VariableAssignment](p.arena)
	va.Name = ast.SetCharset

	if _, ok := p.accept(tokDefault); ok {
		va.Value = Alloc[ast.DefaultExpr](p.arena)
	} else {
		tok := p.next()
		va.Value = ast.NewValueExpr(tok.Lit, "", "")
	}
	return []*ast.VariableAssignment{va}
}

// parseSetRole parses: SET ROLE { DEFAULT | NONE | ALL [EXCEPT ...] | role_list }
func (p *HandParser) parseSetRole() ast.StmtNode {
	p.next() // consume ROLE
	stmt := Alloc[ast.SetRoleStmt](p.arena)

	if _, ok := p.accept(tokDefault); ok {
		stmt.SetRoleOpt = ast.SetRoleDefault
	} else if _, ok := p.accept(tokNone); ok {
		stmt.SetRoleOpt = ast.SetRoleNone
	} else if _, ok := p.accept(tokAll); ok {
		stmt.SetRoleOpt = ast.SetRoleAll
		if _, ok := p.accept(tokExcept); ok {
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
			p.error(p.peek().Offset, "Invalid role specification")
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

	if _, ok := p.accept(tokNone); ok {
		stmt.SetRoleOpt = ast.SetRoleNone
	} else if _, ok := p.accept(tokAll); ok {
		stmt.SetRoleOpt = ast.SetRoleAll
	} else {
		stmt.SetRoleOpt = ast.SetRoleRegular
		stmt.RoleList = p.parseRoleList()
	}

	p.expect(tokTo)

	// Parse user list (similar to role list but returning *auth.UserIdentity)
	var users []*auth.UserIdentity
	for {
		user := p.parseUserIdentity()
		if user == nil {
			p.error(p.peek().Offset, "Invalid user specification")
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
	if tok.Tp == tokStringLit {
		stmt.Instance = tok.Lit
		p.next()
	} else if isIdentLike(tok.Tp) {
		stmt.Type = p.parseVariableName() // Assuming Type is identifier-like (e.g. TIKV)
	} else {
		p.error(tok.Offset, "Expected config Type or Instance string")
		return nil
	}

	// Parse Name (e.g. log.level, auto-compaction-mode)
	stmt.Name = p.parseConfigName()
	if stmt.Name == "" {
		p.error(p.peek().Offset, "Expected config Name")
		return nil
	}

	p.expect(tokEq)

	// Parse Value
	stmt.Value = p.parseExpression(precNone)

	return stmt
}

// parseConfigName parses configuration names which can contain identifiers, dots, and dashes.
// Uses explicit token type checks to avoid consuming operators like '=' which isIdentLike
// incorrectly treats as identifiers (since tokEq > 0xFF and is not reserved).
func (p *HandParser) parseConfigName() string {
	var sb strings.Builder
	for {
		tok := p.peek()
		// Accept identifiers, unreserved keywords, and string literals.
		// Exclude operator tokens (tokAssignmentEq=58201 and above) which have
		// Tp > 0xFF but are NOT identifiers/keywords.
		if tok.Tp == tokStringLit || (tok.Tp >= tokIdentifier && tok.Tp < tokAssignmentEq && tok.Lit != "") {
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
	p.expect(tokTransaction)

	stmt := Alloc[ast.SetStmt](p.arena)

	for {
		va := Alloc[ast.VariableAssignment](p.arena)
		va.IsSystem = true
		va.IsGlobal = isGlobal

		if _, ok := p.accept(tokIsolation); ok {
			// ISOLATION LEVEL { READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE }
			p.expect(tokLevel)
			var level string
			if _, ok := p.accept(tokRead); ok {
				if _, ok := p.accept(tokUncommitted); ok {
					level = "READ-UNCOMMITTED"
				} else {
					p.expect(tokCommitted)
					level = "READ-COMMITTED"
				}
			} else if _, ok := p.accept(tokRepeatable); ok {
				p.expect(tokRead)
				level = "REPEATABLE-READ"
			} else if _, ok := p.accept(tokSerializable); ok {
				level = "SERIALIZABLE"
			} else {
				p.error(p.peek().Offset, "expected isolation level")
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
			p.expect(tokRead)
			if _, ok := p.accept(tokWrite); ok {
				va.Name = "tx_read_only"
				va.Value = p.newValueExpr("0")
			} else {
				p.expect(tokOnly)
				// Check for AS OF clause
				var asOfTs string
				if p.peek().Tp == tokAsOf {
					p.next()
					p.expect(tokTimestamp)
					asOfTs = p.next().Lit
				} else if p.peek().Tp == tokAs {
					p.next()
					if p.peek().Tp == tokOf {
						p.next()
						p.expect(tokTimestamp)
						asOfTs = p.next().Lit
					} else {
						// Just 'AS' without 'OF'? Should not happen here in valid SQL
						p.error(p.peek().Offset, "expected OF after AS")
					}
				}

				if asOfTs != "" {
					va.Name = "tx_read_ts"
					va.Value = p.newValueExpr(asOfTs)
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
		p.error(tok.Offset, "expected ENABLED or DISABLED after SET BINDING")
		return nil
	}

	// FOR
	p.expect(tokFor)

	// SQL DIGEST 'digest_string' or <statement>
	if p.peekKeyword(tokSql, "SQL") {
		m := p.mark()
		p.next() // consume SQL
		if p.peek().IsKeyword("DIGEST") {
			p.next() // consume DIGEST
			if tok, ok := p.expect(tokStringLit); ok {
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
	if _, ok := p.accept(tokUsing); ok {
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

	if p.peek().Tp == tokExplore {
		p.next()
		stmt.Explore = true
		stmt.Format = "" // Format is empty for EXPLORE
		if tok, ok := p.accept(tokStringLit); ok {
			stmt.SQLDigest = tok.Lit
			return stmt
		}
	}

	// [ANALYZE]
	if _, ok := p.accept(tokAnalyze); ok {
		stmt.Analyze = true
	}

	// [FORMAT = 'format']
	if p.peek().Tp == tokFormat {
		p.next()
		p.expectAny(tokEq, tokAssignmentEq)
		fmt := p.next()
		stmt.Format = fmt.Lit
	}

	// EXPLAIN [FORMAT = ...] FOR CONNECTION N
	if _, ok := p.accept(tokFor); ok {
		// FOR CONNECTION n
		p.expect(tokConnection)
		forStmt := Alloc[ast.ExplainForStmt](p.arena)
		forStmt.Format = stmt.Format
		forStmt.ConnectionID = p.parseUint64()
		return forStmt
	}

	// EXPLAIN [ANALYZE] [FORMAT = ...] 'plan_digest' — string literal = plan digest
	if p.peek().Tp == tokStringLit {
		stmt.PlanDigest = p.next().Lit
		return stmt
	}

	// EXPLAIN SELECT|INSERT|UPDATE|DELETE|... → parse sub-statement
	var sub ast.StmtNode
	subStartOff := p.peek().Offset
	switch p.peek().Tp {
	case tokSelect, tokInsert, tokReplace, tokUpdate, tokDelete,
		tokAlter, tokCreate, tokDrop, tokSet, tokShow, tokWith, tokTruncate,
		tokRename, tokAnalyze, tokLoad, tokGrant, tokRevoke, tokTable, tokValues,
		tokImport, '(':
		sub = p.parseStatement()
	default:
		// EXPLAIN|DESCRIBE tablename → maps to SHOW COLUMNS
		if p.peek().Tp == tokIdentifier || p.peek().Tp > 0xFF {
			tn := p.parseTableName()
			if tn == nil {
				return nil
			}
			showStmt := Alloc[ast.ShowStmt](p.arena)
			showStmt.Tp = ast.ShowColumns
			showStmt.Table = tn
			// Check for column specifier after table name
			if p.peek().Tp == tokIdentifier || p.peek().Tp == tokStringLit {
				col := &ast.ColumnName{Name: ast.NewCIStr(p.next().Lit)}
				showStmt.Column = col
			}
			stmt.Stmt = showStmt
			return stmt
		}
		return nil
	}

	if sub == nil {
		return nil
	}
	// Set the source text on the sub-statement so that ExecStmt.Text() returns
	// the correct SQL body (used by EXPLAIN EXPLORE, plan binding, etc.).
	subEndOff := p.peek().Offset
	if subEndOff > subStartOff {
		sub.SetText(nil, strings.TrimSpace(p.src[subStartOff:subEndOff]))
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
	p.expect(tokLock)
	// Accept TABLE or TABLES
	if _, ok := p.accept(tokTables); !ok {
		p.expect(tokTable)
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
	case tokRead:
		p.next()
		if _, ok := p.accept(tokLocal); ok {
			tl.Type = ast.TableLockReadLocal
		} else {
			tl.Type = ast.TableLockRead
		}
	case tokWrite:
		p.next()
		if _, ok := p.accept(tokLocal); ok {
			tl.Type = ast.TableLockWriteLocal
		} else {
			tl.Type = ast.TableLockWrite
		}
	}
	return tl, true
}
