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

// grant_revoke_parser.go handles GRANT and REVOKE statements for both
// privilege grants (GRANT priv ON obj TO user) and role grants (GRANT role TO user).

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// parseGrantStmt parses GRANT statements (both GRANT privilege and GRANT role).
// Uses the same RoleOrPrivElemList approach as yacc to handle ambiguous cases
// like GRANT 'rolename'@host ON ... which could look like either a role or privilege.
func (p *HandParser) parseGrantStmt() ast.StmtNode {
	p.expect(grant)

	// GRANT PROXY ON ... is a special case
	if p.peek().Tp == proxy {
		return p.parseGrantProxyStmt()
	}

	// Parse a unified RoleOrPriv list (matching yacc's RoleOrPrivElemList).
	roleOrPrivs := p.parseRoleOrPrivElemList()

	if _, ok := p.accept(on); ok {
		// GRANT ... ON ... TO ... (privilege grant)
		// Parse the full statement structure first (matching yacc, which validates
		// syntax before running semantic actions like convertToPriv). This ensures
		// syntax errors in the body take priority over conversion errors.
		stmt := Alloc[ast.GrantStmt](p.arena)

		// Parse ObjectType: TABLE, FUNCTION, PROCEDURE
		stmt.ObjectType = p.parseObjectType()

		// Parse grant level (PrivLevel)
		stmt.Level = p.parseGrantLevel()

		p.expect(to)
		stmt.Users = p.parseUserSpecList()
		if stmt.Users == nil {
			return nil
		}

		// Optional REQUIRE clause
		if _, ok := p.accept(require); ok {
			stmt.AuthTokenOrTLSOptions = p.parseTLSOptions()
		}

		// Optional WITH GRANT OPTION (or WITH MAX_QUERIES_PER_HOUR etc. which are parsed but ignored)
		if _, ok := p.accept(with); ok {
			if _, ok := p.accept(grant); ok {
				p.expect(option)
				stmt.WithGrant = true
			} else {
				// WITH MAX_QUERIES_PER_HOUR NUM, MAX_UPDATES_PER_HOUR NUM, etc.
				// MySQL compatibility: parsed but ignored
				for {
					pk := p.peek()
					if !pk.IsKeyword("MAX_QUERIES_PER_HOUR") && !pk.IsKeyword("MAX_UPDATES_PER_HOUR") &&
						!pk.IsKeyword("MAX_CONNECTIONS_PER_HOUR") && !pk.IsKeyword("MAX_USER_CONNECTIONS") {
						break
					}
					p.next()
					p.expect(intLit) // consume the number (yacc: NUM = intLit)
				}
			}
		}

		// Check for trailing unconsumed tokens before convertToPriv (matching yacc,
		// which produces syntax errors for unconsumed tokens before semantic actions).
		if next := p.peek(); next.Tp != ';' && next.Tp != EOF {
			p.syntaxErrorAt(next)
			return nil
		}

		// Now convert roleOrPrivs to privileges (deferred to match yacc behavior).
		privs, err := p.convertToPriv(roleOrPrivs)
		if err != nil {
			p.errs = append(p.errs, err)
			return nil
		}
		stmt.Privs = privs

		// Check for PROXY privilege (level fixup)
		if level, handled := p.parseProxyLevel(stmt.Privs, "GRANT"); handled {
			if level == nil {
				return nil
			}
			stmt.Level = level
		}

		return stmt
	}

	// GRANT ... TO ... (role grant)
	roles, err := p.convertToRole(roleOrPrivs)
	if err != nil {
		p.errs = append(p.errs, err)
		return nil
	}

	stmt := Alloc[ast.GrantRoleStmt](p.arena)
	stmt.Roles = roles

	p.expect(to)

	// Parse user list
	for {
		user := p.parseUserIdentity()
		if user == nil {
			return nil
		}
		stmt.Users = append(stmt.Users, user)
		if _, ok := p.accept(','); !ok {
			break
		}
	}

	return stmt
}

// parseGrantProxyStmt parses GRANT PROXY ON ... TO ...
func (p *HandParser) parseGrantProxyStmt() ast.StmtNode {
	p.next() // consume PROXY
	p.expect(on)
	localUser := p.parseUserIdentity()
	if localUser == nil {
		return nil
	}
	p.expect(to)
	var users []*auth.UserIdentity
	for {
		user := p.parseUserIdentity()
		if user == nil {
			return nil
		}
		users = append(users, user)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	withGrant := false
	if _, ok := p.accept(with); ok {
		p.expect(grant)
		p.expect(option)
		withGrant = true
	}
	return &ast.GrantProxyStmt{
		LocalUser:     localUser,
		ExternalUsers: users,
		WithGrant:     withGrant,
	}
}

// parseGrantLevel parses the object level for GRANT: *.*, db.*, db.tbl, *, tbl
func (p *HandParser) parseGrantLevel() *ast.GrantLevel {
	level := Alloc[ast.GrantLevel](p.arena)

	if _, ok := p.accept('*'); ok {
		if _, ok := p.accept('.'); ok {
			p.expect('*')
			level.Level = ast.GrantLevelGlobal
		} else {
			level.Level = ast.GrantLevelDB
		}
		return level
	}

	// db.* or db.tbl or just tbl
	tok := p.next()
	name := tok.Lit
	if _, ok := p.accept('.'); ok {
		level.DBName = name
		if _, ok := p.accept('*'); ok {
			level.Level = ast.GrantLevelDB
		} else {
			tblTok := p.next()
			level.TableName = tblTok.Lit
			level.Level = ast.GrantLevelTable
		}
	} else {
		level.TableName = name
		level.Level = ast.GrantLevelTable
	}

	return level
}

// parseRevokeStmt parses REVOKE statements (both REVOKE privilege and REVOKE role).
// Uses the same RoleOrPrivElemList approach as yacc.
func (p *HandParser) parseRevokeStmt() ast.StmtNode {
	p.expect(revoke)

	// Parse a unified RoleOrPriv list (matching yacc's RoleOrPrivElemList).
	roleOrPrivs := p.parseRoleOrPrivElemList()

	if _, ok := p.accept(on); ok {
		// REVOKE ... ON ... FROM ... (privilege revoke)
		// Parse full statement structure first, then convert (matching yacc).
		stmt := Alloc[ast.RevokeStmt](p.arena)

		// Parse ObjectType: TABLE, FUNCTION, PROCEDURE
		stmt.ObjectType = p.parseObjectType()

		// Parse grant level
		stmt.Level = p.parseGrantLevel()

		p.expect(from)
		stmt.Users = p.parseUserSpecList()
		if stmt.Users == nil {
			return nil
		}

		// Check for trailing unconsumed tokens before convertToPriv.
		if next := p.peek(); next.Tp != ';' && next.Tp != EOF {
			p.syntaxErrorAt(next)
			return nil
		}

		// Now convert roleOrPrivs to privileges (deferred to match yacc behavior).
		privs, err := p.convertToPriv(roleOrPrivs)
		if err != nil {
			p.errs = append(p.errs, err)
			return nil
		}
		stmt.Privs = privs

		// Check for PROXY privilege (level fixup)
		if level, handled := p.parseProxyLevel(stmt.Privs, "REVOKE"); handled {
			if level == nil {
				return nil
			}
			stmt.Level = level
		}

		return stmt
	}

	// REVOKE ... FROM ... (role revoke or REVOKE ALL ... FROM ...)
	// Check for REVOKE ALL, GRANT OPTION FROM ... (special syntax).
	// yacc requires exactly 2 elements: ALL + GRANT OPTION (isRevokeAllGrant).
	if p.isRevokeAllGrant(roleOrPrivs) {
		stmt := Alloc[ast.RevokeStmt](p.arena)
		stmt.ObjectType = ast.ObjectTypeNone
		stmt.Privs = []*ast.PrivElem{{Priv: mysql.AllPriv}, {Priv: mysql.GrantPriv}}
		stmt.Level = Alloc[ast.GrantLevel](p.arena)
		stmt.Level.Level = ast.GrantLevelGlobal
		p.expect(from)
		stmt.Users = p.parseUserSpecList()
		if stmt.Users == nil {
			return nil
		}
		return stmt
	}

	// REVOKE role1, role2 FROM user1, user2
	roles, err := p.convertToRole(roleOrPrivs)
	if err != nil {
		p.errs = append(p.errs, err)
		return nil
	}

	stmt := Alloc[ast.RevokeRoleStmt](p.arena)
	stmt.Roles = roles

	p.expect(from)

	// Parse user list
	for {
		user := p.parseUserIdentity()
		if user == nil {
			return nil
		}
		stmt.Users = append(stmt.Users, user)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return stmt
}

// parseRoleOrPrivElemList parses a comma-separated list of items that could be
// either privilege elements or role identities. This matches yacc's RoleOrPrivElemList
// which defers the decision until ON or TO/FROM is seen.
func (p *HandParser) parseRoleOrPrivElemList() []*ast.RoleOrPriv {
	var list []*ast.RoleOrPriv
	for {
		elem := p.parseRoleOrPrivElem()
		if elem == nil {
			break
		}
		list = append(list, elem)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return list
}

// parseRoleOrPrivElem parses a single element that is either a privilege or a role.
// Yacc grammar: RoleOrPrivElem = PrivElem | RolenameWithoutIdent | ExtendedPriv | special
func (p *HandParser) parseRoleOrPrivElem() *ast.RoleOrPriv {
	tok := p.peek()

	// Try as PrivElem first (known privilege keywords).
	priv := p.tryParsePrivilege()
	if priv != nil {
		return &ast.RoleOrPriv{Node: priv}
	}

	// RolenameWithoutIdent: stringLit or RolenameComposed (stringLit/StringName + @host)
	if tok.Tp == stringLit {
		p.next()
		if p.peek().Tp == singleAtIdentifier {
			// stringLit @host → RolenameComposed
			hostTok := p.next()
			hostname := strings.TrimPrefix(hostTok.Lit, "@")
			return &ast.RoleOrPriv{
				Node: &auth.RoleIdentity{Username: tok.Lit, Hostname: strings.ToLower(hostname)},
			}
		}
		// Bare stringLit → RolenameWithoutIdent
		return &ast.RoleOrPriv{
			Node: &auth.RoleIdentity{Username: tok.Lit, Hostname: "%"},
		}
	}

	// ExtendedPriv or bare role name: one or more identifiers
	if tok.Tp == identifier {
		// Check if this looks like a role with @host
		if p.peekN(1).Tp == singleAtIdentifier {
			p.next() // consume identifier
			hostTok := p.next()
			hostname := strings.TrimPrefix(hostTok.Lit, "@")
			return &ast.RoleOrPriv{
				Node: &auth.RoleIdentity{Username: tok.Lit, Hostname: strings.ToLower(hostname)},
			}
		}
		// Extended privilege (or bare role name): sequence of identifiers.
		// Store as Symbols so both ToPriv() and ToRole() can convert it.
		_, name := p.parseExtendedPrivName("")
		if name != "" {
			return &ast.RoleOrPriv{Symbols: name}
		}
	}

	// LOAD FROM S3 / SELECT INTO S3
	if tok.Tp == load && p.peekN(1).Tp == from {
		return p.parseSpecialPriv("LOAD", from, "S3")
	}
	if tok.Tp == selectKwd && p.peekN(1).Tp == into {
		return p.parseSpecialPriv("SELECT", into, "S3")
	}

	return nil
}

// parseSpecialPriv handles LOAD FROM S3 and SELECT INTO S3 special privileges.
func (p *HandParser) parseSpecialPriv(firstKeyword string, _ int, lastKeyword string) *ast.RoleOrPriv {
	p.next()              // consume first keyword (LOAD or SELECT)
	middleTok := p.next() // consume middle keyword (FROM or INTO)
	tok := p.next()       // consume last keyword (S3)
	if !strings.EqualFold(tok.Lit, lastKeyword) {
		p.syntaxErrorAt(tok)
		return nil
	}
	name := firstKeyword + " " + strings.ToUpper(middleTok.Lit) + " " + strings.ToUpper(tok.Lit)
	return &ast.RoleOrPriv{Symbols: name}
}

// tryParsePrivilege tries to parse a known privilege keyword.
// Returns nil without consuming tokens if the next token is not a privilege keyword.
func (p *HandParser) tryParsePrivilege() *ast.PrivElem {
	priv := Alloc[ast.PrivElem](p.arena)
	tok := p.peek()
	switch tok.Tp {
	case all:
		p.next()
		p.accept(privileges)
		priv.Priv = mysql.AllPriv
	case selectKwd:
		// Don't match SELECT INTO S3 as a regular SELECT privilege
		if p.peekN(1).Tp == into {
			return nil
		}
		p.next()
		priv.Priv = mysql.SelectPriv
	case insert:
		p.next()
		priv.Priv = mysql.InsertPriv
	case update:
		p.next()
		priv.Priv = mysql.UpdatePriv
	case deleteKwd:
		p.next()
		priv.Priv = mysql.DeletePriv
	case drop:
		p.next()
		if p.peek().Tp == role {
			p.next()
			priv.Priv = mysql.DropRolePriv
		} else {
			priv.Priv = mysql.DropPriv
		}
	case grant:
		p.next()
		p.expect(option)
		priv.Priv = mysql.GrantPriv
	case index:
		p.next()
		priv.Priv = mysql.IndexPriv
	case alter:
		p.next()
		if p.peek().Tp == routine {
			p.next()
			priv.Priv = mysql.AlterRoutinePriv
		} else {
			priv.Priv = mysql.AlterPriv
		}
	case execute:
		p.next()
		priv.Priv = mysql.ExecutePriv
	case config:
		p.next()
		priv.Priv = mysql.ConfigPriv
	case references:
		p.next()
		priv.Priv = mysql.ReferencesPriv
	case usage:
		p.next()
		priv.Priv = mysql.UsagePriv
	case process:
		p.next()
		priv.Priv = mysql.ProcessPriv
	case super:
		p.next()
		priv.Priv = mysql.SuperPriv
	case event:
		p.next()
		priv.Priv = mysql.EventPriv
	case file:
		p.next()
		priv.Priv = mysql.FilePriv
	case trigger:
		p.next()
		priv.Priv = mysql.TriggerPriv
	case shutdown:
		p.next()
		priv.Priv = mysql.ShutdownPriv
	case reload:
		p.next()
		priv.Priv = mysql.ReloadPriv
	case replication:
		p.next()
		next := p.peek()
		if next.IsKeyword("CLIENT") {
			p.next()
			priv.Priv = mysql.ReplicationClientPriv
		} else if next.IsKeyword("SLAVE") {
			p.next()
			priv.Priv = mysql.ReplicationSlavePriv
		} else {
			return nil // yacc requires REPLICATION CLIENT or REPLICATION SLAVE
		}
	case create:
		p.next()
		switch p.peek().Tp {
		case view:
			p.next()
			priv.Priv = mysql.CreateViewPriv
		case user:
			p.next()
			priv.Priv = mysql.CreateUserPriv
		case role:
			p.next()
			priv.Priv = mysql.CreateRolePriv
		case temporary:
			p.next()
			p.expect(tables)
			priv.Priv = mysql.CreateTMPTablePriv
		case tablespace:
			p.next()
			priv.Priv = mysql.CreateTablespacePriv
		default:
			if p.peek().Tp == routine {
				p.next()
				priv.Priv = mysql.CreateRoutinePriv
			} else {
				priv.Priv = mysql.CreatePriv
			}
		}
	case show:
		p.next()
		if p.peek().Tp == databases {
			p.next()
			priv.Priv = mysql.ShowDBPriv
		} else if p.peek().Tp == view {
			p.next()
			priv.Priv = mysql.ShowViewPriv
		} else {
			return nil // yacc only accepts SHOW DATABASES or SHOW VIEW
		}
	case lock:
		p.next()
		p.expect(tables)
		priv.Priv = mysql.LockTablesPriv
	default:
		return nil
	}

	// Optional column list: (col1, col2, ...)
	if p.peek().Tp == '(' {
		p.next()
		for {
			col := &ast.ColumnName{}
			if colTok, ok := p.expectIdentLike(); ok {
				col.Name = ast.NewCIStr(colTok.Lit)
			}
			priv.Cols = append(priv.Cols, col)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}

	return priv
}

// isRevokeAllGrant checks if roleOrPrivList matches the yacc special case:
// exactly [ALL, GRANT OPTION]. This is the "second syntax" for REVOKE:
// REVOKE ALL PRIVILEGES, GRANT OPTION FROM user [, user] ...
func (*HandParser) isRevokeAllGrant(roleOrPrivList []*ast.RoleOrPriv) bool {
	if len(roleOrPrivList) != 2 {
		return false
	}
	priv, err := roleOrPrivList[0].ToPriv()
	if err != nil {
		return false
	}
	if priv.Priv != mysql.AllPriv {
		return false
	}
	priv, err = roleOrPrivList[1].ToPriv()
	if err != nil {
		return false
	}
	return priv.Priv == mysql.GrantPriv
}

// convertToPriv converts a RoleOrPriv list to PrivElem list.
// Matches yacc's convertToPriv function behavior.
func (*HandParser) convertToPriv(roleOrPrivs []*ast.RoleOrPriv) ([]*ast.PrivElem, error) {
	privs := make([]*ast.PrivElem, 0, len(roleOrPrivs))
	for _, rp := range roleOrPrivs {
		pe, err := rp.ToPriv()
		if err != nil {
			return nil, err
		}
		privs = append(privs, pe)
	}
	return privs, nil
}

// convertToRole converts a RoleOrPriv list to RoleIdentity list.
// Matches yacc's convertToRole function behavior.
func (*HandParser) convertToRole(roleOrPrivs []*ast.RoleOrPriv) ([]*auth.RoleIdentity, error) {
	roles := make([]*auth.RoleIdentity, 0, len(roleOrPrivs))
	for _, rp := range roleOrPrivs {
		r, err := rp.ToRole()
		if err != nil {
			return nil, err
		}
		roles = append(roles, r)
	}
	return roles, nil
}

// parseExtendedPrivName consumes tokens forming a dynamic/extended privilege name.
// In yacc, ExtendedPriv is defined as: identifier { identifier }*
// Only accepts identifier tokens (not keywords). If initPart is non-empty, it is
// prepended (e.g. "SELECT" for "SELECT INTO S3"). The current token is consumed.
// Returns (mysql.ExtendedPriv, name) on success, or (0, "") if no tokens match.
func (p *HandParser) parseExtendedPrivName(initPart string) (mysql.PrivilegeType, string) {
	var parts []string
	if initPart != "" {
		p.next() // consume the initial keyword (e.g. SELECT)
		parts = append(parts, initPart)
	}
	for {
		tok := p.peek()
		// Only accept identifier tokens — matching yacc's ExtendedPriv rule.
		if tok.Tp != identifier {
			break
		}
		parts = append(parts, tok.Lit)
		p.next()
	}
	if len(parts) == 0 {
		return 0, ""
	}
	return mysql.ExtendedPriv, strings.Join(parts, " ")
}
