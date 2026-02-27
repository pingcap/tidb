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
func (p *HandParser) parseGrantStmt() ast.StmtNode {
	p.expect(grant)

	// Detect GRANT ROLE vs GRANT PRIVILEGE.
	// GRANT ROLE: GRANT 'role1', 'role2' TO ...
	// GRANT PRIVILEGE: GRANT priv1, priv2 ON ...
	// Look ahead for TO vs ON to distinguish role vs privilege grants.
	if p.peek().Tp == stringLit || p.peek().Tp == identifier {
		if p.isRoleStatement(to) {
			return p.parseGrantRoleStmt()
		}
	}

	stmt := Alloc[ast.GrantStmt](p.arena)

	// Parse privilege list
	stmt.Privs = p.parsePrivileges()

	p.expect(on)

	// Parse ObjectType: TABLE, FUNCTION, PROCEDURE
	stmt.ObjectType = p.parseObjectType()

	// Check for PROXY privilege
	if level, handled := p.parseProxyLevel(stmt.Privs, "GRANT"); handled {
		if level == nil {
			return nil
		}
		stmt.Level = level
	} else {
		stmt.Level = p.parseGrantLevel()
	}

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
				p.next() // consume the number
			}
		}
	}

	return stmt
}

// parseGrantRoleStmt parses GRANT 'role1', 'role2' TO 'user1'@'host', 'user2'@'host'
func (p *HandParser) parseGrantRoleStmt() ast.StmtNode {
	stmt := Alloc[ast.GrantRoleStmt](p.arena)

	// Parse role list
	stmt.Roles, stmt.Users = p.parseRoleListAndUserList(to)
	if stmt.Roles == nil || stmt.Users == nil {
		return nil
	}

	return stmt
}

// parsePrivileges parses a list of privileges for GRANT/REVOKE.
func (p *HandParser) parsePrivileges() []*ast.PrivElem {
	var privs []*ast.PrivElem
	for {
		priv := p.parsePrivilege()
		if priv == nil {
			return privs
		}
		privs = append(privs, priv)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return privs
}

// parsePrivilege parses a single privilege keyword.
func (p *HandParser) parsePrivilege() *ast.PrivElem {
	priv := Alloc[ast.PrivElem](p.arena)
	tok := p.peek()
	switch tok.Tp {
	case all:
		p.next()
		p.accept(privileges)
		priv.Priv = mysql.AllPriv
	case selectKwd:
		// Check for dynamic privileges starting with SELECT, e.g., SELECT INTO S3
		if p.peekN(1).Tp == into {
			priv.Priv, priv.Name = p.parseExtendedPrivName("SELECT")
			if priv.Name == "" {
				return nil
			}
		} else {
			p.next()
			priv.Priv = mysql.SelectPriv
		}
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
		// DROP ROLE compound privilege
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
		// ALTER ROUTINE compound privilege
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
		// REPLICATION CLIENT or REPLICATION SLAVE
		next := p.next()
		if next.IsKeyword("CLIENT") {
			priv.Priv = mysql.ReplicationClientPriv
		} else {
			priv.Priv = mysql.ReplicationSlavePriv
		}
	case create:
		p.next()
		// CREATE VIEW, CREATE ROUTINE, CREATE USER, CREATE ROLE,
		// CREATE TEMPORARY TABLES, CREATE TABLESPACE
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
			p.accept(tables) // TABLES
			priv.Priv = mysql.CreateTMPTablePriv
		case tablespace:
			p.next()
			priv.Priv = mysql.CreateTablespacePriv
		default:
			// Check for 'CREATE ROUTINE'
			if p.peek().Tp == routine {
				p.next()
				priv.Priv = mysql.CreateRoutinePriv
			} else {
				priv.Priv = mysql.CreatePriv
			}
		}
	case show:
		p.next()
		// SHOW DATABASES or SHOW VIEW
		if p.peek().Tp == databases || p.peek().Tp == database {
			p.next()
			priv.Priv = mysql.ShowDBPriv
		} else if p.peek().Tp == view {
			p.next()
			priv.Priv = mysql.ShowViewPriv
		}
	case lock:
		p.next()
		p.accept(tables) // LOCK TABLES
		priv.Priv = mysql.LockTablesPriv
	default:
		priv.Priv, priv.Name = p.parseExtendedPrivName("")
		if priv.Name == "" {
			return nil
		}
	}

	// Optional column list: (col1, col2, ...)
	if p.peek().Tp == '(' {
		p.next()
		for {
			col := &ast.ColumnName{}
			if tok, ok := p.expectIdentLike(); ok {
				col.Name = ast.NewCIStr(tok.Lit)
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
func (p *HandParser) parseRevokeStmt() ast.StmtNode {
	p.expect(revoke)

	// Detect REVOKE ROLE vs REVOKE PRIVILEGE.
	// REVOKE ROLE: REVOKE 'role1', 'role2' FROM ...
	// REVOKE PRIVILEGE: REVOKE priv1, priv2 ON ...
	// Look ahead for FROM vs ON to distinguish role vs privilege revocation.
	if p.peek().Tp == stringLit || p.peek().Tp == identifier {
		if p.isRoleStatement(from) {
			return p.parseRevokeRoleStmt()
		}
	}

	stmt := Alloc[ast.RevokeStmt](p.arena)
	stmt.ObjectType = ast.ObjectTypeNone
	stmt.Privs = p.parsePrivileges()

	if _, ok := p.accept(on); ok {
		// Parse ObjectType: TABLE, FUNCTION, PROCEDURE
		stmt.ObjectType = p.parseObjectType()

		// Check for PROXY privilege
		if level, handled := p.parseProxyLevel(stmt.Privs, "REVOKE"); handled {
			if level == nil {
				return nil
			}
			stmt.Level = level
		} else {
			stmt.Level = p.parseGrantLevel()
		}
	} else {
		// No ON clause: REVOKE ALL [PRIVILEGES], GRANT OPTION FROM ...
		stmt.Level = Alloc[ast.GrantLevel](p.arena)
		stmt.Level.Level = ast.GrantLevelGlobal
	}

	p.expect(from)
	stmt.Users = p.parseUserSpecList()
	if stmt.Users == nil {
		return nil
	}
	return stmt
}

// parseRevokeRoleStmt parses REVOKE 'role1', 'role2' FROM 'user1'@'host', 'user2'@'host'
func (p *HandParser) parseRevokeRoleStmt() ast.StmtNode {
	stmt := Alloc[ast.RevokeRoleStmt](p.arena)

	// Parse role list
	stmt.Roles, stmt.Users = p.parseRoleListAndUserList(from)
	if stmt.Roles == nil || stmt.Users == nil {
		return nil
	}
	return stmt
}

// parseRoleListAndUserList parses a role list followed by a connector keyword (TO/FROM)
// and then a user list. Shared between GRANT ROLE and REVOKE ROLE.
func (p *HandParser) parseRoleListAndUserList(connector int) (roles []*auth.RoleIdentity, users []*auth.UserIdentity) {
	// Parse role list
	for {
		role := p.parseRoleIdentity()
		if role == nil {
			return nil, nil
		}
		roles = append(roles, role)
		if _, ok := p.accept(','); !ok {
			break
		}
	}

	p.expect(connector)

	// Parse user list
	for {
		user := p.parseUserIdentity()
		if user == nil {
			return nil, nil
		}
		users = append(users, user)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return roles, users
}

// parseExtendedPrivName consumes tokens forming a dynamic/extended privilege name
// until a delimiter (,  ON  (  ;  EOF) is reached. If initPart is non-empty, it is
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
		if tok.Tp == ',' || tok.Tp == on || tok.Tp == '(' || tok.Tp == ';' || tok.Tp == 0 {
			break
		}
		// Extended privilege names consist of keywords and identifiers,
		// not string literals or user/system variables (e.g. 'C' @host).
		if tok.Tp == stringLit || tok.Tp == singleAtIdentifier || tok.Tp == doubleAtIdentifier {
			break
		}
		if tok.Tp == identifier {
			parts = append(parts, tok.Lit)
		} else if tok.Lit != "" {
			parts = append(parts, tok.Lit)
		} else {
			break
		}
		p.next()
	}
	if len(parts) == 0 {
		return 0, ""
	}
	return mysql.ExtendedPriv, strings.Join(parts, " ")
}
