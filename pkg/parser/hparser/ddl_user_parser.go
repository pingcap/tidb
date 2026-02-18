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
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// parseCreateUserStmt parses CREATE USER statements.
func (p *HandParser) parseCreateUserStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateUserStmt](p.arena)
	p.expect(tokCreate)
	if _, ok := p.accept(tokRole); ok {
		stmt.IsCreateRole = true
	} else {
		p.expect(tokUser)
	}

	stmt.IfNotExists = p.acceptIfNotExists()

	for {
		spec := p.parseUserSpec()
		if spec == nil {
			return nil
		}
		stmt.Specs = append(stmt.Specs, spec)
		if _, ok := p.accept(','); !ok {
			break
		}
	}

	// Parse optional user attributes/options
	// [REQUIRE ...]
	if _, ok := p.accept(tokRequire); ok {
		stmt.AuthTokenOrTLSOptions = p.parseTLSOptions()
	}

	// [WITH resource_option ...]
	if _, ok := p.accept(tokWith); ok {
		stmt.ResourceOptions = p.parseResourceOptions()
	}

	// [password_option | lock_option] ...
	stmt.PasswordOrLockOptions = p.parsePasswordAndLockOptions()

	// [COMMENT 'string' | ATTRIBUTE 'json'] ...
	stmt.CommentOrAttributeOption = p.parseCommentOrAttributeOption()

	// [RESOURCE GROUP name]
	if opt := p.parseUserResourceGroupOption(); opt != nil {
		stmt.ResourceGroupNameOption = opt
	}

	return stmt
}

// parseTLSOptions parses SSL / X509 / CIPHER / ISSUER / SUBJECT (after REQUIRE is already consumed)
func (p *HandParser) parseTLSOptions() []*ast.AuthTokenOrTLSOption {
	var opts []*ast.AuthTokenOrTLSOption
	for {
		opt := Alloc[ast.AuthTokenOrTLSOption](p.arena)
		tok := p.peek()
		switch tok.Tp {
		case tokNone:
			p.next()
			opt.Type = ast.TlsNone
		case tokSSL:
			p.next()
			opt.Type = ast.Ssl
		case tokX509:
			p.next()
			opt.Type = ast.X509
		case tokCipher:
			p.parseTLSOptionString(opt, ast.Cipher)
		case tokIssuer:
			p.parseTLSOptionString(opt, ast.Issuer)
		case tokSubject:
			p.parseTLSOptionString(opt, ast.Subject)
		case tokSAN:
			p.parseTLSOptionString(opt, ast.SAN)
		case tokTokenIssuer:
			p.parseTLSOptionString(opt, ast.TokenIssuer)
		default:
			// Unknown option
			return opts
		}
		opts = append(opts, opt)
		if _, ok := p.accept(tokAnd); !ok {
			// Check if next is a TLS option keyword
			next := p.peek()
			switch next.Tp {
			case tokSSL, tokNone, tokX509, tokCipher, tokIssuer, tokSubject, tokSAN, tokTokenIssuer:
				continue
			}
			break
		}
	}
	return opts
}

// parseResourceOptions parses MAX_QUERIES_PER_HOUR count ... (after WITH is already consumed)
func (p *HandParser) parseResourceOptions() []*ast.ResourceOption {
	var opts []*ast.ResourceOption
	for {
		opt := Alloc[ast.ResourceOption](p.arena)
		tok := p.peek()
		switch tok.Tp {
		case tokMaxQueriesPerHour:
			p.next()
			opt.Type = ast.MaxQueriesPerHour
			opt.Count, _ = p.parseInt64()
		case tokMaxUpdatesPerHour:
			p.next()
			opt.Type = ast.MaxUpdatesPerHour
			opt.Count, _ = p.parseInt64()
		case tokMaxConnectionsPerHour:
			p.next()
			opt.Type = ast.MaxConnectionsPerHour
			opt.Count, _ = p.parseInt64()
		case tokMaxUserConnections:
			p.next()
			opt.Type = ast.MaxUserConnections
			opt.Count, _ = p.parseInt64()
		default:
			return opts
		}

		opts = append(opts, opt)
	}
}

// parsePasswordAndLockOptions parses PASSWORD EXPIRE..., ACCOUNT LOCK...
func (p *HandParser) parsePasswordAndLockOptions() []*ast.PasswordOrLockOption {
	var opts []*ast.PasswordOrLockOption
	for {
		opt := Alloc[ast.PasswordOrLockOption](p.arena)
		tok := p.peek()
		if tok.Tp == tokPassword {
			p.next()
			// PASSWORD EXPIRE ...
			next := p.peek()
			if next.Tp == tokExpire {
				p.next() // consume EXPIRE
				opt.Type = ast.PasswordExpire
				// Check for optional DEFAULT, NEVER, INTERVAL N DAY
				if p.peek().Tp == tokDefault {
					p.next()
					opt.Type = ast.PasswordExpireDefault
				} else if p.peekKeyword(tokNever, "NEVER") {
					p.next()
					opt.Type = ast.PasswordExpireNever
				} else if p.peek().Tp == tokInterval {
					p.next() // INTERVAL
					opt.Type = ast.PasswordExpireInterval
					if val, ok := p.expect(tokIntLit); ok {
						count, _ := strconv.ParseInt(val.Lit, 10, 64)
						opt.Count = count
					}
					// DAY is usually expected but might be implied or checked
					if p.peekKeyword(tokDay, "DAY") {
						p.next()
					}
				}
			} else if next.Tp == tokHistory {
				p.next() // HISTORY
				opt.Type = ast.PasswordHistory
				if p.peek().Tp == tokDefault {
					p.next()
					opt.Type = ast.PasswordHistoryDefault
				} else if val, ok := p.accept(tokIntLit); ok {
					count, _ := strconv.ParseInt(val.Lit, 10, 64)
					opt.Count = count
				}
			} else if next.Tp == tokReuse {
				p.next() // REUSE
				// INTERVAL N DAY
				if p.peek().Tp == tokInterval {
					p.next()
					opt.Type = ast.PasswordReuseInterval
					if val, ok := p.expect(tokIntLit); ok {
						count, _ := strconv.ParseInt(val.Lit, 10, 64)
						opt.Count = count
					}
					if p.peekKeyword(tokDay, "DAY") {
						p.next()
					}
				} else if p.peek().Tp == tokDefault {
					p.next()
					opt.Type = ast.PasswordReuseDefault
				}
			} else if next.Tp == tokRequire {
				// PASSWORD REQUIRE CURRENT [DEFAULT | OPTIONAL]
				p.next() // REQUIRE
				if p.peek().Tp == tokCurrent {
					p.next() // CURRENT
					if p.peek().Tp == tokDefault {
						p.next()
						opt.Type = ast.PasswordRequireCurrentDefault
					} else if p.peek().Tp == tokOptional {
						p.next()
						// ast.PasswordRequireCurrentOptional ? (Not in my list, maybe 0?)
					}
				}
			} else {
				// Just PASSWORD token? unlikely in CREATE USER options
				return opts
			}
		} else if tok.Tp == tokLock || tok.Tp == tokUnlock {
			if p.next().Tp == tokLock {
				opt.Type = ast.Lock
			} else {
				opt.Type = ast.Unlock
			}
		} else if tok.Tp == tokAccount {
			p.next() // ACCOUNT
			if p.peek().Tp == tokLock || p.peek().Tp == tokUnlock {
				if p.next().Tp == tokLock {
					opt.Type = ast.Lock
				} else {
					opt.Type = ast.Unlock
				}
			} else {
				return opts
			}
		} else if tok.Tp == tokFailedLoginAttempts {
			p.next()
			opt.Type = ast.FailedLoginAttempts
			if val, ok := p.expect(tokIntLit); ok {
				count, _ := strconv.ParseInt(val.Lit, 10, 64)
				opt.Count = count
			}
		} else if tok.Tp == tokPasswordLockTime {
			p.next()
			opt.Type = ast.PasswordLockTime
			if p.peek().Tp == tokUnbounded {
				p.next()
				opt.Type = ast.PasswordLockTimeUnbounded
			} else if val, ok := p.accept(tokIntLit); ok {
				count, _ := strconv.ParseInt(val.Lit, 10, 64)
				opt.Count = count
			}
		} else {
			return opts
		}
		opts = append(opts, opt)
	}
}

// parseCommentOrAttributeOption parses COMMENT 'string' or ATTRIBUTE 'json'
func (p *HandParser) parseCommentOrAttributeOption() *ast.CommentOrAttributeOption {
	var optType int
	switch p.peek().Tp {
	case tokComment:
		optType = ast.UserCommentType
	case tokAttribute:
		optType = ast.UserAttributeType
	default:
		return nil
	}
	p.next()
	if val, ok := p.expect(tokStringLit); ok {
		opt := Alloc[ast.CommentOrAttributeOption](p.arena)
		opt.Type = optType
		opt.Value = val.Lit
		return opt
	}
	return nil
}

// parseDropUserStmt parses DROP USER statements.
func (p *HandParser) parseDropUserStmt() ast.StmtNode {
	stmt := Alloc[ast.DropUserStmt](p.arena)
	p.expect(tokDrop)
	if _, ok := p.accept(tokRole); ok {
		stmt.IsDropRole = true
	} else {
		p.expect(tokUser)
	}

	stmt.IfExists = p.acceptIfExists()

	for {
		user := p.parseUserIdentity()
		if user == nil {
			return nil
		}

		stmt.UserList = append(stmt.UserList, user)
		if _, ok := p.accept(','); !ok {
			break
		}
	}

	return stmt
}

// parseGrantStmt parses GRANT statements (both GRANT privilege and GRANT role).
func (p *HandParser) parseGrantStmt() ast.StmtNode {
	p.expect(tokGrant)

	// Detect GRANT ROLE vs GRANT PRIVILEGE.
	// GRANT ROLE: GRANT 'role1', 'role2' TO ...
	// GRANT PRIVILEGE: GRANT priv1, priv2 ON ...
	// When the first token is a string literal, it's always a role grant.
	// When the first token is an identifier, we need to look ahead through the comma-separated
	// list to see if it ends with TO (role) or ON (privilege).
	if p.peek().Tp == tokStringLit {
		return p.parseGrantRoleStmt()
	}
	if p.peek().Tp == tokIdentifier {
		if p.isRoleStatement(tokTo) {
			return p.parseGrantRoleStmt()
		}
	}

	stmt := Alloc[ast.GrantStmt](p.arena)

	// Parse privilege list
	stmt.Privs = p.parsePrivileges()

	p.expect(tokOn)

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

	p.expect(tokTo)
	stmt.Users = p.parseUserSpecList()
	if stmt.Users == nil {
		return nil
	}

	// Optional REQUIRE clause
	if _, ok := p.accept(tokRequire); ok {
		stmt.AuthTokenOrTLSOptions = p.parseTLSOptions()
	}

	// Optional WITH GRANT OPTION
	if _, ok := p.accept(tokWith); ok {
		if _, ok := p.accept(tokGrant); ok {
			p.expect(tokOption)
			stmt.WithGrant = true
		}
	}

	return stmt
}

// parseGrantRoleStmt parses GRANT 'role1', 'role2' TO 'user1'@'host', 'user2'@'host'
func (p *HandParser) parseGrantRoleStmt() ast.StmtNode {
	stmt := Alloc[ast.GrantRoleStmt](p.arena)

	// Parse role list
	stmt.Roles, stmt.Users = p.parseRoleListAndUserList(tokTo)
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
	case tokAll:
		p.next()
		p.accept(tokPrivileges)
		priv.Priv = mysql.AllPriv
	case tokSelect:
		// Check for dynamic privileges starting with SELECT, e.g., SELECT INTO S3
		if p.peekN(1).Tp == tokInto {
			priv.Priv, priv.Name = p.parseExtendedPrivName("SELECT")
			if priv.Name == "" {
				return nil
			}
		} else {
			p.next()
			priv.Priv = mysql.SelectPriv
		}
	case tokInsert:
		p.next()
		priv.Priv = mysql.InsertPriv
	case tokUpdate:
		p.next()
		priv.Priv = mysql.UpdatePriv
	case tokDelete:
		p.next()
		priv.Priv = mysql.DeletePriv
	case tokDrop:
		p.next()
		priv.Priv = mysql.DropPriv
	case tokGrant:
		p.next()
		p.expect(tokOption)
		priv.Priv = mysql.GrantPriv
	case tokIndex:
		p.next()
		priv.Priv = mysql.IndexPriv
	case tokAlter:
		p.next()
		// ALTER ROUTINE compound privilege
		if p.peek().Tp == tokRoutine {
			p.next()
			priv.Priv = mysql.AlterRoutinePriv
		} else {
			priv.Priv = mysql.AlterPriv
		}
	case tokExecute:
		p.next()
		priv.Priv = mysql.ExecutePriv
	case tokConfig:
		p.next()
		priv.Priv = mysql.ConfigPriv
	case tokReferences:
		p.next()
		priv.Priv = mysql.ReferencesPriv
	case tokUsage:
		p.next()
		priv.Priv = mysql.UsagePriv
	case tokProcess:
		p.next()
		priv.Priv = mysql.ProcessPriv
	case tokSuper:
		p.next()
		priv.Priv = mysql.SuperPriv
	case tokEvent:
		p.next()
		priv.Priv = mysql.EventPriv
	case tokFile:
		p.next()
		priv.Priv = mysql.FilePriv
	case tokTrigger:
		p.next()
		priv.Priv = mysql.TriggerPriv
	case tokShutdown:
		p.next()
		priv.Priv = mysql.ShutdownPriv
	case tokReload:
		p.next()
		priv.Priv = mysql.ReloadPriv
	case tokReplication:
		p.next()
		// REPLICATION CLIENT or REPLICATION SLAVE
		next := p.next()
		if next.IsKeyword("CLIENT") {
			priv.Priv = mysql.ReplicationClientPriv
		} else {
			priv.Priv = mysql.ReplicationSlavePriv
		}
	case tokCreate:
		p.next()
		// CREATE VIEW, CREATE ROUTINE, CREATE USER, CREATE TEMPORARY TABLES, CREATE TABLESPACE
		switch p.peek().Tp {
		case tokView:
			p.next()
			priv.Priv = mysql.CreateViewPriv
		case tokUser:
			p.next()
			priv.Priv = mysql.CreateUserPriv
		case tokTemporary:
			p.next()
			p.accept(tokTables) // TABLES
			priv.Priv = mysql.CreateTMPTablePriv
		case tokTablespace:
			p.next()
			priv.Priv = mysql.CreateTablespacePriv
		default:
			// Check for 'CREATE ROUTINE'
			if p.peek().Tp == tokRoutine {
				p.next()
				priv.Priv = mysql.CreateRoutinePriv
			} else {
				priv.Priv = mysql.CreatePriv
			}
		}
	case tokShow:
		p.next()
		// SHOW DATABASES or SHOW VIEW
		if p.peek().Tp == tokDatabases || p.peek().Tp == tokDatabase {
			p.next()
			priv.Priv = mysql.ShowDBPriv
		} else if p.peek().Tp == tokView {
			p.next()
			priv.Priv = mysql.ShowViewPriv
		}
	case tokLock:
		p.next()
		p.accept(tokTables) // LOCK TABLES
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
			if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
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

// parseAlterUserStmt parses ALTER USER [IF EXISTS] user [options]
func (p *HandParser) parseAlterUserStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterUserStmt](p.arena)
	p.expect(tokAlter)
	p.expect(tokUser)

	stmt.IfExists = p.acceptIfExists()

	// Handle ALTER USER USER() IDENTIFIED BY ... (CurrentAuth form)
	if p.peek().Tp == tokUser && p.peekN(1).Tp == '(' {
		p.next() // consume USER
		p.next() // consume (
		p.expect(')')

		// Parse auth option for CurrentAuth
		if _, ok := p.accept(tokIdentified); ok {
			stmt.CurrentAuth = Alloc[ast.AuthOption](p.arena)
			if _, ok := p.accept(tokBy); ok {
				if tok, ok := p.expect(tokStringLit); ok {
					stmt.CurrentAuth.ByAuthString = true
					stmt.CurrentAuth.AuthString = tok.Lit
				}
			}
		}
	} else {
		for {
			spec := p.parseUserSpec()
			if spec == nil {
				p.error(p.peek().Offset, "expected user specification")
				return nil
			}
			stmt.Specs = append(stmt.Specs, spec)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	// Options
	if _, ok := p.accept(tokRequire); ok {
		stmt.AuthTokenOrTLSOptions = p.parseTLSOptions()
	}
	if _, ok := p.accept(tokWith); ok {
		stmt.ResourceOptions = p.parseResourceOptions()
	}
	stmt.PasswordOrLockOptions = p.parsePasswordAndLockOptions()
	if c := p.parseCommentOrAttributeOption(); c != nil {
		stmt.CommentOrAttributeOption = c
	}
	// Resource Group option?
	// Check CreateUserStmt logic for Resource Group.
	// Reuse parseCreateUserStmt logic?
	// Snippet 26301 showed RESOURCE GROUP logic.
	// I'll add it here.
	if opt := p.parseUserResourceGroupOption(); opt != nil {
		stmt.ResourceGroupNameOption = opt
	}

	return stmt
}

// parseRenameUserStmt parses RENAME USER old_user TO new_user, ...
func (p *HandParser) parseRenameUserStmt() ast.StmtNode {
	stmt := Alloc[ast.RenameUserStmt](p.arena)
	p.expect(tokRename)
	p.expect(tokUser)

	for {
		u2u := Alloc[ast.UserToUser](p.arena)
		oldSpec := p.parseUserSpec()
		if oldSpec == nil {
			return nil
		}
		u2u.OldUser = oldSpec.User

		p.expect(tokTo)

		newSpec := p.parseUserSpec()
		if newSpec == nil {
			return nil
		}
		u2u.NewUser = newSpec.User

		stmt.UserToUsers = append(stmt.UserToUsers, u2u)

		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return stmt
}

// parseRevokeStmt parses REVOKE statements (both REVOKE privilege and REVOKE role).
func (p *HandParser) parseRevokeStmt() ast.StmtNode {
	p.expect(tokRevoke)

	// Detect REVOKE ROLE vs REVOKE PRIVILEGE.
	// REVOKE ROLE: REVOKE 'role1', 'role2' FROM ...
	// REVOKE PRIVILEGE: REVOKE priv1, priv2 ON ...
	// When the first token is a string literal, it's always a role revocation.
	// When the first token is an identifier, scan ahead for FROM vs ON.
	if p.peek().Tp == tokStringLit {
		return p.parseRevokeRoleStmt()
	}
	if p.peek().Tp == tokIdentifier {
		if p.isRoleStatement(tokFrom) {
			return p.parseRevokeRoleStmt()
		}
	}

	stmt := Alloc[ast.RevokeStmt](p.arena)
	stmt.ObjectType = ast.ObjectTypeNone
	stmt.Privs = p.parsePrivileges()

	if _, ok := p.accept(tokOn); ok {
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

	p.expect(tokFrom)
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
	stmt.Roles, stmt.Users = p.parseRoleListAndUserList(tokFrom)
	if stmt.Roles == nil || stmt.Users == nil {
		return nil
	}
	return stmt
}

// parseUserSpec parses 'user'@'host' [IDENTIFIED BY 'password']
func (p *HandParser) parseUserSpec() *ast.UserSpec {
	spec := Alloc[ast.UserSpec](p.arena)
	spec.User = p.parseUserIdentity()
	if spec.User == nil {
		return nil
	}

	if _, ok := p.accept(tokIdentified); ok {
		spec.AuthOpt = Alloc[ast.AuthOption](p.arena)
		if _, ok := p.accept(tokWith); ok {
			// IDENTIFIED WITH 'auth_plugin' [BY 'password' | AS 'hash']
			if tok, ok := p.expectAny(tokStringLit, tokIdentifier); ok {
				spec.AuthOpt.AuthPlugin = tok.Lit
			}
			if _, ok := p.accept(tokBy); ok {
				if tok, ok := p.expect(tokStringLit); ok {
					spec.AuthOpt.ByAuthString = true
					spec.AuthOpt.AuthString = tok.Lit
				}
			} else if _, ok := p.accept(tokAs); ok {
				if tok, ok := p.expectAny(tokStringLit, tokHexLit); ok {
					spec.AuthOpt.ByHashString = true
					if tok.Tp == tokHexLit {
						// Decode hex literal 0x... to binary string
						hexStr := tok.Lit
						if len(hexStr) > 2 && hexStr[0] == '0' && (hexStr[1] == 'x' || hexStr[1] == 'X') {
							hexStr = hexStr[2:]
						}
						decoded, err := hex.DecodeString(hexStr)
						if err == nil {
							spec.AuthOpt.HashString = string(decoded)
						} else {
							spec.AuthOpt.HashString = tok.Lit
						}
					} else {
						spec.AuthOpt.HashString = tok.Lit
					}
				}
			}
		} else if _, ok := p.accept(tokAs); ok {
			// IDENTIFIED AS 'hashstring' (restored form from BY PASSWORD)
			if tok, ok := p.expectAny(tokStringLit, tokHexLit); ok {
				spec.AuthOpt.ByHashString = true
				spec.AuthOpt.HashString = tok.Lit
			}
		} else {
			// IDENTIFIED BY [PASSWORD] 'password'
			p.expect(tokBy)
			if _, ok := p.accept(tokPassword); ok {
				// IDENTIFIED BY PASSWORD 'hashstring' (deprecated pre-hashed form)
				// mysql_native_password is implied when using BY PASSWORD
				spec.AuthOpt.AuthPlugin = "mysql_native_password"
				if tok, ok := p.expect(tokStringLit); ok {
					spec.AuthOpt.ByHashString = true
					spec.AuthOpt.HashString = tok.Lit
				}
			} else {
				if tok, ok := p.expect(tokStringLit); ok {
					spec.AuthOpt.ByAuthString = true
					spec.AuthOpt.AuthString = tok.Lit
				}
			}
		}
	}

	return spec
}

func (p *HandParser) parseTLSOptionString(opt *ast.AuthTokenOrTLSOption, tp ast.AuthTokenOrTLSOptionType) {
	p.next()
	if val, ok := p.expect(tokStringLit); ok {
		opt.Type = tp
		opt.Value = val.Lit
	}
}

func (p *HandParser) parseUserResourceGroupOption() *ast.ResourceGroupNameOption {
	if p.peek().Tp == tokResource {
		p.next()
		p.expect(tokGroup)
		opt := Alloc[ast.ResourceGroupNameOption](p.arena)
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			opt.Value = tok.Lit
		}
		return opt
	}
	return nil
}

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
		if tok.Tp == ',' || tok.Tp == tokOn || tok.Tp == '(' || tok.Tp == ';' || tok.Tp == 0 {
			break
		}
		if tok.Tp == tokIdentifier {
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
