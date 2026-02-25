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
	"encoding/hex"
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseCreateUserStmt parses CREATE USER statements.
func (p *HandParser) parseCreateUserStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateUserStmt](p.arena)
	p.expect(create)
	if _, ok := p.accept(role); ok {
		stmt.IsCreateRole = true
	} else {
		p.expect(user)
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
	if _, ok := p.accept(require); ok {
		stmt.AuthTokenOrTLSOptions = p.parseTLSOptions()
	}

	// [WITH resource_option ...]
	if _, ok := p.accept(with); ok {
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
		case none:
			p.next()
			opt.Type = ast.TlsNone
		case ssl:
			p.next()
			opt.Type = ast.Ssl
		case x509:
			p.next()
			opt.Type = ast.X509
		case cipher:
			p.parseTLSOptionString(opt, ast.Cipher)
		case issuer:
			p.parseTLSOptionString(opt, ast.Issuer)
		case subject:
			p.parseTLSOptionString(opt, ast.Subject)
		case san:
			p.parseTLSOptionString(opt, ast.SAN)
		case tokenIssuer:
			p.parseTLSOptionString(opt, ast.TokenIssuer)
		default:
			// Unknown option
			return opts
		}
		opts = append(opts, opt)
		if _, ok := p.accept(and); !ok {
			// Check if next is a TLS option keyword
			next := p.peek()
			switch next.Tp {
			case ssl, none, x509, cipher, issuer, subject, san, tokenIssuer:
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
		case maxQueriesPerHour:
			p.next()
			opt.Type = ast.MaxQueriesPerHour
			opt.Count, _ = p.parseInt64()
		case maxUpdatesPerHour:
			p.next()
			opt.Type = ast.MaxUpdatesPerHour
			opt.Count, _ = p.parseInt64()
		case maxConnectionsPerHour:
			p.next()
			opt.Type = ast.MaxConnectionsPerHour
			opt.Count, _ = p.parseInt64()
		case maxUserConnections:
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
		if tok.Tp == password {
			p.next()
			// PASSWORD EXPIRE ...
			next := p.peek()
			if next.Tp == expire {
				p.next() // consume EXPIRE
				opt.Type = ast.PasswordExpire
				// Check for optional DEFAULT, NEVER, INTERVAL N DAY
				if p.peek().Tp == defaultKwd {
					p.next()
					opt.Type = ast.PasswordExpireDefault
				} else if p.peekKeyword(never, "NEVER") {
					p.next()
					opt.Type = ast.PasswordExpireNever
				} else if p.peek().Tp == interval {
					p.next() // INTERVAL
					opt.Type = ast.PasswordExpireInterval
					if val, ok := p.expect(intLit); ok {
						count, _ := strconv.ParseInt(val.Lit, 10, 64)
						opt.Count = count
					}
					// DAY is usually expected but might be implied or checked
					if p.peekKeyword(day, "DAY") {
						p.next()
					}
				}
			} else if next.Tp == history {
				p.next() // HISTORY
				opt.Type = ast.PasswordHistory
				if p.peek().Tp == defaultKwd {
					p.next()
					opt.Type = ast.PasswordHistoryDefault
				} else if val, ok := p.accept(intLit); ok {
					count, _ := strconv.ParseInt(val.Lit, 10, 64)
					opt.Count = count
				}
			} else if next.Tp == reuse {
				p.next() // REUSE
				// INTERVAL N DAY
				if p.peek().Tp == interval {
					p.next()
					opt.Type = ast.PasswordReuseInterval
					if val, ok := p.expect(intLit); ok {
						count, _ := strconv.ParseInt(val.Lit, 10, 64)
						opt.Count = count
					}
					if p.peekKeyword(day, "DAY") {
						p.next()
					}
				} else if p.peek().Tp == defaultKwd {
					p.next()
					opt.Type = ast.PasswordReuseDefault
				}
			} else if next.Tp == require {
				// PASSWORD REQUIRE CURRENT [DEFAULT | OPTIONAL]
				p.next() // REQUIRE
				if p.peek().Tp == current {
					p.next() // CURRENT
					if p.peek().Tp == defaultKwd {
						p.next()
						opt.Type = ast.PasswordRequireCurrentDefault
					} else if p.peek().Tp == optional {
						p.next()
						// ast.PasswordRequireCurrentOptional ? (Not in my list, maybe 0?)
					}
				}
			} else {
				// Just PASSWORD token? unlikely in CREATE USER options
				return opts
			}
		} else if tok.Tp == lock || tok.Tp == unlock {
			if p.next().Tp == lock {
				opt.Type = ast.Lock
			} else {
				opt.Type = ast.Unlock
			}
		} else if tok.Tp == account {
			p.next() // ACCOUNT
			if p.peek().Tp != lock && p.peek().Tp != unlock {
				return opts
			}
			if p.next().Tp == lock {
				opt.Type = ast.Lock
			} else {
				opt.Type = ast.Unlock
			}
		} else if tok.Tp == failedLoginAttempts {
			p.next()
			opt.Type = ast.FailedLoginAttempts
			if val, ok := p.expect(intLit); ok {
				count, _ := strconv.ParseInt(val.Lit, 10, 64)
				opt.Count = count
			}
		} else if tok.Tp == passwordLockTime {
			p.next()
			opt.Type = ast.PasswordLockTime
			if p.peek().Tp == unbounded {
				p.next()
				opt.Type = ast.PasswordLockTimeUnbounded
			} else if val, ok := p.accept(intLit); ok {
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
	case comment:
		optType = ast.UserCommentType
	case attribute:
		optType = ast.UserAttributeType
	default:
		return nil
	}
	p.next()
	if val, ok := p.expect(stringLit); ok {
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
	p.expect(drop)
	if _, ok := p.accept(role); ok {
		stmt.IsDropRole = true
	} else {
		p.expect(user)
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

// parseAlterUserStmt parses ALTER USER [IF EXISTS] user [options]
func (p *HandParser) parseAlterUserStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterUserStmt](p.arena)
	p.expect(alter)
	p.expect(user)

	stmt.IfExists = p.acceptIfExists()

	// Handle ALTER USER USER() IDENTIFIED BY ... (CurrentAuth form)
	if p.peek().Tp == user && p.peekN(1).Tp == '(' {
		p.next() // consume USER
		p.next() // consume (
		p.expect(')')

		// Parse auth option for CurrentAuth
		if _, ok := p.accept(identified); ok {
			stmt.CurrentAuth = Alloc[ast.AuthOption](p.arena)
			if _, ok := p.accept(by); ok {
				if tok, ok := p.expect(stringLit); ok {
					stmt.CurrentAuth.ByAuthString = true
					stmt.CurrentAuth.AuthString = tok.Lit
				}
			}
		}
	} else {
		for {
			spec := p.parseUserSpec()
			if spec == nil {
				p.syntaxErrorAt(p.peek())
				return nil
			}
			stmt.Specs = append(stmt.Specs, spec)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	// Options
	if _, ok := p.accept(require); ok {
		stmt.AuthTokenOrTLSOptions = p.parseTLSOptions()
	}
	if _, ok := p.accept(with); ok {
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
	p.expect(rename)
	p.expect(user)

	for {
		u2u := Alloc[ast.UserToUser](p.arena)
		oldSpec := p.parseUserSpec()
		if oldSpec == nil {
			return nil
		}
		u2u.OldUser = oldSpec.User

		p.expect(to)

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

// parseUserSpec parses 'user'@'host' [IDENTIFIED BY 'password']
func (p *HandParser) parseUserSpec() *ast.UserSpec {
	spec := Alloc[ast.UserSpec](p.arena)
	spec.User = p.parseUserIdentity()
	if spec.User == nil {
		return nil
	}

	if _, ok := p.accept(identified); ok {
		spec.AuthOpt = Alloc[ast.AuthOption](p.arena)
		if _, ok := p.accept(with); ok {
			// IDENTIFIED WITH 'auth_plugin' [BY 'password' | AS 'hash']
			if tok, ok := p.expectAny(stringLit, identifier); ok {
				spec.AuthOpt.AuthPlugin = tok.Lit
			}
			if _, ok := p.accept(by); ok {
				if tok, ok := p.expect(stringLit); ok {
					spec.AuthOpt.ByAuthString = true
					spec.AuthOpt.AuthString = tok.Lit
				}
			} else if _, ok := p.accept(as); ok {
				if tok, ok := p.expectAny(stringLit, hexLit); ok {
					spec.AuthOpt.ByHashString = true
					if tok.Tp == hexLit {
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
		} else if _, ok := p.accept(as); ok {
			// IDENTIFIED AS 'hashstring' (restored form from BY PASSWORD)
			if tok, ok := p.expectAny(stringLit, hexLit); ok {
				spec.AuthOpt.ByHashString = true
				spec.AuthOpt.HashString = tok.Lit
			}
		} else {
			// IDENTIFIED BY [PASSWORD] 'password'
			p.expect(by)
			if _, ok := p.accept(password); ok {
				// IDENTIFIED BY PASSWORD 'hashstring' (deprecated pre-hashed form)
				// mysql_native_password is implied when using BY PASSWORD
				spec.AuthOpt.AuthPlugin = "mysql_native_password"
				if tok, ok := p.expect(stringLit); ok {
					spec.AuthOpt.ByHashString = true
					spec.AuthOpt.HashString = tok.Lit
				}
			} else {
				if tok, ok := p.expect(stringLit); ok {
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
	if val, ok := p.expect(stringLit); ok {
		opt.Type = tp
		opt.Value = val.Lit
	}
}

func (p *HandParser) parseUserResourceGroupOption() *ast.ResourceGroupNameOption {
	if p.peek().Tp == resource {
		p.next()
		p.expect(group)
		opt := Alloc[ast.ResourceGroupNameOption](p.arena)
		if tok, ok := p.expectAny(identifier, stringLit); ok {
			opt.Value = tok.Lit
		}
		return opt
	}
	return nil
}
