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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Account DDL and authentication grammar translated from
//! `pkg/parser/ddl_user_parser.go` plus the account identity primitives in
//! `pkg/parser/parser_helpers.go`.

use tidb_ast::{
    AlterUserDualPassword, AlterUserResourceKind, AlterUserResourceOption, AlterUserStmt,
    AlterUserTlsOption, CreateUserAuth, CreateUserCredential, CreateUserSpec, DdlStmt,
    RenameUserPair, RoleSpec, UserSpec,
};
use tidb_lexer::{is_reserved, TokenKind};

use crate::{decode_string, PResult, Parser};

#[derive(Clone, Copy)]
enum UserDdlKind {
    Create,
    Alter,
    Rename,
    Drop,
}

impl Parser {
    /// Recognizes the complete account-owned DDL prefixes. Keeping the
    /// classifier here lets the statement root retain only dispatch while the
    /// Go-owned leaf controls every token after the statement leader.
    pub(crate) fn is_user_ddl_statement(&self) -> bool {
        self.user_ddl_kind().is_some()
    }

    pub(crate) fn parse_user_ddl_statement(&mut self) -> PResult<DdlStmt> {
        match self
            .user_ddl_kind()
            .ok_or_else(|| self.err_here("expected account DDL statement"))?
        {
            UserDdlKind::Create => self.parse_create_user_or_role(),
            UserDdlKind::Alter => Ok(DdlStmt::AlterUser(Box::new(parse_alter_user(self)?))),
            UserDdlKind::Rename => self.parse_rename_user(),
            UserDdlKind::Drop => self.parse_drop_user_or_role(),
        }
    }

    fn user_ddl_kind(&self) -> Option<UserDdlKind> {
        if self.is_kw("CREATE") && (self.is_kw_at(1, "USER") || self.is_kw_at(1, "ROLE")) {
            Some(UserDdlKind::Create)
        } else if self.is_kw("ALTER") && self.is_kw_at(1, "USER") {
            Some(UserDdlKind::Alter)
        } else if self.is_kw("RENAME") && self.is_kw_at(1, "USER") {
            Some(UserDdlKind::Rename)
        } else if self.is_kw("DROP") && (self.is_kw_at(1, "USER") || self.is_kw_at(1, "ROLE")) {
            Some(UserDdlKind::Drop)
        } else {
            None
        }
    }

    fn parse_create_user_or_role(&mut self) -> PResult<DdlStmt> {
        self.expect_kw("CREATE")?;
        if self.is_kw("ROLE") {
            self.bump();
            return Ok(DdlStmt::CreateRole {
                if_not_exists: self.parse_if_not_exists()?,
                roles: self.parse_create_role_list()?,
            });
        }

        self.expect_kw("USER")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let mut users = vec![self.parse_create_user_spec()?];
        while self.is_op(",") {
            self.bump();
            users.push(self.parse_create_user_spec()?);
        }
        // Go `parseCreateUserStmt` owns REQUIRE/WITH at statement scope,
        // after every comma-separated account specification and before
        // password/account policy options. Reuse the already source-shaped
        // shared parsers so ALTER USER retains exactly its current grammar.
        let tls_options = if self.is_kw("REQUIRE") {
            self.bump();
            parse_tls_options(self)?
        } else {
            Vec::new()
        };
        let resource_options = if self.is_kw("WITH") {
            self.bump();
            parse_resource_options(self)?
        } else {
            Vec::new()
        };
        let password_options = self.parse_create_user_password_options()?;
        let comment_or_attribute = self.parse_create_user_comment_or_attribute()?;
        // Go parses RESOURCE GROUP after the statement-global annotation, not
        // as a per-user or `WITH` resource option. Reuse ALTER USER's exact
        // identifier/string-name grammar while keeping the typed payload on
        // CREATE USER itself.
        let resource_group = if self.is_kw("RESOURCE") {
            self.bump();
            self.expect_kw("GROUP")?;
            Some(parse_resource_group_name(self)?)
        } else {
            None
        };
        Ok(DdlStmt::CreateUser {
            if_not_exists,
            users,
            tls_options,
            resource_options,
            password_options,
            comment_or_attribute,
            resource_group,
        })
    }

    fn parse_rename_user(&mut self) -> PResult<DdlStmt> {
        self.expect_kw("RENAME")?;
        self.expect_kw("USER")?;
        let mut pairs = Vec::new();
        loop {
            let old_user = self.parse_user_spec()?;
            self.expect_kw("TO")?;
            let new_user = self.parse_user_spec()?;
            pairs.push(RenameUserPair { old_user, new_user });
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        Ok(DdlStmt::RenameUser { pairs })
    }

    fn parse_drop_user_or_role(&mut self) -> PResult<DdlStmt> {
        self.expect_kw("DROP")?;
        let is_role = if self.is_kw("ROLE") {
            self.bump();
            true
        } else {
            self.expect_kw("USER")?;
            false
        };
        let if_exists = self.parse_if_exists()?;
        let mut users = vec![self.parse_user_spec()?];
        while self.is_op(",") {
            self.bump();
            users.push(self.parse_user_spec()?);
        }
        Ok(DdlStmt::DropUser {
            is_role,
            if_exists,
            users,
        })
    }

    /// Parses `parseUserIdentity` and its `parseHostname` helper. Account
    /// hosts are decoded and lower-cased, and a missing host defaults to `%`.
    pub(crate) fn parse_user_spec(&mut self) -> PResult<UserSpec> {
        if self.is_kw("CURRENT_USER") || (self.is_kw("USER") && self.is_op_at(1, "(")) {
            self.bump();
            if self.is_op("(") {
                self.bump();
                self.expect_op(")")?;
            }
            return Ok(UserSpec {
                current_user: true,
                user: String::new(),
                host: String::new(),
            });
        }
        let token = self.peek().clone();
        let user = match token.kind {
            TokenKind::Str => {
                self.bump();
                decode_string(&token.text)
            }
            TokenKind::Ident => self.bump().text,
            TokenKind::Keyword if !is_reserved(&token.text) => self.bump().text,
            _ => return Err(self.err_here("expected a username")),
        };
        let host = if self.peek().kind == TokenKind::UserVar {
            crate::decode_at_name(&self.bump().text).to_lowercase()
        } else {
            "%".to_string()
        };
        Ok(UserSpec {
            current_user: false,
            user,
            host,
        })
    }

    /// Parses one CREATE/ALTER/GRANT user specification from Go's
    /// `parseUserSpec`, including every authentication spelling represented by
    /// this AST.
    pub(crate) fn parse_create_user_spec(&mut self) -> PResult<CreateUserSpec> {
        let user = self.parse_user_spec()?;
        let auth = if self.is_kw("IDENTIFIED") {
            self.bump();
            if self.is_kw("WITH") {
                self.bump();
                let plugin = self.parse_auth_plugin()?;
                let credential = if self.is_kw("BY") {
                    self.bump();
                    Some(CreateUserCredential::By(self.parse_string_literal(
                        "expected a string after IDENTIFIED WITH ... BY",
                    )?))
                } else if self.is_kw("AS") {
                    self.bump();
                    Some(CreateUserCredential::As(self.parse_auth_hash()?))
                } else {
                    None
                };
                Some(CreateUserAuth::With { plugin, credential })
            } else {
                self.expect_kw("BY")?;
                if self.is_kw("PASSWORD") {
                    self.bump();
                    Some(CreateUserAuth::With {
                        plugin: "mysql_native_password".to_string(),
                        credential: Some(CreateUserCredential::As(
                            self.parse_string_literal("expected a hash string after BY PASSWORD")?,
                        )),
                    })
                } else {
                    Some(CreateUserAuth::By(self.parse_string_literal(
                        "expected a string after IDENTIFIED BY",
                    )?))
                }
            }
        } else {
            None
        };
        Ok(CreateUserSpec {
            user,
            auth,
            dual_password: None,
        })
    }

    pub(crate) fn parse_create_user_password_options(
        &mut self,
    ) -> PResult<Vec<tidb_ast::CreateUserPasswordOption>> {
        let mut options = Vec::new();
        loop {
            let option = if self.is_kw("PASSWORD") {
                self.bump();
                if self.is_kw("EXPIRE") {
                    self.bump();
                    let expire = if self.is_kw("DEFAULT") {
                        self.bump();
                        tidb_ast::AlterUserPasswordExpire::Default
                    } else if self.is_kw("NEVER") {
                        self.bump();
                        tidb_ast::AlterUserPasswordExpire::Never
                    } else if self.is_kw("INTERVAL") {
                        self.bump();
                        let days = self.parse_user_policy_count(
                            "expected an integer after PASSWORD EXPIRE INTERVAL",
                        )?;
                        self.expect_kw("DAY")?;
                        tidb_ast::AlterUserPasswordExpire::Interval(days)
                    } else {
                        tidb_ast::AlterUserPasswordExpire::Expire
                    };
                    tidb_ast::CreateUserPasswordOption::Expire(expire)
                } else if self.is_kw("HISTORY") {
                    self.bump();
                    if self.is_kw("DEFAULT") {
                        self.bump();
                        tidb_ast::CreateUserPasswordOption::HistoryDefault
                    } else {
                        tidb_ast::CreateUserPasswordOption::History(self.parse_user_policy_count(
                            "expected an integer after PASSWORD HISTORY",
                        )?)
                    }
                } else if self.is_kw("REUSE") {
                    self.bump();
                    self.expect_kw("INTERVAL")?;
                    if self.is_kw("DEFAULT") {
                        self.bump();
                        tidb_ast::CreateUserPasswordOption::ReuseDefault
                    } else {
                        let days = self.parse_user_policy_count(
                            "expected an integer after PASSWORD REUSE INTERVAL",
                        )?;
                        self.expect_kw("DAY")?;
                        tidb_ast::CreateUserPasswordOption::ReuseInterval(days)
                    }
                } else if self.is_kw("REQUIRE") {
                    self.bump();
                    self.expect_kw("CURRENT")?;
                    self.expect_kw("DEFAULT")?;
                    tidb_ast::CreateUserPasswordOption::RequireCurrentDefault
                } else {
                    return Err(self.err_here("expected user password policy"));
                }
            } else if self.is_kw("ACCOUNT") {
                self.bump();
                if self.is_kw("LOCK") {
                    self.bump();
                    tidb_ast::CreateUserPasswordOption::AccountLock
                } else if self.is_kw("UNLOCK") {
                    self.bump();
                    tidb_ast::CreateUserPasswordOption::AccountUnlock
                } else {
                    return Err(self.err_here("expected LOCK or UNLOCK after ACCOUNT"));
                }
            } else if self.is_kw("FAILED_LOGIN_ATTEMPTS") {
                self.bump();
                tidb_ast::CreateUserPasswordOption::FailedLoginAttempts(
                    self.parse_user_policy_count(
                        "expected an integer after FAILED_LOGIN_ATTEMPTS",
                    )?,
                )
            } else if self.is_kw("PASSWORD_LOCK_TIME") {
                self.bump();
                if self.is_kw("UNBOUNDED") {
                    self.bump();
                    tidb_ast::CreateUserPasswordOption::PasswordLockTimeUnbounded
                } else {
                    tidb_ast::CreateUserPasswordOption::PasswordLockTime(
                        self.parse_user_policy_count(
                            "expected an integer after PASSWORD_LOCK_TIME",
                        )?,
                    )
                }
            } else {
                break;
            };
            options.push(option);
        }
        Ok(options)
    }

    fn parse_user_policy_count(&mut self, message: &str) -> PResult<i64> {
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here(message));
        }
        self.bump();
        Ok(token.text.parse().unwrap_or(i64::MAX))
    }

    pub(crate) fn parse_create_user_comment_or_attribute(
        &mut self,
    ) -> PResult<Option<tidb_ast::CreateUserCommentOrAttribute>> {
        let kind = if self.is_kw("COMMENT") {
            self.bump();
            Some(true)
        } else if self.is_kw("ATTRIBUTE") {
            self.bump();
            Some(false)
        } else {
            None
        };
        match kind {
            Some(true) => Ok(Some(tidb_ast::CreateUserCommentOrAttribute::Comment(
                self.parse_string_literal("expected a string after CREATE USER COMMENT")?,
            ))),
            Some(false) => Ok(Some(tidb_ast::CreateUserCommentOrAttribute::Attribute(
                self.parse_string_literal("expected a string after CREATE USER ATTRIBUTE")?,
            ))),
            None => Ok(None),
        }
    }

    fn parse_auth_plugin(&mut self) -> PResult<String> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Str => {
                self.bump();
                Ok(decode_string(&token.text))
            }
            TokenKind::Ident => Ok(self.bump().text),
            TokenKind::Keyword if !is_reserved(&token.text) => Ok(self.bump().text),
            _ => Err(self.err_here("expected an authentication plugin")),
        }
    }

    fn parse_auth_hash(&mut self) -> PResult<String> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Str => {
                self.bump();
                Ok(decode_string(&token.text))
            }
            TokenKind::HexLit => {
                self.bump();
                decode_auth_hex(&token.text).ok_or_else(|| self.err_here("invalid UTF-8 auth hash"))
            }
            _ => Err(self.err_here("expected a string or hexadecimal auth hash after AS")),
        }
    }

    /// CREATE ROLE uses Go's strict `parseRoleIdentity`, unlike SET ROLE's
    /// account-based `parseUserAsRole` path in `set.rs`.
    fn parse_create_role_spec(&mut self) -> PResult<RoleSpec> {
        let token = self.peek().clone();
        let composed = self.peek_n(1).kind == TokenKind::UserVar;
        let role = match token.kind {
            TokenKind::Ident => self.bump().text,
            TokenKind::Str => {
                self.bump();
                decode_string(&token.text)
            }
            TokenKind::Keyword if !is_reserved(&token.text) && composed => self.bump().text,
            _ => return Err(self.err_here("expected a role name")),
        };
        let host = if self.peek().kind == TokenKind::UserVar {
            crate::decode_at_name(&self.bump().text).to_lowercase()
        } else {
            "%".to_string()
        };
        Ok(RoleSpec { role, host })
    }

    fn parse_create_role_list(&mut self) -> PResult<Vec<RoleSpec>> {
        let mut roles = vec![self.parse_create_role_spec()?];
        while self.is_op(",") {
            self.bump();
            roles.push(self.parse_create_role_spec()?);
        }
        Ok(roles)
    }
}

/// Parses the account-owned `SHOW CREATE USER` form from Go's
/// `parseShowCreate`. It is kept with user grammar because its target is a
/// [`UserSpec`], not an ordinary schema object name.
pub(crate) fn parse_show_create_user(parser: &mut Parser) -> PResult<UserSpec> {
    parser.expect_kw("SHOW")?;
    parser.expect_kw("CREATE")?;
    parser.expect_kw("USER")?;
    parser.parse_user_spec()
}

pub(crate) fn parse_alter_user(parser: &mut Parser) -> PResult<AlterUserStmt> {
    parser.expect_kw("ALTER")?;
    parser.expect_kw("USER")?;
    let if_exists = parser.parse_if_exists()?;
    let (users, user_function_auth, user_function_dual_password) =
        if parser.is_kw("USER") && parser.is_op_at(1, "(") {
            let (auth, dual_password) = parse_user_function(parser)?;
            (Vec::new(), auth, dual_password)
        } else {
            let mut users = vec![parse_user_spec(parser)?];
            while parser.is_op(",") {
                parser.bump();
                users.push(parse_user_spec(parser)?);
            }
            (users, None, None)
        };

    let tls_options = if parser.is_kw("REQUIRE") {
        parser.bump();
        parse_tls_options(parser)?
    } else {
        Vec::new()
    };
    let resource_options = if parser.is_kw("WITH") {
        parser.bump();
        parse_resource_options(parser)?
    } else {
        Vec::new()
    };
    let password_options = parser.parse_create_user_password_options()?;
    let comment_or_attribute = parser.parse_create_user_comment_or_attribute()?;
    let resource_group = if parser.is_kw("RESOURCE") {
        parser.bump();
        parser.expect_kw("GROUP")?;
        Some(parse_resource_group_name(parser)?)
    } else {
        None
    };
    Ok(AlterUserStmt {
        if_exists,
        users,
        user_function_auth,
        user_function_dual_password,
        tls_options,
        resource_options,
        password_options,
        comment_or_attribute,
        resource_group,
    })
}

fn parse_user_spec(parser: &mut Parser) -> PResult<CreateUserSpec> {
    let mut spec = parser.parse_create_user_spec()?;
    if parser.is_kw("RETAIN") {
        let can_retain = matches!(
            spec.auth,
            Some(CreateUserAuth::By(_))
                | Some(CreateUserAuth::With {
                    credential: Some(CreateUserCredential::By(_)),
                    ..
                })
        );
        if !can_retain {
            return Err(parser.err_here("RETAIN CURRENT PASSWORD requires IDENTIFIED BY"));
        }
        parser.bump();
        parser.expect_kw("CURRENT")?;
        parser.expect_kw("PASSWORD")?;
        spec.dual_password = Some(AlterUserDualPassword::RetainCurrent);
    } else if parser.is_kw("DISCARD") {
        if spec.auth.is_some() {
            return Err(parser.err_here("DISCARD OLD PASSWORD cannot follow authentication"));
        }
        parser.bump();
        parser.expect_kw("OLD")?;
        parser.expect_kw("PASSWORD")?;
        spec.dual_password = Some(AlterUserDualPassword::DiscardOld);
    }
    Ok(spec)
}

fn parse_user_function(
    parser: &mut Parser,
) -> PResult<(Option<String>, Option<AlterUserDualPassword>)> {
    parser.expect_kw("USER")?;
    parser.expect_op("(")?;
    parser.expect_op(")")?;
    if parser.is_kw("DISCARD") {
        parser.bump();
        parser.expect_kw("OLD")?;
        parser.expect_kw("PASSWORD")?;
        return Ok((None, Some(AlterUserDualPassword::DiscardOld)));
    }
    parser.expect_kw("IDENTIFIED")?;
    parser.expect_kw("BY")?;
    if parser.is_kw("PASSWORD") {
        return Err(parser.err_here("ALTER USER USER() does not support IDENTIFIED BY PASSWORD"));
    }
    let password =
        parser.parse_string_literal("expected a string after ALTER USER USER() IDENTIFIED BY")?;
    let dual_password = if parser.is_kw("RETAIN") {
        parser.bump();
        parser.expect_kw("CURRENT")?;
        parser.expect_kw("PASSWORD")?;
        Some(AlterUserDualPassword::RetainCurrent)
    } else {
        None
    };
    Ok((Some(password), dual_password))
}

fn parse_resource_group_name(parser: &mut Parser) -> PResult<String> {
    let token = parser.peek().clone();
    match token.kind {
        TokenKind::Str => {
            parser.bump();
            Ok(decode_string(&token.text))
        }
        TokenKind::Ident => Ok(parser.bump().text),
        TokenKind::Keyword if !crate::is_reserved(&token.text) => Ok(parser.bump().text),
        _ => Err(parser.err_here("expected ALTER USER resource group name")),
    }
}

fn parse_tls_options(parser: &mut Parser) -> PResult<Vec<AlterUserTlsOption>> {
    let mut options = Vec::new();
    loop {
        let option = if parser.is_kw("NONE") {
            parser.bump();
            AlterUserTlsOption::None
        } else if parser.is_kw("SSL") {
            parser.bump();
            AlterUserTlsOption::Ssl
        } else if parser.is_kw("X509") {
            parser.bump();
            AlterUserTlsOption::X509
        } else if parser.is_kw("CIPHER") {
            parser.bump();
            AlterUserTlsOption::Cipher(parse_tls_value(parser)?)
        } else if parser.is_kw("ISSUER") {
            parser.bump();
            AlterUserTlsOption::Issuer(parse_tls_value(parser)?)
        } else if parser.is_kw("SUBJECT") {
            parser.bump();
            AlterUserTlsOption::Subject(parse_tls_value(parser)?)
        } else if parser.is_kw("SAN") {
            parser.bump();
            AlterUserTlsOption::San(parse_tls_value(parser)?)
        } else if parser.is_kw("TOKEN_ISSUER") {
            parser.bump();
            AlterUserTlsOption::TokenIssuer(parse_tls_value(parser)?)
        } else {
            break;
        };
        let duplicate_parse_error = matches!(
            option,
            AlterUserTlsOption::None | AlterUserTlsOption::Ssl | AlterUserTlsOption::X509
        ) && options.contains(&option);
        if duplicate_parse_error {
            return Err(parser.err_here("duplicate ALTER USER TLS option"));
        }
        options.push(option);
        if parser.is_kw("AND") {
            parser.bump();
        }
    }
    Ok(options)
}

fn parse_tls_value(parser: &mut Parser) -> PResult<String> {
    let token = parser.peek().clone();
    if token.kind != TokenKind::Str {
        return Err(parser.err_here("expected TLS option string"));
    }
    parser.bump();
    Ok(decode_string(&token.text))
}

fn parse_resource_options(parser: &mut Parser) -> PResult<Vec<AlterUserResourceOption>> {
    let mut options = Vec::new();
    while let Some(kind) = if parser.is_kw("MAX_QUERIES_PER_HOUR") {
        Some(AlterUserResourceKind::MaxQueriesPerHour)
    } else if parser.is_kw("MAX_UPDATES_PER_HOUR") {
        Some(AlterUserResourceKind::MaxUpdatesPerHour)
    } else if parser.is_kw("MAX_CONNECTIONS_PER_HOUR") {
        Some(AlterUserResourceKind::MaxConnectionsPerHour)
    } else if parser.is_kw("MAX_USER_CONNECTIONS") {
        Some(AlterUserResourceKind::MaxUserConnections)
    } else {
        None
    } {
        parser.bump();
        let token = parser.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(parser.err_here("expected resource option count"));
        }
        parser.bump();
        options.push(AlterUserResourceOption {
            kind,
            count: token
                .text
                .parse::<i64>()
                .map_err(|_| parser.err_here("resource option count out of range"))?,
        });
    }
    Ok(options)
}

fn decode_auth_hex(raw: &str) -> Option<String> {
    let digits = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .or_else(|| {
            raw.strip_prefix("x'")
                .and_then(|value| value.strip_suffix('\''))
        })
        .or_else(|| {
            raw.strip_prefix("X'")
                .and_then(|value| value.strip_suffix('\''))
        })?;
    if digits.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(digits.len() / 2);
    for pair in digits.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(pair).ok()?;
        bytes.push(u8::from_str_radix(pair, 16).ok()?);
    }
    String::from_utf8(bytes).ok()
}
