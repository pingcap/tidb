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

//! GRANT/REVOKE privilege grammar translated from
//! `pkg/parser/grant_revoke_parser.go`.

#[path = "privilege/role_grant.rs"]
mod role_grant;

use tidb_ast::{
    GrantLevel, GrantObjectType, GrantPrivilege, GrantStmt, RevokeStmt, ShowGrantsStmt,
};
use tidb_lexer::TokenKind;

use crate::{PResult, Parser};

impl Parser {
    /// Direct translation of Go's `parseShowGrants`: the optional `FOR`
    /// account owns an optional `USING` role list.
    pub(crate) fn parse_show_grants(&mut self) -> PResult<ShowGrantsStmt> {
        self.expect_kw("SHOW")?;
        self.expect_kw("GRANTS")?;
        let user = if self.is_kw("FOR") {
            self.bump();
            Some(self.parse_user_spec()?)
        } else {
            None
        };
        let mut roles = Vec::new();
        if self.is_kw("USING") {
            if user.is_none() {
                return Err(self.err_here("SHOW GRANTS USING requires FOR user"));
            }
            self.bump();
            roles.push(self.parse_user_spec()?);
            while self.is_op(",") {
                self.bump();
                roles.push(self.parse_user_spec()?);
            }
        }
        Ok(ShowGrantsStmt { user, roles })
    }

    /// Direct translation of Go's `parseGrantStmt` privilege branch. It
    /// retains per-account `IDENTIFIED` authentication in the same typed
    /// account payload used by CREATE USER and carries the statement-level
    /// `REQUIRE` TLS payload before the optional grant flag.
    pub(crate) fn parse_grant_privilege_stmt(&mut self) -> PResult<GrantStmt> {
        self.expect_kw("GRANT")?;
        let mut privileges = vec![self.parse_grant_privilege()?];
        while self.is_op(",") {
            self.bump();
            privileges.push(self.parse_grant_privilege()?);
        }
        self.expect_kw("ON")?;
        let object_type = self.parse_grant_object_type();
        let level = self.parse_grant_level()?;
        self.expect_kw("TO")?;
        let mut users = vec![self.parse_create_user_spec()?];
        while self.is_op(",") {
            self.bump();
            users.push(self.parse_create_user_spec()?);
        }
        let tls_options = if self.is_kw("REQUIRE") {
            self.bump();
            self.parse_grant_tls_options()?
        } else {
            Vec::new()
        };
        let with_grant = if self.is_kw("WITH") {
            self.bump();
            self.expect_kw("GRANT")?;
            self.expect_kw("OPTION")?;
            true
        } else {
            false
        };
        Ok(GrantStmt {
            privileges,
            object_type,
            level,
            users,
            tls_options,
            with_grant,
        })
    }

    fn parse_grant_tls_options(&mut self) -> PResult<Vec<tidb_ast::AlterUserTlsOption>> {
        let mut options = Vec::new();
        loop {
            let option = if self.is_kw("NONE") {
                self.bump();
                tidb_ast::AlterUserTlsOption::None
            } else if self.is_kw("SSL") {
                self.bump();
                tidb_ast::AlterUserTlsOption::Ssl
            } else if self.is_kw("X509") {
                self.bump();
                tidb_ast::AlterUserTlsOption::X509
            } else if self.is_kw("CIPHER") {
                self.bump();
                tidb_ast::AlterUserTlsOption::Cipher(self.parse_grant_tls_value()?)
            } else if self.is_kw("ISSUER") {
                self.bump();
                tidb_ast::AlterUserTlsOption::Issuer(self.parse_grant_tls_value()?)
            } else if self.is_kw("SUBJECT") {
                self.bump();
                tidb_ast::AlterUserTlsOption::Subject(self.parse_grant_tls_value()?)
            } else if self.is_kw("SAN") {
                self.bump();
                tidb_ast::AlterUserTlsOption::San(self.parse_grant_tls_value()?)
            } else if self.is_kw("TOKEN_ISSUER") {
                self.bump();
                tidb_ast::AlterUserTlsOption::TokenIssuer(self.parse_grant_tls_value()?)
            } else {
                break;
            };
            let duplicate_simple = matches!(
                option,
                tidb_ast::AlterUserTlsOption::None
                    | tidb_ast::AlterUserTlsOption::Ssl
                    | tidb_ast::AlterUserTlsOption::X509
            ) && options.iter().any(|existing| existing == &option);
            if duplicate_simple {
                return Err(self.err_here("duplicate GRANT TLS option"));
            }
            options.push(option);
            if self.is_kw("AND") {
                self.bump();
            }
        }
        if options.is_empty() {
            return Err(self.err_here("expected GRANT REQUIRE option"));
        }
        Ok(options)
    }

    fn parse_grant_tls_value(&mut self) -> PResult<String> {
        let token = self.peek().clone();
        if token.kind != TokenKind::Str {
            return Err(self.err_here("expected GRANT TLS option string"));
        }
        self.bump();
        Ok(crate::decode_string(&token.text))
    }

    /// Direct translation of the privilege branch in Go's `parseRevokeStmt`.
    /// Identifier-only dynamic privileges are retained as a typed
    /// [`GrantPrivilege::dynamic`] payload. Go's `REVOKE ALL, GRANT OPTION
    /// FROM ...` production is converted to the same `RevokeStmt` shape as
    /// the `ON ... FROM ...` branch, with an implicit global level; detect
    /// that exact two-privilege form before requiring `ON`.
    pub(crate) fn parse_revoke_privilege_stmt(&mut self) -> PResult<RevokeStmt> {
        self.expect_kw("REVOKE")?;
        let mut privileges = vec![self.parse_revoke_privilege()?];
        while self.is_op(",") {
            self.bump();
            privileges.push(self.parse_revoke_privilege()?);
        }
        if self.is_kw("FROM") {
            let is_all_grant_option = privileges.len() == 2
                && privileges[0].name == "ALL"
                && privileges[0].columns.is_empty()
                && !privileges[0].dynamic
                && privileges[1].name == "GRANT OPTION"
                && privileges[1].columns.is_empty()
                && !privileges[1].dynamic;
            if !is_all_grant_option {
                return Err(self.err_here("expected ON in REVOKE privilege statement"));
            }
            self.bump();
            let mut users = vec![self.parse_user_spec()?];
            while self.is_op(",") {
                self.bump();
                users.push(self.parse_user_spec()?);
            }
            return Ok(RevokeStmt {
                privileges,
                object_type: None,
                level: GrantLevel::Global,
                users,
            });
        }
        self.expect_kw("ON")?;
        let object_type = self.parse_grant_object_type();
        let level = self.parse_grant_level()?;
        self.expect_kw("FROM")?;
        let mut users = vec![self.parse_user_spec()?];
        while self.is_op(",") {
            self.bump();
            users.push(self.parse_user_spec()?);
        }
        Ok(RevokeStmt {
            privileges,
            object_type,
            level,
            users,
        })
    }

    /// Go's `ExtendedPriv` grammar accepts any identifier-only privilege name
    /// on the `REVOKE ... ON ... FROM ...` branch. Keep that arbitrary name
    /// in the typed dynamic payload; executor support remains a separate
    /// capability boundary.
    fn parse_revoke_privilege(&mut self) -> PResult<GrantPrivilege> {
        self.parse_grant_privilege()
    }

    /// Go's shared `parseObjectType` helper for privilege GRANT/REVOKE.
    fn parse_grant_object_type(&mut self) -> Option<GrantObjectType> {
        if self.is_kw("TABLE") {
            self.bump();
            Some(GrantObjectType::Table)
        } else if self.is_kw("FUNCTION") {
            self.bump();
            Some(GrantObjectType::Function)
        } else if self.is_kw("PROCEDURE") {
            self.bump();
            Some(GrantObjectType::Procedure)
        } else {
            None
        }
    }

    /// Direct translation of Go's `parseGrantLevel`: `*.*`, `*`, `db.*`,
    /// `db.table`, or a single table name.
    fn parse_grant_level(&mut self) -> PResult<GrantLevel> {
        if self.is_op("*") {
            self.bump();
            if self.is_op(".") {
                self.bump();
                self.expect_op("*")?;
                return Ok(GrantLevel::Global);
            }
            return Ok(GrantLevel::Database(None));
        }
        let first = self.parse_name_or_keyword()?;
        if !self.is_op(".") {
            return Ok(GrantLevel::Table {
                database: None,
                table: first,
            });
        }
        self.bump();
        if self.is_op("*") {
            self.bump();
            Ok(GrantLevel::Database(Some(first)))
        } else {
            Ok(GrantLevel::Table {
                database: Some(first),
                table: self.parse_name_or_keyword()?,
            })
        }
    }

    /// Direct translation of Go's `tryParsePrivilege` plus its
    /// `ExtendedPriv` fallback. Dynamic privileges are identifier-only and
    /// restore as the canonical uppercase phrase just like Go's
    /// `format.RestoreCtx.WriteKeyWord` path.
    fn parse_grant_privilege(&mut self) -> PResult<GrantPrivilege> {
        self.parse_privilege(true)
    }

    fn parse_privilege(&mut self, allow_extended: bool) -> PResult<GrantPrivilege> {
        let (name, dynamic) = if self.is_kw("ALL") {
            self.bump();
            if self.is_kw("PRIVILEGES") {
                self.bump();
            }
            ("ALL".to_string(), false)
        } else if self.is_kw("SELECT") {
            self.bump();
            ("SELECT".to_string(), false)
        } else if self.is_kw("INSERT") {
            self.bump();
            ("INSERT".to_string(), false)
        } else if self.is_kw("UPDATE") {
            self.bump();
            ("UPDATE".to_string(), false)
        } else if self.is_kw("DELETE") {
            self.bump();
            ("DELETE".to_string(), false)
        } else if self.is_kw("DROP") {
            self.bump();
            if self.is_kw("ROLE") {
                self.bump();
                ("DROP ROLE".to_string(), false)
            } else {
                ("DROP".to_string(), false)
            }
        } else if self.is_kw("GRANT") {
            self.bump();
            self.expect_kw("OPTION")?;
            ("GRANT OPTION".to_string(), false)
        } else if self.is_kw("INDEX") {
            self.bump();
            ("INDEX".to_string(), false)
        } else if self.is_kw("ALTER") {
            self.bump();
            if self.is_kw("ROUTINE") {
                self.bump();
                ("ALTER ROUTINE".to_string(), false)
            } else {
                ("ALTER".to_string(), false)
            }
        } else if self.is_kw("EXECUTE") {
            self.bump();
            ("EXECUTE".to_string(), false)
        } else if self.is_kw("CONFIG") {
            self.bump();
            ("CONFIG".to_string(), false)
        } else if self.is_kw("REFERENCES") {
            self.bump();
            ("REFERENCES".to_string(), false)
        } else if self.is_kw("USAGE") {
            self.bump();
            ("USAGE".to_string(), false)
        } else if self.is_kw("PROCESS") {
            self.bump();
            ("PROCESS".to_string(), false)
        } else if self.is_kw("SUPER") {
            self.bump();
            ("SUPER".to_string(), false)
        } else if self.is_kw("EVENT") {
            self.bump();
            ("EVENT".to_string(), false)
        } else if self.is_kw("FILE") {
            self.bump();
            ("FILE".to_string(), false)
        } else if self.is_kw("TRIGGER") {
            self.bump();
            ("TRIGGER".to_string(), false)
        } else if self.is_kw("SHUTDOWN") {
            self.bump();
            ("SHUTDOWN".to_string(), false)
        } else if self.is_kw("RELOAD") {
            self.bump();
            ("RELOAD".to_string(), false)
        } else if self.is_kw("REPLICATION") {
            self.bump();
            if self.is_kw("CLIENT") {
                self.bump();
                ("REPLICATION CLIENT".to_string(), false)
            } else if self.is_kw("SLAVE") {
                self.bump();
                ("REPLICATION SLAVE".to_string(), false)
            } else {
                return Err(self.err_here("expected CLIENT or SLAVE after REPLICATION"));
            }
        } else if self.is_kw("CREATE") {
            self.bump();
            if self.is_kw("VIEW") {
                self.bump();
                ("CREATE VIEW".to_string(), false)
            } else if self.is_kw("USER") {
                self.bump();
                ("CREATE USER".to_string(), false)
            } else if self.is_kw("ROLE") {
                self.bump();
                ("CREATE ROLE".to_string(), false)
            } else if self.is_kw("TEMPORARY") {
                self.bump();
                self.expect_kw("TABLES")?;
                ("CREATE TEMPORARY TABLES".to_string(), false)
            } else if self.is_kw("TABLESPACE") {
                self.bump();
                ("CREATE TABLESPACE".to_string(), false)
            } else if self.is_kw("ROUTINE") {
                self.bump();
                ("CREATE ROUTINE".to_string(), false)
            } else {
                ("CREATE".to_string(), false)
            }
        } else if self.is_kw("SHOW") {
            self.bump();
            if self.is_kw("DATABASES") {
                self.bump();
                ("SHOW DATABASES".to_string(), false)
            } else if self.is_kw("VIEW") {
                self.bump();
                ("SHOW VIEW".to_string(), false)
            } else {
                return Err(self.err_here("expected DATABASES or VIEW after SHOW"));
            }
        } else if self.is_kw("LOCK") {
            self.bump();
            self.expect_kw("TABLES")?;
            ("LOCK TABLES".to_string(), false)
        } else {
            if !allow_extended {
                return Err(self.err_here("expected a standard privilege"));
            }
            let mut words = Vec::new();
            while self.peek().kind == TokenKind::Ident {
                words.push(self.bump().text.to_ascii_uppercase());
            }
            if words.is_empty() {
                return Err(self.err_here("expected a privilege"));
            }
            (words.join(" "), true)
        };
        let mut columns = Vec::new();
        if self.is_op("(") {
            self.bump();
            columns.push(self.parse_name_or_keyword()?);
            while self.is_op(",") {
                self.bump();
                columns.push(self.parse_name_or_keyword()?);
            }
            self.expect_op(")")?;
        }
        Ok(GrantPrivilege {
            name,
            columns,
            dynamic,
        })
    }
}
