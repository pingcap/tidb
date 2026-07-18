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

//! Complete SET grammar translated from `pkg/parser/set_explain_parser.go`.
//! The statement router distinguishes bindings first; this leaf then owns
//! account passwords, roles, user variables, and ordinary session/system
//! settings without routing any of them through the generic assignment AST.

use tidb_ast::{
    CharsetSetKind, DefaultRoleSelection, Expr, RoleSpec, SessionStmt, SetDefaultRoleStmt,
    SetPasswordStmt, SetResourceGroupStmt, SetRoleSelection, SetRoleStmt, SetSessionStatesStmt,
    SetStmt, SetUserVarStmt, SetVariableValue, SystemVariableAssignment, SystemVariableScope,
    UserVariableAssignment,
};
use tidb_lexer::{canonical_charset, TokenKind};

use crate::{decode_string, prec, PResult, Parser};

impl Parser {
    /// Recognizes SET families whose typed payload is not the ordinary
    /// system/session-variable [`SetStmt`]. The top-level router uses this
    /// exact predicate before falling through to [`Self::parse_session_set_statement`].
    pub(crate) fn is_specialized_set_statement(&self) -> bool {
        self.is_kw("SET")
            && (self.is_kw_at(1, "PASSWORD")
                || self.is_kw_at(1, "ROLE")
                || (self.is_kw_at(1, "DEFAULT") && self.is_kw_at(2, "ROLE"))
                || (self.peek_n(1).kind == TokenKind::UserVar
                    && !self.peek_n(1).text.starts_with("@@")))
    }

    pub(crate) fn parse_specialized_set_statement(&mut self) -> PResult<SessionStmt> {
        if self.is_kw_at(1, "PASSWORD") {
            return Ok(SessionStmt::SetPassword(Box::new(
                self.parse_set_password()?,
            )));
        }
        if self.is_kw_at(1, "ROLE") || (self.is_kw_at(1, "DEFAULT") && self.is_kw_at(2, "ROLE")) {
            return self.parse_set_role_command();
        }
        if self.peek_n(1).kind == TokenKind::UserVar && !self.peek_n(1).text.starts_with("@@") {
            return Ok(SessionStmt::SetUserVar(Box::new(
                self.parse_set_uservar_stmt()?,
            )));
        }
        Err(self.err_here("expected specialized SET statement"))
    }

    fn parse_set_password(&mut self) -> PResult<SetPasswordStmt> {
        self.expect_kw("SET")?;
        self.expect_kw("PASSWORD")?;
        let user = if self.is_kw("FOR") {
            self.bump();
            Some(self.parse_user_spec()?)
        } else {
            None
        };
        if self.is_op(":=") {
            self.bump();
        } else {
            self.expect_op("=")?;
        }
        let password = if self.is_kw("PASSWORD") {
            self.bump();
            self.expect_op("(")?;
            let token = self.peek().clone();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected a string in PASSWORD(...)"));
            }
            self.bump();
            self.expect_op(")")?;
            decode_string(&token.text)
        } else {
            let token = self.peek().clone();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected a password string"));
            }
            self.bump();
            decode_string(&token.text)
        };
        let retain_current_password = if self.is_kw("RETAIN") {
            self.bump();
            self.expect_kw("CURRENT")?;
            self.expect_kw("PASSWORD")?;
            true
        } else {
            false
        };
        Ok(SetPasswordStmt {
            user,
            password,
            retain_current_password,
        })
    }

    fn parse_set_role_command(&mut self) -> PResult<SessionStmt> {
        self.expect_kw("SET")?;
        if self.is_kw("ROLE") {
            self.bump();
            let selection = if self.is_kw("DEFAULT") {
                self.bump();
                SetRoleSelection::Default
            } else if self.is_kw("NONE") {
                self.bump();
                SetRoleSelection::None
            } else if self.is_kw("ALL") {
                self.bump();
                if self.is_kw("EXCEPT") {
                    self.bump();
                    SetRoleSelection::AllExcept(self.parse_set_role_list()?)
                } else {
                    SetRoleSelection::All
                }
            } else {
                SetRoleSelection::Roles(self.parse_set_role_list()?)
            };
            return Ok(SessionStmt::SetRole(Box::new(SetRoleStmt { selection })));
        }

        self.expect_kw("DEFAULT")?;
        self.expect_kw("ROLE")?;
        let selection = if self.is_kw("NONE") {
            self.bump();
            DefaultRoleSelection::None
        } else if self.is_kw("ALL") {
            self.bump();
            DefaultRoleSelection::All
        } else {
            DefaultRoleSelection::Roles(self.parse_set_role_list()?)
        };
        self.expect_kw("TO")?;
        let mut users = vec![self.parse_user_spec()?];
        while self.is_op(",") {
            self.bump();
            users.push(self.parse_user_spec()?);
        }
        Ok(SessionStmt::SetDefaultRole(Box::new(SetDefaultRoleStmt {
            selection,
            users,
        })))
    }

    /// Direct translation of Go's `parseUserAsRole`: SET ROLE parses an
    /// account identity and then projects username/host into RoleIdentity.
    /// This is deliberately wider than CREATE ROLE's strict role-name helper.
    fn parse_set_role_spec(&mut self) -> PResult<RoleSpec> {
        let user = self.parse_user_spec()?;
        Ok(RoleSpec {
            role: user.user,
            host: user.host,
        })
    }

    fn parse_set_role_list(&mut self) -> PResult<Vec<RoleSpec>> {
        let mut roles = vec![self.parse_set_role_spec()?];
        while self.is_op(",") {
            self.bump();
            roles.push(self.parse_set_role_spec()?);
        }
        Ok(roles)
    }

    fn parse_set_uservar_stmt(&mut self) -> PResult<SetUserVarStmt> {
        self.expect_kw("SET")?;
        let mut assignments = vec![self.parse_set_uservar_assignment()?];
        while self.is_op(",") {
            self.bump();
            assignments.push(self.parse_set_uservar_assignment()?);
        }
        Ok(SetUserVarStmt { assignments })
    }

    fn parse_set_uservar_assignment(&mut self) -> PResult<UserVariableAssignment> {
        let token = self.bump();
        let name = token
            .text
            .strip_prefix('@')
            .filter(|name| !name.starts_with(['@', '\'', '"', '`']))
            .ok_or_else(|| self.err_here("malformed user variable"))?
            .to_string();
        if self.is_op(":=") {
            self.bump();
        } else {
            self.expect_op("=")?;
        }
        let value = self.parse_set_expr_value()?;
        Ok(UserVariableAssignment { name, value })
    }

    /// Routes ordinary current-session SET families after the top-level
    /// parser excludes bindings, account/role commands, and `@user_var`.
    pub(crate) fn parse_session_set_statement(&mut self) -> PResult<SessionStmt> {
        if self.is_set_session_states_command() {
            return Ok(SessionStmt::SetSessionStates(Box::new(
                self.parse_set_session_states()?,
            )));
        }
        if self.is_set_resource_group_command() {
            return Ok(SessionStmt::SetResourceGroup(Box::new(
                self.parse_set_resource_group()?,
            )));
        }
        if self.is_set_charset_command() {
            return self.parse_set_charset_command();
        }
        Ok(SessionStmt::Set(Box::new(self.parse_set_stmt()?)))
    }

    fn parse_set_stmt(&mut self) -> PResult<SetStmt> {
        self.expect_kw("SET")?;
        if self.is_kw("TRANSACTION") {
            return self.parse_set_transaction(true);
        }
        if self.is_kw("SESSION") && self.is_kw_at(1, "TRANSACTION") {
            self.bump();
            return self.parse_set_transaction(false);
        }
        let mut assignments = Vec::new();
        loop {
            assignments.push(self.parse_system_variable_assignment()?);
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        Ok(SetStmt { assignments })
    }

    fn is_set_charset_command(&self) -> bool {
        self.is_kw("SET")
            && (self.is_kw_at(1, "NAMES")
                || self.is_kw_at(1, "CHARSET")
                || ((self.is_kw_at(1, "CHARACTER") || self.is_kw_at(1, "CHAR"))
                    && self.is_kw_at(2, "SET")))
    }

    fn is_set_session_states_command(&self) -> bool {
        self.is_kw("SET") && self.is_kw_at(1, "SESSION_STATES")
    }

    fn is_set_resource_group_command(&self) -> bool {
        self.is_kw("SET") && self.is_kw_at(1, "RESOURCE") && self.is_kw_at(2, "GROUP")
    }

    fn parse_set_session_states(&mut self) -> PResult<SetSessionStatesStmt> {
        self.expect_kw("SET")?;
        self.expect_kw("SESSION_STATES")?;
        let token = self.peek().clone();
        if token.kind != TokenKind::Str {
            return Err(self.err_here("expected serialized session state string"));
        }
        self.bump();
        Ok(SetSessionStatesStmt {
            session_states: decode_string(&token.text),
        })
    }

    fn parse_set_resource_group(&mut self) -> PResult<SetResourceGroupStmt> {
        self.expect_kw("SET")?;
        self.expect_kw("RESOURCE")?;
        self.expect_kw("GROUP")?;
        let token = self.peek().clone();
        let name = match token.kind {
            // The hand parser consumes this position directly; identifiers
            // and DEFAULT all restore as identifier names.
            TokenKind::Ident | TokenKind::Keyword => {
                self.bump();
                token.text
            }
            _ => return Err(self.err_here("expected resource group name")),
        };
        Ok(SetResourceGroupStmt { name })
    }

    fn parse_set_charset_command(&mut self) -> PResult<SessionStmt> {
        self.expect_kw("SET")?;
        let kind = if self.is_kw("NAMES") {
            self.bump();
            CharsetSetKind::Names
        } else if self.is_kw("CHARSET") {
            self.bump();
            CharsetSetKind::Charset
        } else {
            self.bump(); // CHARACTER or CHAR
            self.expect_kw("SET")?;
            CharsetSetKind::Charset
        };
        let charset = if self.is_kw("DEFAULT") {
            self.bump();
            None
        } else {
            Some(self.parse_set_charset_name()?)
        };
        let collation = if kind == CharsetSetKind::Names && self.is_kw("COLLATE") {
            self.bump();
            if self.is_kw("DEFAULT") {
                self.bump();
                None
            } else {
                Some(self.parse_set_collation_name()?)
            }
        } else {
            None
        };
        Ok(SessionStmt::SetCharset {
            kind,
            charset,
            collation,
        })
    }

    fn parse_set_charset_name(&mut self) -> PResult<String> {
        let name = if self.peek().kind == TokenKind::Str {
            decode_string(&self.bump().text)
        } else {
            self.parse_charset_name()?
        };
        canonical_charset(&name)
            .map(str::to_owned)
            .ok_or_else(|| self.err_here("unknown character set"))
    }

    fn parse_set_collation_name(&mut self) -> PResult<String> {
        if self.peek().kind == TokenKind::Str {
            Ok(decode_string(&self.bump().text))
        } else {
            self.parse_charset_name()
        }
    }

    fn parse_system_variable_assignment(&mut self) -> PResult<SystemVariableAssignment> {
        let scope = if self.peek().kind == TokenKind::UserVar {
            self.parse_atat_system_variable_scope_and_name()?
        } else {
            let scope = if self.is_kw("GLOBAL") && !self.is_assignment_at(1) {
                self.bump();
                SystemVariableScope::Global
            } else if self.is_kw("INSTANCE") && !self.is_assignment_at(1) {
                self.bump();
                SystemVariableScope::Instance
            } else if (self.is_kw("SESSION") || self.is_kw("LOCAL")) && !self.is_assignment_at(1) {
                self.bump();
                SystemVariableScope::Session
            } else {
                SystemVariableScope::Session
            };
            let name = self.parse_system_variable_name()?;
            (scope, name)
        };
        if self.is_op(":=") {
            self.bump();
        } else {
            self.expect_op("=")?;
        }
        let value = self.parse_set_variable_value()?;
        Ok(SystemVariableAssignment {
            scope: scope.0,
            name: scope.1,
            value,
        })
    }

    fn is_assignment_at(&self, offset: usize) -> bool {
        self.peek_n(offset).kind == TokenKind::Op
            && matches!(self.peek_n(offset).text.as_str(), "=" | ":=")
    }

    fn parse_atat_system_variable_scope_and_name(
        &mut self,
    ) -> PResult<(SystemVariableScope, String)> {
        let text = self.bump().text;
        let rest = text
            .strip_prefix("@@")
            .ok_or_else(|| self.err_here("expected a system variable"))?;
        let (scope, name) = match rest.split_once('.') {
            Some((prefix, name)) if prefix.eq_ignore_ascii_case("GLOBAL") => {
                (SystemVariableScope::Global, name)
            }
            Some((prefix, name)) if prefix.eq_ignore_ascii_case("INSTANCE") => {
                (SystemVariableScope::Instance, name)
            }
            Some((prefix, name))
                if prefix.eq_ignore_ascii_case("SESSION")
                    || prefix.eq_ignore_ascii_case("LOCAL") =>
            {
                (SystemVariableScope::Session, name)
            }
            _ => (SystemVariableScope::Session, rest),
        };
        if name.is_empty() || name.split('.').count() > 2 {
            return Err(self.err_here("expected a one- or two-part system variable name"));
        }
        // Go's `ast.VariableAssignment.Restore` receives the normalized
        // lower-case variable name even when the source wrote `SQL_MODE` in
        // upper case. Keep this normalization local to SET's variable leaf;
        // expression-level `@@` references retain their own AST contract.
        Ok((scope, name.to_ascii_lowercase()))
    }

    fn parse_system_variable_name(&mut self) -> PResult<String> {
        let first = match self.peek().kind {
            TokenKind::Ident | TokenKind::Keyword => self.bump().text,
            _ => return Err(self.err_here("expected a system variable name")),
        };
        if !self.is_op(".") {
            return Ok(first);
        }
        self.bump();
        let second = match self.peek().kind {
            TokenKind::Ident | TokenKind::Keyword => self.bump().text,
            _ => return Err(self.err_here("expected a system variable name after '.'")),
        };
        Ok(format!("{}.{}", first, second))
    }

    fn parse_set_variable_value(&mut self) -> PResult<SetVariableValue> {
        if self.is_kw("DEFAULT") {
            self.bump();
            return Ok(SetVariableValue::Default);
        }
        // Go's SetExpr treats BINARY and ON as SET-specific strings. OFF
        // deliberately remains an ordinary identifier expression.
        if self.is_kw("BINARY") {
            self.bump();
            return Ok(SetVariableValue::Expr(Expr::String("BINARY".to_string())));
        }
        if self.is_kw("ON") {
            self.bump();
            return Ok(SetVariableValue::Expr(Expr::String("ON".to_string())));
        }
        let expr = self.parse_set_expr_value()?;
        // Go's SET parser restores a bare trailing-dot decimal (`1.`) as the
        // integer spelling (`1`). This is a SET-value boundary, so normalize
        // only the direct decimal leaf rather than changing every expression
        // context's numeric restore contract at once.
        Ok(SetVariableValue::Expr(expr))
    }

    /// Parses the expression arm of Go's `parseSetExpr` and applies its
    /// direct SET-only numeric spelling normalization.
    fn parse_set_expr_value(&mut self) -> PResult<Expr> {
        let expr = self.parse_expr(prec::NONE)?;
        Ok(match expr {
            Expr::Decimal(value) if value.ends_with('.') => {
                Expr::Decimal(value.trim_end_matches('.').to_owned())
            }
            other => other,
        })
    }

    fn parse_set_transaction(&mut self, one_shot: bool) -> PResult<SetStmt> {
        self.expect_kw("TRANSACTION")?;
        if self.is_kw("ISOLATION") {
            self.bump();
            self.expect_kw("LEVEL")?;
            let level = if self.is_kw("READ") {
                self.bump();
                if self.is_kw("UNCOMMITTED") {
                    self.bump();
                    "READ-UNCOMMITTED"
                } else if self.is_kw("COMMITTED") {
                    self.bump();
                    "READ-COMMITTED"
                } else {
                    return Err(self.err_here("expected UNCOMMITTED or COMMITTED after READ"));
                }
            } else if self.is_kw("REPEATABLE") {
                self.bump();
                self.expect_kw("READ")?;
                "REPEATABLE-READ"
            } else if self.is_kw("SERIALIZABLE") {
                self.bump();
                "SERIALIZABLE"
            } else {
                return Err(self.err_here("expected an isolation level"));
            };
            let name = if one_shot {
                "tx_isolation_one_shot"
            } else {
                "tx_isolation"
            };
            Ok(SetStmt {
                assignments: vec![SystemVariableAssignment {
                    scope: SystemVariableScope::Session,
                    name: name.to_string(),
                    value: SetVariableValue::Expr(Expr::String(level.to_string())),
                }],
            })
        } else if self.is_kw("READ") {
            self.bump();
            if self.is_kw("ONLY") {
                self.bump();
                // Go's `parseSetTransaction` gives the stale-read form its
                // own `tx_read_ts` assignment rather than folding it into
                // the ordinary `tx_read_only` boolean.  Keep the expression
                // in the existing SetStmt envelope so source ordering and
                // generic SET execution boundaries remain unchanged.
                if self.is_kw("AS OF") {
                    self.bump();
                    self.expect_kw("TIMESTAMP")?;
                    return Ok(SetStmt {
                        assignments: vec![SystemVariableAssignment {
                            scope: SystemVariableScope::Session,
                            name: "tx_read_ts".to_string(),
                            value: SetVariableValue::Expr(self.parse_set_expr_value()?),
                        }],
                    });
                }
                Ok(SetStmt {
                    assignments: vec![SystemVariableAssignment {
                        scope: SystemVariableScope::Session,
                        name: "tx_read_only".to_string(),
                        value: SetVariableValue::Expr(Expr::String("1".to_string())),
                    }],
                })
            } else if self.is_kw("WRITE") {
                self.bump();
                Ok(SetStmt {
                    assignments: vec![SystemVariableAssignment {
                        scope: SystemVariableScope::Session,
                        name: "tx_read_only".to_string(),
                        value: SetVariableValue::Expr(Expr::String("0".to_string())),
                    }],
                })
            } else {
                Err(self.err_here("expected ONLY or WRITE after READ"))
            }
        } else {
            Err(self.err_here("expected ISOLATION LEVEL or READ ONLY/WRITE"))
        }
    }
}
