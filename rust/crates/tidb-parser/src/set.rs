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

//! SET grammar translated from `pkg/parser/set_explain_parser.go`. The
//! statement router takes `SET BINDING` and `SET CONFIG` first; every other
//! SET enters [`Parser::parse_set_statement`], Go's `parseSetStmt`.

use tidb_ast::{
    DefaultRoleSelection, Expr, RoleSpec, SessionStmt, SetDefaultRoleStmt, SetPasswordStmt,
    SetResourceGroupStmt, SetRoleSelection, SetRoleStmt, SetSessionStatesStmt, SetStmt,
    VariableAssignment, SET_CHARSET, SET_NAMES,
};
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

impl Parser {
    /// Go `parseSetStmt`: the one-shot forms first, then the generic loop
    /// of `NAMES`, `CHARSET` and variable assignments, mixed in any order.
    pub(crate) fn parse_set_statement(&mut self) -> PResult<SessionStmt> {
        self.expect_kw("SET")?;
        if self.is_kw("TRANSACTION") {
            return Ok(set_stmt(self.parse_set_transaction(false, false)?));
        }
        if (self.is_kw("GLOBAL") || self.is_kw("SESSION")) && self.is_kw_at(1, "TRANSACTION") {
            let is_global = self.bump().text.eq_ignore_ascii_case("GLOBAL");
            return Ok(set_stmt(self.parse_set_transaction(is_global, true)?));
        }
        if self.is_kw("ROLE") || (self.is_kw("DEFAULT") && self.is_kw_at(1, "ROLE")) {
            return self.parse_set_role_command();
        }
        if self.is_kw("SESSION_STATES") {
            return Ok(SessionStmt::SetSessionStates(Box::new(
                self.parse_set_session_states()?,
            )));
        }
        if self.is_kw("RESOURCE") {
            return Ok(SessionStmt::SetResourceGroup(Box::new(
                self.parse_set_resource_group()?,
            )));
        }
        if self.is_kw("PASSWORD") {
            return Ok(SessionStmt::SetPassword(Box::new(
                self.parse_set_password()?,
            )));
        }
        let mut variables = Vec::new();
        loop {
            if self.is_kw("NAMES") {
                self.bump();
                variables.push(self.parse_set_names_assignment()?);
            } else if self.is_kw("CHARACTER") || self.is_kw("CHAR") || self.is_kw("CHARSET") {
                // CHARACTER and CHAR require SET after them.
                if !self.bump().text.eq_ignore_ascii_case("CHARSET") {
                    self.expect_kw("SET")?;
                }
                variables.push(self.parse_set_charset_assignment()?);
            } else {
                variables.push(self.parse_variable_assignment()?);
            }
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        Ok(set_stmt(SetStmt { variables }))
    }

    fn parse_set_password(&mut self) -> PResult<SetPasswordStmt> {
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
            self.decode_string(&token.text)
        } else {
            let token = self.peek().clone();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected a password string"));
            }
            self.bump();
            self.decode_string(&token.text)
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

    fn parse_set_session_states(&mut self) -> PResult<SetSessionStatesStmt> {
        self.expect_kw("SESSION_STATES")?;
        let token = self.peek().clone();
        if token.kind != TokenKind::Str {
            return Err(self.err_here("expected serialized session state string"));
        }
        self.bump();
        Ok(SetSessionStatesStmt {
            session_states: self.decode_string(&token.text),
        })
    }

    fn parse_set_resource_group(&mut self) -> PResult<SetResourceGroupStmt> {
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

    /// Go `parseSetNamesAssignment`, after `NAMES`.
    fn parse_set_names_assignment(&mut self) -> PResult<VariableAssignment> {
        let mut assignment = VariableAssignment {
            name: SET_NAMES.to_owned(),
            value: Expr::Default(None),
            is_instance: false,
            is_global: false,
            is_system: false,
            extend_value: None,
        };
        if self.is_kw("DEFAULT") {
            self.bump();
            return Ok(assignment);
        }
        assignment.value = self.parse_set_charset_name()?;
        if self.is_kw("COLLATE") {
            self.bump();
            if self.is_kw("DEFAULT") {
                self.bump();
            } else {
                // Go: `COLLATE StringName`, a string literal or identifier,
                // kept as `ast.NewValueExpr(lit, "", "")`.
                let collation = match self.peek().kind {
                    TokenKind::Str => self.bumped_string(),
                    TokenKind::Ident | TokenKind::Keyword => self.bump().text,
                    _ => return Err(self.err_here("expected a collation name")),
                };
                assignment.extend_value = Some(Expr::RawString(collation));
            }
        }
        Ok(assignment)
    }

    /// Go `parseSetCharsetAssignment`, after `CHARSET` / `CHARACTER SET`.
    fn parse_set_charset_assignment(&mut self) -> PResult<VariableAssignment> {
        let value = if self.is_kw("DEFAULT") {
            self.bump();
            Expr::Default(None)
        } else {
            self.parse_set_charset_name()?
        };
        Ok(VariableAssignment {
            name: SET_CHARSET.to_owned(),
            value,
            is_instance: false,
            is_global: false,
            is_system: false,
            extend_value: None,
        })
    }

    /// Go's charset operand of `SET NAMES` / `SET CHARSET`: whatever token
    /// comes next, validated by `charset.GetCharsetInfo` -- a name it does
    /// not support is `ErrUnknownCharacterSet` at parse time, so no earlier
    /// assignment of the statement runs -- and replaced by its canonical name.
    fn parse_set_charset_name(&mut self) -> PResult<Expr> {
        let name = if self.peek().kind == TokenKind::Str {
            self.bumped_string()
        } else {
            self.bump().text
        };
        let info = tidb_datatype::get_charset_info(&name).map_err(|_| {
            self.err_coded(
                &tidb_error::catalog::parser::ERR_UNKNOWN_CHARACTER_SET,
                &format!("Unknown character set: '{name}'"),
            )
        })?;
        // Go `ast.NewValueExpr(cs.Name, "", "")`: no charset, so the restore
        // carries no introducer.
        Ok(Expr::RawString(info.name))
    }

    /// Go `parseVariableAssignment`.
    fn parse_variable_assignment(&mut self) -> PResult<VariableAssignment> {
        if self.peek().kind == TokenKind::UserVar && !self.peek().text.starts_with("@@") {
            let token = self.bump();
            let name = self.decode_at_name(&token.text);
            self.expect_eq_or_assign()?;
            let value = self.parse_set_expr_value()?;
            return Ok(VariableAssignment {
                name,
                value,
                is_instance: false,
                is_global: false,
                is_system: false,
                extend_value: None,
            });
        }
        if self.peek().kind == TokenKind::UserVar {
            let (is_global, is_instance, name) = self.parse_atat_system_variable_name()?;
            self.expect_eq_or_assign()?;
            return Ok(VariableAssignment {
                is_global,
                is_instance,
                ..VariableAssignment::system(name, self.parse_set_expr()?)
            });
        }
        // Go: a scope keyword directly followed by `=` is the variable's
        // name (`SET GLOBAL = DEFAULT`), not a scope.
        let (is_global, is_instance) = if self.is_assignment_at(1) {
            (false, false)
        } else if self.is_kw("GLOBAL") {
            self.bump();
            (true, false)
        } else if self.is_kw("INSTANCE") {
            self.bump();
            (false, true)
        } else {
            if self.is_kw("SESSION") || self.is_kw("LOCAL") {
                self.bump();
            }
            (false, false)
        };
        let name = self.parse_system_variable_name()?;
        self.expect_eq_or_assign()?;
        Ok(VariableAssignment {
            is_global,
            is_instance,
            ..VariableAssignment::system(name, self.parse_set_expr()?)
        })
    }

    /// Go `acceptEqOrAssign`.
    fn expect_eq_or_assign(&mut self) -> PResult<()> {
        if self.is_op(":=") {
            self.bump();
            Ok(())
        } else {
            self.expect_op("=")
        }
    }

    fn is_assignment_at(&self, offset: usize) -> bool {
        self.peek_n(offset).kind == TokenKind::Op
            && matches!(self.peek_n(offset).text.as_str(), "=" | ":=")
    }

    /// Go's `doubleAtIdentifier` arm: `(is_global, is_instance, name)`, the
    /// name lowercased as Go lowercases the whole token.
    fn parse_atat_system_variable_name(&mut self) -> PResult<(bool, bool, String)> {
        let text = self.bump().text;
        let rest = text
            .strip_prefix("@@")
            .ok_or_else(|| self.err_here("expected a system variable"))?;
        let (is_global, is_instance, name) = match rest.split_once('.') {
            Some((prefix, name)) if prefix.eq_ignore_ascii_case("GLOBAL") => (true, false, name),
            Some((prefix, name)) if prefix.eq_ignore_ascii_case("INSTANCE") => (false, true, name),
            Some((prefix, name))
                if prefix.eq_ignore_ascii_case("SESSION")
                    || prefix.eq_ignore_ascii_case("LOCAL") =>
            {
                (false, false, name)
            }
            _ => (false, false, rest),
        };
        if name.is_empty() || name.split('.').count() > 2 {
            return Err(self.err_here("expected a one- or two-part system variable name"));
        }
        Ok((
            is_global,
            is_instance,
            decode_set_variable_name(self, name).to_ascii_lowercase(),
        ))
    }

    /// Go `parseVariableName`.
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

    /// Go `parseSetExpr`: `DEFAULT` is Go's `DefaultExpr`; `BINARY` and `ON`
    /// are SET-specific strings, while `OFF` stays an identifier.
    fn parse_set_expr(&mut self) -> PResult<Expr> {
        if self.is_kw("DEFAULT") {
            self.bump();
            return Ok(Expr::Default(None));
        }
        if self.is_kw("BINARY") {
            self.bump();
            return Ok(Expr::String("BINARY".to_string()));
        }
        if self.is_kw("ON") {
            self.bump();
            return Ok(Expr::String("ON".to_string()));
        }
        self.parse_set_expr_value()
    }

    /// Go's `parseExpression` at a SET value, with Go's restore of a bare
    /// trailing-dot decimal (`1.`) as the integer spelling (`1`).
    fn parse_set_expr_value(&mut self) -> PResult<Expr> {
        let expr = self.parse_expr(prec::NONE)?;
        Ok(match expr {
            Expr::Decimal(value) if value.ends_with('.') => {
                Expr::Decimal(value.trim_end_matches('.').to_owned())
            }
            other => other,
        })
    }

    /// Go `parseSetTransaction`: each comma-separated characteristic is one
    /// system assignment; `has_scope` picks `tx_isolation` over the one-shot
    /// `tx_isolation_one_shot`.
    fn parse_set_transaction(&mut self, is_global: bool, has_scope: bool) -> PResult<SetStmt> {
        self.expect_kw("TRANSACTION")?;
        let mut variables = Vec::new();
        loop {
            let (name, value) = if self.is_kw("ISOLATION") {
                self.bump();
                self.expect_kw("LEVEL")?;
                let level = if self.is_kw("READ") {
                    self.bump();
                    if self.is_kw("UNCOMMITTED") {
                        self.bump();
                        "READ-UNCOMMITTED"
                    } else {
                        self.expect_kw("COMMITTED")?;
                        "READ-COMMITTED"
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
                let name = if has_scope {
                    "tx_isolation"
                } else {
                    "tx_isolation_one_shot"
                };
                (name, Expr::String(level.to_owned()))
            } else {
                self.expect_kw("READ")?;
                if self.is_kw("WRITE") {
                    self.bump();
                    ("tx_read_only", Expr::String("0".to_owned()))
                } else {
                    self.expect_kw("ONLY")?;
                    if self.is_kw("AS OF") {
                        self.bump();
                        self.expect_kw("TIMESTAMP")?;
                        ("tx_read_ts", self.parse_expr(prec::NONE)?)
                    } else {
                        ("tx_read_only", Expr::String("1".to_owned()))
                    }
                }
            };
            variables.push(VariableAssignment {
                is_global,
                ..VariableAssignment::system(name, value)
            });
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        Ok(SetStmt { variables })
    }
}

fn set_stmt(set: SetStmt) -> SessionStmt {
    SessionStmt::Set(Box::new(set))
}

fn decode_set_variable_name(parser: &Parser, raw: &str) -> String {
    if matches!(raw.as_bytes().first(), Some(b'\'') | Some(b'"')) {
        parser.decode_string(raw)
    } else if raw.starts_with('`') && raw.ends_with('`') && raw.len() >= 2 {
        raw[1..raw.len() - 1].replace("``", "`")
    } else {
        raw.to_string()
    }
}
