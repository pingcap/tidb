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

//! The no-`ON` role branches of Go's `parseGrantStmt` and `parseRevokeStmt`.

use tidb_ast::{GrantRoleStmt, RevokeRoleStmt, RoleSpec};
use tidb_lexer::{is_reserved, TokenKind};

use crate::{decode_string, PResult, Parser};

impl Parser {
    /// Separates Go's no-`ON` role branches from privilege and PROXY forms
    /// before consuming anything. `ALL` remains on the existing privilege
    /// path because `REVOKE ALL, GRANT OPTION` is a separate Go conversion.
    pub(crate) fn starts_role_membership(&self, terminator: &str) -> bool {
        if self.is_kw_at(1, "PROXY") || self.is_kw_at(1, "ALL") {
            return false;
        }
        for offset in 1..=64 {
            let token = self.peek_n(offset);
            if token.kind == TokenKind::Eof || token.text.eq_ignore_ascii_case("ON") {
                return false;
            }
            if token.text.eq_ignore_ascii_case(terminator) {
                return true;
            }
        }
        false
    }

    pub(crate) fn parse_grant_role_stmt(&mut self) -> PResult<GrantRoleStmt> {
        self.expect_kw("GRANT")?;
        let roles = self.parse_grant_revoke_role_list()?;
        self.expect_kw("TO")?;
        Ok(GrantRoleStmt {
            roles,
            users: self.parse_grant_revoke_role_users()?,
        })
    }

    pub(crate) fn parse_revoke_role_stmt(&mut self) -> PResult<RevokeRoleStmt> {
        self.expect_kw("REVOKE")?;
        let roles = self.parse_grant_revoke_role_list()?;
        self.expect_kw("FROM")?;
        Ok(RevokeRoleStmt {
            roles,
            users: self.parse_grant_revoke_role_users()?,
        })
    }

    fn parse_grant_revoke_role_list(&mut self) -> PResult<Vec<RoleSpec>> {
        let mut roles = vec![self.parse_grant_revoke_role()?];
        while self.is_op(",") {
            self.bump();
            roles.push(self.parse_grant_revoke_role()?);
        }
        Ok(roles)
    }

    fn parse_grant_revoke_role_users(&mut self) -> PResult<Vec<tidb_ast::UserSpec>> {
        let mut users = vec![self.parse_user_spec()?];
        while self.is_op(",") {
            self.bump();
            users.push(self.parse_user_spec()?);
        }
        Ok(users)
    }

    /// Go's strict `parseRoleIdentity`, shared with CREATE ROLE rather than
    /// the looser account identity parser used for statement targets.
    fn parse_grant_revoke_role(&mut self) -> PResult<RoleSpec> {
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
}
