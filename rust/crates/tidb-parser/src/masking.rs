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

//! Complete structural translation of `pkg/parser/ddl_masking_parser.go`.

use tidb_ast::{
    AlterMaskingPolicyAction, AlterTableAction, CreateMaskingPolicyStmt, DdlStmt,
    MaskingPolicyRestrictOps, MaskingPolicyState,
};
use tidb_lexer::{is_reserved, TokenKind};

use crate::{decode_string, prec, PResult, Parser};

impl Parser {
    pub(crate) fn is_create_masking_policy_source_statement(&self) -> bool {
        (self.is_kw("CREATE") && self.is_kw_at(1, "MASKING") && self.is_kw_at(2, "POLICY"))
            || (self.is_kw("CREATE")
                && self.is_kw_at(1, "OR")
                && self.is_kw_at(2, "REPLACE")
                && self.is_kw_at(3, "MASKING")
                && self.is_kw_at(4, "POLICY"))
    }

    pub(crate) fn parse_create_masking_policy_source_statement(&mut self) -> PResult<DdlStmt> {
        self.expect_kw("CREATE")?;
        let or_replace = if self.is_kw("OR") {
            self.bump();
            self.expect_kw("REPLACE")?;
            true
        } else {
            false
        };
        self.expect_kw("MASKING")?;
        self.expect_kw("POLICY")?;
        let if_not_exists = self.parse_if_not_exists()?;
        if or_replace && if_not_exists {
            return Err(self.err_here("'OR REPLACE' and 'IF NOT EXISTS' are mutually exclusive"));
        }
        let name = self.parse_masking_policy_ident_like()?;
        self.expect_kw("ON")?;
        let table = self.parse_masking_policy_table_name()?;
        self.expect_op("(")?;
        let column = self.parse_masking_policy_ident_like()?;
        self.expect_op(")")?;
        self.expect_kw("AS")?;
        let expr = self.parse_expr(prec::NONE)?;
        let restrict_ops = self.parse_masking_policy_restrict_on_opt()?;
        let state = self.parse_masking_policy_state_opt();
        Ok(DdlStmt::CreateMaskingPolicy(Box::new(
            CreateMaskingPolicyStmt {
                or_replace,
                if_not_exists,
                name,
                table,
                column,
                expr,
                restrict_ops,
                state,
            },
        )))
    }

    pub(crate) fn is_masking_policy_alter_action(&self) -> bool {
        (self.is_kw("ADD")
            || self.is_kw("DROP")
            || self.is_kw("MODIFY")
            || self.is_kw("ENABLE")
            || self.is_kw("DISABLE"))
            && self.is_kw_at(1, "MASKING")
            && self.is_kw_at(2, "POLICY")
    }

    pub(crate) fn parse_masking_policy_alter_action(&mut self) -> PResult<AlterTableAction> {
        let action = if self.is_kw("ADD") {
            self.bump();
            self.expect_kw("MASKING")?;
            self.expect_kw("POLICY")?;
            let name = self.parse_masking_policy_ident_like()?;
            self.expect_kw("ON")?;
            self.expect_op("(")?;
            let column = self.parse_masking_policy_ident_like()?;
            self.expect_op(")")?;
            self.expect_kw("AS")?;
            let expr = self.parse_expr(prec::NONE)?;
            let restrict_ops = self.parse_masking_policy_restrict_on_opt()?;
            let state = self.parse_masking_policy_state_opt();
            AlterMaskingPolicyAction::Add {
                name,
                column,
                expr,
                restrict_ops,
                state,
            }
        } else if self.is_kw("MODIFY") {
            self.bump();
            self.expect_kw("MASKING")?;
            self.expect_kw("POLICY")?;
            let name = self.parse_masking_policy_ident_like()?;
            self.expect_kw("SET")?;
            if self.is_kw("RESTRICT") {
                let restrict_ops = self.parse_required_masking_policy_restrict_on()?;
                AlterMaskingPolicyAction::ModifyRestrict { name, restrict_ops }
            } else {
                let option = self.parse_masking_policy_ident_like()?;
                if !option.eq_ignore_ascii_case("expression") {
                    return Err(self.err_here("unsupported masking policy modify option"));
                }
                self.expect_op("=")?;
                AlterMaskingPolicyAction::ModifyExpression {
                    name,
                    expr: self.parse_expr(prec::NONE)?,
                }
            }
        } else {
            let kind = self.bump().text.to_ascii_uppercase();
            self.expect_kw("MASKING")?;
            self.expect_kw("POLICY")?;
            let name = self.parse_masking_policy_ident_like()?;
            match kind.as_str() {
                "ENABLE" => AlterMaskingPolicyAction::Enable(name),
                "DISABLE" => AlterMaskingPolicyAction::Disable(name),
                "DROP" => AlterMaskingPolicyAction::Drop(name),
                _ => unreachable!("masking alter action was classified before parsing"),
            }
        };
        Ok(AlterTableAction::MaskingPolicy(Box::new(action)))
    }

    fn parse_masking_policy_restrict_on_opt(&mut self) -> PResult<MaskingPolicyRestrictOps> {
        if !self.is_kw("RESTRICT") {
            return Ok(MaskingPolicyRestrictOps::default());
        }
        self.parse_required_masking_policy_restrict_on()
    }

    fn parse_required_masking_policy_restrict_on(&mut self) -> PResult<MaskingPolicyRestrictOps> {
        self.expect_kw("RESTRICT")?;
        self.expect_kw("ON")?;
        if self.is_kw("NONE") {
            self.bump();
            return Ok(MaskingPolicyRestrictOps::default());
        }
        self.expect_op("(")?;
        let mut operations = MaskingPolicyRestrictOps::default();
        loop {
            let token = self.peek().clone();
            let name = self.parse_masking_policy_ident_like()?;
            if !operations.insert_name(&name) {
                return Err(crate::ParseError {
                    message: format!("unsupported masking policy restrict operation: {name}"),
                    offset: token.offset,
                });
            }
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        self.expect_op(")")?;
        Ok(operations)
    }

    fn parse_masking_policy_state_opt(&mut self) -> MaskingPolicyState {
        if self.is_kw("ENABLE") {
            self.bump();
            MaskingPolicyState::Enabled
        } else if self.is_kw("DISABLE") {
            self.bump();
            MaskingPolicyState::Disabled
        } else {
            MaskingPolicyState::ImplicitEnabled
        }
    }

    /// Go's `expectIdentLike` accepts ordinary identifiers, unreserved
    /// keywords, and string literals. Keep this source-local instead of
    /// widening the root parser's neutral name primitive.
    fn parse_masking_policy_ident_like(&mut self) -> PResult<String> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Ident => Ok(self.bump().text),
            TokenKind::Keyword if !is_reserved(&token.text) => Ok(self.bump().text),
            TokenKind::Str => {
                self.bump();
                Ok(decode_string(&token.text))
            }
            _ => Err(self.err_here("expected a masking-policy identifier")),
        }
    }

    /// Go's `expectTableName` is `[schema.]table`, not the generic Rust
    /// parser's arbitrary-length dotted path. Its first and second slots use
    /// the same identifier-like token class for this source domain.
    fn parse_masking_policy_table_name(&mut self) -> PResult<Vec<String>> {
        let mut name = vec![self.parse_masking_policy_ident_like()?];
        if self.is_op(".") {
            self.bump();
            name.push(self.parse_masking_policy_ident_like()?);
        }
        Ok(name)
    }
}
