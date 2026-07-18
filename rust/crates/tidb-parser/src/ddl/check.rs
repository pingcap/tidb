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

//! Shared table-, column-, and `ALTER TABLE ADD` CHECK-constraint grammar.

use tidb_ast::CheckConstraintDefinition;

use crate::{prec, PResult, Parser};

impl Parser {
    /// Parses the body shared by all Go `CHECK` grammar productions after
    /// their leading `CHECK` keyword. A column-level `CHECK (...) NOT NULL`
    /// is a parser-time shorthand: Go preserves the CHECK and injects a
    /// separate `ColumnOptionNotNull`, rather than treating `NOT NULL` as a
    /// property of the check itself. The boolean return carries exactly that
    /// structural follow-up for the column owner; table/ALTER callers set
    /// `allow_not_null` false and therefore reject it.
    pub(super) fn parse_check_constraint(
        &mut self,
        name: Option<String>,
        allow_not_null: bool,
    ) -> PResult<(CheckConstraintDefinition, bool)> {
        self.expect_op("(")?;
        let expression = self.parse_expr(prec::NONE)?;
        self.expect_op(")")?;
        let mut injected_not_null = false;
        let enforced = if self.is_kw("NOT") {
            self.bump();
            if allow_not_null && self.is_kw("NULL") {
                self.bump();
                injected_not_null = true;
                true
            } else {
                self.expect_kw("ENFORCED")?;
                false
            }
        } else if self.is_kw("ENFORCED") {
            self.bump();
            true
        } else {
            true
        };
        Ok((
            CheckConstraintDefinition {
                name,
                expression,
                enforced,
            },
            injected_not_null,
        ))
    }
}
