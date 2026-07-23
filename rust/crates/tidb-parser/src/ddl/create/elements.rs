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

//! Comma-framed `CREATE TABLE` element dispatch.
//!
//! This is Go's `HandParser.parseTableElementList`. It owns only deciding
//! whether an element is a column or a table constraint and the comma loop.
//! The column, CHECK, index, and foreign-key leaves retain their own grammar
//! and typed AST construction.

use tidb_ast::{ColumnDef, IndexConstraintKind, TableConstraint};

use crate::{is_name_or_keyword, PResult, Parser};

impl Parser {
    /// Parses the contents between the parentheses owned by the CREATE TABLE
    /// envelope. The caller consumes both parentheses so CTAS parentheses and
    /// element-list parentheses never share a boundary.
    pub(super) fn parse_table_element_list(
        &mut self,
    ) -> PResult<(Vec<ColumnDef>, Vec<TableConstraint>)> {
        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();
        loop {
            if self.is_kw("KEY") || self.is_kw("INDEX") {
                let kind = IndexConstraintKind::Index;
                self.bump();
                let if_not_exists = self.parse_if_not_exists()?;
                let name = self.parse_optional_index_name()?;
                let is_empty_index = name.as_deref() == Some("");
                table_constraints.push(TableConstraint::Index(self.parse_index_constraint(
                    kind,
                    if_not_exists,
                    name,
                    is_empty_index,
                    true,
                )?));
            } else if self.is_kw("VECTOR") {
                self.bump();
                self.expect_kw("INDEX")?;
                let if_not_exists = self.parse_if_not_exists()?;
                let name = self.parse_optional_index_name()?;
                let is_empty_index = name.as_deref() == Some("");
                table_constraints.push(TableConstraint::Index(self.parse_index_constraint(
                    IndexConstraintKind::Vector,
                    if_not_exists,
                    name,
                    is_empty_index,
                    true,
                )?));
            } else if self.is_kw("FULLTEXT") {
                self.bump();
                if self.is_kw("KEY") || self.is_kw("INDEX") {
                    self.bump();
                }
                let name = self.parse_optional_index_name()?;
                table_constraints.push(TableConstraint::Index(self.parse_index_constraint(
                    IndexConstraintKind::Fulltext,
                    false,
                    name,
                    false,
                    false,
                )?));
            } else if self.is_kw("COLUMNAR") {
                self.bump();
                self.expect_kw("INDEX")?;
                let if_not_exists = self.parse_if_not_exists()?;
                let name = self.parse_optional_index_name()?;
                let is_empty_index = name.as_deref() == Some("");
                table_constraints.push(TableConstraint::Index(self.parse_index_constraint(
                    IndexConstraintKind::Columnar,
                    if_not_exists,
                    name,
                    is_empty_index,
                    true,
                )?));
            } else if self.is_kw("GLOBAL") || self.is_kw("LOCAL") {
                return Err(self.err_here("expected index constraint kind"));
            } else if self.is_table_constraint_start() {
                table_constraints.push(self.parse_table_constraint()?);
            } else if is_name_or_keyword(self.peek()) {
                columns.push(self.parse_column_def()?);
            } else {
                return Err(self.err_here("unsupported table element"));
            }
            if self.is_op(",") {
                self.bump();
                continue;
            }
            break;
        }
        Ok((columns, table_constraints))
    }

    pub(crate) fn is_table_constraint_start(&self) -> bool {
        self.is_kw("CONSTRAINT")
            || self.is_kw("PRIMARY")
            || self.is_kw("UNIQUE")
            || self.is_kw("KEY")
            || self.is_kw("INDEX")
            || self.is_kw("FULLTEXT")
            || self.is_kw("VECTOR")
            || self.is_kw("COLUMNAR")
            || self.is_kw("CHECK")
            || self.is_kw("FOREIGN")
    }

    /// Dispatches the prefix grammar that chooses the existing typed
    /// index/check/foreign-key leaves. It intentionally owns no payload
    /// grammar itself.
    pub(crate) fn parse_table_constraint(&mut self) -> PResult<TableConstraint> {
        let constraint_name = if self.is_kw("CONSTRAINT") {
            self.bump();
            if self.is_ident_like_name() {
                Some(self.parse_ident_like_name()?)
            } else {
                None
            }
        } else {
            None
        };
        if self.is_kw("PRIMARY") {
            self.bump();
            self.expect_kw("KEY")?;
            let inline_name = self.parse_optional_index_name()?;
            let name = constraint_name.or(inline_name);
            let is_empty_index = name.as_deref() == Some("");
            Ok(TableConstraint::Index(self.parse_index_constraint(
                IndexConstraintKind::PrimaryKey,
                false,
                name,
                is_empty_index,
                true,
            )?))
        } else if self.is_kw("UNIQUE") {
            self.bump();
            if self.is_kw("KEY") || self.is_kw("INDEX") {
                self.bump();
            }
            let inline_name = self.parse_optional_index_name()?;
            let name = constraint_name.or(inline_name);
            let is_empty_index = name.as_deref() == Some("");
            Ok(TableConstraint::Index(self.parse_index_constraint(
                IndexConstraintKind::Unique,
                false,
                name,
                is_empty_index,
                true,
            )?))
        } else if self.is_kw("KEY") || self.is_kw("INDEX") {
            self.bump();
            let if_not_exists = self.parse_if_not_exists()?;
            let inline_name = self.parse_optional_index_name()?;
            let name = constraint_name.or(inline_name);
            let is_empty_index = name.as_deref() == Some("");
            Ok(TableConstraint::Index(self.parse_index_constraint(
                IndexConstraintKind::Index,
                if_not_exists,
                name,
                is_empty_index,
                true,
            )?))
        } else if self.is_kw("FULLTEXT") {
            self.bump();
            if self.is_kw("KEY") || self.is_kw("INDEX") {
                self.bump();
            }
            let inline_name = self.parse_optional_index_name()?;
            Ok(TableConstraint::Index(self.parse_index_constraint(
                IndexConstraintKind::Fulltext,
                false,
                constraint_name.or(inline_name),
                false,
                false,
            )?))
        } else if self.is_kw("VECTOR") {
            self.bump();
            self.expect_kw("INDEX")?;
            let if_not_exists = self.parse_if_not_exists()?;
            let inline_name = self.parse_optional_index_name()?;
            let name = constraint_name.or(inline_name);
            let is_empty_index = name.as_deref() == Some("");
            Ok(TableConstraint::Index(self.parse_index_constraint(
                IndexConstraintKind::Vector,
                if_not_exists,
                name,
                is_empty_index,
                true,
            )?))
        } else if self.is_kw("COLUMNAR") {
            self.bump();
            self.expect_kw("INDEX")?;
            let if_not_exists = self.parse_if_not_exists()?;
            let inline_name = self.parse_optional_index_name()?;
            let name = constraint_name.or(inline_name);
            let is_empty_index = name.as_deref() == Some("");
            Ok(TableConstraint::Index(self.parse_index_constraint(
                IndexConstraintKind::Columnar,
                if_not_exists,
                name,
                is_empty_index,
                true,
            )?))
        } else if self.is_kw("CHECK") {
            self.bump();
            let (check, injected_not_null) = self.parse_check_constraint(constraint_name, false)?;
            debug_assert!(!injected_not_null);
            Ok(TableConstraint::Check(check))
        } else if self.is_kw("FOREIGN") {
            self.bump();
            self.expect_kw("KEY")?;
            let if_not_exists = self.parse_if_not_exists()?;
            let inline_name = if self.is_ident_like_name() {
                Some(self.parse_ident_like_name()?)
            } else {
                None
            };
            Ok(TableConstraint::ForeignKey(
                self.parse_foreign_key_constraint(constraint_name.or(inline_name), if_not_exists)?,
            ))
        } else {
            Err(self.err_here("expected table constraint kind"))
        }
    }
}
