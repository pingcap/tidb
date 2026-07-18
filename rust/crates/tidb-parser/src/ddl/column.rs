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

//! Shared CREATE/ALTER column-definition envelope parsing.
//!
//! Column-option grammar is split into sibling leaves beneath `ddl/column/`:
//! dispatch in `options`, generated-column grammar in `generated`, and the
//! DDL-only DEFAULT/ON UPDATE time family in `time`. This envelope stays
//! shared by CREATE and ALTER column definitions.

use tidb_ast::{ColumnDef, ColumnOption, ColumnType, InlineKeyOption};

use crate::{PResult, Parser};

use super::field_type::{
    normalize_binary_charset, type_rejects_charset, type_supports_string_options,
};

impl Parser {
    /// Parses Go's shared `HandParser.parseColumnDef` envelope:
    /// `name type [options...]`. CREATE TABLE and every column-bearing ALTER
    /// action use this exact routine, so neither statement family owns a
    /// duplicate implementation.
    pub(super) fn parse_column_def(&mut self) -> PResult<ColumnDef> {
        let mut path = self.parse_name_path()?;
        let name = path.pop().expect("parse_name_path always has one segment");
        self.parse_column_def_after_name_with_qualifier(name, path)
    }

    /// Parses the shared column type/options tail after the source name.
    fn parse_column_def_after_name_with_qualifier(
        &mut self,
        name: String,
        qualifier: Vec<String>,
    ) -> PResult<ColumnDef> {
        // Go handles `SERIAL` before `parseFieldType`: it is a column
        // definition macro, not a real type. Retain its three injected
        // options in source order, then parse any ordinary option tail.
        if self.is_kw("SERIAL") {
            self.bump();
            let mut options = vec![
                ColumnOption::NotNull,
                ColumnOption::AutoIncrement,
                ColumnOption::InlineKey(InlineKeyOption::unique(false)),
            ];
            options.extend(self.parse_column_options("BIGINT")?);
            return Ok(ColumnDef {
                qualifier,
                name,
                ty: ColumnType {
                    name: "BIGINT".to_owned(),
                    args: Vec::new(),
                    unsigned: true,
                    zerofill: false,
                    binary: false,
                    charset: None,
                },
                options,
            });
        }
        let mut ty = self.parse_column_type()?;
        // Direct port of `parseStringOptions`: BINARY may come before a
        // charset clause or trail it, and `CHARACTER SET binary` canonicalizes
        // a character family to its binary storage type. These modifiers are
        // grammar-owned by the field type, so consume them before ordinary
        // column options such as NOT NULL and COLLATE.
        let supports_string_options = type_supports_string_options(&ty.name);
        // Go's `parseStringOptions` has two terminal alternatives.  `BYTE`
        // changes the storage type and returns immediately; `ASCII` installs
        // LATIN1 and also returns immediately.  In particular, neither may
        // consume a following charset or a second field-type `BINARY`.
        // Keep that boundary explicit instead of letting the common modifier
        // tail accidentally accept combinations the Go parser rejects.
        let terminal_string_option = if supports_string_options && self.is_kw("BYTE") {
            self.bump();
            ty.charset = Some("BINARY".to_owned());
            normalize_binary_charset(&mut ty);
            true
        } else if supports_string_options && self.is_kw("ASCII") {
            // Go's ASCII branch returns immediately from parseStringOptions:
            // it is latin1 and does not consume a following BINARY token.
            self.bump();
            ty.charset = Some("LATIN1".to_owned());
            true
        } else if supports_string_options && self.is_kw("BINARY") {
            self.bump();
            ty.binary = true;
            false
        } else {
            false
        };
        if !terminal_string_option {
            if self.accept_column_charset_kw() {
                if !type_supports_string_options(&ty.name) || type_rejects_charset(&ty.name) {
                    return Err(self.err_here("column type does not allow CHARACTER SET"));
                }
                ty.charset = Some(self.parse_charset_name()?.to_ascii_uppercase());
                if ty.charset.as_deref() == Some("BINARY") {
                    normalize_binary_charset(&mut ty);
                }
            }
            if supports_string_options && self.is_kw("BINARY") {
                self.bump();
                if !type_rejects_charset(&ty.name) {
                    ty.binary = true;
                }
            }
        }
        let options = self.parse_column_options(&ty.name)?;
        Ok(ColumnDef {
            qualifier,
            name,
            ty,
            options,
        })
    }

    /// Go's `CharsetKw`: `CHARACTER SET`, `CHARSET`, or `CHAR SET`.
    fn accept_column_charset_kw(&mut self) -> bool {
        if self.is_kw("CHARSET") {
            self.bump();
            return true;
        }
        if (self.is_kw("CHARACTER") || self.is_kw("CHAR")) && self.is_kw_at(1, "SET") {
            self.bump();
            self.bump();
            return true;
        }
        false
    }
}
