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

//! Inline column `PRIMARY`/`KEY`/`UNIQUE` option parsing.
//!
//! This is the direct Rust ownership boundary for Go
//! `HandParser.parseGlobalLocalOption`.  In particular, `GLOBAL` is retained
//! and `LOCAL` is consumed without becoming AST state, just as Go leaves an
//! empty `ColumnOption.StrValue` for both local and omitted locality.

use tidb_ast::{ColumnOption, InlineKeyOption, PrimaryKeyStorage};

use crate::{PResult, Parser};

impl Parser {
    /// Parses one Go inline key option after the shared column-option
    /// dispatcher has identified `PRIMARY`, `KEY`, or `UNIQUE`.
    pub(super) fn parse_inline_key_option(&mut self) -> PResult<ColumnOption> {
        if self.is_kw("PRIMARY") {
            self.bump();
            self.expect_kw("KEY")?;
            return Ok(ColumnOption::InlineKey(InlineKeyOption::primary(
                self.parse_inline_primary_key_storage(),
                self.parse_inline_key_global_local(),
            )));
        }

        if self.is_kw("KEY") {
            // Go's `case primary, key` accepts bare KEY as a PRIMARY KEY
            // column option. It is not merely an alias in table constraints.
            self.bump();
            return Ok(ColumnOption::InlineKey(InlineKeyOption::primary(
                self.parse_inline_primary_key_storage(),
                self.parse_inline_key_global_local(),
            )));
        }

        self.expect_kw("UNIQUE")?;
        if self.is_kw("KEY") {
            self.bump();
        }
        Ok(ColumnOption::InlineKey(InlineKeyOption::unique(
            self.parse_inline_key_global_local(),
        )))
    }

    /// The primary-only storage suffix. Its position before locality follows
    /// Go `parseColumnOptions`: `CLUSTERED`/`NONCLUSTERED`, then
    /// `parseGlobalLocalOption`.
    fn parse_inline_primary_key_storage(&mut self) -> Option<PrimaryKeyStorage> {
        if self.is_kw("CLUSTERED") {
            self.bump();
            Some(PrimaryKeyStorage::Clustered)
        } else if self.is_kw("NONCLUSTERED") {
            self.bump();
            Some(PrimaryKeyStorage::NonClustered)
        } else {
            None
        }
    }

    /// Direct port of Go `HandParser.parseGlobalLocalOption`.
    fn parse_inline_key_global_local(&mut self) -> bool {
        if self.is_kw("GLOBAL") {
            self.bump();
            true
        } else {
            if self.is_kw("LOCAL") {
                self.bump();
            }
            false
        }
    }
}
