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

//! Creation-side `CREATE TABLE ... SPLIT` parser leaf.

use tidb_ast::{CreateTableSplit, CreateTableSplitTarget};

use crate::{PResult, Parser};

impl Parser {
    /// Direct port of the final `[SPLIT ...]` loop in Go's
    /// `HandParser.parseCreateTableStmt`. It runs after the GLOBAL TEMPORARY
    /// `ON COMMIT` tail, while AST restore intentionally places the payload
    /// before CTAS and ON COMMIT, matching `ast.CreateTableStmt.Restore`.
    pub(super) fn parse_create_table_splits(&mut self) -> PResult<Vec<CreateTableSplit>> {
        let mut splits = Vec::new();
        while self.is_kw("SPLIT") {
            self.bump();
            // Go accepts but canonicalizes away the optional REGION word.
            if self.is_kw("REGION") {
                self.bump();
            }
            let target = if self.is_kw("PRIMARY") {
                self.bump();
                self.expect_kw("KEY")?;
                CreateTableSplitTarget::PrimaryKey
            } else if self.is_kw("INDEX") {
                self.bump();
                CreateTableSplitTarget::Index(self.parse_name_or_keyword()?)
            } else if self.is_kw("TABLE") {
                self.bump();
                CreateTableSplitTarget::Table
            } else if self.is_kw("BY") || self.is_kw("BETWEEN") {
                // Go's `parseSplitIndexOption` treats the value grammar as
                // the implicit table-level target.
                CreateTableSplitTarget::Table
            } else {
                return Err(self.err_here("expected SPLIT TABLE, PRIMARY KEY, or INDEX"));
            };
            splits.push(CreateTableSplit {
                target,
                option: self.parse_split_option()?,
            });
        }
        Ok(splits)
    }
}
