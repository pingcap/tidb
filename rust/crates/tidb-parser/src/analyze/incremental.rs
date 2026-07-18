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

//! `ANALYZE INCREMENTAL TABLE` grammar transit from
//! `pkg/parser/ddl_drop_parser.go#HandParser.parseAnalyzeTableStmt`.

use tidb_ast::{AnalyzeIncrementalStmt, AnalyzeIncrementalTarget};

use crate::{is_name_or_keyword, PResult, Parser};

impl Parser {
    /// Parses only the `INCREMENTAL` branch of Go's common ANALYZE TABLE
    /// production. Ordinary ANALYZE and all other incremental payloads retain
    /// their existing, separate ownership boundaries.
    pub(crate) fn parse_analyze_incremental(&mut self) -> PResult<AnalyzeIncrementalStmt> {
        self.expect_kw("ANALYZE")?;
        self.expect_kw("INCREMENTAL")?;
        self.expect_kw("TABLE")?;

        let mut tables = vec![self.parse_name_path()?];
        while self.is_op(",") {
            self.bump();
            tables.push(self.parse_name_path()?);
        }

        let target = if self.is_kw("PARTITION") {
            self.bump();
            let mut partitions = vec![self.parse_name()?];
            while self.is_op(",") {
                self.bump();
                partitions.push(self.parse_name()?);
            }
            AnalyzeIncrementalTarget::Partitions { tables, partitions }
        } else {
            AnalyzeIncrementalTarget::Tables(tables)
        };

        let indexes = if self.is_kw("INDEX") {
            self.bump();
            let mut indexes = Vec::new();
            if self.is_kw("PRIMARY") {
                indexes.push(self.bump().text);
            } else if is_name_or_keyword(self.peek()) {
                indexes.push(self.parse_name_or_keyword()?);
            }
            while self.is_op(",") {
                self.bump();
                if self.is_kw("PRIMARY") {
                    indexes.push(self.bump().text);
                } else if is_name_or_keyword(self.peek()) {
                    indexes.push(self.parse_name_or_keyword()?);
                } else {
                    return Err(self.err_here("expected index name after ','"));
                }
            }
            Some(indexes)
        } else {
            None
        };

        Ok(AnalyzeIncrementalStmt { target, indexes })
    }
}
