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

use tidb_ast::AnalyzeIncrementalStmt;

use crate::{PResult, Parser};

impl Parser {
    /// Parses only the `INCREMENTAL` branch of Go's common ANALYZE TABLE
    /// production. Ordinary ANALYZE and all other incremental payloads retain
    /// their existing, separate ownership boundaries.
    pub(crate) fn parse_analyze_incremental(&mut self) -> PResult<AnalyzeIncrementalStmt> {
        self.expect_kw("ANALYZE")?;
        let no_write_to_binlog = if self.is_kw("NO_WRITE_TO_BINLOG") || self.is_kw("LOCAL") {
            self.bump();
            true
        } else {
            false
        };
        self.expect_kw("INCREMENTAL")?;
        self.expect_kw("TABLE")?;
        let ordinary = self.parse_analyze_table_body(no_write_to_binlog)?;
        Ok(AnalyzeIncrementalStmt {
            no_write_to_binlog: ordinary.no_write_to_binlog,
            tables: ordinary.tables,
            partitions: ordinary.partitions,
            target: ordinary.target,
            options: ordinary.options,
        })
    }
}
