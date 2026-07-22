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

//! Generated-column option body shared by `CREATE` and `ALTER TABLE`.
//!
//! This is the direct Rust leaf for Go
//! `HandParser.parseGeneratedColumnBody` in `pkg/parser/ddl_table_parser.go`.
//! The adjacent MariaDB `AS ROW START|END` markers stay here because Go parses
//! them in the same `AS`/`GENERATED` option branch.  `column::options` owns
//! option ordering and validation; it delegates this one grammar body so all
//! column-definition callers share an identical generated-column path.

use tidb_ast::ColumnOption;

use crate::{prec, PResult, Parser};

impl Parser {
    /// Parses Go's `parseGeneratedColumnBody` envelope after its outer
    /// `AS`/`GENERATED ALWAYS AS` dispatch. The VIRTUAL default is represented
    /// as `stored: false`, exactly as Go's `ColumnOption.Stored` default.
    pub(super) fn parse_generated_or_mariadb_row_option(&mut self) -> PResult<ColumnOption> {
        if self.is_kw("GENERATED") {
            self.bump();
            self.expect_kw("ALWAYS")?;
            self.expect_kw("AS")?;
        } else {
            self.bump();
        }

        if self.enable_mariadb && self.is_kw("ROW") {
            self.bump();
            if self.is_kw("START") {
                self.bump();
                return Ok(ColumnOption::MariaDbRowStart);
            }
            if self.is_kw("END") {
                self.bump();
                return Ok(ColumnOption::MariaDbRowEnd);
            }
            return Err(self.err_here("expected START or END after AS ROW"));
        }

        self.expect_op("(")?;
        let expression_start = self.peek().offset;
        let expression = self.parse_expr(prec::NONE)?;
        let expression_end = self.peek().offset;
        let expression_text = if expression_end > expression_start {
            self.source[expression_start..expression_end]
                .trim()
                .as_bytes()
                .to_vec()
        } else {
            Vec::new()
        };
        self.expect_op(")")?;
        let stored = if self.is_kw("STORED") {
            self.bump();
            true
        } else {
            if self.is_kw("VIRTUAL") {
                self.bump();
            }
            false
        };
        Ok(ColumnOption::Generated {
            expression,
            expression_text,
            stored,
        })
    }
}
