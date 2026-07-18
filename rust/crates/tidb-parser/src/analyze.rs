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

//! `ANALYZE TABLE` grammar translated from
//! `pkg/parser/ddl_drop_parser.go:252-410`.

use tidb_ast::{AnalyzeOption, AnalyzeOptionKind, AnalyzeTableStmt, AnalyzeTarget};
use tidb_lexer::TokenKind;

#[path = "analyze/incremental.rs"]
mod incremental;

use crate::{is_name_or_keyword, PResult, Parser};

impl Parser {
    /// Parses the current typed subset: table and partition lists, one target
    /// selector, and ordered `TOPN`/`BUCKETS` options. Other Go payload fields
    /// remain explicit parse errors until represented by the AST.
    pub(crate) fn parse_analyze_table(&mut self) -> PResult<AnalyzeTableStmt> {
        self.expect_kw("ANALYZE")?;
        self.expect_kw("TABLE")?;

        let mut tables = vec![self.parse_name_path()?];
        while self.is_op(",") {
            self.bump();
            tables.push(self.parse_name_path()?);
        }

        let mut partitions = Vec::new();
        if self.is_kw("PARTITION") {
            self.bump();
            partitions.push(self.parse_name()?);
            while self.is_op(",") {
                self.bump();
                partitions.push(self.parse_name()?);
            }
        }

        let target = if self.is_kw("INDEX") {
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
            AnalyzeTarget::Index(indexes)
        } else if self.is_kw("ALL") {
            self.bump();
            self.expect_kw("COLUMNS")?;
            AnalyzeTarget::AllColumns
        } else if self.is_kw("COLUMNS") {
            self.bump();
            let mut columns = vec![self.parse_name()?];
            while self.is_op(",") {
                self.bump();
                columns.push(self.parse_name()?);
            }
            AnalyzeTarget::Columns(columns)
        } else {
            AnalyzeTarget::Default
        };

        let mut options = Vec::new();
        if self.is_kw("WITH") {
            self.bump();
            loop {
                let token = self.peek().clone();
                if !matches!(
                    token.kind,
                    TokenKind::IntLit | TokenKind::DecLit | TokenKind::FloatLit
                ) {
                    return Err(self.err_here("expected ANALYZE option number"));
                }
                self.bump();
                let kind = if self.is_kw("TOPN") {
                    self.bump();
                    AnalyzeOptionKind::TopN
                } else if self.is_kw("BUCKETS") {
                    self.bump();
                    AnalyzeOptionKind::Buckets
                } else {
                    return Err(
                        self.err_here("expected TOPN or BUCKETS after ANALYZE option number")
                    );
                };
                options.push(AnalyzeOption {
                    value: token.text,
                    kind,
                });
                if self.is_op(",") {
                    self.bump();
                } else {
                    break;
                }
            }
        }

        Ok(AnalyzeTableStmt {
            tables,
            partitions,
            target,
            options,
        })
    }
}
