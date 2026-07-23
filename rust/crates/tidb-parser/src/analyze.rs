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

use tidb_ast::{
    AnalyzeOption, AnalyzeOptionKind, AnalyzeTableStmt, AnalyzeTarget, HistogramOperation,
};
use tidb_lexer::TokenKind;

#[path = "analyze/incremental.rs"]
mod incremental;

use crate::{is_ident_like_name, PResult, Parser};

impl Parser {
    /// Parses Go's complete `AnalyzeTableStmt` grammar.
    pub(crate) fn parse_analyze_table(&mut self) -> PResult<AnalyzeTableStmt> {
        self.expect_kw("ANALYZE")?;
        let no_write_to_binlog = if self.is_kw("NO_WRITE_TO_BINLOG") || self.is_kw("LOCAL") {
            self.bump();
            true
        } else {
            false
        };
        self.expect_kw("TABLE")?;

        self.parse_analyze_table_body(no_write_to_binlog)
    }

    pub(crate) fn parse_analyze_table_body(
        &mut self,
        no_write_to_binlog: bool,
    ) -> PResult<AnalyzeTableStmt> {
        let mut tables = vec![self.parse_table_name()?];
        while self.is_op(",") {
            self.bump();
            tables.push(self.parse_table_name()?);
        }

        let mut partitions = Vec::new();
        if self.is_kw("PARTITION") {
            self.bump();
            partitions = self.parse_ident_like_name_list()?;
        }

        let target =
            if (self.is_kw("UPDATE") || self.is_kw("DROP")) && self.is_kw_at(1, "HISTOGRAM") {
                let operation = if self.is_kw("UPDATE") {
                    HistogramOperation::Update
                } else {
                    HistogramOperation::Drop
                };
                self.bump();
                self.bump();
                self.expect_kw("ON")?;
                let columns = self.parse_analyze_simple_column_names()?;
                AnalyzeTarget::Histogram { operation, columns }
            } else if self.is_kw("INDEX") {
                self.bump();
                let mut indexes = Vec::new();
                if self.is_kw("PRIMARY") {
                    indexes.push(self.bump().text);
                } else if is_ident_like_name(self.peek()) {
                    indexes.push(self.parse_ident_like_name()?);
                }
                while self.is_op(",") {
                    self.bump();
                    if self.is_kw("PRIMARY") {
                        indexes.push(self.bump().text);
                    } else if is_ident_like_name(self.peek()) {
                        indexes.push(self.parse_ident_like_name()?);
                    } else {
                        return Err(self.err_here("expected index name after ','"));
                    }
                }
                AnalyzeTarget::Index(indexes)
            } else if self.is_kw("ALL") {
                self.bump();
                self.expect_kw("COLUMNS")?;
                AnalyzeTarget::AllColumns
            } else if self.is_kw("PREDICATE") {
                self.bump();
                self.expect_kw("COLUMNS")?;
                AnalyzeTarget::PredicateColumns
            } else if self.is_kw("COLUMNS") {
                self.bump();
                let columns = self.parse_analyze_simple_column_names()?;
                AnalyzeTarget::Columns(columns)
            } else {
                AnalyzeTarget::Default
            };

        let options = self.parse_analyze_options()?;
        Ok(AnalyzeTableStmt {
            tables,
            partitions,
            no_write_to_binlog,
            target,
            options,
        })
    }

    pub(crate) fn parse_analyze_options(&mut self) -> PResult<Vec<AnalyzeOption>> {
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
                } else if self.is_kw("CMSKETCH") {
                    self.bump();
                    if self.is_kw("WIDTH") {
                        self.bump();
                        AnalyzeOptionKind::CmSketchWidth
                    } else if self.is_kw("DEPTH") {
                        self.bump();
                        AnalyzeOptionKind::CmSketchDepth
                    } else {
                        return Err(self.err_here("expected WIDTH or DEPTH after CMSKETCH"));
                    }
                } else if self.is_kw("SAMPLES") {
                    self.bump();
                    AnalyzeOptionKind::Samples
                } else if self.is_kw("SAMPLERATE") {
                    self.bump();
                    AnalyzeOptionKind::SampleRate
                } else if self.is_kw("NDVRATE") {
                    self.bump();
                    AnalyzeOptionKind::NdvRate
                } else {
                    return Err(self.err_here("expected ANALYZE option name after number"));
                };
                options.push(AnalyzeOption {
                    value: token.text,
                    kind,
                });
                if self.is_op(",") {
                    self.bump();
                } else if matches!(
                    self.peek().kind,
                    TokenKind::IntLit | TokenKind::DecLit | TokenKind::FloatLit
                ) {
                    continue;
                } else {
                    break;
                }
            }
        }

        Ok(options)
    }

    fn parse_analyze_simple_column_names(&mut self) -> PResult<Vec<String>> {
        let mut columns = Vec::new();
        loop {
            let token = self.bump();
            if self.is_op(".") {
                return Err(self.err_here("ANALYZE column names must be unqualified"));
            }
            columns.push(crate::table_name_token_text(token));
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        Ok(columns)
    }
}
