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

//! Remaining `misc.go` statement grammar.

use tidb_ast::{
    BinlogStmt, CalibrateResourceOption, CalibrateResourceStmt, CalibrateWorkload,
    CreateStatisticsStmt, ExtendedStatsType, KillStmt, KillTarget, RecommendIndexOption,
    RecommendIndexStmt, ServerControlStmt, SetConfigStmt, SetConfigTarget, TraceStmt,
};
use tidb_lexer::TokenKind;

use crate::{decode_string, prec, PResult, Parser};

impl Parser {
    pub(crate) fn parse_trace(&mut self) -> PResult<TraceStmt> {
        self.expect_kw("TRACE")?;
        let mut format = "row".to_string();
        let mut trace_plan = false;
        let mut trace_plan_target = String::new();
        if self.is_kw("PLAN") {
            self.bump();
            trace_plan = true;
            if self.is_kw("TARGET") {
                self.bump();
                self.expect_op("=")?;
                trace_plan_target = self.parse_misc_string("TRACE PLAN target")?;
            }
        } else if self.is_kw("FORMAT") {
            self.bump();
            self.expect_op("=")?;
            format = self.parse_misc_string("TRACE format")?;
        }
        let statement_start = self.peek().offset;
        let mut statement = self.parse_statement()?;
        let statement_end = if self.at_eof() {
            self.source.len()
        } else {
            self.peek().offset
        };
        if statement_end > statement_start {
            statement.set_text(
                None,
                self.source[statement_start..statement_end]
                    .trim_end_matches([';', ' ', '\t', '\n'])
                    .as_bytes()
                    .to_vec(),
            );
        }
        Ok(TraceStmt {
            format,
            trace_plan,
            trace_plan_target,
            statement: Box::new(statement),
        })
    }

    pub(crate) fn parse_binlog(&mut self) -> PResult<BinlogStmt> {
        self.expect_kw("BINLOG")?;
        Ok(BinlogStmt {
            value: self.parse_misc_string("BINLOG payload")?,
        })
    }

    pub(crate) fn parse_kill(&mut self) -> PResult<KillStmt> {
        self.expect_kw("KILL")?;
        let tidb_extension = if self.is_kw("TIDB") {
            self.bump();
            true
        } else {
            false
        };
        let query = if self.is_kw("QUERY") {
            self.bump();
            true
        } else {
            if self.is_kw("CONNECTION") {
                self.bump();
            }
            false
        };
        let target = if self.peek().kind == TokenKind::IntLit {
            KillTarget::ConnectionId(self.parse_misc_u64("connection ID")?)
        } else {
            KillTarget::Expr(self.parse_expr(prec::NONE)?)
        };
        Ok(KillStmt {
            query,
            tidb_extension,
            target,
        })
    }

    pub(crate) fn parse_set_config(&mut self) -> PResult<SetConfigStmt> {
        self.expect_kw("SET")?;
        self.expect_kw("CONFIG")?;
        let target = if self.peek().kind == TokenKind::Str {
            SetConfigTarget::Instance(decode_string(&self.bump().text))
        } else {
            SetConfigTarget::Component(self.parse_name_or_keyword()?.to_ascii_lowercase())
        };
        let mut name = String::new();
        while !self.is_op("=") && !self.at_eof() {
            name.push_str(&self.bump().text);
        }
        if name.is_empty() {
            return Err(self.err_here("expected configuration name"));
        }
        self.expect_op("=")?;
        Ok(SetConfigStmt {
            target,
            name,
            value: self.parse_expr(prec::NONE)?,
        })
    }

    pub(crate) fn parse_recommend_index(&mut self) -> PResult<RecommendIndexStmt> {
        self.expect_kw("RECOMMEND")?;
        self.expect_kw("INDEX")?;
        if self.is_kw("RUN") {
            self.bump();
            let sql = if self.is_kw("FOR") {
                self.bump();
                Some(self.parse_misc_string("recommendation SQL")?)
            } else {
                None
            };
            let options = if self.is_kw("WITH") {
                self.bump();
                self.parse_recommend_options()?
            } else {
                Vec::new()
            };
            Ok(RecommendIndexStmt::Run { sql, options })
        } else if self.is_kw("SHOW") {
            self.bump();
            if self.is_kw("OPTION") {
                self.bump();
            }
            Ok(RecommendIndexStmt::ShowOption)
        } else if self.is_kw("APPLY") || self.is_kw("IGNORE") {
            let apply = self.is_kw("APPLY");
            self.bump();
            let id = self.parse_misc_i64("recommendation ID")?;
            Ok(if apply {
                RecommendIndexStmt::Apply(id)
            } else {
                RecommendIndexStmt::Ignore(id)
            })
        } else if self.is_kw("SET") {
            self.bump();
            Ok(RecommendIndexStmt::Set(self.parse_recommend_options()?))
        } else if self.is_kw("STATUS") {
            self.bump();
            Ok(RecommendIndexStmt::Status)
        } else if self.is_kw("CANCEL") {
            self.bump();
            Ok(RecommendIndexStmt::Cancel)
        } else {
            Err(self.err_here("expected RECOMMEND INDEX action"))
        }
    }

    pub(crate) fn parse_create_statistics(&mut self) -> PResult<CreateStatisticsStmt> {
        self.expect_kw("CREATE")?;
        self.expect_kw("STATISTICS")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = crate::table_name_token_text(self.bump());
        self.expect_op("(")?;
        let stats_type_token = self.bump();
        let stats_type = if stats_type_token.text.eq_ignore_ascii_case("DEPENDENCY") {
            ExtendedStatsType::Dependency
        } else if stats_type_token.text.eq_ignore_ascii_case("CORRELATION") {
            ExtendedStatsType::Correlation
        } else {
            // Go leaves the zero-value enum (CARDINALITY) for every other token.
            ExtendedStatsType::Cardinality
        };
        self.expect_op(")")?;
        self.expect_kw("ON")?;
        let table = self.parse_table_name()?;
        self.expect_op("(")?;
        let mut columns = vec![self.parse_column_name_path()?];
        while self.is_op(",") {
            self.bump();
            columns.push(self.parse_column_name_path()?);
        }
        self.expect_op(")")?;
        Ok(CreateStatisticsStmt {
            if_not_exists,
            name,
            stats_type,
            table,
            columns,
        })
    }

    pub(crate) fn parse_server_control(&mut self) -> PResult<ServerControlStmt> {
        if self.is_kw("SHUTDOWN") {
            self.bump();
            Ok(ServerControlStmt::Shutdown)
        } else if self.is_kw("RESTART") {
            self.bump();
            Ok(ServerControlStmt::Restart)
        } else {
            self.expect_kw("HELP")?;
            Ok(ServerControlStmt::Help(
                self.parse_misc_string("HELP topic")?,
            ))
        }
    }

    pub(crate) fn parse_calibrate_resource(&mut self) -> PResult<CalibrateResourceStmt> {
        self.expect_kw("CALIBRATE")?;
        self.expect_kw("RESOURCE")?;
        if self.is_kw("WORKLOAD") {
            self.bump();
            let value = self.bump().text.to_ascii_uppercase();
            let workload = match value.as_str() {
                "TPCC" => CalibrateWorkload::Tpcc,
                "OLTP_READ_WRITE" => CalibrateWorkload::OltpReadWrite,
                "OLTP_READ_ONLY" => CalibrateWorkload::OltpReadOnly,
                "OLTP_WRITE_ONLY" => CalibrateWorkload::OltpWriteOnly,
                "TPCH_10" => CalibrateWorkload::Tpch10,
                _ => return Err(self.err_here("unknown CALIBRATE workload")),
            };
            return Ok(CalibrateResourceStmt {
                workload: Some(workload),
                options: Vec::new(),
            });
        }

        let mut options = Vec::new();
        while !self.at_eof() && !self.is_op(";") {
            if self.is_op(",") {
                self.bump();
            }
            let kind = self.peek().text.to_ascii_uppercase();
            if !matches!(kind.as_str(), "START_TIME" | "END_TIME" | "DURATION") {
                break;
            }
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            let option = match kind.as_str() {
                "START_TIME" => CalibrateResourceOption::StartTime(self.parse_expr(prec::NONE)?),
                "END_TIME" => CalibrateResourceOption::EndTime(self.parse_expr(prec::NONE)?),
                _ if self.peek().kind == TokenKind::Str => {
                    CalibrateResourceOption::DurationString(decode_string(&self.bump().text))
                }
                _ if self.is_kw("INTERVAL") => {
                    self.bump();
                    let value = self.parse_expr(prec::NONE)?;
                    let unit = self.parse_name_or_keyword()?;
                    CalibrateResourceOption::DurationInterval { value, unit }
                }
                _ => CalibrateResourceOption::Duration(self.parse_expr(prec::NONE)?),
            };
            if options
                .iter()
                .any(|existing| same_calibrate_kind(existing, &option))
            {
                return Err(self.err_here("duplicated CALIBRATE option"));
            }
            options.push(option);
        }
        Ok(CalibrateResourceStmt {
            workload: None,
            options,
        })
    }

    fn parse_recommend_options(&mut self) -> PResult<Vec<RecommendIndexOption>> {
        let mut options = Vec::new();
        loop {
            let name = self.parse_name_or_keyword()?;
            self.expect_op("=")?;
            options.push(RecommendIndexOption {
                name,
                value: self.parse_expr(prec::NONE)?,
            });
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        Ok(options)
    }

    fn parse_misc_string(&mut self, expected: &str) -> PResult<String> {
        let token = self.bump();
        if token.kind != TokenKind::Str {
            return Err(self.err_here(&format!("expected {expected}")));
        }
        Ok(decode_string(&token.text))
    }

    fn parse_misc_u64(&mut self, expected: &str) -> PResult<u64> {
        let token = self.bump();
        token
            .text
            .parse::<u64>()
            .map_err(|_| self.err_here(&format!("expected {expected}")))
    }

    fn parse_misc_i64(&mut self, expected: &str) -> PResult<i64> {
        let token = self.bump();
        token
            .text
            .parse::<i64>()
            .map_err(|_| self.err_here(&format!("expected {expected}")))
    }
}

fn same_calibrate_kind(left: &CalibrateResourceOption, right: &CalibrateResourceOption) -> bool {
    matches!(
        (left, right),
        (
            CalibrateResourceOption::StartTime(_),
            CalibrateResourceOption::StartTime(_)
        ) | (
            CalibrateResourceOption::EndTime(_),
            CalibrateResourceOption::EndTime(_)
        ) | (
            CalibrateResourceOption::DurationString(_)
                | CalibrateResourceOption::DurationInterval { .. }
                | CalibrateResourceOption::Duration(_),
            CalibrateResourceOption::DurationString(_)
                | CalibrateResourceOption::DurationInterval { .. }
                | CalibrateResourceOption::Duration(_)
        )
    )
}
