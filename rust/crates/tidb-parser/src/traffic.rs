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

//! Complete hand-parser translation of `pkg/parser/traffic_parser.go`.

use tidb_ast::{
    AdminStmt, RefreshStatsMode, RefreshStatsStmt, StatsObject, TrafficCaptureOption,
    TrafficReplayOption, TrafficStmt,
};
use tidb_lexer::TokenKind;

use crate::{decode_string, is_ident_like_name, PResult, Parser};

impl Parser {
    pub(crate) fn is_traffic_source_statement(&self) -> bool {
        self.is_kw("TRAFFIC")
            || ((self.is_kw("SHOW") || self.is_kw("CANCEL")) && self.is_kw_at(1, "TRAFFIC"))
            || (self.is_kw("REFRESH") && self.is_kw_at(1, "STATS"))
    }

    pub(crate) fn parse_traffic_source_statement(&mut self) -> PResult<AdminStmt> {
        if self.is_kw("TRAFFIC") {
            Ok(AdminStmt::Traffic(Box::new(self.parse_traffic()?)))
        } else if self.is_kw("SHOW") {
            self.bump();
            self.parse_traffic_jobs(TrafficStmt::ShowJobs)
        } else if self.is_kw("CANCEL") {
            self.bump();
            self.parse_traffic_jobs(TrafficStmt::CancelJobs)
        } else {
            Ok(AdminStmt::RefreshStats(Box::new(
                self.parse_refresh_stats()?,
            )))
        }
    }

    fn parse_traffic(&mut self) -> PResult<TrafficStmt> {
        self.expect_kw("TRAFFIC")?;
        if self.is_kw("CAPTURE") {
            self.bump();
            self.expect_kw("TO")?;
            let dir = self.parse_traffic_string("expected TRAFFIC CAPTURE path string")?;
            let mut options = Vec::new();
            while !self.at_eof() && !self.is_op(";") {
                let option = self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                if option.text.eq_ignore_ascii_case("DURATION") {
                    let value = self.parse_traffic_string("expected DURATION string")?;
                    if value.as_bytes().last().is_some_and(u8::is_ascii_digit) {
                        return Err(self.err_here("invalid TRAFFIC CAPTURE duration"));
                    }
                    options.push(TrafficCaptureOption::Duration(value));
                } else if option.text.eq_ignore_ascii_case("ENCRYPTION_METHOD") {
                    options.push(TrafficCaptureOption::EncryptionMethod(
                        self.parse_traffic_string("expected ENCRYPTION_METHOD string")?,
                    ));
                } else if option.text.eq_ignore_ascii_case("COMPRESS") {
                    let value = self.bump();
                    if value.kind == TokenKind::Str
                        || (!value.text.eq_ignore_ascii_case("TRUE")
                            && !value.text.eq_ignore_ascii_case("FALSE"))
                    {
                        return Err(self.err_here("invalid boolean value for COMPRESS"));
                    }
                    options.push(TrafficCaptureOption::Compress(
                        value.text.eq_ignore_ascii_case("TRUE"),
                    ));
                } else {
                    return Err(self.err_here("unknown TRAFFIC CAPTURE option"));
                }
            }
            Ok(TrafficStmt::Capture { dir, options })
        } else if self.is_kw("REPLAY") {
            self.bump();
            self.expect_kw("FROM")?;
            let dir = self.parse_traffic_string("expected TRAFFIC REPLAY path string")?;
            let mut options = Vec::new();
            while !self.at_eof() && !self.is_op(";") {
                let option = self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                if option.text.eq_ignore_ascii_case("USER") {
                    options.push(TrafficReplayOption::User(
                        self.parse_traffic_string("expected TRAFFIC REPLAY USER string")?,
                    ));
                } else if option.text.eq_ignore_ascii_case("PASSWORD") {
                    options.push(TrafficReplayOption::Password(
                        self.parse_traffic_string("expected TRAFFIC REPLAY PASSWORD string")?,
                    ));
                } else if option.text.eq_ignore_ascii_case("SPEED") {
                    let value = self.bump();
                    options.push(TrafficReplayOption::Speed(traffic_token_value(&value)));
                } else if option.text.eq_ignore_ascii_case("READ_ONLY")
                    || option.text.eq_ignore_ascii_case("READONLY")
                {
                    let value = self.bump();
                    options.push(TrafficReplayOption::ReadOnly(
                        traffic_token_value(&value).eq_ignore_ascii_case("TRUE"),
                    ));
                } else {
                    return Err(self.err_here("unknown TRAFFIC REPLAY option"));
                }
            }
            Ok(TrafficStmt::Replay { dir, options })
        } else {
            Err(self.err_here("expected TRAFFIC CAPTURE or REPLAY"))
        }
    }

    fn parse_traffic_jobs(&mut self, statement: TrafficStmt) -> PResult<AdminStmt> {
        self.expect_kw("TRAFFIC")?;
        self.expect_kw("JOBS")?;
        Ok(AdminStmt::Traffic(Box::new(statement)))
    }

    fn parse_refresh_stats(&mut self) -> PResult<RefreshStatsStmt> {
        self.expect_kw("REFRESH")?;
        self.expect_kw("STATS")?;
        let objects = self.parse_stats_object_list()?;
        let mode = if self.is_kw("FULL") {
            self.bump();
            Some(RefreshStatsMode::Full)
        } else if self.is_kw("LITE") {
            self.bump();
            Some(RefreshStatsMode::Lite)
        } else {
            None
        };
        let cluster_wide = if self.is_kw("CLUSTER") {
            self.bump();
            true
        } else {
            false
        };
        Ok(RefreshStatsStmt {
            objects,
            mode,
            cluster_wide,
        })
    }

    /// Parses Go's shared non-empty `StatsObjectList`. `FLUSH STATS_DELTA`
    /// can call this source-owned helper when that separate statement owner
    /// gains its typed payload.
    pub(crate) fn parse_stats_object_list(&mut self) -> PResult<Vec<StatsObject>> {
        let mut objects = Vec::new();
        loop {
            let object = if self.is_op("*") {
                self.bump();
                self.expect_op(".")?;
                self.expect_op("*")?;
                StatsObject::Global
            } else if is_ident_like_name(self.peek()) {
                let first = self.bump().text;
                if self.is_op(".") {
                    self.bump();
                    if self.is_op("*") {
                        self.bump();
                        StatsObject::Database(first)
                    } else if is_ident_like_name(self.peek()) {
                        StatsObject::Table {
                            database: Some(first),
                            table: self.bump().text,
                        }
                    } else {
                        return Err(self.err_here("expected table name or '*'"));
                    }
                } else {
                    StatsObject::Table {
                        database: None,
                        table: first,
                    }
                }
            } else {
                return Err(self.err_here("expected statistics object"));
            };
            objects.push(object);
            if !self.is_op(",") {
                return Ok(objects);
            }
            self.bump();
        }
    }

    fn parse_traffic_string(&mut self, message: &str) -> PResult<String> {
        if self.peek().kind != TokenKind::Str {
            return Err(self.err_here(message));
        }
        Ok(decode_string(&self.bump().text))
    }
}

fn traffic_token_value(token: &tidb_lexer::Token) -> String {
    if token.kind == TokenKind::Str {
        decode_string(&token.text)
    } else {
        token.text.clone()
    }
}
