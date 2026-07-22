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

//! Complete standalone `FLUSH` grammar from `pkg/parser/misc_stmt_parser.go`.

use tidb_ast::{FlushLogType, FlushStmt, FlushTarget};

use crate::{is_ident_like_name, PResult, Parser};

impl Parser {
    pub(crate) fn parse_flush(&mut self) -> PResult<FlushStmt> {
        self.expect_kw("FLUSH")?;
        let no_write_to_binlog = if self.is_kw("NO_WRITE_TO_BINLOG") || self.is_kw("LOCAL") {
            self.bump();
            true
        } else {
            false
        };

        let target = if self.is_kw("STATUS") {
            self.bump();
            FlushTarget::Status
        } else if self.is_kw("PRIVILEGES") {
            self.bump();
            FlushTarget::Privileges
        } else if self.is_kw("STATS_DELTA") {
            self.bump();
            let objects = self.parse_stats_object_list()?;
            let cluster = if self.is_kw("CLUSTER") {
                self.bump();
                true
            } else {
                false
            };
            FlushTarget::StatsDelta { objects, cluster }
        } else if self.is_kw("TABLE") || self.is_kw("TABLES") {
            self.bump();
            let mut tables = Vec::new();
            if !self.is_kw("WITH") && is_ident_like_name(self.peek()) {
                tables.push(self.parse_ident_like_name_path()?);
                while self.is_op(",") {
                    self.bump();
                    tables.push(self.parse_ident_like_name_path()?);
                }
            }
            let read_lock = if self.is_kw("WITH") {
                self.bump();
                self.expect_kw("READ")?;
                self.expect_kw("LOCK")?;
                true
            } else {
                false
            };
            FlushTarget::Tables { tables, read_lock }
        } else if self.is_kw("TIDB") {
            self.bump();
            self.expect_kw("PLUGINS")?;
            let mut plugins = Vec::new();
            if is_ident_like_name(self.peek()) {
                plugins.push(self.parse_ident_like_name()?);
                while self.is_op(",") {
                    self.bump();
                    plugins.push(self.parse_ident_like_name()?);
                }
            }
            FlushTarget::TiDbPlugins(plugins)
        } else if self.is_kw("HOSTS") {
            self.bump();
            FlushTarget::Hosts
        } else if self.is_kw("CLIENT_ERRORS_SUMMARY") {
            self.bump();
            FlushTarget::ClientErrorsSummary
        } else {
            let log_type = if self.is_kw("LOGS") {
                self.bump();
                Some(FlushLogType::Default)
            } else if self.is_kw("BINARY") {
                self.bump();
                self.expect_kw("LOGS")?;
                Some(FlushLogType::Binary)
            } else if self.is_kw("ENGINE") {
                self.bump();
                self.expect_kw("LOGS")?;
                Some(FlushLogType::Engine)
            } else if self.is_kw("ERROR") {
                self.bump();
                self.expect_kw("LOGS")?;
                Some(FlushLogType::Error)
            } else if self.is_kw("GENERAL") {
                self.bump();
                self.expect_kw("LOGS")?;
                Some(FlushLogType::General)
            } else if self.is_kw("SLOW") {
                self.bump();
                self.expect_kw("LOGS")?;
                Some(FlushLogType::Slow)
            } else {
                None
            };
            FlushTarget::Logs(log_type.ok_or_else(|| self.err_here("unsupported FLUSH target"))?)
        };
        Ok(FlushStmt {
            no_write_to_binlog,
            target,
        })
    }
}
