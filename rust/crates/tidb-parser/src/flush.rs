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

//! Standalone `FLUSH` grammar translated from
//! `pkg/parser/misc_stmt_parser.go:146-249`.

use tidb_ast::FlushStmt;

use crate::{is_ident_like_name, PResult, Parser};

impl Parser {
    /// Parses the current state-free standalone forms. Modifiers, plugin/log,
    /// host, client-error, and statistics targets remain explicit gaps because
    /// their AST and execution contracts are distinct.
    pub(crate) fn parse_flush(&mut self) -> PResult<FlushStmt> {
        self.expect_kw("FLUSH")?;
        if self.is_kw("STATUS") {
            self.bump();
            return Ok(FlushStmt::Status);
        }
        if self.is_kw("PRIVILEGES") {
            self.bump();
            return Ok(FlushStmt::Privileges);
        }
        if !(self.is_kw("TABLE") || self.is_kw("TABLES")) {
            return Err(self.err_here("expected STATUS, PRIVILEGES, TABLE, or TABLES after FLUSH"));
        }
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
        Ok(FlushStmt::Tables { tables, read_lock })
    }
}
