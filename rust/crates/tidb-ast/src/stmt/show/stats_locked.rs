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

//! `SHOW STATS_LOCKED [LIKE|WHERE]` typed payload.

use crate::Expr;

/// TiDB's locked-table-statistics metadata inspection statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatsLockedStmt {
    /// Optional filter over `mysql.stats_table_locked` rows.
    pub filter: Option<ShowStatsLockedFilter>,
}

/// The Go shared SHOW filter alternatives for `STATS_LOCKED`.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatsLockedFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowStatsLockedStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW STATS_LOCKED");
        match &self.filter {
            None => {}
            Some(ShowStatsLockedFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowStatsLockedFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}
