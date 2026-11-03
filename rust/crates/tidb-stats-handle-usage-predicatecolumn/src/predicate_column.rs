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

//! Go `pkg/statistics/handle/usage/predicatecolumn/predicate_column.go`'s
//! statements on a statistics session.

use std::collections::HashMap;

use tidb_model::TableItemID;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, StatsSessionContext};

use crate::cluster_predicate_column::ColStatsTimeInfo;

/// Go `SaveColumnStatsUsageForTable`: every entry replaced, a missing
/// timestamp written as Go's `"NULL"` text (which `CONVERT_TZ` turns into
/// NULL).
pub fn save_column_stats_usage_for_table<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    col_stats_usage: &HashMap<TableItemID, ColStatsTimeInfo>,
) -> Result<(), SqlExecError> {
    for (col_id, stats_usage) in col_stats_usage {
        let text = |time: Option<tidb_datatype::Time>| {
            time.map_or_else(|| "NULL".to_owned(), |time| time.to_string())
        };
        let last_used_at = text(stats_usage.last_used_at);
        let last_analyzed_at = text(stats_usage.last_analyzed_at);
        exec_rows(
            sctx,
            "REPLACE INTO mysql.column_stats_usage (table_id, column_id, last_used_at, last_analyzed_at) VALUES (%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))",
            &[
                SqlArg::Signed(col_id.table_id),
                SqlArg::Signed(col_id.id),
                SqlArg::from(last_used_at.as_str()),
                SqlArg::from(last_analyzed_at.as_str()),
            ],
        )?;
    }
    Ok(())
}
