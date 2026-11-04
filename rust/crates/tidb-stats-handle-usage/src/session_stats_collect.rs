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

//! Go `pkg/statistics/handle/usage/session_stats_collect.go`: dumping the
//! session-collected stats deltas (`DumpStatsDeltaToKV`) and column usage
//! (`DumpColStatsUsageEntries`) into `mysql.stats_meta` and
//! `mysql.column_stats_usage`.

use std::time::SystemTime;

use tidb_sqlescape::{must_escape_sql_text, SqlArg};
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, StatsSessionContext};

use tidb_model::TableItemID;


/// One SQL statement in pinned Go `storage.UpdateStatsMeta`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatsDeltaStatement {
    /// Locks existing `stats_table_locked` rows selected by table ID.
    LockLocked(Vec<i64>),
    /// Locks existing `stats_meta` rows selected by table ID.
    LockUnlocked(Vec<i64>),
    /// Upserts every locked-table delta in one statement.
    UpsertLocked(Vec<crate::DeltaUpdate>),
    /// Upserts nonnegative unlocked deltas in one statement.
    UpsertUnlockedPositive(Vec<crate::DeltaUpdate>),
    /// Upserts negative unlocked deltas in one statement.
    UpsertUnlockedNegative(Vec<crate::DeltaUpdate>),
}

/// One batch of Go `DumpColStatsUsageEntries`: a single upsert that advances
/// a stored `last_used_at` only once the throttle interval has passed.
pub fn dump_col_stats_usage_batch<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    batch: &[(TableItemID, SystemTime)],
) -> Result<(), SqlExecError> {
    let threshold_minutes =
        (crate::COL_STATS_USAGE_LAST_USED_THROTTLE_INTERVAL.as_secs() / 60) as i64;
    let mut sql = String::from(
        "INSERT INTO mysql.column_stats_usage (table_id, column_id, last_used_at) VALUES ",
    );
    for (position, (item, used_at)) in batch.iter().enumerate() {
        // UTC, converted into the session's zone by the statement, so the
        // stored instant is right whatever session runs it.
        let last_used_at = chrono::DateTime::<chrono::Utc>::from(*used_at)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        sql.push_str(&must_escape_sql_text(
            "(%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))",
            &[
                SqlArg::Signed(item.table_id),
                SqlArg::Signed(item.id),
                SqlArg::from(last_used_at.as_str()),
            ],
        ));
        if position + 1 < batch.len() {
            sql.push(',');
        }
    }
    sql.push_str(&must_escape_sql_text(
        " ON DUPLICATE KEY UPDATE last_used_at = CASE WHEN last_used_at IS NULL OR TIMESTAMPDIFF(MINUTE, last_used_at, VALUES(last_used_at)) >= %? THEN VALUES(last_used_at) ELSE last_used_at END",
        &[SqlArg::Signed(threshold_minutes)],
    ));
    exec_rows(sctx, &sql, &[])?;
    Ok(())
}
