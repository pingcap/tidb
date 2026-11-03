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

//! Go `pkg/statistics/handle/storage/update.go`.

use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, get_start_ts, StatsSessionContext};
use tidb_variable::table_delta::TableDelta;

/// Go `UpdateStatsMetaVerAndLastHistUpdateVer`.
pub fn update_stats_meta_ver_and_last_hist_update_ver<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    physical_id: i64,
) -> Result<u64, SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    exec_rows(
        sctx,
        "update mysql.stats_meta set version=%?, last_stats_histograms_version=%? where table_id =%?",
        &[
            SqlArg::Unsigned(start_ts),
            SqlArg::Unsigned(start_ts),
            SqlArg::Signed(physical_id),
        ],
    )?;
    Ok(start_ts)
}

/// Go `storage.DeltaUpdate`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeltaUpdate {
    /// Physical or logical table ID to update.
    pub table_id: i64,
    /// Accumulated row and modification deltas.
    pub delta: TableDelta,
    /// Whether the delta is stashed in `mysql.stats_table_locked`.
    pub is_locked: bool,
}

/// Go `UpdateStatsMeta`: each locked table's delta stashed in
/// `mysql.stats_table_locked`, each other table's applied to
/// `mysql.stats_meta` at `start_ts`, after locking the rows they touch.
pub fn update_stats_meta<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    start_ts: u64,
    updates: &[DeltaUpdate],
) -> Result<(), SqlExecError> {
    if updates.is_empty() {
        return Ok(());
    }
    let mut locked_table_ids = Vec::new();
    let mut locked_values = Vec::new();
    let mut unlocked_table_ids = Vec::new();
    let mut unlocked_pos_values = Vec::new();
    let mut unlocked_neg_values = Vec::new();
    for update in updates {
        let (count, delta) = (update.delta.count, update.delta.delta);
        if update.is_locked {
            locked_table_ids.push(update.table_id.to_string());
            locked_values.push(format!("({start_ts}, {}, {count}, {delta})", update.table_id));
        } else {
            unlocked_table_ids.push(update.table_id.to_string());
            if delta < 0 {
                unlocked_neg_values.push(format!(
                    "({start_ts}, {}, {count}, {})",
                    update.table_id,
                    delta.wrapping_neg()
                ));
            } else {
                unlocked_pos_values.push(format!("({start_ts}, {}, {count}, {delta})", update.table_id));
            }
        }
    }
    // Lock the rows first, so the updates below meet no write conflict.
    if !locked_table_ids.is_empty() {
        exec_rows(
            sctx,
            &format!(
                "select * from mysql.stats_table_locked where table_id in ({}) for update",
                locked_table_ids.join(",")
            ),
            &[],
        )?;
    }
    if !unlocked_table_ids.is_empty() {
        exec_rows(
            sctx,
            &format!(
                "select * from mysql.stats_meta where table_id in ({}) for update",
                unlocked_table_ids.join(",")
            ),
            &[],
        )?;
    }
    if !locked_values.is_empty() {
        exec_rows(
            sctx,
            &format!(
                "insert into mysql.stats_table_locked (version, table_id, modify_count, count) values {} \
                 on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), \
                 count = count + values(count)",
                locked_values.join(",")
            ),
            &[],
        )?;
    }
    if !unlocked_pos_values.is_empty() {
        exec_rows(
            sctx,
            &format!(
                "insert into mysql.stats_meta (version, table_id, modify_count, count) values {} \
                 on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), \
                 count = count + values(count)",
                unlocked_pos_values.join(",")
            ),
            &[],
        )?;
    }
    if !unlocked_neg_values.is_empty() {
        exec_rows(
            sctx,
            &format!(
                "insert into mysql.stats_meta (version, table_id, modify_count, count) values {} \
                 on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), \
                 count = if(count > values(count), count - values(count), 0)",
                unlocked_neg_values.join(",")
            ),
            &[],
        )?;
    }
    Ok(())
}
