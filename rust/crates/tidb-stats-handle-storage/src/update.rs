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
use tidb_stats_handle_util::{exec_rows, exec_with_ctx, get_start_ts, StatsSessionContext, STATS_CONTEXT};
use tidb_variable::table_delta::TableDelta;

/// Go `UpdateStatsMetaVerAndLastHistUpdateVer`.
pub fn update_stats_meta_ver_and_last_hist_update_ver<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    physical_id: i64,
) -> Result<u64, SqlExecError> {
    update_stats_meta_version(sctx, true, &[physical_id])
}

/// Go `UpdateStatsMetaVer`: the tables' version moves to the newest TS. It
/// keeps `last_stats_histograms_version`, so the stats cache does not reload
/// the column and index stats.
pub fn update_stats_meta_ver<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    physical_ids: &[i64],
) -> Result<u64, SqlExecError> {
    update_stats_meta_version(sctx, false, physical_ids)
}

/// Go `updateStatsMetaVersion`.
fn update_stats_meta_version<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    update_hist_version: bool,
    physical_ids: &[i64],
) -> Result<u64, SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    let ids: Vec<String> = physical_ids.iter().map(i64::to_string).collect();
    let mut sql = String::from("update mysql.stats_meta set version = %?");
    let mut args = vec![SqlArg::Unsigned(start_ts)];
    if update_hist_version {
        sql.push_str(", last_stats_histograms_version = %?");
        args.push(SqlArg::Unsigned(start_ts));
    }
    sql.push_str(&format!(" where table_id in ({})", ids.join(",")));
    exec_rows(sctx, &sql, &args)?;
    Ok(start_ts)
}

/// Go `execAndCloseRecordSet`: runs a statement whose rows are not needed,
/// such as a lock read. The statement reaches the slow log and the execution
/// duration metrics only when its record set is closed.
fn exec_and_close_record_set<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    sql: &str,
) -> Result<(), SqlExecError> {
    match exec_with_ctx(&*STATS_CONTEXT, sctx, sql, &[])? {
        Some(mut record_set) => record_set.close(),
        None => Ok(()),
    }
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
        exec_and_close_record_set(
            sctx,
            &format!(
                "select * from mysql.stats_table_locked where table_id in ({}) for update",
                locked_table_ids.join(",")
            ),
        )?;
    }
    if !unlocked_table_ids.is_empty() {
        exec_and_close_record_set(
            sctx,
            &format!(
                "select * from mysql.stats_meta where table_id in ({}) for update",
                unlocked_table_ids.join(",")
            ),
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

/// Go `UpdateStatsVersion`: every table's statistics re-versioned (after a
/// cluster flashback).
pub fn update_stats_version<C: StatsSessionContext + ?Sized>(sctx: &C) -> Result<(), SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    exec_rows(sctx, "update mysql.stats_meta set version = %?", &[SqlArg::Unsigned(start_ts)])?;
    exec_rows(sctx, "update mysql.stats_histograms set version = %?", &[SqlArg::Unsigned(start_ts)])?;
    Ok(())
}

/// Go `changeGlobalStatsTables`.
const CHANGE_GLOBAL_STATS_TABLES: [&str; 6] = [
    "stats_meta",
    "stats_top_n",
    "stats_fm_sketch",
    "stats_buckets",
    "stats_histograms",
    "column_stats_usage",
];

/// Go `ChangeGlobalStatsID`: a table's statistics follow its new ID (the
/// data did not change).
pub fn change_global_stats_id<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    from: i64,
    to: i64,
) -> Result<(), SqlExecError> {
    for table in CHANGE_GLOBAL_STATS_TABLES {
        exec_rows(
            sctx,
            &format!("update mysql.{table} set table_id = %? where table_id = %?"),
            &[SqlArg::Signed(to), SqlArg::Signed(from)],
        )?;
    }
    Ok(())
}
