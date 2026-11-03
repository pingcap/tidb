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

//! Go `pkg/statistics/handle/storage/gc.go`'s statements.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_datatype::Datum;
use tidb_log::{Field, Value};
use tidb_model::table_info::TableInfo;
use tidb_model::GoShared;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, get_start_ts, wrap_txn, StatsSessionContext};

/// Go `lockstats.DeleteLockSQL`.
const DELETE_LOCK_SQL: &str = "DELETE FROM mysql.stats_table_locked WHERE table_id = %?";

/// Go `DeleteTableStatsFromKV`: each table's statistics removed and its
/// `stats_meta` re-versioned so other nodes notice. A soft delete (`DROP
/// STATS`) resets the histogram rows instead of deleting them.
pub fn delete_table_stats_from_kv<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    stats_ids: &[i64],
    soft: bool,
) -> Result<(), SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    for &stats_id in stats_ids {
        let id = [SqlArg::Signed(stats_id)];
        exec_rows(
            sctx,
            "update mysql.stats_meta set version = %?, last_stats_histograms_version = %? where table_id = %? ",
            &[
                SqlArg::Unsigned(start_ts),
                SqlArg::Unsigned(start_ts),
                SqlArg::Signed(stats_id),
            ],
        )?;
        if soft {
            exec_rows(
                sctx,
                "update mysql.stats_histograms set distinct_count = 0, null_count = 0, tot_col_size = 0, modify_count = 0, version = %?,cm_sketch = null, stats_ver = 0, flag = 0, correlation = 0, last_analyze_pos = null where table_id = %?",
                &[SqlArg::Unsigned(start_ts), SqlArg::Signed(stats_id)],
            )?;
        } else {
            exec_rows(sctx, "delete from mysql.stats_histograms where table_id = %?", &id)?;
        }
        for sql in [
            "delete from mysql.stats_buckets where table_id = %?",
            "delete from mysql.stats_top_n where table_id = %?",
            "delete from mysql.stats_fm_sketch where table_id = %?",
            "delete from mysql.column_stats_usage where table_id = %?",
            "delete from mysql.analyze_options where table_id = %?",
            DELETE_LOCK_SQL,
        ] {
            exec_rows(sctx, sql, &id)?;
        }
    }
    Ok(())
}

/// Go `gcLastTSVarName`.
const GC_LAST_TS_VAR_NAME: &str = "tidb_stats_gc_last_ts";

/// Go `util.DurationToTS`: the duration's milliseconds as a TSO.
fn duration_to_ts(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX >> 18) << 18
}

/// Go `oracle.GoTimeToTS(time.Now())`.
fn now_ts() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    u64::try_from(millis).unwrap_or(u64::MAX >> 18) << 18
}

/// Go `GCStats`: the statistics of tables (and columns / indexes) dropped
/// since the last GC are removed in two phases, so every node notices the
/// first before the second; `lease` is Go's `max(statsHandle.Lease(),
/// ddlLease)`. `table_info_by_id` is Go `statsHandle.TableInfoByID(is, id)`:
/// a partition's ID answers its table.
pub fn gc_stats<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    lease: Duration,
    historical_stats_duration: Duration,
    table_info_by_id: &dyn Fn(i64) -> Option<GoShared<TableInfo>>,
) -> Result<(), SqlExecError> {
    // To make sure that all the deleted tables' schema and stats info have
    // been acknowledged to all tidb, we only garbage collect version before
    // 10 lease.
    let offset = duration_to_ts(lease.saturating_mul(10));
    let now = now_ts();
    if now < offset {
        return Ok(());
    }
    let gc_ver = now - offset;
    let last_gc = get_last_gc_timestamp(sctx)?;
    let (rows, _) = exec_rows(
        sctx,
        "select table_id from mysql.stats_meta where version >= %? and version < %?",
        &[SqlArg::Unsigned(last_gc), SqlArg::Unsigned(gc_ver)],
    )?;
    for row in rows {
        let table_id = datum_i64(row.first());
        let table = table_info_by_id(table_id);
        gc_table_stats(sctx, table.as_ref(), table_id)?;
        // Go `is.TableByID`: a partition's ID is not a table's.
        let existed = table.is_some_and(|table| table.read().id == table_id);
        if !existed {
            gc_history_stats_from_kv(sctx, table_id)?;
        }
    }
    if let Err(error) = clear_outdated_history_stats(sctx, historical_stats_duration) {
        tidb_logutil::bg_logger().warn(
            "failed to gc outdated historical stats",
            &[
                Field::new(
                    "duration",
                    Value::Duration(
                        i64::try_from(historical_stats_duration.as_nanos()).unwrap_or(i64::MAX),
                    ),
                ),
                tidb_logutil::zap_error(error.to_string()),
            ],
        );
    }
    write_gc_timestamp_to_kv(sctx, gc_ver)
}

/// Go `ClearOutdatedHistoryStats`.
pub fn clear_outdated_history_stats<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    historical_stats_duration: Duration,
) -> Result<(), SqlExecError> {
    let seconds = [SqlArg::Float64(historical_stats_duration.as_secs_f64())];
    let (rows, _) = exec_rows(
        sctx,
        "select count(*) from mysql.stats_meta_history use index (idx_create_time) where create_time <= NOW() - INTERVAL %? SECOND",
        &seconds,
    )?;
    let count = datum_i64(rows.first().and_then(|row| row.first()));
    if count > 0 {
        for _ in 0..for_count(count, 1000) {
            exec_rows(
                sctx,
                "delete from mysql.stats_meta_history use index (idx_create_time) where create_time <= NOW() - INTERVAL %? SECOND limit 1000 ",
                &seconds,
            )?;
        }
        // Go returns inside its `stats_history` loop, so one 50-row
        // statement runs and its closing log line is never reached.
        exec_rows(
            sctx,
            "delete from mysql.stats_history use index (idx_create_time) where create_time <= NOW() - INTERVAL %? SECOND limit 50 ",
            &seconds,
        )?;
    }
    Ok(())
}

/// Go `forCount`.
fn for_count(total: i64, batch: i64) -> i64 {
    total / batch + i64::from(total % batch > 0)
}

/// Go `gcHistoryStatsFromKV`.
fn gc_history_stats_from_kv<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    physical_id: i64,
) -> Result<(), SqlExecError> {
    let id = [SqlArg::Signed(physical_id)];
    exec_rows(sctx, "delete from mysql.stats_history where table_id = %?", &id)?;
    exec_rows(sctx, "delete from mysql.stats_meta_history where table_id = %?", &id)?;
    Ok(())
}

/// Go `deleteHistStatsFromKV`: one column's or index's statistics, the
/// table re-versioned first.
fn delete_hist_stats_from_kv<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    physical_id: i64,
    hist_id: i64,
    is_index: i64,
) -> Result<(), SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    exec_rows(
        sctx,
        "update mysql.stats_meta set version = %?, last_stats_histograms_version = %? where table_id = %? ",
        &[
            SqlArg::Unsigned(start_ts),
            SqlArg::Unsigned(start_ts),
            SqlArg::Signed(physical_id),
        ],
    )?;
    let item = [
        SqlArg::Signed(physical_id),
        SqlArg::Signed(hist_id),
        SqlArg::Signed(is_index),
    ];
    for sql in [
        "delete from mysql.stats_histograms where table_id = %? and hist_id = %? and is_index = %?",
        "delete from mysql.stats_top_n where table_id = %? and hist_id = %? and is_index = %?",
        "delete from mysql.stats_buckets where table_id = %? and hist_id = %? and is_index = %?",
        "delete from mysql.stats_fm_sketch where table_id = %? and hist_id = %? and is_index = %?",
    ] {
        exec_rows(sctx, sql, &item)?;
    }
    if is_index == 0 {
        exec_rows(
            sctx,
            "delete from mysql.column_stats_usage where table_id = %? and column_id = %?",
            &[SqlArg::Signed(physical_id), SqlArg::Signed(hist_id)],
        )?;
    }
    Ok(())
}

/// Go `gcTableStats`: a dropped table first loses its statistics (other
/// nodes notice), and on a later GC its `stats_meta` row; a live table
/// loses the statistics of columns and indexes it no longer has.
fn gc_table_stats<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table: Option<&GoShared<TableInfo>>,
    physical_id: i64,
) -> Result<(), SqlExecError> {
    let (rows, _) = exec_rows(
        sctx,
        "select is_index, hist_id from mysql.stats_histograms where table_id = %?",
        &[SqlArg::Signed(physical_id)],
    )?;
    let Some(table) = table else {
        if !rows.is_empty() {
            // It's the first time to run into it. Delete column/index stats
            // to notify other TiDB nodes.
            tidb_logutil::bg_logger().info(
                "remove stats in GC due to dropped table",
                &[Field::new("tableID", Value::I64(physical_id))],
            );
            return wrap_txn(sctx, |sctx| delete_table_stats_from_kv(sctx, &[physical_id], false));
        }
        // The table has already been deleted in stats and acknowledged to
        // all tidb, so its meta info goes now.
        exec_rows(
            sctx,
            "delete from mysql.stats_meta where table_id = %?",
            &[SqlArg::Signed(physical_id)],
        )?;
        return Ok(());
    };
    let table = table.read();
    for row in rows {
        let (is_index, hist_id) = (datum_i64(row.first()), datum_i64(row.get(1)));
        let find = if is_index == 1 {
            table.indices.iter_deref().any(|index| index.read().id == hist_id)
        } else {
            table.columns.iter_deref().any(|column| column.read().id == hist_id)
        };
        if !find {
            wrap_txn(sctx, |sctx| delete_hist_stats_from_kv(sctx, physical_id, hist_id, is_index))?;
        }
    }
    Ok(())
}

/// Go `getLastGCTimestamp`.
fn get_last_gc_timestamp<C: StatsSessionContext + ?Sized>(sctx: &C) -> Result<u64, SqlExecError> {
    let (rows, _) = exec_rows(
        sctx,
        "SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name=%?",
        &[SqlArg::from(GC_LAST_TS_VAR_NAME)],
    )?;
    let Some(value) = rows.first().and_then(|row| row.first()) else {
        return Ok(0);
    };
    String::from_utf8_lossy(value.go_bytes())
        .parse::<u64>()
        .map_err(|error| Box::new(error) as SqlExecError)
}

/// Go `writeGCTimestampToKV`.
fn write_gc_timestamp_to_kv<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    new_ts: u64,
) -> Result<(), SqlExecError> {
    exec_rows(
        sctx,
        "insert into mysql.tidb (variable_name, variable_value) values (%?, %?) on duplicate key update variable_value = %?",
        &[
            SqlArg::from(GC_LAST_TS_VAR_NAME),
            SqlArg::Unsigned(new_ts),
            SqlArg::Unsigned(new_ts),
        ],
    )?;
    Ok(())
}

/// Go `row.GetInt64(i)`.
fn datum_i64(datum: Option<&Datum>) -> i64 {
    match datum {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}
