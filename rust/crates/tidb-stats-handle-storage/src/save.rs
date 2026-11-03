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

//! Go `pkg/statistics/handle/storage/save.go`: an analysis's statistics are
//! saved by Go's statements on a statistics session, inside the transaction
//! the caller wraps around them (`util.CallWithSCtx(..., util.FlagWrapTxn)`).

use tidb_datatype::Datum;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, get_start_ts, StatsSessionContext};

use crate::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use crate::cluster_stats_write::{bound_blob, save_stats_item_sql, LastAnalyzedAt};
use tidb_model::ColumnInfo;
use tidb_model::table_info::TableInfo;

/// Go `statistics.Version2`.
pub const STATS_VERSION_2: i64 = 2;

/// Go `statistics.AnalyzeResults`, as this tier's analyzer produces it.
pub struct AnalyzeResults<'a> {
    /// Go `TableID.GetStatisticsID()`, `Snapshot`, `Count`, and every
    /// histogram of `Ars` (columns, then indexes).
    pub stats: &'a ClusterTableStats,
    /// Go `TableID.IsPartitionTable()`: a partition keeps its FM sketches
    /// for the global-stats merge.
    pub is_partition: bool,
    /// Go `BaseCount`.
    pub base_count: i64,
    /// Go `BaseModifyCnt`.
    pub base_modify_count: i64,
    /// Go `StatsVer`.
    pub stats_ver: i64,
    /// Go `ForMVIndexOrGlobalIndex`.
    pub for_mv_index_or_global_index: bool,
}

/// Go `row.GetUint64(i)`.
fn datum_u64(datum: Option<&Datum>) -> u64 {
    match datum {
        Some(Datum::UInt(value)) => *value,
        Some(Datum::Int(value)) => *value as u64,
        _ => 0,
    }
}

/// Go `row.GetInt64(i)`.
fn datum_i64(datum: Option<&Datum>) -> i64 {
    match datum {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}

fn exec<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<(), SqlExecError> {
    exec_rows(sctx, sql, arguments).map(|_| ())
}

/// Go `SaveAnalyzeResultToStorage`: returns `statsVer`, the transaction's
/// start TS when `mysql.stats_meta` took this result's version, else 0.
pub fn save_analyze_result_to_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    results: &AnalyzeResults<'_>,
    analyze_snapshot: bool,
) -> Result<u64, SqlExecError> {
    let stats = results.stats;
    let need_dump_fms = results.is_partition;
    let table_id = stats.table_id;
    let version = get_start_ts(sctx)?;
    let result_count = stats.row_count as i64;
    // 1. Save mysql.stats_meta.
    // Lock this row to prevent writing of concurrent analyze; the fake
    // (negative) table ID keeps Go's batch-get / point-get deadlock away.
    let fake_id = (-table_id).to_string();
    let real_id = table_id.to_string();
    let table_ids = [fake_id.as_bytes(), real_id.as_bytes()];
    let (rows, _) = exec_rows(
        sctx,
        "select snapshot, count, modify_count from mysql.stats_meta where table_id in (%?) for update",
        &[SqlArg::Strings(&table_ids)],
    )?;
    let mut stats_ver = 0;
    let (mut cur_cnt, mut cur_modify_cnt) = (0_i64, 0_i64);
    if let Some(row) = rows.first() {
        let snapshot = datum_u64(row.first());
        // A newer version analyze result has been written, so skip this
        // writing.
        if snapshot >= stats.snapshot
            && results.stats_ver == STATS_VERSION_2
            && !results.for_mv_index_or_global_index
        {
            return Ok(stats_ver);
        }
        cur_cnt = datum_u64(row.get(1)) as i64;
        cur_modify_cnt = datum_i64(row.get(2));
    }
    if rows.is_empty() || results.stats_ver != STATS_VERSION_2 {
        // 1-1. No row to update, or stats v1: REPLACE INTO.
        let (snapshot, count) = if results.for_mv_index_or_global_index {
            (0, 0)
        } else {
            (stats.snapshot, result_count)
        };
        exec(
            sctx,
            "replace into mysql.stats_meta (version, table_id, count, snapshot, last_stats_histograms_version) values (%?, %?, %?, %?, %?)",
            &[
                SqlArg::Unsigned(version),
                SqlArg::Signed(table_id),
                SqlArg::Signed(count),
                SqlArg::Unsigned(snapshot),
                SqlArg::Unsigned(version),
            ],
        )?;
        stats_ver = version;
    } else if results.for_mv_index_or_global_index {
        // 1-2. Only the version, for a multi-valued or global index.
        exec(
            sctx,
            "update mysql.stats_meta set version=%?, last_stats_histograms_version=%? where table_id=%?",
            &[
                SqlArg::Unsigned(version),
                SqlArg::Unsigned(version),
                SqlArg::Signed(table_id),
            ],
        )?;
    } else {
        // 1-3. A normal v2 analyze over an existing row.
        let logger = tidb_stats_handle_logutil::stats_logger();
        let modify_cnt = cur_modify_cnt.wrapping_sub(results.base_modify_count).max(0);
        logger.info(
            "incrementally update modifyCount",
            &[
                tidb_log::Field::new("tableID", tidb_log::Value::I64(table_id)),
                tidb_log::Field::new("curModifyCnt", tidb_log::Value::I64(cur_modify_cnt)),
                tidb_log::Field::new("results.BaseModifyCnt", tidb_log::Value::I64(results.base_modify_count)),
                tidb_log::Field::new("modifyCount", tidb_log::Value::I64(modify_cnt)),
            ],
        );
        let cnt = if analyze_snapshot {
            let cnt = cur_cnt
                .wrapping_add(result_count)
                .wrapping_sub(results.base_count)
                .max(0);
            logger.info(
                "incrementally update count",
                &[
                    tidb_log::Field::new("tableID", tidb_log::Value::I64(table_id)),
                    tidb_log::Field::new("curCnt", tidb_log::Value::I64(cur_cnt)),
                    tidb_log::Field::new("results.Count", tidb_log::Value::I64(result_count)),
                    tidb_log::Field::new("results.BaseCount", tidb_log::Value::I64(results.base_count)),
                    tidb_log::Field::new("count", tidb_log::Value::I64(cnt)),
                ],
            );
            cnt
        } else {
            let cnt = result_count.max(0);
            logger.info(
                "directly update count",
                &[
                    tidb_log::Field::new("tableID", tidb_log::Value::I64(table_id)),
                    tidb_log::Field::new("results.Count", tidb_log::Value::I64(result_count)),
                    tidb_log::Field::new("count", tidb_log::Value::I64(cnt)),
                ],
            );
            cnt
        };
        exec(
            sctx,
            "update mysql.stats_meta set version=%?, modify_count=%?, count=%?, snapshot=%?, last_stats_histograms_version=%? where table_id=%?",
            &[
                SqlArg::Unsigned(version),
                SqlArg::Signed(modify_cnt),
                SqlArg::Signed(cnt),
                SqlArg::Unsigned(stats.snapshot),
                SqlArg::Unsigned(version),
                SqlArg::Signed(table_id),
            ],
        )?;
        stats_ver = version;
    }
    // 2. Save histograms.
    for item in stats.columns.iter().chain(&stats.indexes) {
        let statements = save_stats_item_sql(
            table_id,
            item,
            version,
            results.stats_ver,
            need_dump_fms,
            LastAnalyzedAt::Values,
        )
        .map_err(|error| Box::new(error) as SqlExecError)?;
        for sql in statements {
            exec(sctx, &sql, &[])?;
        }
    }
    Ok(stats_ver)
}

/// Go `SaveColOrIdxStatsToStorage`: one histogram, with `stats_meta`'s
/// counts replaced (or, for a negative `count`, only its versions bumped);
/// returns `statsVer`.
pub fn save_col_or_idx_stats_to_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    count: i64,
    modify_count: i64,
    item: &ClusterStatsItem,
    stats_version: i64,
    update_analyze_time: bool,
) -> Result<u64, SqlExecError> {
    let version = get_start_ts(sctx)?;
    // If the count is less than 0, then we do not want to update the modify
    // count and count.
    if count >= 0 {
        exec(
            sctx,
            "replace into mysql.stats_meta (version, table_id, count, modify_count, last_stats_histograms_version) values (%?, %?, %?, %?, %?)",
            &[
                SqlArg::Unsigned(version),
                SqlArg::Signed(table_id),
                SqlArg::Signed(count),
                SqlArg::Signed(modify_count),
                SqlArg::Unsigned(version),
            ],
        )?;
    } else {
        exec(
            sctx,
            "update mysql.stats_meta set version = %?, last_stats_histograms_version = %? where table_id = %?",
            &[
                SqlArg::Unsigned(version),
                SqlArg::Unsigned(version),
                SqlArg::Signed(table_id),
            ],
        )?;
    }
    let last_analyzed_at = if update_analyze_time {
        LastAnalyzedAt::CurrentTimestamp
    } else {
        LastAnalyzedAt::Unchanged
    };
    let statements = save_stats_item_sql(
        table_id,
        item,
        version,
        stats_version,
        false,
        last_analyzed_at,
    )
    .map_err(|error| Box::new(error) as SqlExecError)?;
    for sql in statements {
        exec(sctx, &sql, &[])?;
    }
    Ok(version)
}

/// Go `statstypes.MetaUpdate`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetaUpdate {
    /// Go `PhysicalID`.
    pub physical_id: i64,
    /// Go `Count`.
    pub count: i64,
    /// Go `ModifyCount`.
    pub modify_count: i64,
}

/// Go `SaveMetaToStorage`: one batch upsert of `stats_meta` at the
/// transaction's start TS, which it returns.
pub fn save_meta_to_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    refresh_last_hist_ver: bool,
    meta_updates: &[MetaUpdate],
) -> Result<u64, SqlExecError> {
    let version = get_start_ts(sctx)?;
    let sql = if refresh_last_hist_ver {
        let values = meta_updates
            .iter()
            .map(|update| {
                format!(
                    "({version}, {}, {}, {}, {version})",
                    update.physical_id, update.count, update.modify_count
                )
            })
            .collect::<Vec<_>>();
        format!(
            "insert into mysql.stats_meta (version, table_id, count, modify_count, last_stats_histograms_version) values {} \
             on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count), last_stats_histograms_version = values(last_stats_histograms_version)",
            values.join(",")
        )
    } else {
        let values = meta_updates
            .iter()
            .map(|update| {
                format!(
                    "({version}, {}, {}, {})",
                    update.physical_id, update.count, update.modify_count
                )
            })
            .collect::<Vec<_>>();
        format!(
            "insert into mysql.stats_meta (version, table_id, count, modify_count) values {} \
             on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count)",
            values.join(",")
        )
    };
    exec(sctx, &sql, &[])?;
    Ok(version)
}

/// Go `InsertTableStats2KV`: a new physical table's empty `stats_meta` row
/// and one empty histogram row per column and index; returns the start TS.
pub fn insert_table_stats_2_kv<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    info: &TableInfo,
    physical_id: i64,
) -> Result<u64, SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    exec(
        sctx,
        "insert ignore into mysql.stats_meta (version, table_id, last_stats_histograms_version) values(%?, %?, %?)",
        &[
            SqlArg::Unsigned(start_ts),
            SqlArg::Signed(physical_id),
            SqlArg::Unsigned(start_ts),
        ],
    )?;
    for column in info.columns.iter_deref() {
        exec(
            sctx,
            "insert ignore into mysql.stats_histograms\n\t\t\t\t(table_id, is_index, hist_id, distinct_count, version)\n\t\t\tvalues (%?, 0, %?, 0, %?)",
            &[
                SqlArg::Signed(physical_id),
                SqlArg::Signed(column.read().id),
                SqlArg::Unsigned(start_ts),
            ],
        )?;
    }
    for index in info.indices.iter_deref() {
        exec(
            sctx,
            "insert ignore into mysql.stats_histograms\n\t\t\t\t(table_id, is_index, hist_id, distinct_count, version)\n\t\t\tvalues(%?, 1, %?, 0, %?)",
            &[
                SqlArg::Signed(physical_id),
                SqlArg::Signed(index.read().id),
                SqlArg::Unsigned(start_ts),
            ],
        )?;
    }
    Ok(start_ts)
}

/// Go `InsertColStats2KV`: an added (or modified) column's histogram row
/// for a table that has statistics -- a NULL default counts every row as
/// NULL, a non-NULL one is a single default-value bucket -- and the table
/// re-versioned when any row was new; returns the start TS.
pub fn insert_col_stats_2_kv<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_name: &str,
    physical_id: i64,
    col_infos: &[ColumnInfo],
) -> Result<u64, SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    // The table's count fixes the bucket's count and repeats.
    let (rows, _) = exec_rows(
        sctx,
        "select count from mysql.stats_meta where table_id = %?",
        &[SqlArg::Signed(physical_id)],
    )?;
    // No row: this table has no statistics.
    let Some(row) = rows.first() else {
        return Ok(start_ts);
    };
    let count = datum_i64(row.first());
    let mut has_stats_update = false;
    for col_info in col_infos {
        if col_info.is_virtual_generated() {
            // Virtual generated columns have no column stats: the same zero
            // placeholder as the create-table path.
            exec(
                sctx,
                "insert ignore into mysql.stats_histograms\n\t\t\t\t\t(version, table_id, is_index, hist_id, distinct_count, null_count)\n\t\t\t\tvalues (%?, %?, 0, %?, 0, 0)",
                &[
                    SqlArg::Unsigned(start_ts),
                    SqlArg::Signed(physical_id),
                    SqlArg::Signed(col_info.id),
                ],
            )?;
            has_stats_update |= sctx.affected_rows() > 0;
            continue;
        }
        let value = tidb_table::origin_default::origin_default(col_info, table_name)
            .map_err(|error| Box::new(error) as SqlExecError)?;
        if value.is_null() {
            // Every existing row holds NULL in the new column.
            exec(
                sctx,
                "insert ignore into mysql.stats_histograms\n\t\t\t\t\t(version, table_id, is_index, hist_id, distinct_count, null_count)\n\t\t\t\tvalues (%?, %?, 0, %?, 0, %?)",
                &[
                    SqlArg::Unsigned(start_ts),
                    SqlArg::Signed(physical_id),
                    SqlArg::Signed(col_info.id),
                    SqlArg::Signed(count),
                ],
            )?;
            has_stats_update |= sctx.affected_rows() > 0;
            continue;
        }
        // A new histogram row: its distinct count is always one.
        let total_size = (value.go_bytes().len() as i64).wrapping_mul(count);
        exec(
            sctx,
            "insert ignore into mysql.stats_histograms\n\t\t\t\t(version, table_id, is_index, hist_id, distinct_count, tot_col_size)\n\t\t\tvalues (%?, %?, 0, %?, 1, GREATEST(%?, 0))",
            &[
                SqlArg::Unsigned(start_ts),
                SqlArg::Signed(physical_id),
                SqlArg::Signed(col_info.id),
                SqlArg::Signed(total_size),
            ],
        )?;
        // An ANALYZE may have created the histogram before this event:
        // then the default-value bucket is not inserted.
        if sctx.affected_rows() == 0 {
            continue;
        }
        has_stats_update = true;
        let bound = bound_blob(&value).map_err(|error| Box::new(error) as SqlExecError)?;
        let bound = bound.go_bytes();
        // The one bucket of this column: the default value.
        exec(
            sctx,
            "insert ignore into mysql.stats_buckets\n\t\t\t\t(table_id, is_index, hist_id, bucket_id, repeats, count, lower_bound, upper_bound)\n\t\t\tvalues (%?, 0, %?, 0, %?, %?, %?, %?)",
            &[
                SqlArg::Signed(physical_id),
                SqlArg::Signed(col_info.id),
                SqlArg::Signed(count),
                SqlArg::Signed(count),
                SqlArg::Bytes(Some(bound)),
                SqlArg::Bytes(Some(bound)),
            ],
        )?;
    }
    if !has_stats_update {
        return Ok(start_ts);
    }
    exec(
        sctx,
        "update mysql.stats_meta set version = %?, last_stats_histograms_version = %? where table_id = %?",
        &[
            SqlArg::Unsigned(start_ts),
            SqlArg::Unsigned(start_ts),
            SqlArg::Signed(physical_id),
        ],
    )?;
    Ok(start_ts)
}
