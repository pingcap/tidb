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

//! Go `pkg/statistics/handle/storage/read.go`'s statements.

use std::time::Duration;

use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_model::TableInfo;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats::histogram::Histogram;
use tidb_stats::{
    decode_cmsketch_and_topn, decode_fm_sketch, decode_topn_rows, ColAndIdxExistenceMap, CmsSketch,
    Column, CopyIntent, FmSketch, HistColl, Index, StatsLoadedStatus, Table, TopN,
};
use tidb_stats_handle_util::{exec_rows, StatsSessionContext};

/// Go `StatsMetaCountAndModifyCount` (`for_update`: `...ForUpdate`): the
/// table's `count` and `modify_count`, and whether it has no row.
pub fn stats_meta_count_and_modify_count<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    for_update: bool,
) -> Result<(i64, i64, bool), SqlExecError> {
    let sql = if for_update {
        "select count, modify_count from mysql.stats_meta where table_id = %? for update"
    } else {
        "select count, modify_count from mysql.stats_meta where table_id = %?"
    };
    let (rows, _) = exec_rows(sctx, sql, &[SqlArg::Signed(table_id)])?;
    let Some(row) = rows.first() else {
        return Ok((0, 0, true));
    };
    Ok((datum_i64(row.first()), datum_i64(row.get(1)), false))
}

/// Go `row.GetInt64(i)`.
fn datum_i64(datum: Option<&Datum>) -> i64 {
    match datum {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}

/// Go `row.GetUint64(i)`.
fn datum_u64(datum: Option<&Datum>) -> u64 {
    datum_i64(datum) as u64
}

/// Go `row.GetFloat64(i)`.
fn datum_f64(datum: Option<&Datum>) -> f64 {
    match datum {
        Some(Datum::Real(value)) => *value,
        _ => 0.0,
    }
}

/// Go `row.GetBytes(i)`: `None` for a NULL.
fn datum_bytes(datum: Option<&Datum>) -> Option<&[u8]> {
    datum.and_then(Datum::as_raw_bytes)
}

/// Go `kv.PriorityNormal` / `kv.PriorityHigh` as `statsSelectPrefix` uses
/// them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Priority {
    /// `kv.PriorityNormal`.
    Normal,
    /// `kv.PriorityLow`.
    Low,
    /// `kv.PriorityHigh`.
    High,
}

/// Go `statsSelectPrefix`.
fn stats_select_prefix(priority: Priority) -> &'static str {
    match priority {
        Priority::High => "select high_priority ",
        Priority::Low => "select low_priority ",
        Priority::Normal => "select ",
    }
}

/// Go `execRowsAtSnapshot`.
fn exec_rows_at_snapshot<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    snapshot: u64,
    sql: &str,
    args: &[SqlArg<'_>],
) -> Result<Vec<Vec<Datum>>, SqlExecError> {
    if snapshot == 0 {
        return exec_rows(sctx, sql, args).map(|(rows, _)| rows);
    }
    tidb_stats_handle_util::exec_with_opts(
        sctx,
        &[
            tidb_sqlexec::exec_option_with_snapshot(snapshot),
            std::sync::Arc::new(tidb_sqlexec::exec_option_use_current_session),
        ],
        sql,
        args,
    )
    .map(|(rows, _)| rows)
}

/// Go `StatsMetaByTableIDFromStorage`: `(version, modify_count, count)`,
/// zeros when the table has no row.
pub fn stats_meta_by_table_id_from_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    snapshot: u64,
) -> Result<(u64, i64, i64), SqlExecError> {
    let rows = exec_rows_at_snapshot(
        sctx,
        snapshot,
        "SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version",
        &[SqlArg::Signed(table_id)],
    )?;
    let Some(row) = rows.first() else {
        return Ok((0, 0, 0));
    };
    Ok((datum_u64(row.first()), datum_i64(row.get(1)), datum_i64(row.get(2))))
}

/// Go `HistogramFromStorageWithPriority`.
#[allow(clippy::too_many_arguments)]
pub fn histogram_from_storage_with_priority<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    col_id: i64,
    tp: &FieldType,
    distinct: i64,
    is_index: bool,
    ver: u64,
    null_count: i64,
    tot_col_size: i64,
    corr: f64,
    priority: Priority,
) -> Result<Histogram, SqlExecError> {
    histogram_from_storage_with_params(
        sctx,
        &HistogramLoadParams {
            tp,
            table_id,
            hist_id: col_id,
            ndv: distinct,
            is_index,
            version: ver,
            null_count,
            tot_col_size,
            correlation: corr,
            priority,
            snapshot: 0,
        },
    )
}

/// Go `histogramLoadParams`.
struct HistogramLoadParams<'a> {
    tp: &'a FieldType,
    table_id: i64,
    hist_id: i64,
    ndv: i64,
    is_index: bool,
    version: u64,
    null_count: i64,
    tot_col_size: i64,
    correlation: f64,
    priority: Priority,
    snapshot: u64,
}

/// Go `histogramFromStorageWithParams`.
fn histogram_from_storage_with_params<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    params: &HistogramLoadParams<'_>,
) -> Result<Histogram, SqlExecError> {
    let sql = format!(
        "{}count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %? order by bucket_id",
        stats_select_prefix(params.priority)
    );
    let rows = exec_rows_at_snapshot(
        sctx,
        params.snapshot,
        &sql,
        &[
            SqlArg::Signed(params.table_id),
            SqlArg::Bool(params.is_index),
            SqlArg::Signed(params.hist_id),
        ],
    )?;
    let bucket_size = rows.len();
    let mut tp = params.tp.clone();
    let mut hg = Histogram::new(
        params.hist_id,
        params.ndv,
        params.null_count,
        params.version,
        &tp,
        bucket_size,
        params.tot_col_size,
    );
    hg.correlation = params.correlation;
    let mut total_count = 0_i64;
    for row in &rows {
        let count = datum_i64(row.first());
        let repeats = datum_i64(row.get(1));
        let lower = datum_bytes(row.get(2)).unwrap_or_default().to_vec();
        let upper = datum_bytes(row.get(3)).unwrap_or_default().to_vec();
        let (lower_bound, upper_bound) = if params.is_index {
            (Datum::Bytes(lower), Datum::Bytes(upper))
        } else {
            // For new collation data the stored bound is the collate key,
            // which may be longer than the field type's flen; Go reads it as
            // a blob.
            if tp.eval_type() == EvalType::String
                && tp.code() != FieldTypeCode::Enum
                && tp.code() != FieldTypeCode::Set
            {
                tp = FieldType::new(FieldTypeCode::Blob);
            }
            (convert_bound_from_blob(lower, &tp)?, convert_bound_from_blob(upper, &tp)?)
        };
        total_count += count;
        hg.append_bucket_with_ndv(lower_bound, upper_bound, total_count, repeats, datum_i64(row.get(4)));
    }
    hg.pre_calculate_scalar();
    Ok(hg)
}

/// Go `HistMetaFromStorageWithHighPriority`: one item's histogram metadata
/// (no buckets) and its `stats_ver`, `None` when the item has no row.
/// `column_type` is the column's type (Go `possibleColInfo.FieldType`); an
/// index reads as a blob.
pub fn hist_meta_from_storage_with_high_priority<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    is_index: bool,
    hist_id: i64,
    column_type: Option<&FieldType>,
) -> Result<Option<(Histogram, i64)>, SqlExecError> {
    hist_meta_from_storage(sctx, table_id, is_index, hist_id, column_type, 0, Priority::High)
}

/// Go `histMetaFromStorage`.
fn hist_meta_from_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    is_index: bool,
    hist_id: i64,
    column_type: Option<&FieldType>,
    snapshot: u64,
    priority: Priority,
) -> Result<Option<(Histogram, i64)>, SqlExecError> {
    let blob = FieldType::new(FieldTypeCode::Blob);
    let tp = if is_index { &blob } else { column_type.unwrap_or(&blob) };
    let sql = format!(
        "{}distinct_count, version, null_count, tot_col_size, stats_ver, correlation from mysql.stats_histograms where table_id = %? and hist_id = %? and is_index = %?",
        stats_select_prefix(priority)
    );
    let rows = exec_rows_at_snapshot(
        sctx,
        snapshot,
        &sql,
        &[
            SqlArg::Signed(table_id),
            SqlArg::Signed(hist_id),
            SqlArg::Bool(is_index),
        ],
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let mut hist = Histogram::new(
        hist_id,
        datum_i64(row.first()),
        datum_i64(row.get(2)),
        datum_u64(row.get(1)),
        tp,
        0,
        datum_i64(row.get(3)),
    );
    hist.correlation = datum_f64(row.get(5));
    Ok(Some((hist, datum_i64(row.get(4)))))
}

/// Go `ReadColumnDistributionStats`: one column's metadata, TopN and
/// histogram from one MVCC snapshot with normal priority, without touching
/// the statistics cache. Any read or decoding failure fails the whole read.
pub fn read_column_distribution_stats<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    physical_table_id: i64,
    col_info: &tidb_stats::column::ColumnInfo,
) -> Result<Option<tidb_stats::Column>, SqlExecError> {
    let snapshot = sctx.current_ts()?;
    let (col_id, tp, name) = {
        let info = col_info.read();
        (info.id, info.field_type.clone(), info.name.original().to_owned())
    };
    let Some((hist_meta, stats_ver)) = hist_meta_from_storage(
        sctx,
        physical_table_id,
        false,
        col_id,
        Some(&tp),
        snapshot,
        Priority::Normal,
    )?
    else {
        return Ok(None);
    };
    if hist_meta.null_count < 0 {
        return Err(format!(
            "negative null count {} for physical table {physical_table_id}, column {name}({col_id})",
            hist_meta.null_count
        )
        .into());
    }
    let mut column = tidb_stats::Column {
        physical_id: physical_table_id,
        info: Some(col_info.clone()),
        stats_version: stats_ver,
        stats_loaded_status: tidb_stats::StatsLoadedStatus::full_load(),
        ..Default::default()
    };
    if stats_ver != tidb_stats::stats_version::VERSION_2 {
        column.histogram = hist_meta;
        return Ok(Some(column));
    }
    column.top_n = topn_from_storage_with_params(
        sctx,
        physical_table_id,
        false,
        col_id,
        Priority::Normal,
        snapshot,
    )?;
    column.histogram = histogram_from_storage_with_params(
        sctx,
        &HistogramLoadParams {
            tp: &tp,
            table_id: physical_table_id,
            hist_id: col_id,
            ndv: hist_meta.ndv,
            is_index: false,
            version: hist_meta.last_update_version,
            null_count: hist_meta.null_count,
            tot_col_size: hist_meta.tot_col_size,
            correlation: hist_meta.correlation,
            priority: Priority::Normal,
            snapshot,
        },
    )?;
    Ok(Some(column))
}

/// Go `CheckSkipPartition`: whether the partition has any histogram row of
/// the kind (`false` is Go's `ErrPartitionStatsMissing`).
pub fn check_skip_partition<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    tbl_id: i64,
    is_index: bool,
) -> Result<bool, SqlExecError> {
    let (rows, _) = exec_rows(
        sctx,
        "select distinct_count from mysql.stats_histograms where table_id =%? and is_index = %?",
        &[SqlArg::Signed(tbl_id), SqlArg::Bool(is_index)],
    )?;
    Ok(!rows.is_empty())
}

/// Go `CheckSkipColumnPartiion`: whether the partition has this item's
/// histogram row (`false` is Go's `ErrPartitionColumnStatsMissing`).
pub fn check_skip_column_partition<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    tbl_id: i64,
    is_index: bool,
    hist_id: i64,
) -> Result<bool, SqlExecError> {
    let (rows, _) = exec_rows(
        sctx,
        "select distinct_count from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?",
        &[SqlArg::Signed(tbl_id), SqlArg::Bool(is_index), SqlArg::Signed(hist_id)],
    )?;
    Ok(!rows.is_empty())
}

/// Go `CMSketchFromStorage`.
pub fn cm_sketch_from_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    tbl_id: i64,
    is_index: bool,
    hist_id: i64,
) -> Result<Option<CmsSketch>, SqlExecError> {
    let (rows, _) = exec_rows(
        sctx,
        "select cm_sketch from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?",
        &[SqlArg::Signed(tbl_id), SqlArg::Bool(is_index), SqlArg::Signed(hist_id)],
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    tidb_stats::cmsketch::decode_cmsketch(datum_bytes(row.first()).unwrap_or_default()).map_err(boxed)
}

/// Go `LoadHistogram`: one item's histogram with its buckets, `None` when it
/// has no row.
pub fn load_histogram<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    is_index: bool,
    hist_id: i64,
    table_info: &TableInfo,
) -> Result<Option<Histogram>, SqlExecError> {
    let (rows, _) = exec_rows(
        sctx,
        "select distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?",
        &[SqlArg::Signed(table_id), SqlArg::Bool(is_index), SqlArg::Signed(hist_id)],
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let distinct = datum_i64(row.first());
    let hist_ver = datum_u64(row.get(1));
    let null_count = datum_i64(row.get(2));
    if is_index {
        return histogram_from_storage_with_priority(
            sctx,
            table_id,
            hist_id,
            &FieldType::new(FieldTypeCode::Blob),
            distinct,
            true,
            hist_ver,
            null_count,
            0,
            0.0,
            Priority::Normal,
        )
        .map(Some);
    }
    let tp = table_info
        .columns
        .iter_deref()
        .find_map(|column| {
            let column = column.read();
            (column.id == hist_id).then(|| column.field_type.clone())
        })
        // Go's zero `types.FieldType`: a column the table no longer has.
        .unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified));
    histogram_from_storage_with_priority(
        sctx,
        table_id,
        hist_id,
        &tp,
        distinct,
        false,
        hist_ver,
        null_count,
        datum_i64(row.get(3)),
        datum_f64(row.get(6)),
        Priority::Normal,
    )
    .map(Some)
}

/// Go `CMSketchAndTopNFromStorageWithHighPriority`.
pub fn cm_sketch_and_topn_from_storage_with_high_priority<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    tbl_id: i64,
    is_index: bool,
    hist_id: i64,
    stats_ver: i64,
) -> Result<(Option<CmsSketch>, Option<TopN>), SqlExecError> {
    let (top_n_rows, _) = exec_rows(
        sctx,
        "select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?",
        &[SqlArg::Signed(tbl_id), SqlArg::Bool(is_index), SqlArg::Signed(hist_id)],
    )?;
    let top_n_rows = topn_rows(&top_n_rows);
    // If we are on version higher than 1. Don't read Count-Min Sketch.
    if stats_ver > tidb_stats::stats_version::VERSION_1 {
        return decode_cmsketch_and_topn(None, &top_n_rows).map_err(boxed);
    }
    let (rows, _) = exec_rows(
        sctx,
        "select cm_sketch from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?",
        &[SqlArg::Signed(tbl_id), SqlArg::Bool(is_index), SqlArg::Signed(hist_id)],
    )?;
    let data = rows.first().and_then(|row| datum_bytes(row.first()));
    decode_cmsketch_and_topn(data, &top_n_rows).map_err(boxed)
}

/// Go `DecodeTopN`'s row shape: `(value, count)`.
fn topn_rows(rows: &[Vec<Datum>]) -> Vec<(Vec<u8>, u64)> {
    rows.iter()
        .map(|row| {
            (
                datum_bytes(row.first()).unwrap_or_default().to_vec(),
                datum_u64(row.get(1)),
            )
        })
        .collect()
}

/// Go `TopNFromStorage`.
pub fn topn_from_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    tbl_id: i64,
    is_index: bool,
    hist_id: i64,
) -> Result<Option<TopN>, SqlExecError> {
    topn_from_storage_with_params(sctx, tbl_id, is_index, hist_id, Priority::High, 0)
}

/// Go `topNFromStorageWithParams`.
fn topn_from_storage_with_params<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    tbl_id: i64,
    is_index: bool,
    hist_id: i64,
    priority: Priority,
    snapshot: u64,
) -> Result<Option<TopN>, SqlExecError> {
    let rows = exec_rows_at_snapshot(
        sctx,
        snapshot,
        &format!(
            "{}value, count from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?",
            stats_select_prefix(priority)
        ),
        &[SqlArg::Signed(tbl_id), SqlArg::Bool(is_index), SqlArg::Signed(hist_id)],
    )?;
    Ok(decode_topn_rows(&topn_rows(&rows)))
}

/// Go `FMSketchFromStorage`.
pub fn fm_sketch_from_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    tbl_id: i64,
    is_index: bool,
    hist_id: i64,
) -> Result<Option<FmSketch>, SqlExecError> {
    let (rows, _) = exec_rows(
        sctx,
        "select value from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?",
        &[SqlArg::Signed(tbl_id), SqlArg::Bool(is_index), SqlArg::Signed(hist_id)],
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    decode_fm_sketch(datum_bytes(row.first()))
        .map_err(|error| SqlExecError::from(format!("decode FMSketch: {error:?}")))
}

fn boxed(error: impl std::error::Error + Send + Sync + 'static) -> SqlExecError {
    Box::new(error)
}

/// Go `config.GetGlobalConfig().Performance.LiteInitStats`.
fn lite_init_stats() -> bool {
    tidb_config::config_tree::config::get_global_config()
        .performance
        .lite_init_stats
}

/// Go `indexStatsFromStorage`.
fn index_stats_from_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    row: &[Datum],
    table: &mut Table,
    table_info: &TableInfo,
    load_all: bool,
    lease: Duration,
) -> Result<(), SqlExecError> {
    let hist_id = datum_i64(row.get(2));
    let distinct = datum_i64(row.get(3));
    let hist_ver = datum_u64(row.get(4));
    let null_count = datum_i64(row.get(5));
    let stats_ver = datum_i64(row.get(7));
    let physical_id = table.hist_coll.physical_id;
    let mut idx = table.hist_coll.get_index(hist_id).map(|idx| {
        idx.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    });

    // All the objects in the table share the same stats version. Update here
    // before processing, so it's set even if we return early.
    if stats_ver != tidb_stats::stats_version::VERSION_0 {
        table.hist_coll.stats_version = stats_ver as i32;
        table.last_analyze_version = table.last_analyze_version.max(hist_ver);
    }

    if let Some(idx_info) = table_info.find_index_by_id(hist_id) {
        if let Some(existence) = &table.existence_map {
            existence
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert_index(hist_id, stats_ver != tidb_stats::stats_version::VERSION_0);
        }
        // We will not load buckets, topn and cmsketch if:
        // 1. lease > 0, and:
        // 2. the index doesn't have any of buckets, topn, cmsketch in memory
        //    before, and:
        // 3. loadAll is false.
        // 4. lite-init-stats is true.
        let not_need_load = !lease.is_zero()
            && idx.as_ref().is_none_or(|idx| {
                (!idx.stats_loaded_status.stats_initialized() || idx.is_all_evicted())
                    && idx.histogram.last_update_version < hist_ver
            })
            && !load_all
            && lite_init_stats();
        if not_need_load {
            // If we don't have this index in memory, skip it.
            if idx.is_none() {
                return Ok(());
            }
            let mut evicted = Index {
                histogram: Histogram::new(
                    hist_id,
                    distinct,
                    null_count,
                    hist_ver,
                    &FieldType::new(FieldTypeCode::Blob),
                    0,
                    0,
                ),
                stats_version: stats_ver,
                info: Some(idx_info),
                physical_id,
                ..Index::default()
            };
            if evicted.is_analyzed() {
                evicted.stats_loaded_status = StatsLoadedStatus::all_evicted();
            }
            idx = Some(evicted);
        } else if idx
            .as_ref()
            .is_none_or(|idx| idx.histogram.last_update_version < hist_ver)
            || load_all
        {
            let hg = histogram_from_storage_with_priority(
                sctx,
                physical_id,
                hist_id,
                &FieldType::new(FieldTypeCode::Blob),
                distinct,
                true,
                hist_ver,
                null_count,
                0,
                0.0,
                Priority::Normal,
            )?;
            let (cms, top_n) = cm_sketch_and_topn_from_storage_with_high_priority(
                sctx,
                physical_id,
                true,
                hist_id,
                stats_ver,
            )?;
            // FMSketch is only used when merging partition stats into global
            // stats, which loads everything.
            let fm_sketch = if load_all {
                fm_sketch_from_storage(sctx, physical_id, true, hist_id)?
            } else {
                None
            };
            let mut loaded = Index {
                histogram: hg,
                cmsketch: cms,
                top_n,
                fm_sketch,
                info: Some(idx_info),
                stats_version: stats_ver,
                physical_id,
                ..Index::default()
            };
            if stats_ver != tidb_stats::stats_version::VERSION_0 {
                loaded.stats_loaded_status = StatsLoadedStatus::full_load();
            }
            idx = Some(loaded);
        }
    }
    if let Some(idx) = idx {
        table.hist_coll.set_index(hist_id, idx);
    }
    Ok(())
}

/// Go `columnStatsFromStorage`.
fn column_stats_from_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    row: &[Datum],
    table: &mut Table,
    table_info: &TableInfo,
    load_all: bool,
    lease: Duration,
) -> Result<(), SqlExecError> {
    let hist_id = datum_i64(row.get(2));
    let distinct = datum_i64(row.get(3));
    let hist_ver = datum_u64(row.get(4));
    let null_count = datum_i64(row.get(5));
    let tot_col_size = datum_i64(row.get(6));
    let stats_ver = datum_i64(row.get(7));
    let correlation = datum_f64(row.get(8));
    let physical_id = table.hist_coll.physical_id;
    let mut col = table.hist_coll.get_column(hist_id).map(|col| {
        col.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    });

    // All the objects in the table share the same stats version. Update here
    // before processing, so it's set even if we return early.
    if stats_ver != tidb_stats::stats_version::VERSION_0 {
        table.hist_coll.stats_version = stats_ver as i32;
        table.last_analyze_version = table.last_analyze_version.max(hist_ver);
    }

    if let Some(col_info) = table_info.find_column_by_id(hist_id) {
        // Column stats can be synthesized when adding a column with default
        // values, which keeps statsVer at 0 but still records NDV/null
        // counts, so mark them as existing whenever any value is present.
        if let Some(existence) = &table.existence_map {
            existence
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert_column(
                    hist_id,
                    tidb_stats::stats_version::is_column_analyzed_or_synthesized(
                        stats_ver, distinct, null_count,
                    ),
                );
        }
        let field_type = col_info.read().field_type.clone();
        let is_handle = table_info.pk_is_handle
            && field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY);
        // We will not load buckets, topn and cmsketch if:
        // 1. lease > 0, and:
        // 2. this column is not handle or lite-init-stats is true, and:
        // 3. the column doesn't have any of buckets, topn, cmsketch in memory
        //    before, and:
        // 4. loadAll is false.
        let not_need_load = !lease.is_zero()
            && (!is_handle || lite_init_stats())
            && col.as_ref().is_none_or(|col| {
                (!col.is_stats_initialized() || col.is_all_evicted())
                    && col.histogram.last_update_version < hist_ver
            })
            && !load_all;
        if not_need_load {
            // If we don't have the column in memory currently, just skip it.
            if col.is_none() {
                return Ok(());
            }
            let mut evicted = Column {
                physical_id,
                histogram: Histogram::new(
                    hist_id,
                    distinct,
                    null_count,
                    hist_ver,
                    &field_type,
                    0,
                    tot_col_size,
                ),
                info: Some(col_info),
                is_handle,
                stats_version: stats_ver,
                ..Column::default()
            };
            if evicted.stats_available() {
                evicted.stats_loaded_status = StatsLoadedStatus::all_evicted();
            }
            evicted.histogram.correlation = correlation;
            col = Some(evicted);
        } else if col
            .as_ref()
            .is_none_or(|col| col.histogram.last_update_version < hist_ver)
            || load_all
        {
            let hg = histogram_from_storage_with_priority(
                sctx,
                physical_id,
                hist_id,
                &field_type,
                distinct,
                false,
                hist_ver,
                null_count,
                tot_col_size,
                correlation,
                Priority::Normal,
            )?;
            let (cms, top_n) = cm_sketch_and_topn_from_storage_with_high_priority(
                sctx,
                physical_id,
                false,
                hist_id,
                stats_ver,
            )?;
            let fm_sketch = if load_all {
                fm_sketch_from_storage(sctx, physical_id, false, hist_id)?
            } else {
                None
            };
            let mut loaded = Column {
                physical_id,
                histogram: hg,
                info: Some(col_info),
                cmsketch: cms,
                top_n,
                fm_sketch,
                is_handle,
                stats_version: stats_ver,
                ..Column::default()
            };
            if loaded.stats_available() {
                loaded.stats_loaded_status = StatsLoadedStatus::full_load();
            }
            col = Some(loaded);
        } else if let Some(current) = col.as_mut() {
            if current.histogram.tot_col_size != tot_col_size {
                current.histogram.tot_col_size = tot_col_size;
            }
        }
    }
    if let Some(col) = col {
        table.hist_coll.set_column(hist_id, col);
    }
    Ok(())
}

/// Go `TableStatsFromStorage`: `table` is the cached table (`None` when
/// nothing is cached); `None` back when `mysql.stats_meta` has no row.
pub fn table_stats_from_storage<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    snapshot: u64,
    table_info: &TableInfo,
    table_id: i64,
    load_all: bool,
    lease: Duration,
    table: Option<&Table>,
) -> Result<Option<Table>, SqlExecError> {
    // Delay copying until we know what needs to be modified.
    let cached = table.filter(|_| snapshot == 0);
    let (realtime_count, modify_count, is_null) =
        stats_meta_count_and_modify_count(sctx, table_id, false)?;
    if is_null {
        return Ok(None);
    }
    let (rows, _) = exec_rows(
        sctx,
        "select table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, correlation from mysql.stats_histograms where table_id = %?",
        &[SqlArg::Signed(table_id)],
    )?;
    let mut table = match cached {
        // Only metadata update needed, or the maps get updated.
        Some(cached) => cached.copy_as(if rows.is_empty() {
            CopyIntent::MetaOnly
        } else {
            CopyIntent::BothMapsWritable
        }),
        None => Table {
            existence_map: Some(std::sync::Arc::new(std::sync::RwLock::new(
                ColAndIdxExistenceMap::new(table_info.columns.len(), table_info.indices.len()),
            ))),
            hist_coll: HistColl::new(table_id, 0, 0, 4, 4),
            version: 0,
            last_analyze_version: 0,
            last_stats_hist_version: 0,
            table_info_update_ts: 0,
            is_pk_handle: false,
        },
    };
    // Update metadata.
    table.hist_coll.pseudo = false;
    table.hist_coll.modify_count = modify_count;
    table.hist_coll.realtime_count = realtime_count;
    // Early return if no histogram data to process.
    if rows.is_empty() {
        return Ok(Some(table));
    }
    for row in &rows {
        if datum_i64(row.get(1)) > 0 {
            index_stats_from_storage(sctx, row, &mut table, table_info, load_all, lease)?;
        } else {
            column_stats_from_storage(sctx, row, &mut table, table_info, load_all, lease)?;
        }
    }
    // If DROP STATS executes, we need to reset the stats version to 0. Only
    // reset if we actually have columns/indices in the table.
    if tidb_stats::stats_version::is_analyzed(i64::from(table.hist_coll.stats_version)) {
        let mut has_stats = false;
        let mut all_zero = true;
        table.hist_coll.for_each_column(|_, column| {
            has_stats = true;
            if column.is_analyzed() {
                all_zero = false;
                return true;
            }
            false
        });
        table.hist_coll.for_each_index(|_, index| {
            has_stats = true;
            if index.is_analyzed() {
                all_zero = false;
                return true;
            }
            false
        });
        if has_stats && all_zero {
            table.hist_coll.stats_version = tidb_stats::stats_version::VERSION_0 as i32;
        }
    }
    Ok(Some(table))
}

/// Go `statistics.UTCWithAllowInvalidDateCtx`, as conversion flags.
const UTC_WITH_ALLOW_INVALID_DATE: tidb_datatype::ConversionFlags =
    tidb_datatype::ConversionFlags::from_bits(
        tidb_datatype::ConversionFlags::ALLOW_NEGATIVE_TO_UNSIGNED
            | tidb_datatype::ConversionFlags::IGNORE_ZERO_DATE_ERR
            | tidb_datatype::ConversionFlags::IGNORE_ZERO_IN_DATE_ERR
            | tidb_datatype::ConversionFlags::IGNORE_INVALID_DATE_ERR,
    );

/// Go `convertBoundFromBlob`: a `mysql.stats_buckets` bound read back as a
/// value of `tp`.
fn convert_bound_from_blob(blob: Vec<u8>, tp: &FieldType) -> Result<Datum, SqlExecError> {
    // For `BIT` type, when converting to `BLOB`, it's formatted as an integer
    // (when it's possible). Therefore, we should try to parse it as an
    // integer first.
    if tp.code() == FieldTypeCode::Bit {
        let Ok(mut uint_value) = String::from_utf8_lossy(&blob).parse::<u64>() else {
            // Fail to parse, return the original blob as BIT directly.
            return Ok(Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(blob)));
        };
        let flen = tp.flen();
        let mut too_long = None;
        if flen < 64 && uint_value >= 1 << flen {
            tidb_logutil::bg_logger().warn(
                "bound in stats exceeds the bit length",
                &[
                    tidb_log::Field::new("bound", tidb_log::Value::U64(uint_value)),
                    tidb_log::Field::new("flen", tidb_log::Value::I64(i64::from(flen))),
                ],
            );
            too_long = Some(format!("Data Too Long, field len {flen}"));
            uint_value = (1 << flen) - 1;
        }
        let byte_size = u8::try_from((flen + 7) >> 3).unwrap_or(8);
        let width = tidb_datatype::BinaryLiteralWidth::try_from(byte_size).map_err(boxed)?;
        if let Some(too_long) = too_long {
            return Err(too_long.into());
        }
        return Ok(Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(
            uint_value,
            Some(width),
        )));
    }
    Datum::Bytes(blob)
        .convert_to(tp, UTC_WITH_ALLOW_INVALID_DATE)
        .map(|converted| converted.value)
        .map_err(boxed)
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{BinaryLiteral, Datum, FieldType, FieldTypeCode};

    use super::convert_bound_from_blob;

    /// A column bound is stored as the value's text form, not its datum
    /// encoding: `id BIGINT` bucket bounds "11", "12".
    #[test]
    fn a_column_bound_is_the_values_text_form() {
        let bound = convert_bound_from_blob(b"11".to_vec(), &FieldType::new(FieldTypeCode::LongLong))
            .expect("the bound converts");
        assert_eq!(bound, Datum::Int(11));
    }

    /// `c DECIMAL(10,2)` keeps its stored scale; `d DATETIME` parses back
    /// from its printed form.
    #[test]
    fn decimal_and_datetime_bounds_parse_back_from_their_printed_form() {
        let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
        decimal.set_flen(10);
        decimal.set_decimal(2);
        let bound = convert_bound_from_blob(b"11.00".to_vec(), &decimal).expect("the bound converts");
        assert_eq!(bound.sql_string().unwrap(), "11.00");
        let bound = convert_bound_from_blob(
            b"2024-05-05 05:05:05".to_vec(),
            &FieldType::new(FieldTypeCode::Datetime),
        )
        .expect("the bound converts");
        assert_eq!(bound.sql_string().unwrap(), "2024-05-05 05:05:05");
    }

    /// Go `handletest.TestLoadStatsForBitColumn`: ANALYZE stores a BIT bound
    /// as the decimal text of its value.
    #[test]
    fn bit_column_bounds_load_from_the_four_go_decimal_storage_cases() {
        let cases = [
            (1, b"0".as_slice(), 0_u8, b"1".as_slice(), 1_u8),
            (2, b"2".as_slice(), 2_u8, b"3".as_slice(), 3_u8),
            (6, b"48".as_slice(), 48_u8, b"49".as_slice(), 49_u8),
            (7, b"97".as_slice(), 97_u8, b"98".as_slice(), 98_u8),
        ];
        for (flen, lower, expected_lower, upper, expected_upper) in cases {
            let mut bit = FieldType::new(FieldTypeCode::Bit);
            bit.set_flen(flen);
            let lower = convert_bound_from_blob(lower.to_vec(), &bit).expect("lower BIT bound converts");
            let upper = convert_bound_from_blob(upper.to_vec(), &bit).expect("upper BIT bound converts");
            assert_eq!(lower, Datum::Bit(BinaryLiteral::from(vec![expected_lower])));
            assert_eq!(upper, Datum::Bit(BinaryLiteral::from(vec![expected_upper])));
        }
    }
}
