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

//! Go `pkg/statistics/handle/bootstrap.go`: `Domain.initStats`' initial load
//! of the node's statistics cache, full or lite, on the statistics handle's
//! sessions, under the cache's memory quota.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_sqlexec::SqlExecError;
use tidb_stats::{
    decode_cmsketch_and_topn, ColAndIdxExistenceMap, Column, CopyIntent, HistColl, Index,
    StatsLoadedStatus, Table, TopN,
};
use tidb_stats_handle_cache::StatsCacheImpl;
use tidb_stats_handle_initstats::{get_concurrency, RangeWorker, Task, INIT_STATS_PERCENTAGE};
use tidb_stats_handle_storage::stats_read_writer::StatsSessionPool;
use tidb_stats_handle_syncload::real_tikv_stats::StatsTarget;
use tidb_stats_handle_syncload::stats_watch::{StatsSnapshot, TableStatsState};
use tidb_stats_handle_util::{exec_rows, StatsSessionContext, FLAG_WRAP_TXN};

/// The two startup shapes selected by Go `Domain.initStats`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitialStatsLoad {
    /// Go `Handle.InitStatsLite`: metadata only for columns and indexes.
    Lite,
    /// Go `Handle.InitStats`: metadata-only columns and fully loaded indexes.
    IndexFull,
}

/// Go `initStatsStep`.
const INIT_STATS_STEP: i64 = 2_000;
/// Go `initStatsPercentageInterval`.
const INIT_STATS_PERCENTAGE_INTERVAL: f64 = 100.0 / 3.0;

/// The schema image Go's init-stats lookups read (`TableItemByIDForInitStats`,
/// `TableInfoByIDForInitStats`): every physical ID with its table.
type InfoSchema = HashMap<i64, StatsTarget>;

/// Go `Handle.InitStats` / `Handle.InitStatsLite` on `pool`'s sessions: the
/// statistics of `tables` (only `table_ids` when not empty), as the snapshot
/// the node publishes -- a table with no `mysql.stats_meta` row is pseudo.
///
/// # Errors
///
/// A statement that failed.
pub fn init_stats(
    pool: Arc<dyn StatsSessionPool>,
    tables: &[StatsTarget],
    table_ids: &[i64],
    mode: InitialStatsLoad,
) -> Result<StatsSnapshot, String> {
    let is: Arc<InfoSchema> = Arc::new(
        tables
            .iter()
            .map(|target| (target.physical_id, target.clone()))
            .collect(),
    );
    let cache = Arc::new(StatsCacheImpl::new()?);
    // Go `WithForceBlockGCSession`, `begin` ... `commit`.
    let mut result = Ok(());
    pool.call_with_sctx(
        &mut |sctx| {
            result = match mode {
                InitialStatsLoad::Lite => init_stats_lite_with_session(sctx, &is, &cache, table_ids),
                InitialStatsLoad::IndexFull => {
                    init_stats_with_session(sctx, &pool, &is, &cache, table_ids)
                }
            };
            Ok(())
        },
        &[FLAG_WRAP_TXN],
    )
    .map_err(|error| error.message)?;
    result.map_err(|error| error.to_string())?;
    let snapshot = tables
        .iter()
        .filter(|target| table_ids.is_empty() || table_ids.contains(&target.physical_id))
        .map(|target| {
            let state = cache
                .get(target.physical_id)
                .map_or(TableStatsState::Pseudo, TableStatsState::Loaded);
            (target.physical_id, state)
        })
        .collect();
    cache.close();
    Ok(snapshot)
}

fn int(row: &[Datum], index: usize) -> i64 {
    match row.get(index) {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}

fn uint(row: &[Datum], index: usize) -> u64 {
    int(row, index) as u64
}

fn float(row: &[Datum], index: usize) -> f64 {
    match row.get(index) {
        Some(Datum::Real(value)) => *value,
        _ => 0.0,
    }
}

fn is_null(row: &[Datum], index: usize) -> bool {
    row.get(index).is_none_or(Datum::is_null)
}

/// Go `row.GetBytes(i)`: a NULL reads as no bytes.
fn bytes(row: &[Datum], index: usize) -> &[u8] {
    row.get(index).and_then(Datum::as_raw_bytes).unwrap_or_default()
}

fn ids_in(table_ids: &[i64]) -> String {
    table_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// Go `initStatsMeta4Chunk`.
fn init_stats_meta_4_chunk(is: &InfoSchema, cache: &StatsCacheImpl, rows: &[Vec<Datum>]) -> i64 {
    let mut max_physical_id = 0;
    for row in rows {
        let physical_id = int(row, 1);
        // The table of this physical ID was dropped; its stats_meta row is
        // not yet garbage collected.
        let Some(target) = is.get(&physical_id) else {
            continue;
        };
        max_physical_id = max_physical_id.max(physical_id);
        let snapshot = uint(row, 4);
        let last_analyze_version = snapshot;
        let mut last_stats_hist_version = snapshot;
        if !is_null(row, 5) {
            last_stats_hist_version = last_stats_hist_version.max(uint(row, 5));
        }
        let table = Table {
            hist_coll: HistColl::new(physical_id, int(row, 3), int(row, 2), 4, 4),
            version: uint(row, 0),
            existence_map: Some(Arc::new(RwLock::new(ColAndIdxExistenceMap::new_without_size()))),
            last_analyze_version,
            last_stats_hist_version,
            is_pk_handle: target.table.pk_is_handle,
            ..Table::default()
        };
        cache.put(physical_id, Arc::new(table));
    }
    max_physical_id
}

/// Go `genInitStatsMetaSQL`.
fn gen_init_stats_meta_sql(table_ids: &[i64]) -> String {
    let select_prefix = "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta";
    if table_ids.is_empty() {
        return select_prefix.to_owned();
    }
    format!("{select_prefix} where table_id in ({})", ids_in(table_ids))
}

/// Go `initStatsMeta`.
fn init_stats_meta(
    sctx: &dyn StatsSessionContext,
    is: &InfoSchema,
    cache: &StatsCacheImpl,
    table_ids: &[i64],
) -> Result<i64, SqlExecError> {
    let (rows, _) = exec_rows(sctx, &gen_init_stats_meta_sql(table_ids), &[])?;
    Ok(init_stats_meta_4_chunk(is, cache, &rows))
}

/// Go `initStatsHistograms4ChunkLite`.
fn init_stats_histograms_4_chunk_lite(cache: &StatsCacheImpl, rows: &[Vec<Datum>]) {
    let mut table: Option<Table> = None;
    for row in rows {
        let table_id = int(row, 0);
        if table.as_ref().is_none_or(|table| table.hist_coll.physical_id != table_id) {
            if let Some(done) = table.take() {
                cache.put(done.hist_coll.physical_id, Arc::new(done));
            }
            let Some(cached) = cache.get(table_id) else {
                continue;
            };
            table = Some(cached.copy_as(CopyIntent::MetaOnly));
        }
        let Some(table) = table.as_mut() else {
            continue;
        };
        let is_index = int(row, 1);
        let id = int(row, 2);
        let ndv = int(row, 3);
        let null_count = int(row, 5);
        let stats_ver = int(row, 8);
        // All the objects in the table share the same stats version.
        if stats_ver != tidb_stats::stats_version::VERSION_0 {
            table.hist_coll.stats_version = stats_ver as i32;
        }
        let existence = table.existence_map.as_ref().expect("init stats builds the existence map");
        if is_index > 0 {
            existence
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert_index(id, stats_ver != tidb_stats::stats_version::VERSION_0);
        } else {
            // Column stats can be synthesized when adding a column with
            // default values, which keeps statsVer at 0 but still records
            // NDV/null counts.
            existence
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert_column(
                    id,
                    tidb_stats::stats_version::is_column_analyzed_or_synthesized(
                        stats_ver, ndv, null_count,
                    ),
                );
        }
        if stats_ver != tidb_stats::stats_version::VERSION_0 {
            // The LastAnalyzeVersion is added by ALTER table so its value
            // might be 0.
            table.last_analyze_version = table.last_analyze_version.max(uint(row, 4));
        }
    }
    if let Some(done) = table {
        cache.put(done.hist_coll.physical_id, Arc::new(done));
    }
}

/// Go `initStatsHistograms4Chunk`.
fn init_stats_histograms_4_chunk(
    is: &InfoSchema,
    cache: &StatsCacheImpl,
    rows: &[Vec<Datum>],
    is_cache_full: bool,
) {
    let mut table: Option<Table> = None;
    let mut tbl_info: Option<&StatsTarget> = None;
    for row in rows {
        let (table_id, stats_ver) = (int(row, 0), int(row, 8));
        if table.as_ref().is_none_or(|table| table.hist_coll.physical_id != table_id) {
            tbl_info = None;
            if let Some(done) = table.take() {
                cache.put(done.hist_coll.physical_id, Arc::new(done));
            }
            // This table must be already in the cache since we load
            // stats_meta first.
            let Some(cached) = cache.get(table_id) else {
                continue;
            };
            table = Some(cached.copy_as(CopyIntent::BothMapsWritable));
            // Table not found: likely dropped but stats metadata not yet
            // garbage collected. Skip loading stats for this table.
            tbl_info = is.get(&table_id);
        }
        let (Some(table), Some(target)) = (table.as_mut(), tbl_info) else {
            continue;
        };
        // All the objects in the table share the same stats version.
        if stats_ver != tidb_stats::stats_version::VERSION_0 {
            table.hist_coll.stats_version = stats_ver as i32;
        }
        let (id, ndv, null_count, version, tot_col_size) =
            (int(row, 2), int(row, 3), int(row, 5), uint(row, 4), int(row, 7));
        let existence = Arc::clone(table.existence_map.as_ref().expect("init stats builds the existence map"));
        if int(row, 1) > 0 {
            let Some(idx_info) = target
                .table
                .indices
                .iter_deref()
                .find(|index| index.read().id == id)
            else {
                continue;
            };
            let (mut cms, mut top_n) = (None, None);
            if !is_cache_full {
                let data = (!is_null(row, 6)).then(|| bytes(row, 6));
                match decode_cmsketch_and_topn(data, &[]) {
                    Ok((decoded_cms, decoded_top_n)) => (cms, top_n) = (decoded_cms, decoded_top_n),
                    // Go `terror.Log`, the index keeps no CM sketch.
                    Err(error) => tidb_logutil::bg_logger()
                        .error("decode cm sketch failed", &[tidb_logutil::zap_error(error)]),
                }
            }
            let histogram = tidb_stats::histogram::Histogram::new(
                id,
                ndv,
                null_count,
                version,
                &FieldType::new(FieldTypeCode::Blob),
                32,
                0,
            );
            let mut index = Index {
                histogram,
                cmsketch: cms,
                top_n,
                info: Some(idx_info.clone()),
                stats_version: stats_ver,
                physical_id: table_id,
                ..Index::default()
            };
            if stats_ver != tidb_stats::stats_version::VERSION_0 {
                // AllEvicted first; it is AllLoad once the buckets load.
                index.stats_loaded_status = StatsLoadedStatus::all_evicted();
                // The LastAnalyzeVersion is added by ALTER table so its
                // value might be 0.
                table.last_analyze_version = table.last_analyze_version.max(version);
            }
            table.hist_coll.set_index(id, index);
            existence
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert_index(id, stats_ver != tidb_stats::stats_version::VERSION_0);
        } else {
            let Some(col_info) = target
                .table
                .cols()
                .iter_deref()
                .find(|column| column.read().id == id)
            else {
                continue;
            };
            let field_type = col_info.read().field_type.clone();
            let mut histogram = tidb_stats::histogram::Histogram::new(
                id,
                ndv,
                null_count,
                version,
                &field_type,
                0,
                tot_col_size,
            );
            histogram.correlation = float(row, 9);
            let mut column = Column {
                histogram,
                physical_id: table.hist_coll.physical_id,
                info: Some(col_info.clone()),
                is_handle: target.table.pk_is_handle
                    && field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                stats_version: stats_ver,
                ..Column::default()
            };
            if stats_ver != tidb_stats::stats_version::VERSION_0 {
                // The LastAnalyzeVersion is added by ALTER table so its
                // value might be 0.
                table.last_analyze_version = table.last_analyze_version.max(version);
                // We will also set int primary key's loaded status to
                // evicted.
                column.stats_loaded_status = StatsLoadedStatus::all_evicted();
            } else if column.histogram.ndv > 0 || column.histogram.null_count > 0 {
                // A column with NDV or nulls has its statistics too (Go
                // `StatsAvailable`), so its status is evicted as well.
                column.stats_loaded_status = StatsLoadedStatus::all_evicted();
            }
            // Otherwise the column's stats is not initialized.
            table.hist_coll.set_column(id, column);
            existence
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert_column(
                    id,
                    tidb_stats::stats_version::is_column_analyzed_or_synthesized(
                        stats_ver, ndv, null_count,
                    ),
                );
        }
    }
    if let Some(done) = table {
        cache.put(done.hist_coll.physical_id, Arc::new(done));
    }
}

/// Go `genHistSQLOptions`.
enum GenHistSqlOptions<'a> {
    /// Paging over `[start, end)`.
    Paging([i64; 2]),
    /// The listed table IDs (every table when empty).
    TableIds(&'a [i64]),
}

/// Go `genInitStatsHistogramsSQL`: every `stats_histograms` row, which the
/// existence map needs.
fn gen_init_stats_histograms_sql(options: &GenHistSqlOptions<'_>) -> String {
    // Keep the ORDER_INDEX(tbl) hint: `tbl` still exists on clusters
    // bootstrapped before stats_histograms moved to a clustered PRIMARY KEY.
    let select_prefix = "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation from mysql.stats_histograms";
    let order_suffix = " order by table_id";
    match options {
        GenHistSqlOptions::TableIds(ids) if ids.is_empty() => format!("{select_prefix}{order_suffix}"),
        GenHistSqlOptions::Paging([start, end]) => {
            format!("{select_prefix} where table_id >= {start} and table_id < {end}{order_suffix}")
        }
        GenHistSqlOptions::TableIds(ids) => {
            format!("{select_prefix} where table_id in ({}){order_suffix}", ids_in(ids))
        }
    }
}

/// Go `initStatsHistogramsLite`.
fn init_stats_histograms_lite(
    sctx: &dyn StatsSessionContext,
    cache: &StatsCacheImpl,
    table_ids: &[i64],
) -> Result<(), SqlExecError> {
    let sql = gen_init_stats_histograms_sql(&GenHistSqlOptions::TableIds(table_ids));
    let (rows, _) = exec_rows(sctx, &sql, &[])?;
    init_stats_histograms_4_chunk_lite(cache, &rows);
    // The same table may continue in the next chunk: drain LFU async
    // admission before it is read again.
    cache.wait_for_async_updates();
    Ok(())
}

/// Go `initStatsHistogramsByPaging`: one task on its own pool session.
fn init_stats_histograms_by_paging(
    pool: &dyn StatsSessionPool,
    is: &InfoSchema,
    cache: &StatsCacheImpl,
    task: Task,
    total_memory: u64,
) -> Result<(), SqlExecError> {
    with_session(pool, |sctx| {
        let sql = gen_init_stats_histograms_sql(&GenHistSqlOptions::Paging([task.start_tid, task.end_tid]));
        let (rows, _) = exec_rows(sctx, &sql, &[])?;
        init_stats_histograms_4_chunk(is, cache, &rows, is_full_cache(cache, total_memory));
        cache.wait_for_async_updates();
        Ok(())
    })
}

/// Go `Pool.SPool().WithSession`.
fn with_session(
    pool: &dyn StatsSessionPool,
    mut f: impl FnMut(&dyn StatsSessionContext) -> Result<(), SqlExecError>,
) -> Result<(), SqlExecError> {
    pool.call_with_sctx(&mut |sctx| f(sctx), &[])
        .map_err(|error| SqlExecError::from(error.message))
}

/// Go `loadStrategy`: `maxTidStrategy` for a full load, `tableListStrategy`
/// for listed tables.
enum LoadStrategy {
    MaxTid(i64),
    TableList(Vec<i64>),
}

impl LoadStrategy {
    /// Go `newLoadStrategy`.
    fn new(max_table_id: i64, table_ids: &[i64]) -> Self {
        if table_ids.is_empty() {
            Self::MaxTid(max_table_id)
        } else {
            Self::TableList(table_ids.to_vec())
        }
    }

    /// Go `calculateTotalTaskCnt`.
    fn calculate_total_task_cnt(&self) -> u64 {
        match self {
            Self::MaxTid(max_tid) => {
                let mut total = 1;
                if *max_tid > INIT_STATS_STEP * 2 {
                    total = max_tid / INIT_STATS_STEP;
                }
                total as u64
            }
            Self::TableList(ids) => ids.len() as u64,
        }
    }

    /// Go `generateAndSendTasks`.
    fn generate_and_send_tasks(&self, worker: &RangeWorker) {
        match self {
            Self::MaxTid(max_tid) => {
                let mut tid = 0;
                while tid <= *max_tid {
                    worker.send_task(Task {
                        start_tid: tid,
                        end_tid: tid + INIT_STATS_STEP,
                    });
                    tid += INIT_STATS_STEP;
                }
            }
            Self::TableList(ids) => {
                for id in ids {
                    worker.send_task(Task {
                        start_tid: *id,
                        end_tid: id + 1,
                    });
                }
            }
        }
    }
}

/// Runs Go's `initstats.NewRangeWorker(...)`, `LoadStats`,
/// `generateAndSendTasks`, `Wait`.
fn run_range_worker(
    name: &str,
    process: impl Fn(Task) -> Result<(), SqlExecError> + Send + Sync + 'static,
    concurrency: isize,
    strategy: &LoadStrategy,
) {
    let worker = RangeWorker::new(
        name,
        move |task| process(task),
        concurrency,
        strategy.calculate_total_task_cnt(),
        INIT_STATS_PERCENTAGE_INTERVAL,
    );
    worker.load_stats();
    strategy.generate_and_send_tasks(&worker);
    worker.wait();
}

/// Go `initStatsTopN4Chunk`.
fn init_stats_top_n_4_chunk(
    cache: &StatsCacheImpl,
    rows: &[Vec<Datum>],
    total_memory: u64,
    tables_with_buckets: &HashSet<i64>,
) {
    if is_full_cache(cache, total_memory) {
        return;
    }
    // A table with no buckets has every index fully loaded at bootstrap.
    let mark_all_indexes_full_load_if_no_buckets = |table: &Table| {
        if !tables_with_buckets.contains(&table.hist_coll.physical_id) {
            table.hist_coll.set_all_indices_full_load_for_bootstrap();
        }
    };
    let mut affected = Vec::new();
    let mut table: Option<Table> = None;
    for row in rows {
        let table_id = int(row, 0);
        if table.as_ref().is_none_or(|table| table.hist_coll.physical_id != table_id) {
            if let Some(done) = table.take() {
                mark_all_indexes_full_load_if_no_buckets(&done);
                cache.put(done.hist_coll.physical_id, Arc::new(done));
            }
            let Some(cached) = cache.get(table_id) else {
                continue;
            };
            // Existing index histograms are modified: a deep copy.
            table = Some(cached.copy_as(CopyIntent::AllDataWritable));
        }
        let Some(table) = table.as_ref() else {
            continue;
        };
        let Some(index) = table.hist_coll.get_index(int(row, 1)) else {
            continue;
        };
        {
            let mut idx = index.write().unwrap_or_else(std::sync::PoisonError::into_inner);
            if idx.cmsketch.is_none() && idx.stats_version <= tidb_stats::stats_version::VERSION_1 {
                continue;
            }
            idx.top_n
                .get_or_insert_with(|| TopN::new(32))
                .append(bytes(row, 2), uint(row, 3));
        }
        affected.push(index);
    }
    if let Some(done) = table {
        mark_all_indexes_full_load_if_no_buckets(&done);
        cache.put(done.hist_coll.physical_id, Arc::new(done));
    }
    for index in affected {
        if let Some(top_n) = index
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .top_n
            .as_mut()
        {
            top_n.sort();
        }
    }
}

/// Go `genInitStatsTopNSQLForIndexes` (paging).
fn gen_init_stats_top_n_sql_for_indexes(table_range: [i64; 2]) -> String {
    format!(
        "select /*+ ORDER_INDEX(mysql.stats_top_n,tbl) */ HIGH_PRIORITY table_id, hist_id, value, count from mysql.stats_top_n where is_index = 1 and table_id >= {} and table_id < {} order by table_id",
        table_range[0], table_range[1]
    )
}

/// Go `getTablesWithBucketsInRange`.
fn get_tables_with_buckets_in_range(
    sctx: &dyn StatsSessionContext,
    table_range: [i64; 2],
) -> Result<HashSet<i64>, SqlExecError> {
    // Keep the USE_INDEX(tbl) hint for upgraded clusters.
    let sql = format!(
        "select /*+ USE_INDEX(stats_buckets, tbl) */ HIGH_PRIORITY distinct table_id from mysql.stats_buckets where is_index = 1 and table_id >= {} and table_id < {}",
        table_range[0], table_range[1]
    );
    let (rows, _) = exec_rows(sctx, &sql, &[])?;
    Ok(rows.iter().map(|row| int(row, 0)).collect())
}

/// Go `initStatsTopNByPaging`.
fn init_stats_top_n_by_paging(
    pool: &dyn StatsSessionPool,
    cache: &StatsCacheImpl,
    task: Task,
    total_memory: u64,
) -> Result<(), SqlExecError> {
    with_session(pool, |sctx| {
        let tables_with_buckets = get_tables_with_buckets_in_range(sctx, [task.start_tid, task.end_tid])?;
        let sql = gen_init_stats_top_n_sql_for_indexes([task.start_tid, task.end_tid]);
        let (rows, _) = exec_rows(sctx, &sql, &[])?;
        init_stats_top_n_4_chunk(cache, &rows, total_memory, &tables_with_buckets);
        cache.wait_for_async_updates();
        Ok(())
    })
}

/// Go `initStatsBuckets4Chunk`.
fn init_stats_buckets_4_chunk(cache: &StatsCacheImpl, rows: &[Vec<Datum>]) {
    let mut table: Option<Table> = None;
    for row in rows {
        let (table_id, hist_id) = (int(row, 0), int(row, 1));
        if table.as_ref().is_none_or(|table| table.hist_coll.physical_id != table_id) {
            if let Some(done) = table.take() {
                done.hist_coll.set_all_indices_full_load_for_bootstrap();
                cache.put(done.hist_coll.physical_id, Arc::new(done));
            }
            let Some(cached) = cache.get(table_id) else {
                continue;
            };
            // Existing index histograms are modified: a deep copy.
            table = Some(cached.copy_as(CopyIntent::AllDataWritable));
        }
        let Some(table) = table.as_ref() else {
            continue;
        };
        let Some(index) = table.hist_coll.get_index(hist_id) else {
            continue;
        };
        index
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .histogram
            .append_bucket_with_ndv(
                Datum::Bytes(bytes(row, 4).to_vec()),
                Datum::Bytes(bytes(row, 5).to_vec()),
                int(row, 2),
                int(row, 3),
                int(row, 6),
            );
    }
    if let Some(done) = table {
        done.hist_coll.set_all_indices_full_load_for_bootstrap();
        cache.put(done.hist_coll.physical_id, Arc::new(done));
    }
}

/// Go `genInitStatsBucketsSQLForIndexes` (paging).
fn gen_init_stats_buckets_sql_for_indexes(table_range: [i64; 2]) -> String {
    // Keep the ORDER_INDEX(tbl) hint for upgraded clusters.
    format!(
        "select /*+ ORDER_INDEX(mysql.stats_buckets,tbl) */ HIGH_PRIORITY table_id, hist_id, count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where is_index=1 and table_id >= {} and table_id < {} order by table_id",
        table_range[0], table_range[1]
    )
}

/// Go `initStatsBucketsByPaging`.
fn init_stats_buckets_by_paging(
    pool: &dyn StatsSessionPool,
    cache: &StatsCacheImpl,
    task: Task,
) -> Result<(), SqlExecError> {
    with_session(pool, |sctx| {
        let sql = gen_init_stats_buckets_sql_for_indexes([task.start_tid, task.end_tid]);
        let (rows, _) = exec_rows(sctx, &sql, &[])?;
        init_stats_buckets_4_chunk(cache, &rows);
        cache.wait_for_async_updates();
        Ok(())
    })
}

/// Go `initStatsLiteWithSession`.
fn init_stats_lite_with_session(
    sctx: &dyn StatsSessionContext,
    is: &InfoSchema,
    cache: &StatsCacheImpl,
    table_ids: &[i64],
) -> Result<(), SqlExecError> {
    let start = std::time::Instant::now();
    init_stats_meta(sctx, is, cache, table_ids)?;
    // initStatsMeta adds tables without an internal wait; the histogram
    // load reads them at once.
    cache.wait_for_async_updates();
    log_duration("Complete loading the stats meta in the lite mode", start);
    let start = std::time::Instant::now();
    init_stats_histograms_lite(sctx, cache, table_ids)?;
    log_duration("Complete loading the histogram in the lite mode", start);
    Ok(())
}

/// Go `initStatsWithSession`.
fn init_stats_with_session(
    sctx: &dyn StatsSessionContext,
    pool: &Arc<dyn StatsSessionPool>,
    is: &Arc<InfoSchema>,
    cache: &Arc<StatsCacheImpl>,
    table_ids: &[i64],
) -> Result<(), SqlExecError> {
    INIT_STATS_PERCENTAGE.store(0.0);
    let _done = scopeguard(|| INIT_STATS_PERCENTAGE.store(100.0));
    let total_memory = tidb_memory::mem_total().map_err(|error| Box::new(error) as SqlExecError)?;
    let start = std::time::Instant::now();
    let max_table_id = init_stats_meta(sctx, is, cache, table_ids)?;
    // initStatsMeta adds tables without an internal wait; the histogram
    // load reads them at once.
    cache.wait_for_async_updates();
    log_duration("Complete loading the stats meta", start);
    INIT_STATS_PERCENTAGE.store(INIT_STATS_PERCENTAGE_INTERVAL);

    let concurrency = get_concurrency();
    let strategy = LoadStrategy::new(max_table_id, table_ids);
    let start = std::time::Instant::now();
    // Go `initStatsHistogramsConcurrently`.
    {
        let (pool, is, cache) = (Arc::clone(pool), Arc::clone(is), Arc::clone(cache));
        run_range_worker(
            "histogram",
            move |task| init_stats_histograms_by_paging(pool.as_ref(), &is, &cache, task, total_memory),
            concurrency,
            &strategy,
        );
    }
    log_duration("Complete loading the histogram", start);

    let start = std::time::Instant::now();
    // Go `initStatsTopNConcurrently`.
    if !is_full_cache(cache, total_memory) {
        let (pool, cache) = (Arc::clone(pool), Arc::clone(cache));
        run_range_worker(
            "TopN",
            move |task| {
                if is_full_cache(&cache, total_memory) {
                    return Ok(());
                }
                init_stats_top_n_by_paging(pool.as_ref(), &cache, task, total_memory)
            },
            concurrency,
            &strategy,
        );
    }
    INIT_STATS_PERCENTAGE.store(INIT_STATS_PERCENTAGE_INTERVAL * 2.0);
    log_duration("Complete loading the topn", start);

    let start = std::time::Instant::now();
    // Go `initStatsBucketsAndCalcPreScalar` / `initStatsBucketsConcurrently`.
    if !is_full_cache(cache, total_memory) {
        {
            let (pool, cache) = (Arc::clone(pool), Arc::clone(cache));
            run_range_worker(
                "bucket",
                move |task| {
                    if is_full_cache(&cache, total_memory) {
                        return Ok(());
                    }
                    init_stats_buckets_by_paging(pool.as_ref(), &cache, task)
                },
                concurrency,
                &strategy,
            );
        }
        for table in cache.values() {
            table.hist_coll.calculate_pre_scalar_counts();
            cache.put(table.hist_coll.physical_id, table);
        }
    }
    // CalcPreScalar writes tables back; drain before publishing the cache.
    cache.wait_for_async_updates();
    log_duration("Complete loading the bucket", start);
    Ok(())
}

fn log_duration(message: &str, start: std::time::Instant) {
    tidb_stats_handle_logutil::stats_logger().info(
        message,
        &[tidb_log::Field::new(
            "duration",
            tidb_log::Value::Duration(i64::try_from(start.elapsed().as_nanos()).unwrap_or(i64::MAX)),
        )],
    );
}

/// Go `isFullCache`.
fn is_full_cache(cache: &StatsCacheImpl, total: u64) -> bool {
    let mem_quota = tidb_vardef::STATS_CACHE_MEM_QUOTA.load(Ordering::SeqCst);
    let consumed = cache.mem_consumed();
    consumed.max(0) as u64 >= total / 4 || (consumed >= mem_quota && mem_quota != 0)
}

struct ScopeGuard<F: FnMut()>(F);

impl<F: FnMut()> Drop for ScopeGuard<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

/// Go `defer f()`.
fn scopeguard<F: FnMut()>(f: F) -> ScopeGuard<F> {
    ScopeGuard(f)
}
