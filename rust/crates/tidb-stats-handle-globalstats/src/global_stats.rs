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

//! Go `pkg/statistics/handle/globalstats/global_stats.go` and
//! `global_stats_merger.go`: the merge of partition-level statistics into
//! the global statistics of a partitioned table, which ANALYZE runs in
//! dynamic prune mode.

use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::time::Duration;

use chrono::TimeZone;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_model::TableInfo;
use tidb_sqlexec::SqlExecError;
use tidb_sqlkiller::SqlKiller;
use tidb_stats::histogram::Histogram;
use tidb_stats::{fm_sketch_ndv, merge_fm_sketch, CmsSketch, FmSketch, StatsLoadedStatus, TopN};
use tidb_stats_handle_storage::cluster_stats_load::ClusterStatsItem;
use tidb_stats_handle_storage::read;
use tidb_stats_handle_storage::stats_read_writer::{self, StatsSessionPool};
use tidb_stats_handle_util::StatsSessionContext;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use crate::{is_local_unique, merge_partition_histogram_topn, unique_global_ndv, GlobalStatsMergeError};

/// Go `statstypes.GlobalStatsInfo`: what one global merge covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalStatsInfo {
    /// Go `IsIndex == 1`.
    pub is_index: bool,
    /// Go `HistIDs`: the columns, or the one index; empty means every column
    /// that is not virtual generated.
    pub hist_ids: Vec<i64>,
    /// Go `StatsVersion`.
    pub stats_version: i64,
}

/// Go `GlobalStats`: the merged statistics. Columns have one entry per
/// merged column, an index has one.
#[derive(Debug, Default)]
pub struct GlobalStats {
    /// Go `Hg`.
    pub hg: Vec<Option<Histogram>>,
    /// Go `Cms`.
    pub cms: Vec<Option<CmsSketch>>,
    /// Go `TopN`.
    pub topn: Vec<Option<TopN>>,
    /// Go `MissingPartitionStats`.
    pub missing_partition_stats: Vec<String>,
    /// Go `Num`.
    pub num: usize,
    /// Go `Count`.
    pub count: i64,
    /// Go `ModifyCount`.
    pub modify_count: i64,
}

/// Go `newGlobalStats`.
fn new_global_stats(hist_count: usize) -> GlobalStats {
    GlobalStats {
        hg: vec![None; hist_count],
        cms: vec![None; hist_count],
        topn: vec![None; hist_count],
        missing_partition_stats: Vec::new(),
        num: hist_count,
        count: 0,
        modify_count: 0,
    }
}

/// What Go's merge reads from the caller's session and its `opts`.
pub struct MergeSession<'a, TZ> {
    /// Go `SessionVars.SkipMissingPartitionStats`.
    pub skip_missing_partition_stats: bool,
    /// Go `StmtCtx.TimeZone()`.
    pub timezone: Option<&'a TZ>,
    /// Go `&sctx.GetSessionVars().SQLKiller`, the caller's.
    pub killer: &'a SqlKiller,
    /// Go `opts[ast.AnalyzeOptNumTopN]`.
    pub num_topn: u32,
    /// Go `opts[ast.AnalyzeOptNumBuckets]`.
    pub num_buckets: usize,
    /// Go `tidb_merge_partition_stats_concurrency`, the TopN merge workers.
    pub partition_merge_concurrency: usize,
}

/// A global merge's failure.
#[derive(Debug)]
pub enum GlobalStatsError {
    /// A statistics-session statement failed (`storage.*FromStorage`).
    Exec(SqlExecError),
    /// The statistics session pool or a save failed.
    Sql(SqlQueryError),
    /// Go `types.ErrPartitionColumnStatsMissing`, which ANALYZE reports as a
    /// warning.
    Mysql(tidb_terror::MysqlError),
    /// The TopN and histogram merge failed.
    Merge(GlobalStatsMergeError),
    /// Go `errors.New(...)`: a worker panic or an inconsistency.
    Other(String),
}

impl std::fmt::Display for GlobalStatsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exec(error) => error.fmt(formatter),
            Self::Sql(error) => formatter.write_str(&error.message),
            Self::Mysql(error) => formatter.write_str(&error.message),
            Self::Merge(error) => error.fmt(formatter),
            Self::Other(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for GlobalStatsError {}

/// Go `MergePartitionStats2GlobalStats`.
///
/// # Errors
///
/// The merge's failure; a missing column or index of a partition is
/// [`GlobalStatsError::Mysql`] unless `skip_missing_partition_stats`.
pub fn merge_partition_stats_2_global_stats<TZ: TimeZone + Sync>(
    session: &MergeSession<'_, TZ>,
    stats_pool: &dyn StatsSessionPool,
    global_table_info: &TableInfo,
    is_index: bool,
    hist_ids: &[i64],
) -> Result<GlobalStats, GlobalStatsError> {
    let mut worker = GlobalStatsMerger::new(global_table_info, hist_ids);
    worker.merge_partition_stats_2_global_stats(session, stats_pool, is_index)?;
    Ok(worker.result())
}

/// Go `MergePartitionStats2GlobalStatsByTableID`, given the table Go looks up
/// by `tableID` (`statsHandle.TableInfoByID(is, tableID)`).
///
/// # Errors
///
/// See [`merge_partition_stats_2_global_stats`].
pub fn merge_partition_stats_2_global_stats_by_table_id<TZ: TimeZone + Sync>(
    session: &MergeSession<'_, TZ>,
    stats_pool: &dyn StatsSessionPool,
    global_table_info: &TableInfo,
    is_index: bool,
    hist_ids: &[i64],
) -> Result<GlobalStats, GlobalStatsError> {
    let global_stats =
        merge_partition_stats_2_global_stats(session, stats_pool, global_table_info, is_index, hist_ids)?;
    if !global_stats.missing_partition_stats.is_empty() {
        let item = if is_index {
            match hist_ids.first() {
                Some(id) => format!("index {}", find_index_name_by_id(global_table_info, *id)),
                None => "index".to_owned(),
            }
        } else {
            "columns".to_owned()
        };
        tidb_logutil::bg_logger().warn(
            "missing partition stats when merging global stats",
            &[
                tidb_log::Field::new("table", tidb_log::Value::Str(global_table_info.name.lowercase().to_owned())),
                tidb_log::Field::new("item", tidb_log::Value::Str(item)),
                tidb_log::Field::new(
                    "missing",
                    tidb_log::Value::Str(format!("{:?}", global_stats.missing_partition_stats)),
                ),
            ],
        );
    }
    Ok(global_stats)
}

/// Go `WriteGlobalStatsToStorage`: saves each merged histogram; a failed
/// save is logged and the last item's result is returned, as Go's loop
/// leaves `err`.
///
/// # Errors
///
/// The last save's failure.
pub fn write_global_stats_to_storage(
    stats_pool: &dyn StatsSessionPool,
    stats_lease: Duration,
    global_stats: &GlobalStats,
    info: &GlobalStatsInfo,
    gid: i64,
) -> Result<(), SqlQueryError> {
    let mut result = Ok(());
    for i in 0..global_stats.num {
        let Some(hg) = global_stats.hg[i].clone() else {
            // All partitions have no stats so global stats are not created.
            continue;
        };
        let hist_id = hg.id;
        // fms for global stats doesn't need to dump to kv.
        let item = ClusterStatsItem {
            id: hist_id,
            is_index: info.is_index,
            stats_ver: info.stats_version,
            flag: 0,
            load_status: StatsLoadedStatus::full_load(),
            histogram: hg,
            topn: global_stats.topn[i].clone(),
            cms: global_stats.cms[i].clone(),
            fm_sketch: None,
        };
        result = stats_read_writer::save_col_or_idx_stats_to_storage(
            stats_pool,
            stats_lease,
            gid,
            global_stats.count,
            global_stats.modify_count,
            &item,
            info.stats_version,
            true,
        )
        .map(|_| ());
        if let Err(error) = &result {
            tidb_stats_handle_logutil::stats_logger().warn(
                "save global-level stats to storage failed",
                &[
                    tidb_log::Field::new("histID", tidb_log::Value::I64(hist_id)),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.message.clone())),
                    tidb_log::Field::new("tableID", tidb_log::Value::I64(gid)),
                ],
            );
        }
    }
    result
}

/// Go `TableInfo.FindIndexNameByID`.
fn find_index_name_by_id(table: &TableInfo, id: i64) -> String {
    table
        .indices
        .iter_deref()
        .find_map(|index| {
            let index = index.read();
            (index.id == id).then(|| index.name.lowercase().to_owned())
        })
        .unwrap_or_default()
}

/// Go `TableInfo.FindColumnNameByID`.
fn find_column_name_by_id(table: &TableInfo, id: i64) -> String {
    table
        .cols()
        .iter_deref()
        .find_map(|column| {
            let column = column.read();
            (column.id == id).then(|| column.name.lowercase().to_owned())
        })
        .unwrap_or_default()
}

/// Go `mergeItem`.
type MergeItem<T> = (usize, T);

/// Go `statsWrapper`: one column or index in every partition.
struct StatsWrapper {
    all_hg: Vec<Histogram>,
    all_topn: Vec<Option<TopN>>,
}

/// Go `globalStatsMerger`: an IO worker loads each partition's FM sketches,
/// CM sketches, histograms and TopNs, and a CPU worker merges them.
struct GlobalStatsMerger<'a> {
    global_table_info: &'a TableInfo,
    global_stats: GlobalStats,
    /// Go `PartitionDefinition`: each partition's lower-case name.
    partition_definition: HashMap<i64, String>,
    /// Go `skipPartition`, keyed by `(histID, partitionID)`.
    skip_partition: HashSet<(i64, i64)>,
    hist_ids: Vec<i64>,
    global_stats_ndv: Vec<i64>,
    partition_ids: Vec<i64>,
    skip_missing_partition_stats: bool,
}

impl<'a> GlobalStatsMerger<'a> {
    /// Go `newGlobalStatsMerger`.
    fn new(global_table_info: &'a TableInfo, hist_ids: &[i64]) -> Self {
        Self {
            global_table_info,
            global_stats: GlobalStats::default(),
            partition_definition: HashMap::new(),
            skip_partition: HashSet::new(),
            hist_ids: hist_ids.to_vec(),
            global_stats_ndv: Vec::new(),
            partition_ids: Vec::new(),
            skip_missing_partition_stats: false,
        }
    }

    /// Go `prepare`: the partitions' counts and which partition items are
    /// missing. Go reads each partition's `TableInfo` through
    /// `TableInfoByID`, which is the global table's.
    fn prepare(&mut self, sctx: &dyn StatsSessionContext, is_index: bool) -> Result<(), GlobalStatsError> {
        if self.hist_ids.is_empty() {
            for column in self.global_table_info.cols().iter_deref() {
                let column = column.read();
                // The virtual generated column stats can not be merged to the
                // global stats.
                if column.is_virtual_generated() {
                    continue;
                }
                self.hist_ids.push(column.id);
            }
        }
        self.global_stats = new_global_stats(self.hist_ids.len());
        self.global_stats_ndv = Vec::with_capacity(self.global_stats.num);
        let definitions = self
            .global_table_info
            .partition
            .as_ref()
            .map(|partition| partition.read().definitions.snapshot())
            .unwrap_or_default();
        for definition in definitions {
            let partition_id = definition.id;
            self.partition_ids.push(partition_id);
            self.partition_definition
                .insert(partition_id, definition.name.lowercase().to_owned());
            let (realtime_count, modify_count, is_null) =
                read::stats_meta_count_and_modify_count(sctx, partition_id, false).map_err(GlobalStatsError::Exec)?;
            if !is_null {
                // In a partition, we will only update globalStats.Count once.
                self.global_stats.count = self.global_stats.count.wrapping_add(realtime_count);
                self.global_stats.modify_count = self.global_stats.modify_count.wrapping_add(modify_count);
            }
            // Go `skipPartition`: `ErrPartitionStatsMissing` without any
            // statistics of the kind.
            if !skip_partition(sctx, partition_id, is_index)? {
                self.deal_err_partition_stats_missing(partition_id);
                continue;
            }
            for idx in 0..self.hist_ids.len() {
                // Go `skipColumnPartition`: `ErrPartitionColumnStatsMissing`.
                if !skip_column_partition(sctx, partition_id, is_index, self.hist_ids[idx])? {
                    self.deal_err_partition_column_stats_missing(is_index, partition_id, idx)?;
                }
            }
        }
        Ok(())
    }

    /// Go `dealErrPartitionStatsMissing`.
    fn deal_err_partition_stats_missing(&mut self, partition_id: i64) {
        let missing_part = format!("partition `{}`", self.partition_definition[&partition_id]);
        self.global_stats.missing_partition_stats.push(missing_part);
        for hist_id in &self.hist_ids {
            self.skip_partition.insert((*hist_id, partition_id));
        }
    }

    /// Go `dealErrPartitionColumnStatsMissing`.
    fn deal_err_partition_column_stats_missing(
        &mut self,
        is_index: bool,
        partition_id: i64,
        idx: usize,
    ) -> Result<(), GlobalStatsError> {
        let partition = &self.partition_definition[&partition_id];
        let missing_part = if is_index {
            format!(
                "partition `{partition}` index `{}`",
                find_index_name_by_id(self.global_table_info, self.hist_ids[idx])
            )
        } else {
            format!(
                "partition `{partition}` column `{}`",
                find_column_name_by_id(self.global_table_info, self.hist_ids[idx])
            )
        };
        if !self.skip_missing_partition_stats {
            return Err(GlobalStatsError::Mysql(tidb_terror::MysqlError::from_terror(
                &tidb_datatype::types_errors::ERR_PARTITION_COLUMN_STATS_MISSING.gen_with_stack_by_args(&[format!(
                    "table `{}` {missing_part}",
                    self.global_table_info.name.lowercase()
                )
                .into()]),
            )));
        }
        self.global_stats.missing_partition_stats.push(missing_part);
        self.skip_partition.insert((self.hist_ids[idx], partition_id));
        Ok(())
    }

    fn skipped(&self, hist_idx: usize, partition_id: i64) -> bool {
        self.skip_partition.contains(&(self.hist_ids[hist_idx], partition_id))
    }

    /// Go `ioWorker`. A closed receiver is Go's `cpuWorkerExitChan`: the IO
    /// worker stops without an error of its own.
    fn io_worker(
        &self,
        sctx: &dyn StatsSessionContext,
        is_index: bool,
        fmsketch: SyncSender<MergeItem<Option<FmSketch>>>,
        cmsketch: SyncSender<MergeItem<Option<CmsSketch>>>,
        histogram_and_topn: SyncSender<MergeItem<StatsWrapper>>,
        io_worker_exit_when_err: &AtomicBool,
    ) -> Result<(), GlobalStatsError> {
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), GlobalStatsError> {
            self.load_fmsketch(sctx, is_index, fmsketch)?;
            self.load_cmsketch(sctx, is_index, cmsketch)?;
            self.load_histogram_and_topn(sctx, is_index, histogram_and_topn)
        }))
        .unwrap_or_else(|panic| Err(GlobalStatsError::Other(panic_message(&*panic))));
        if result.is_err() {
            io_worker_exit_when_err.store(true, Ordering::SeqCst);
        }
        result
    }

    /// Go `loadFmsketch`.
    fn load_fmsketch(
        &self,
        sctx: &dyn StatsSessionContext,
        is_index: bool,
        fmsketch: SyncSender<MergeItem<Option<FmSketch>>>,
    ) -> Result<(), GlobalStatsError> {
        for i in 0..self.global_stats.num {
            for partition_id in &self.partition_ids {
                if self.skipped(i, *partition_id) {
                    continue;
                }
                let sketch = read::fm_sketch_from_storage(sctx, *partition_id, is_index, self.hist_ids[i])
                    .map_err(GlobalStatsError::Exec)?;
                if fmsketch.send((i, sketch)).is_err() {
                    tidb_stats_handle_logutil::stats_logger().warn("ioWorker detects CPUWorker has exited", &[]);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Go `loadCMsketch`.
    fn load_cmsketch(
        &self,
        sctx: &dyn StatsSessionContext,
        is_index: bool,
        cmsketch: SyncSender<MergeItem<Option<CmsSketch>>>,
    ) -> Result<(), GlobalStatsError> {
        for i in 0..self.global_stats.num {
            for partition_id in &self.partition_ids {
                if self.skipped(i, *partition_id) {
                    continue;
                }
                let sketch = read::cm_sketch_from_storage(sctx, *partition_id, is_index, self.hist_ids[i])
                    .map_err(GlobalStatsError::Exec)?;
                if cmsketch.send((i, sketch)).is_err() {
                    tidb_stats_handle_logutil::stats_logger().warn("ioWorker detects CPUWorker has exited", &[]);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Go `loadHistogramAndTopN`.
    fn load_histogram_and_topn(
        &self,
        sctx: &dyn StatsSessionContext,
        is_index: bool,
        histogram_and_topn: SyncSender<MergeItem<StatsWrapper>>,
    ) -> Result<(), GlobalStatsError> {
        for i in 0..self.global_stats.num {
            let mut hists = Vec::with_capacity(self.partition_ids.len());
            let mut topn = Vec::with_capacity(self.partition_ids.len());
            for partition_id in &self.partition_ids {
                if self.skipped(i, *partition_id) {
                    continue;
                }
                let hist = read::load_histogram(sctx, *partition_id, is_index, self.hist_ids[i], self.global_table_info)
                    .map_err(GlobalStatsError::Exec)?
                    .ok_or_else(|| {
                        GlobalStatsError::Other(format!(
                            "histogram {} disappeared from partition {partition_id}",
                            self.hist_ids[i]
                        ))
                    })?;
                let t = read::top_n_from_storage(sctx, *partition_id, is_index, self.hist_ids[i])
                    .map_err(GlobalStatsError::Exec)?;
                hists.push(hist);
                topn.push(t);
            }
            // This only happens when there are no records for the histogram
            // and TopN in the system tables.
            if hists.is_empty() && topn.is_empty() {
                continue;
            }
            if histogram_and_topn
                .send((i, StatsWrapper { all_hg: hists, all_topn: topn }))
                .is_err()
            {
                tidb_stats_handle_logutil::stats_logger().warn("ioWorker detects CPUWorker has exited", &[]);
                return Ok(());
            }
        }
        Ok(())
    }

    /// Go `cpuWorker`: Go closes `cpuWorkerExitChan` on return, which is the
    /// receivers dropping here.
    fn cpu_worker<TZ: TimeZone + Sync>(
        &mut self,
        session: &MergeSession<'_, TZ>,
        is_index: bool,
        fmsketch: Receiver<MergeItem<Option<FmSketch>>>,
        cmsketch: Receiver<MergeItem<Option<CmsSketch>>>,
        histogram_and_topn: Receiver<MergeItem<StatsWrapper>>,
        io_worker_exit_when_err: &AtomicBool,
    ) -> Result<(), GlobalStatsError> {
        catch_unwind(AssertUnwindSafe(|| -> Result<(), GlobalStatsError> {
            let fms = self.deal_fm_sketch(&fmsketch);
            if io_worker_exit_when_err.load(Ordering::SeqCst) {
                return Ok(());
            }
            for sketch in &fms {
                // Update the global NDV.
                self.global_stats_ndv
                    .push(fm_sketch_ndv(sketch.as_ref()).min(self.global_stats.count));
            }
            drop(fms);
            self.deal_cm_sketch(&cmsketch)?;
            self.deal_histogram_and_topn(session, is_index, &histogram_and_topn)
        }))
        .unwrap_or_else(|panic| Err(GlobalStatsError::Other(panic_message(&*panic))))
    }

    /// Go `dealFMSketch`.
    fn deal_fm_sketch(&self, fmsketch: &Receiver<MergeItem<Option<FmSketch>>>) -> Vec<Option<FmSketch>> {
        let mut fms: Vec<Option<FmSketch>> = vec![None; self.global_stats.num];
        while let Ok((idx, item)) = fmsketch.recv() {
            let Some(item) = item else { continue };
            match &mut fms[idx] {
                Some(destination) => merge_fm_sketch(Some(destination), Some(&item)),
                slot @ None => *slot = Some(item),
            }
        }
        fms
    }

    /// Go `dealCMSketch`.
    /// A closed channel ends it, as Go's closed `cmsketch` or
    /// `ioWorkerExitWhenErrChan` does: the IO worker drops its sender either
    /// way.
    fn deal_cm_sketch(&mut self, cmsketch: &Receiver<MergeItem<Option<CmsSketch>>>) -> Result<(), GlobalStatsError> {
        while let Ok((idx, item)) = cmsketch.recv() {
            let Some(item) = item else { continue };
            match &mut self.global_stats.cms[idx] {
                Some(destination) => destination
                    .merge(&item)
                    .map_err(|error| GlobalStatsError::Other(error.to_string()))?,
                slot @ None => *slot = Some(item),
            }
        }
        Ok(())
    }

    /// Go `dealHistogramAndTopN`.
    fn deal_histogram_and_topn<TZ: TimeZone + Sync>(
        &mut self,
        session: &MergeSession<'_, TZ>,
        is_index: bool,
        histogram_and_topn: &Receiver<MergeItem<StatsWrapper>>,
    ) -> Result<(), GlobalStatsError> {
        while let Ok((idx, wrapper)) = histogram_and_topn.recv() {
            let hist_id = self.hist_ids[idx];
            let ndv = if is_local_unique(self.global_table_info, is_index, hist_id) {
                unique_global_ndv(&wrapper.all_hg, self.global_stats.count)
            } else {
                self.global_stats_ndv[idx]
            };
            let field_type = if is_index {
                FieldType::new(FieldTypeCode::Blob)
            } else {
                self.global_table_info
                    .cols()
                    .iter_deref()
                    .find_map(|column| {
                        let column = column.read();
                        (column.id == hist_id).then(|| column.field_type.clone())
                    })
                    .ok_or_else(|| GlobalStatsError::Other(format!("unknown analyzed column ID {hist_id}")))?
            };
            let merged = merge_partition_histogram_topn(
                session.timezone,
                2,
                session.num_topn,
                session.num_buckets,
                &field_type,
                is_index,
                session.partition_merge_concurrency,
                ndv,
                None,
                wrapper.all_hg,
                wrapper.all_topn,
                session.killer,
            )
            .map_err(GlobalStatsError::Merge)?;
            self.global_stats.topn[idx] = merged.topn;
            self.global_stats.hg[idx] = merged.histogram;
        }
        Ok(())
    }

    /// Go `Result`.
    fn result(self) -> GlobalStats {
        self.global_stats
    }

    /// Go `MergePartitionStats2GlobalStats`: `prepare`, then the IO worker on
    /// the statistics session and the CPU worker beside it.
    fn merge_partition_stats_2_global_stats<TZ: TimeZone + Sync>(
        &mut self,
        session: &MergeSession<'_, TZ>,
        stats_pool: &dyn StatsSessionPool,
        is_index: bool,
    ) -> Result<(), GlobalStatsError> {
        self.skip_missing_partition_stats = session.skip_missing_partition_stats;
        let mut outcome: Result<(), GlobalStatsError> = Ok(());
        stats_pool
            .call_with_sctx(
                &mut |sctx| {
                    outcome = match self.prepare(sctx, is_index) {
                        // A statement error leaves through the pool, as Go's
                        // callback returns it.
                        Err(GlobalStatsError::Exec(error)) => return Err(error),
                        Err(error) => Err(error),
                        Ok(()) => self.run_workers(sctx, session, is_index),
                    };
                    Ok(())
                },
                &[],
            )
            .map_err(GlobalStatsError::Sql)?;
        outcome
    }

    fn run_workers<TZ: TimeZone + Sync>(
        &mut self,
        sctx: &dyn StatsSessionContext,
        session: &MergeSession<'_, TZ>,
        is_index: bool,
    ) -> Result<(), GlobalStatsError> {
        // These capacities are Go's channel sizes.
        let (fm_sender, fm_receiver) = sync_channel(5);
        let (cms_sender, cms_receiver) = sync_channel(5);
        let (hist_sender, hist_receiver) = sync_channel(0);
        let io_worker_exit_when_err = AtomicBool::new(false);
        let mut cpu_merger = GlobalStatsMerger {
            global_table_info: self.global_table_info,
            global_stats: std::mem::take(&mut self.global_stats),
            partition_definition: HashMap::new(),
            skip_partition: HashSet::new(),
            hist_ids: self.hist_ids.clone(),
            global_stats_ndv: Vec::new(),
            partition_ids: Vec::new(),
            skip_missing_partition_stats: self.skip_missing_partition_stats,
        };
        // The IO worker reads the skip set and counts; the CPU worker owns
        // the result it builds.
        let io_view = GlobalStatsMerger {
            global_table_info: self.global_table_info,
            global_stats: GlobalStats {
                num: cpu_merger.global_stats.num,
                ..GlobalStats::default()
            },
            partition_definition: HashMap::new(),
            skip_partition: std::mem::take(&mut self.skip_partition),
            hist_ids: self.hist_ids.clone(),
            global_stats_ndv: Vec::new(),
            partition_ids: self.partition_ids.clone(),
            skip_missing_partition_stats: self.skip_missing_partition_stats,
        };
        let (io_result, cpu_result) = std::thread::scope(|scope| {
            let flag = &io_worker_exit_when_err;
            let merger = &mut cpu_merger;
            let cpu = scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                merger.cpu_worker(session, is_index, fm_receiver, cms_receiver, hist_receiver, flag)
            }));
            let io_result = io_view.io_worker(sctx, is_index, fm_sender, cms_sender, hist_sender, flag);
            let cpu_result = cpu
                .join()
                .unwrap_or_else(|panic| Err(GlobalStatsError::Other(panic_message(&*panic))));
            (io_result, cpu_result)
        });
        self.skip_partition = io_view.skip_partition;
        self.global_stats = cpu_merger.global_stats;
        self.global_stats_ndv = cpu_merger.global_stats_ndv;
        match (io_result, cpu_result) {
            (Ok(()), result) => result,
            (Err(io), Ok(())) => Err(io),
            // Go `stderrors.Join(err, err1)`.
            (Err(io), Err(cpu)) => Err(GlobalStatsError::Other(format!("{io}\n{cpu}"))),
        }
    }
}

/// Go `skipPartition`: whether the partition has statistics of the kind
/// (`false` is Go's `ErrPartitionStatsMissing`).
fn skip_partition(sctx: &dyn StatsSessionContext, partition_id: i64, is_index: bool) -> Result<bool, GlobalStatsError> {
    read::check_skip_partition(sctx, partition_id, is_index).map_err(GlobalStatsError::Exec)
}

/// Go `skipColumnPartition`: whether the partition has the item's statistics
/// (`false` is Go's `ErrPartitionColumnStatsMissing`).
fn skip_column_partition(
    sctx: &dyn StatsSessionContext,
    partition_id: i64,
    is_index: bool,
    hist_id: i64,
) -> Result<bool, GlobalStatsError> {
    read::check_skip_column_partiion(sctx, partition_id, is_index, hist_id).map_err(GlobalStatsError::Exec)
}

/// Go `errors.New(fmt.Sprint(r))` for a recovered panic.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_owned())
}
