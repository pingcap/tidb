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

//! Go `IndexLookUpExecutor` (`pkg/executor/distsql.go`) over the `kv.TiKV`
//! store: an index worker reads the index plans' handles in batches and, for
//! each batch, hands a `lookupTableTask` to a table worker -- a table read
//! over the batch's handles -- and to the result channel, which `Next` drains
//! in index order.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;
use tidb_planner_core_operator_physicalop::{PhysicalIndexLookUpReader, PhysicalPlan, PushedDownLimit};
use tidb_txnkv::Handle;

/// Go `size.SizeOfInterface` (a `kv.Handle`), `unsafe.Sizeof(chunk.Row{})`
/// and `size.SizeOfInt`.
const SIZEOF_INTERFACE: usize = 16;
const SIZEOF_CHUNK_ROW: usize = 16;
const SIZEOF_INT: usize = 8;

/// Go `cap` of a slice of `cap` elements after `append` makes its length
/// `len`.
fn go_append_cap(len: usize, cap: usize, element_size: usize) -> usize {
    if len <= cap {
        return cap;
    }
    tidb_goruntime::go_runtime::go_64_next_slice_capacity_for_element(
        len,
        cap,
        element_size,
        tidb_goruntime::go_runtime::GoSliceElementLayout::PointerBearing,
    )
}

/// Go `LookupTableTaskChannelSize`.
const LOOKUP_TABLE_TASK_CHANNEL_SIZE: usize = 50;

/// Go `getHandleType`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GetHandleType {
    FromIndex,
    FromTable,
}

/// What both workers read of the executor (Go reads its fields through `w.idxLookup`).
struct Shared {
    ctx: crate::StmtContext,
    client: Arc<dyn tidb_txnkv::Client>,
    /// Go `tblPlans`.
    table_plans: Vec<PhysicalPlan>,
    /// Go `getPhysicalPlanIDs(e.tblPlans)`.
    table_plan_ids: Vec<isize>,
    /// Go `e.table`'s metadata.
    table_info: tidb_model::TableInfo,
    /// Go `primaryKeyIndex`, as the key codec reads it.
    primary_key_index: Option<tidb_tablecodec::IndexInfo>,
    /// The table as the key codec reads it.
    codec_table_info: tidb_tablecodec::TableInfo,
    /// Go `index.Name.O`, which a lookup inconsistency names.
    index_name: String,
    /// Go `handleCols`.
    handle_cols: Vec<Column>,
    /// Go `handleIdx`: the handle columns' offsets in a table row.
    handle_idx: Vec<usize>,
    /// Go `needPartitionHandle(getHandleFromIndex)` / `(getHandleFromTable)`.
    partition_handle_from_index: bool,
    partition_handle_from_table: bool,
    /// Go `keepOrder`.
    keep_order: bool,
    /// Go `startTS`.
    start_ts: u64,
    /// Go `columns`, which each task's table reader decodes.
    columns: Vec<tidb_model::GoShared<tidb_model::ColumnInfo>>,
    /// Go `getPhysicalTableID(e.table)`.
    physical_table_id: i64,
    /// Go `avgRowSize`.
    avg_row_size: f64,
    /// Go `e.finished`.
    finished: AtomicBool,
    /// Go `idxPlans[0].ID()` and its cop tasks' count and rows, which the
    /// index usage report reads.
    index_scan_plan_id: isize,
    index_cop_count_and_rows: Mutex<(u64, u64)>,
    /// Go `memTracker` (`NewTracker(e.ID(), -1)`), which each table task's
    /// tracker hangs under.
    mem_tracker: Arc<tidb_memory::Tracker>,
    /// The table reader a task builds: its shape, which `TryNewCacheChunk`
    /// allocates by, and `exec.Next`'s RU v2 recording of it.
    table_reader_meta: ExecutorMeta,
    table_reader_ruv2: Option<tidb_executor_internal_exec::ruv2::Ruv2NextCache>,
    /// Go `e.stats` (`initRuntimeStats`), when the statement collects them.
    stats: Option<crate::index_lookup_stats::IndexLookUpStatsHandle>,
}

/// Go `lookupTableTask` as the table worker answers it: the rows of the
/// task's handles, in index order when the read keeps order.
struct TaskRows {
    chunks: Vec<Chunk>,
    /// `(chunk, row)` of each row, in the order `Next` returns them.
    order: Vec<(usize, usize)>,
    /// Go `memUsage` / `memTracker`: what the task charged, released when
    /// the next task replaces it.
    mem_usage: i64,
    mem_tracker: Arc<tidb_memory::Tracker>,
}

/// Go `lookupTableTask` as the index worker hands it to a table worker.
struct LookupTask {
    /// Go `id`.
    id: i64,
    handles: Vec<Handle>,
    /// Go `cap(task.handles)`.
    handles_cap: usize,
    /// The partition the task's handles belong to, when the index worker
    /// reads one partition a key-range group.
    partition: Option<i64>,
    done: SyncSender<Result<TaskRows, ExecError>>,
}

/// One task on the result channel: Go `lookupTableTask.doneCh`.
type TaskDone = Receiver<Result<TaskRows, ExecError>>;

/// Go `IndexLookUpExecutor`.
pub struct IndexLookUpExecutor {
    meta: ExecutorMeta,
    shared: Arc<Shared>,
    /// Go `idxPlans`.
    index_plans: Vec<PhysicalPlan>,
    /// Go `idxPlanUnNatureOrders` (`FlattenTreePushDownPlan`).
    index_plans_un_nature_orders: std::collections::BTreeMap<usize, usize>,
    /// Go `indexLookUpPushDown`: the index request looks the rows up in
    /// TiKV itself, answering the handles it could not as index rows.
    index_lookup_push_down: bool,
    /// Go `getPhysicalPlanIDs(e.idxPlans)` and `getIndexPlanRootID()`.
    index_plan_ids: Vec<isize>,
    index_root_plan_id: isize,
    /// Go `index.ID`.
    index_id: i64,
    /// Go `prunedPartitions` (`partitionTableMode`), else the table's id.
    table_ids: Vec<i64>,
    /// Go `partitionTableMode`.
    partition_table_mode: bool,
    /// Go `partitionIDMap`: a global index's pruned partitions.
    partition_id_map: std::collections::BTreeSet<i64>,
    /// Go `partitionRangeMap`: an index join task's ranges of each partition.
    partition_range_map: Option<std::collections::HashMap<i64, Vec<tidb_ranger::types::Range>>>,
    /// Go `ranges`.
    ranges: Vec<tidb_ranger::types::Range>,
    /// Go `is.GroupByColIdxs`: the columns whose values group `ranges` into
    /// `groupedRanges`.
    group_by_col_idxs: Vec<usize>,
    /// Go `groupedRanges`: the scan's `GroupedRanges`.
    grouped_ranges: Vec<Vec<tidb_ranger::types::Range>>,
    /// Go `corColInAccess`'s `rebuildIndexRanges`.
    range_rebuild: Option<crate::access_path::RangeRebuilder>,
    desc: bool,
    /// Go `byItems`, over the index request's output.
    by_items: Vec<tidb_executor_sortexec::SortByItem>,
    /// Go `dagPB.OutputOffsets` (`buildIndexScanOutputOffsets`).
    index_output_offsets: Vec<u32>,
    /// Go `getRetTpsForIndexReader()`.
    index_types: Vec<FieldType>,
    /// Go `PushedLimit`.
    pushed_limit: Option<PushedDownLimit>,
    /// Go `indexLookupSize` / `indexLookupConcurrency`.
    index_lookup_size: usize,
    index_lookup_concurrency: usize,
    /// Go `indexPaging` and `idxPlans[0].StatsCount()` (`calculateBatchSize`).
    index_paging: bool,
    est_rows: usize,
    /// Go `idxNetDataSize`.
    idx_net_data_size: f64,
    /// Go `StmtCtx.MemTracker`, which `memTracker` attaches to on `Open`.
    stmt_mem_tracker: Arc<tidb_memory::Tracker>,
    /// Go `groupedKVRanges` (`buildTableKeyRanges`).
    kv_ranges: Vec<Vec<tidb_txnkv::KeyRange>>,
    /// Go `resultCh`, `resultCurr` and its cursor.
    result_ch: Option<Receiver<TaskDone>>,
    result_curr: Option<(TaskRows, usize)>,
    workers: Vec<std::thread::JoinHandle<()>>,
    worker_started: bool,
    /// Go `dummy`: not a real reader, only the KV ranges a union scan reads
    /// the cached or temporary table's rows over (`setDummy`).
    dummy: bool,
}

impl IndexLookUpExecutor {
    /// Go `buildIndexLookUpReader` for a `kv.TiKV` lookup:
    /// `buildNoRangeIndexLookUpReader`, then, for a dynamically pruned
    /// partitioned table, its partitions -- a `TableDualExec` when none
    /// survive.
    ///
    /// # Errors
    /// A lookup without an index or table scan, a store without a client,
    /// or a failed partition pruning.
    pub fn build(
        meta: ExecutorMeta,
        ctx: &crate::StmtContext,
        is: &tidb_infoschema::SessionExtendedInfoSchema,
        v: &PhysicalIndexLookUpReader,
    ) -> Result<Box<dyn Executor>, ExecError> {
        let dual = meta.clone();
        Ok(match Self::build_no_range(meta, ctx, is, v)? {
            Some(lookup) => Box::new(lookup),
            None => Box::new(crate::table_dual::TableDualExec::new(dual, 0)),
        })
    }

    /// Go `buildNoRangeIndexLookUpReader` and `buildIndexLookUpReader`'s
    /// partition pruning: the lookup, or `None` when no partition survives.
    ///
    /// # Errors
    /// As [`Self::build`].
    pub(crate) fn build_no_range(
        meta: ExecutorMeta,
        ctx: &crate::StmtContext,
        is: &tidb_infoschema::SessionExtendedInfoSchema,
        v: &PhysicalIndexLookUpReader,
    ) -> Result<Option<Self>, ExecError> {
        let internal = |message: &'static str| ExecError::Internal(message.into());
        // Go `IndexPlans` / `TablePlans`, flattened from the plans
        // themselves: the reader's flattened copies predate the rewrites
        // that bind them.
        let index_plan = v.index_plan.as_deref().ok_or_else(|| internal("the lookup has no index plan"))?;
        // Go `IndexPlans, IndexPlansUnNatureOrders = FlattenTreePushDownPlan(IndexPlan)`
        // under a pushed-down lookup.
        let (index_plans, index_plans_un_nature_orders) = if v.index_lookup_push_down {
            let (plans, un_nature_orders) = crate::builder_utils::flatten_tree_push_down_plan(index_plan);
            (plans.into_iter().cloned().collect::<Vec<_>>(), un_nature_orders)
        } else {
            (
                crate::builder_utils::flatten_table_plan(index_plan).into_iter().cloned().collect(),
                std::collections::BTreeMap::new(),
            )
        };
        let table_plans =
            crate::builder_utils::flatten_table_plan(v.table_plan.as_deref().ok_or_else(|| internal("the lookup has no table plan"))?);
        let PhysicalPlan::IndexScan(scan) = &index_plans[0] else {
            return Err(internal("the lookup's index plans start with no index scan"));
        };
        let PhysicalPlan::TableScan(ts) = table_plans[0] else {
            return Err(internal("the lookup's table plans start with no table scan"));
        };
        let store = ctx.txn_store().ok_or_else(|| internal("the index lookup has no store"))?;
        let client = store.client().ok_or_else(|| internal("the store has no coprocessor client"))?;
        let table = is
            .physical_kv_table_by_id(scan.physical_table_id)
            .ok_or_else(|| internal("the index lookup's table is absent from the catalog"))?;
        let table_info = table.meta().read().clone();
        // Go: a temporary table's lookup is a dummy; its rows are the union
        // scan's.
        let dummy = table_info.temp_table_type != tidb_model::TempTableType::NONE;
        let index = table_info
            .indices
            .iter_deref()
            .find(|index| index.read().id == scan.index_id)
            .map(|index| index.read().clone())
            .ok_or_else(|| internal("the index lookup's index is absent from its table"))?;
        // Go `ExtraHandleCol` / `CommonHandleCols`, resolved against the
        // table side's schema.
        let (handle_cols, primary_key_index): (Vec<Column>, _) = match &v.extra_handle_col {
            Some(column) => (vec![column.clone()], None),
            // Go `tables.FindPrimaryIndex(tbl.Meta())`, as the key codec
            // reads it.
            None => (
                v.common_handle_cols.clone(),
                table.codec_table_info().indices.into_iter().find(|index| index.primary),
            ),
        };
        let handle_idx = handle_cols
            .iter()
            .map(|column| usize::try_from(column.index).map_err(|_| internal("an unresolved handle column")))
            .collect::<Result<Vec<_>, _>>()?;
        // Go `partitionTableMode`: a dynamically pruned partitioned table
        // (not through a global index) reads its pruned partitions.
        let mut table_ids = vec![scan.physical_table_id];
        let mut partition_table_mode = false;
        // A global index keeps its entries of the pruned partitions (Go
        // `partitionIDMap`).
        let mut partition_id_map = std::collections::BTreeSet::new();
        if !ctx.static_partition_prune() && scan.physical_table_id == table_info.id && table_info.get_partition_info().is_some() {
            let pids = crate::index_reader::get_partition_ids_after_pruning(is, &table_info, v.plan_part_info.as_deref(), ctx)?;
            if index.global {
                partition_id_map = pids.into_iter().collect();
            } else if pids.is_empty() {
                return Ok(None);
            } else {
                table_ids = pids;
                partition_table_mode = true;
            }
        }
        let index_schema = scan.base.base.schema().ok_or_else(|| internal("the index scan has no schema"))?;
        let is_common_handle = !(handle_cols.len() == 1 && handle_cols[0].id == tidb_model::column::EXTRA_HANDLE_ID)
            && table_info.is_common_handle;
        // Go `buildIndexScanOutputOffsets`: the order-by columns, the handle,
        // and the physical table id when the scan needs it.
        let need_extra_output_col = table_info.get_partition_info().is_some()
            && (index.global || (!scan.by_items.is_empty() && !ctx.static_partition_prune()));
        let handle_len = if v.common_handle_cols.is_empty() { 1 } else { v.common_handle_cols.len() };
        let mut index_output_offsets = Vec::new();
        let mut index_types = Vec::new();
        let mut by_items = Vec::new();
        for item in &scan.by_items {
            let tidb_expr::expression::Expression::Column(column) = &item.expr else {
                return Err(internal("Not support non-column in orderBy pushed down"));
            };
            let position = index_schema
                .columns
                .iter()
                .position(|candidate| candidate.id == column.id)
                .ok_or_else(|| internal("Not found order by related columns in indexScan.schema"))?;
            index_output_offsets.push(position as u32);
            let mut key = column.clone();
            key.index = by_items.len() as i64;
            index_types.push(column.ret_type.clone().unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::Null)));
            by_items.push(tidb_executor_sortexec::SortByItem {
                expr: tidb_expr::expression::Expression::Column(key),
                desc: item.desc,
            });
        }
        // Go `len(columns)+i`: the handle columns after the index columns,
        // where `InitSchema` appends them, then the physical table id.
        let index_columns = index.columns.len();
        index_output_offsets.extend((0..handle_len).map(|i| (index_columns + i) as u32));
        if need_extra_output_col {
            index_output_offsets.push((index_columns + handle_len) as u32);
        }
        if is_common_handle {
            index_types.extend(handle_cols.iter().map(|column| {
                column.ret_type.clone().unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::Null))
            }));
        } else {
            index_types.push(FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
        }
        // Go `needPartitionHandle`.
        let keep_order = scan.keep_order;
        let partition_handle_from_index = index.global || (partition_table_mode && keep_order);
        let partition_handle_from_table = (index.global || partition_table_mode) && keep_order;
        if partition_handle_from_index {
            index_types.push(FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
        }
        // Go `GetAvgTableRowSize`.
        let avg_table_row_size = v.table_plan.as_deref().map_or(0.0, |plan| {
            plan.cop_avg_row_size(false, tidb_config::tikvcfg::get_global_config().tikv_client.enable_chunk_rpc)
        });
        let index_plan_ids: Vec<isize> = index_plans.iter().map(|plan| plan.id() as isize).collect();
        let est_rows = index_plans[0].stats_count().map_or(0, |count| count as usize);
        let table_plan_ids: Vec<isize> = table_plans.iter().map(|plan| plan.id() as isize).collect();
        let shared = Shared {
            ctx: ctx.clone(),
            client,
            table_plans: table_plans.iter().copied().cloned().collect(),
            table_plan_ids,
            index_name: index.name.original().to_owned(),
            codec_table_info: table.codec_table_info(),
            table_info,
            primary_key_index,
            handle_cols,
            handle_idx,
            partition_handle_from_index,
            partition_handle_from_table,
            keep_order,
            start_ts: store.start_ts(),
            columns: ts.columns.clone(),
            physical_table_id: ts.physical_table_id,
            avg_row_size: avg_table_row_size,
            finished: AtomicBool::new(false),
            index_scan_plan_id: index_plans[0].id() as isize,
            index_cop_count_and_rows: Mutex::new((0, 0)),
            mem_tracker: ctx.statement_memory().operator_tracker(meta.id()),
            // Go `BuildNewBaseExecutorV2(..., e.Schema(), e.getTableRootPlanID())`.
            table_reader_meta: ExecutorMeta::new(
                meta.schema().clone(),
                table_plans.last().map_or(meta.id(), |plan| i64::from(plan.id())),
                meta.init_cap(),
                meta.max_chunk_size(),
            )
            .with_alloc_pool(meta.alloc_pool().cloned()),
            table_reader_ruv2: tidb_executor_internal_exec::ruv2::Ruv2NextCache::new(
                "*executor.TableReaderExecutor",
                ctx.ruv2_metrics(),
            ),
            // Go `initRuntimeStats` and `prepareHandleFetch`'s
            // `indexScanBasicStats` of the index plan root.
            stats: ctx.executor_runtime_stats_source().map(|source| {
                let index_root = index_plans.last().map_or(meta.id() as i64, |plan| i64::from(plan.id()));
                crate::index_lookup_stats::IndexLookUpStatsHandle {
                    stats: Arc::new(crate::index_lookup_stats::IndexLookUpRunTimeStats::new(
                        ctx.index_lookup_concurrency(),
                    )),
                    index_scan_basic: (index_root != meta.id() as i64)
                        .then(|| source.basic_runtime_stats(index_root))
                        .flatten(),
                    source: Arc::clone(source),
                    table_root_plan_id: table_plans.last().map_or(meta.id() as i64, |plan| i64::from(plan.id())),
                    plan_id: meta.id() as i64,
                    index_end_recorded: false,
                }
            }),
        };
        Ok(Some(Self {
            index_root_plan_id: *index_plan_ids.last().unwrap_or(&(meta.id() as isize)),
            meta,
            shared: Arc::new(shared),
            index_plans_un_nature_orders,
            index_lookup_push_down: v.index_lookup_push_down,
            index_plans: index_plans.clone(),
            index_plan_ids,
            index_id: index.id,
            table_ids,
            partition_table_mode,
            partition_id_map,
            partition_range_map: None,
            ranges: scan.ranges.clone(),
            group_by_col_idxs: scan.group_by_col_idxs.clone(),
            grouped_ranges: scan.grouped_ranges.clone(),
            range_rebuild: crate::driver::physical_builder::correlated_index_range_rebuilder(scan, ctx),
            desc: scan.desc,
            by_items,
            index_output_offsets,
            index_types,
            pushed_limit: v.pushed_limit,
            index_lookup_size: ctx.index_lookup_size(),
            index_lookup_concurrency: ctx.index_lookup_concurrency(),
            index_paging: v.paging,
            est_rows,
            idx_net_data_size: avg_table_row_size,
            stmt_mem_tracker: Arc::clone(ctx.statement_memory().stmt_tracker()),
            kv_ranges: Vec::new(),
            result_ch: None,
            result_curr: None,
            workers: Vec::new(),
            worker_started: false,
            dummy,
        }))
    }

    /// Go `IndexLookUpExecutor.open`: `memTracker` reset and attached again,
    /// and a fresh `finished`.
    fn open_inner(&mut self) -> Result<(), ExecError> {
        self.shared.mem_tracker.reset();
        self.shared.mem_tracker.attach_to(&self.stmt_mem_tracker);
        // Go `open`'s `e.finished = make(chan struct{})`.
        self.shared.finished.store(false, Ordering::Release);
        *self.shared.index_cop_count_and_rows.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = (0, 0);
        Ok(())
    }

    /// Go `buildIndexLookUpReaderForIndexJoin`'s `e.groupedKVRanges =
    /// [{PhysicalTableID, kvRange}]` then `e.open(ctx)`.
    ///
    /// # Errors
    /// As [`Executor::open`].
    pub(crate) fn open_with_kv_ranges(&mut self, kv_ranges: Vec<tidb_txnkv::KeyRange>) -> Result<(), ExecError> {
        self.kv_ranges = vec![kv_ranges];
        self.open_inner()
    }

    /// Go `buildIndexLookUpReaderForIndexJoin` over a dynamically pruned
    /// partitioned table: the task's partitions (`prunedPartitions`, `None`
    /// keeps a global index's table), ranges and per-partition ranges
    /// (`partitionRangeMap`), then `exec.Open`.
    ///
    /// # Errors
    /// As [`Executor::open`].
    pub(crate) fn open_for_index_join(
        &mut self,
        partitions: Option<Vec<i64>>,
        ranges: Vec<tidb_ranger::types::Range>,
        partition_range_map: Option<std::collections::HashMap<i64, Vec<tidb_ranger::types::Range>>>,
    ) -> Result<(), ExecError> {
        if let Some(partitions) = partitions {
            self.table_ids = partitions;
            self.partition_table_mode = true;
        }
        self.ranges = ranges;
        self.range_rebuild = None;
        self.partition_range_map = partition_range_map;
        self.open()
    }

    /// Go `setDummy`.
    pub(crate) fn set_dummy(&mut self) {
        self.dummy = true;
    }

    /// What Go's `buildMemIndexLookUpReader` reads of an open lookup: its
    /// `groupedKVRanges` with each group's physical table, and `columns`.
    pub(crate) fn grouped_kv_ranges_and_columns(
        &self,
    ) -> (Vec<(i64, &[tidb_txnkv::KeyRange])>, &[tidb_model::GoShared<tidb_model::ColumnInfo>]) {
        let groups = self
            .kv_ranges
            .iter()
            .enumerate()
            .map(|(idx, ranges)| {
                let physical_table_id = if self.partition_table_mode {
                    self.table_ids[idx % self.table_ids.len()]
                } else {
                    self.shared.physical_table_id
                };
                (physical_table_id, ranges.as_slice())
            })
            .collect();
        (groups, &self.shared.columns)
    }

    /// Go `partitionIDMap`, which the union scan above reads.
    pub(crate) fn partition_id_map(&self) -> &std::collections::BTreeSet<i64> {
        &self.partition_id_map
    }

    /// Go `getPhysicalTableID(e.table)` and `e.index.ID`.
    pub(crate) fn physical_table_id_and_index_id(&self) -> (i64, i64) {
        (self.shared.physical_table_id, self.index_id)
    }

    /// Go `buildTableKeyRanges`: one key-range group a range group and
    /// physical table.
    fn build_table_key_ranges(&self) -> Result<Vec<Vec<tidb_txnkv::KeyRange>>, ExecError> {
        // Go `corColInAccess`: the ranges rebuilt over the outer row, and
        // `groupedRanges` with them when the scan groups them.
        let (ranges, grouped_ranges) = match &self.range_rebuild {
            Some(rebuild) => {
                let ranges = rebuild().map_err(|error| ExecError::Internal(format!("{error:?}").into()))?;
                let grouped_ranges = if self.group_by_col_idxs.is_empty() {
                    Vec::new()
                } else {
                    tidb_planner::find_best_task::dispatch::group_ranges_by_cols(&ranges, &self.group_by_col_idxs)
                        .map_err(|error| ExecError::Internal(error.into()))?
                };
                (ranges, grouped_ranges)
            }
            None => (self.ranges.clone(), self.grouped_ranges.clone()),
        };
        // Go `groupedRanges`, or the ranges as one group.
        let grouped_ranges = if grouped_ranges.is_empty() { vec![ranges] } else { grouped_ranges };
        let zone = tidb_exprctx::Columns::time_zone(&self.shared.ctx);
        let mut kv_ranges = Vec::with_capacity(grouped_ranges.len() * self.table_ids.len());
        for ranges in &grouped_ranges {
            // Go `buildKeyRanges`: `IndexRangesToKVRangesWithInterruptSignal`
            // per physical table (over its `partitionRangeMap` ranges when it
            // has them), charging `memTracker`.
            for &physical_id in &self.table_ids {
                let ranges = self.partition_range_map.as_ref().and_then(|map| map.get(&physical_id)).unwrap_or(ranges);
                kv_ranges.push(
                    tidb_distsql::index_ranges_to_kv_ranges_with_mem_tracker(
                        Some(&zone),
                        physical_id,
                        self.index_id,
                        ranges,
                        &self.shared.mem_tracker,
                    )
                    .map_err(|error| ExecError::Internal(format!("{error:?}").into()))?,
                );
            }
        }
        Ok(kv_ranges)
    }

    /// Go `calculateBatchSize`.
    fn calculate_batch_size(&self, init_batch_size: usize, max_batch_size: usize) -> usize {
        if self.index_paging {
            return init_batch_size.min(max_batch_size);
        }
        calculate_batch_size(self.est_rows, init_batch_size, max_batch_size)
    }

    /// Go `startWorkers` / `startIndexWorker`.
    fn start_workers(&mut self, init_batch_size: usize) -> Result<(), ExecError> {
        let statement = crate::remote_scan::pushdown_statement_context(&self.shared.ctx);
        let kv_ranges = std::mem::take(&mut self.kv_ranges);
        let plans: Vec<&PhysicalPlan> = self.index_plans.iter().collect();
        let mut dag = crate::builder_utils::construct_dag_req(
            &self.shared.ctx,
            &statement,
            &plans,
            tidb_kv::store_type::StoreType::TiKv,
        )
        .map_err(|error| ExecError::Internal(error.to_string().into()))?;
        let mut by_items = self.by_items.clone();
        let mut index_types = self.index_types.clone();
        // Go `tblScanIdxForRewritePartitionID`.
        let mut tbl_scan_idx_for_rewrite_partition_id = None;
        if self.index_lookup_push_down {
            // Go `buildIndexLookUpPushDownDAGReq`: the lookup's index rows
            // are its intermediate channel, the table plan's rows the final
            // output.
            for (&child, &parent) in &self.index_plans_un_nature_orders {
                dag.executors[child].parent_idx = Some(parent as u32);
            }
            let intermediate_output_index = dag
                .executors
                .iter()
                .position(|executor| executor.tp == Some(tidb_proto::tipb::ExecType::TypeIndexLookUp as i32))
                .ok_or_else(|| ExecError::Internal("IndexLookUp executor not found".into()))?;
            dag.intermediate_output_channels = vec![tidb_proto::tipb::IntermediateOutputChannel {
                executor_idx: intermediate_output_index as u32,
                output_offsets: self.index_output_offsets.clone(),
            }];
            let output_len = self.index_plans.last().and_then(|plan| plan.schema()).map_or(0, |schema| schema.len());
            dag.output_offsets = (0..output_len as u32).collect();
            if self.partition_table_mode {
                tbl_scan_idx_for_rewrite_partition_id = Some(
                    dag.executors
                        .iter()
                        .position(|executor| executor.tp == Some(tidb_proto::tipb::ExecType::TypeTableScan as i32))
                        .ok_or_else(|| {
                            ExecError::Internal("cannot find table scan executor in for partition index lookup push down".into())
                        })?,
                );
            }
        } else {
            dag.output_offsets.clone_from(&self.index_output_offsets);
        }
        // Go: one key-range group needs no merge -- its order-by columns
        // leave the request.
        if kv_ranges.len() == 1 {
            dag.output_offsets.drain(..by_items.len());
            index_types.drain(..by_items.len());
            by_items.clear();
        }
        let need_merge = self.shared.keep_order && !by_items.is_empty() && kv_ranges.len() > 1;
        let max_batch_size = self.index_lookup_size;
        let batch_size = self.calculate_batch_size(init_batch_size, max_batch_size);
        let (result_tx, result_rx) = sync_channel::<TaskDone>(LOOKUP_TABLE_TASK_CHANNEL_SIZE);
        self.result_ch = Some(result_rx);
        // Go's workerPool: up to `indexLookupConcurrency` table workers.
        let (work_tx, work_rx) = sync_channel::<LookupTask>(LOOKUP_TABLE_TASK_CHANNEL_SIZE);
        let work_rx = Arc::new(Mutex::new(work_rx));
        // Go `tableRequest` (`buildTableReq`), rebuilt on `open` for
        // `corColInTblSide`.
        let table_plans: Vec<&PhysicalPlan> = self.shared.table_plans.iter().collect();
        let mut table_request = crate::builder_utils::construct_dag_req(
            &self.shared.ctx,
            &statement,
            &table_plans,
            tidb_kv::store_type::StoreType::TiKv,
        )
        .map_err(|error| ExecError::Internal(error.to_string().into()))?;
        table_request.output_offsets = (0..self.meta.schema().columns.len() as u32).collect();
        let table_request = Arc::new(table_request);
        for _ in 0..self.index_lookup_concurrency.max(1) {
            let shared = Arc::clone(&self.shared);
            let work_rx = Arc::clone(&work_rx);
            let table_request = Arc::clone(&table_request);
            // Go's table workers run under `e.workerCtx`, the context of the
            // `Next` that started them.
            self.workers.push(std::thread::spawn(tidb_executor_internal_exec::ruv2::inherit_goroutine(move || loop {
                let next = work_rx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).recv();
                let Ok(task) = next else {
                    return;
                };
                if shared.finished.load(Ordering::Acquire) {
                    return;
                }
                let started = std::time::Instant::now();
                let done = task.done.clone();
                let rows = execute_table_task(&shared, &table_request, task);
                // Go `execTableTask`'s `TableRowScan` / `TableTaskNum`.
                if let (Some(stats), Ok(rows)) = (shared.stats.as_ref(), rows.as_ref()) {
                    stats.table_task_recorder().record_table_task(started, rows.order.len());
                }
                let _ = done.send(rows);
            })));
        }
        // Go `startIndexWorker`: the index worker's tracker, which its
        // requests charge (`SetMemTracker(tracker)`).
        let index_mem_tracker = tidb_memory::Tracker::new(tidb_memory::LABEL_FOR_INDEX_WORKER, -1);
        index_mem_tracker.attach_to(&self.shared.mem_tracker);
        let index_worker = IndexWorker {
            shared: Arc::clone(&self.shared),
            meta: self.meta.clone(),
            statement,
            dag,
            kv_ranges,
            table_ids: self.table_ids.clone(),
            index_types,
            by_items,
            need_merge,
            desc: self.desc,
            index_plan_ids: self.index_plan_ids.clone(),
            index_root_plan_id: self.index_root_plan_id,
            idx_net_data_size: self.idx_net_data_size,
            pushed_limit: self.pushed_limit,
            batch_size,
            max_batch_size,
            scanned_keys: 0,
            mem_tracker: index_mem_tracker,
            next_task_id: 0,
            partition_table_mode: self.partition_table_mode,
            index_paging: self.index_paging,
            index_lookup_push_down: self.index_lookup_push_down,
            tbl_scan_idx_for_rewrite_partition_id,
        };
        self.workers.push(std::thread::spawn(tidb_executor_internal_exec::ruv2::inherit_goroutine(move || {
            index_worker.run(&result_tx, &work_tx);
        })));
        self.worker_started = true;
        Ok(())
    }

    /// Go `getResultTask`.
    fn get_result_task(&mut self) -> Result<bool, ExecError> {
        if self.result_curr.as_ref().is_some_and(|(rows, cursor)| *cursor < rows.order.len()) {
            return Ok(true);
        }
        let Some(result_ch) = self.result_ch.as_ref() else {
            return Ok(false);
        };
        let start = std::time::Instant::now();
        let Ok(done) = result_ch.recv() else {
            return Ok(false);
        };
        let index_fetched = std::time::Instant::now();
        let rows = done
            .recv()
            .map_err(|_| ExecError::Internal("an index lookup table task ended without its rows".into()))??;
        // Go: release the memory usage of the last task before handling a
        // new one.
        if let Some((curr, _)) = &self.result_curr {
            curr.mem_tracker.consume(-curr.mem_usage);
        }
        // Go `getResultTask`'s `NextWaitIndexScan` / `NextWaitTableLookUpResp`.
        if let Some(stats) = self.shared.stats.as_ref() {
            stats.stats.record_next_wait(index_fetched - start, std::time::Duration::ZERO, index_fetched.elapsed());
        }
        self.result_curr = Some((rows, 0));
        Ok(true)
    }
}

/// Go `CalculateBatchSize`.
fn calculate_batch_size(est_rows: usize, init_batch_size: usize, max_batch_size: usize) -> usize {
    let mut batch_size = init_batch_size.min(max_batch_size);
    if est_rows >= max_batch_size {
        return max_batch_size;
    }
    while batch_size < est_rows {
        batch_size *= 2;
        if batch_size >= max_batch_size {
            return max_batch_size;
        }
    }
    batch_size
}

/// Go `indexWorker`.
struct IndexWorker {
    shared: Arc<Shared>,
    meta: ExecutorMeta,
    statement: tidb_distsql::remote_scan::PushdownStatementContext,
    dag: tidb_proto::tipb::DagRequest,
    kv_ranges: Vec<Vec<tidb_txnkv::KeyRange>>,
    table_ids: Vec<i64>,
    index_types: Vec<FieldType>,
    by_items: Vec<tidb_executor_sortexec::SortByItem>,
    need_merge: bool,
    desc: bool,
    index_plan_ids: Vec<isize>,
    index_root_plan_id: isize,
    idx_net_data_size: f64,
    pushed_limit: Option<PushedDownLimit>,
    batch_size: usize,
    max_batch_size: usize,
    scanned_keys: u64,
    /// Go `startIndexWorker`'s tracker.
    mem_tracker: Arc<tidb_memory::Tracker>,
    /// Go `lookupTableTask.id`.
    next_task_id: i64,
    /// Go `partitionTableMode`.
    partition_table_mode: bool,
    /// Go `indexPaging`.
    index_paging: bool,
    /// Go `indexLookUpPushDown`.
    index_lookup_push_down: bool,
    /// Go `tblScanIdxForRewritePartitionID`: the pushed lookup's table scan,
    /// whose table id each partition's request rewrites.
    tbl_scan_idx_for_rewrite_partition_id: Option<usize>,
}

impl IndexWorker {
    /// Go `startIndexWorker`'s goroutine: the handles of every key range,
    /// one result at a time (or, for a merge, every range merged by the
    /// order-by columns), cut into table tasks.
    fn run(
        mut self,
        result_tx: &SyncSender<TaskDone>,
        work_tx: &SyncSender<LookupTask>,
    ) {
        let total = self.kv_ranges.len();
        let ranges = std::mem::take(&mut self.kv_ranges);
        let fail = |error: ExecError| {
            // Go `syncErr`: a failed task that carries the error.
            let (done_tx, done_rx) = sync_channel(1);
            let _ = done_tx.send(Err(error));
            let _ = result_tx.send(done_rx);
        };
        if self.index_lookup_push_down {
            if let Err(error) = self.run_push_down(ranges, result_tx, work_tx) {
                fail(error);
            }
            return;
        }
        // Go `startIndexWorker`: one request a key-range group, all sent
        // before any handle is read, and merged by the order-by columns when
        // the read keeps order across groups.
        let mut results: Vec<Box<dyn Executor>> = Vec::with_capacity(total);
        for (idx, kv_range) in ranges.into_iter().enumerate() {
            match self.select_result(idx, kv_range, total) {
                Ok(result) => {
                    results.push(Box::new(SelectResultExec::new(self.index_meta(), result, Arc::clone(&self.shared))));
                }
                Err(error) => return fail(error),
            }
        }
        if self.need_merge {
            let mut merged = match crate::range_group_merge::SortedSelectResults::new(
                self.index_meta(),
                self.by_items.clone(),
                results,
                self.shared.ctx.clone(),
            ) {
                Ok(merged) => merged,
                Err(error) => return fail(error),
            };
            if let Err(error) = merged.open() {
                return fail(error);
            }
            results = vec![Box::new(merged)];
        }
        // Go `fetchHandles`' `prepareHandleFetch`: one chunk from the
        // session's allocator for every result.
        let max_chunk_size = self.meta.max_chunk_size();
        let mut chk = self.meta.new_chunk_with_capacity(&self.index_types, max_chunk_size, max_chunk_size);
        let handle_offsets = self.handle_offsets();
        for (idx, source) in results.iter_mut().enumerate() {
            // Go `tableLookUpTask.partitionTable = prunedPartitions[curResultIdx]`.
            let partition = self.partition_table_mode.then(|| self.table_ids[idx % self.table_ids.len()]);
            match self.fetch_handles(source.as_mut(), partition, &mut chk, &handle_offsets, result_tx, work_tx) {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    fail(error);
                    break;
                }
            }
        }
        // Go `defer results.Close()`.
        for mut source in results {
            let _ = source.close();
        }
    }

    /// The executor shape of one index result's rows.
    fn index_meta(&self) -> ExecutorMeta {
        let columns = self
            .index_types
            .iter()
            .enumerate()
            .map(|(i, ft)| {
                let mut column = Column::new(i as i64, ft.clone());
                column.index = i as i64;
                column
            })
            .collect();
        ExecutorMeta::new(Schema::new(columns), self.meta.id(), self.meta.max_chunk_size(), self.meta.max_chunk_size())
    }

    /// Go `buildIndexSelectResultForRange`.
    fn select_result(
        &self,
        range_idx: usize,
        mut kv_range: Vec<tidb_txnkv::KeyRange>,
        total_ranges: usize,
    ) -> Result<tidb_distsql::select_response::SelectResponseIter, ExecError> {
        let mut dag = self.dag.clone();
        if let Some(idx) = self.tbl_scan_idx_for_rewrite_partition_id {
            // Go: the pushed lookup's table scan reads the range's
            // partition, so it encodes the table handle key correctly.
            if let Some(scan) = dag.executors[idx].tbl_scan.as_mut() {
                scan.table_id = Some(self.table_ids[range_idx % self.table_ids.len()]);
            }
        }
        kv_range.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        let statement = self
            .statement
            .clone()
            .with_estimated_net_bytes(self.idx_net_data_size / total_ranges.max(1) as f64);
        crate::cop_reader::open_table_read(
            self.shared.client.as_ref(),
            &statement,
            crate::cop_reader::TableRead {
                store_type: tidb_kv::store_type::StoreType::TiKv,
                dag,
                key_ranges: kv_range,
                key_range_hints: Vec::new(),
                partition_id_and_ranges: Vec::new(),
                batch_cop: false,
                schema_version: 0,
                connection_id: tidb_exprctx::Columns::connection_id(&self.shared.ctx).unwrap_or_default(),
                connection_alias: self.shared.ctx.ddl_session_alias().to_owned(),
                start_ts: self.shared.start_ts,
                keep_order: self.shared.keep_order,
                desc: self.desc,
                // Go `SetPaging(e.indexPaging)`; a pushed lookup does not page.
                paging: self.index_paging && !self.index_lookup_push_down,
                // Go `e.RetFieldTypes()` under a pushed lookup, whose index
                // rows are the intermediate channel's.
                field_types: if self.index_lookup_push_down {
                    self.meta.ret_field_types().to_vec()
                } else {
                    self.index_types.clone()
                },
                intermediate_output_types: if self.index_lookup_push_down {
                    vec![self.index_types.clone()]
                } else {
                    Vec::new()
                },
                cop_plan_ids: self.index_plan_ids.clone(),
                root_plan_id: self.index_root_plan_id,
                mem_tracker: Some(Arc::clone(&self.mem_tracker)),
            },
        )
        .map_err(|error| ExecError::Internal(error.into()))
    }

    /// Go `fetchHandles` over one source: its handles in batches of
    /// `batchSize` (doubling to `maxBatchSize`), each a table task. Answers
    /// whether the reading goes on (false: the executor finished or the
    /// pushed limit is reached).
    fn fetch_handles(
        &mut self,
        source: &mut dyn Executor,
        partition: Option<i64>,
        chk: &mut Chunk,
        handle_offsets: &[usize],
        result_tx: &SyncSender<TaskDone>,
        work_tx: &SyncSender<LookupTask>,
    ) -> Result<bool, ExecError> {
        loop {
            if let Some(limit) = self.pushed_limit {
                if self.scanned_keys >= limit.count + limit.offset {
                    return Ok(false);
                }
            }
            let start = std::time::Instant::now();
            let (handles, handles_cap) = self.extract_task_handles(source, chk, handle_offsets)?;
            let finish_fetch = std::time::Instant::now();
            if handles.is_empty() {
                return Ok(true);
            }
            if self.shared.finished.load(Ordering::Acquire) {
                return Ok(false);
            }
            let finish_build = std::time::Instant::now();
            let (done_tx, done_rx) = sync_channel(1);
            let task = LookupTask { id: self.next_task_id, handles, handles_cap, partition, done: done_tx };
            self.next_task_id += 1;
            if work_tx.send(task).is_err() || result_tx.send(done_rx).is_err() {
                return Ok(false);
            }
            // Go `buildAndDispatchLookupTasks`' `FetchHandle`, `TaskWait`,
            // `FetchHandleTotal`.
            if let Some(stats) = self.shared.stats.as_ref() {
                stats.stats.record_index_task(finish_fetch - start, start.elapsed(), finish_build.elapsed());
            }
        }
    }

    /// Go `startIndexWorker` under `indexLookUpPushDown`: each range's
    /// rows the store looked up, and the handles it could not, in batches;
    /// the rows are completed tasks, the handles table tasks.
    fn run_push_down(
        &mut self,
        ranges: Vec<Vec<tidb_txnkv::KeyRange>>,
        result_tx: &SyncSender<TaskDone>,
        work_tx: &SyncSender<LookupTask>,
    ) -> Result<(), ExecError> {
        let total = ranges.len();
        let mut results = Vec::with_capacity(total);
        for (idx, kv_range) in ranges.into_iter().enumerate() {
            results.push(self.select_result(idx, kv_range, total)?);
        }
        let handle_offsets = self.handle_offsets();
        let mut outcome = Ok(());
        'results: for (idx, result) in results.iter_mut().enumerate() {
            let partition = self.partition_table_mode.then(|| self.table_ids[idx % self.table_ids.len()]);
            loop {
                if let Some(limit) = self.pushed_limit {
                    if self.scanned_keys >= limit.count + limit.offset {
                        break 'results;
                    }
                }
                let start = std::time::Instant::now();
                let (rows, handles, exhausted) = match self.extract_look_up_push_down_rows_or_handles(result, &handle_offsets) {
                    Ok(extracted) => extracted,
                    Err(error) => {
                        outcome = Err(error);
                        break 'results;
                    }
                };
                let finish_fetch = std::time::Instant::now();
                if self.shared.finished.load(Ordering::Acquire) {
                    break 'results;
                }
                // Go `buildCompletedTask`: the looked-up rows, done.
                if !rows.is_empty() {
                    let mut chunk = Chunk::new(self.meta.ret_field_types(), rows.len(), rows.len());
                    for row in &rows {
                        for (col_idx, datum) in row.iter().enumerate() {
                            chunk.append_datum(col_idx, datum);
                        }
                    }
                    let mem_tracker = tidb_memory::Tracker::new(self.next_task_id, -1);
                    self.next_task_id += 1;
                    let (done_tx, done_rx) = sync_channel(1);
                    let _ = done_tx.send(Ok(TaskRows {
                        order: (0..rows.len()).map(|row| (0, row)).collect(),
                        chunks: vec![chunk],
                        mem_usage: 0,
                        mem_tracker,
                    }));
                    if result_tx.send(done_rx).is_err() {
                        break 'results;
                    }
                }
                if !handles.is_empty() {
                    let (done_tx, done_rx) = sync_channel(1);
                    let handles_cap = handles.capacity();
                    let task = LookupTask { id: self.next_task_id, handles, handles_cap, partition, done: done_tx };
                    self.next_task_id += 1;
                    if work_tx.send(task).is_err() || result_tx.send(done_rx).is_err() {
                        break 'results;
                    }
                }
                if let Some(stats) = self.shared.stats.as_ref() {
                    stats.stats.record_index_task(finish_fetch - start, start.elapsed(), finish_fetch.elapsed());
                }
                if exhausted {
                    break;
                }
            }
        }
        for mut result in results {
            let _ = result.close();
            let (requests, rows) = result.runtime_stats().get_cop_count_and_rows(self.shared.index_scan_plan_id);
            let mut count = self.shared.index_cop_count_and_rows.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            count.0 += requests;
            count.1 += rows;
        }
        outcome
    }

    /// Go `extractLookUpPushDownRowsOrHandles`: up to `batchSize` rows
    /// (channel 1, looked up) and handles (channel 0, the index rows the
    /// store could not look up), and whether the result is exhausted.
    fn extract_look_up_push_down_rows_or_handles(
        &mut self,
        iter: &mut tidb_distsql::select_response::SelectResponseIter,
        handle_offsets: &[usize],
    ) -> Result<(Vec<Vec<Datum>>, Vec<Handle>, bool), ExecError> {
        const CHANNEL_IDX_INDEX: usize = 0;
        const CHANNEL_IDX_ROW: usize = 1;
        let start = std::time::Instant::now();
        let start_scan_keys = self.scanned_keys;
        let mut rows = Vec::new();
        let mut handles = Vec::new();
        let mut index_row = Chunk::new(&self.index_types, 1, 1);
        let mut exhausted = false;
        while handles.len() + rows.len() < self.batch_size {
            let Some(row) = iter
                .next_row()
                .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)))?
            else {
                exhausted = true;
                break;
            };
            self.scanned_keys += 1;
            if let Some(limit) = self.pushed_limit {
                if self.scanned_keys <= limit.offset {
                    continue;
                }
                if self.scanned_keys > limit.offset + limit.count {
                    // Skip the handles after Offset+Count.
                    break;
                }
            }
            match row.channel_index {
                CHANNEL_IDX_ROW => rows.push(row.row),
                CHANNEL_IDX_INDEX => {
                    index_row.reset();
                    for (col_idx, datum) in row.row.iter().enumerate() {
                        index_row.append_datum(col_idx, datum);
                    }
                    handles.push(get_handle(&self.shared, index_row.get_row(0), handle_offsets, GetHandleType::FromIndex)?);
                }
                other => return Err(ExecError::Internal(format!("unexpected channel index {other}").into())),
            }
        }
        // Go's deferred `indexScanBasicStats.Record(time.Since(startTime), cnt)`.
        if let Some(basic) = self.shared.stats.as_ref().and_then(|stats| stats.index_scan_basic.as_ref()) {
            basic.record_next(start.elapsed(), (self.scanned_keys - start_scan_keys) as i64);
        }
        if !exhausted && handles.len() + rows.len() >= self.batch_size {
            self.batch_size = (self.batch_size * 2).min(self.max_batch_size);
        }
        Ok((rows, handles, exhausted))
    }

    /// Go `getHandleOffsets`.
    fn handle_offsets(&self) -> Vec<usize> {
        let mut num_cols_without_pid = self.index_types.len();
        if self.shared.partition_handle_from_index {
            num_cols_without_pid -= 1;
        }
        let handle_cols = self.shared.handle_cols.len();
        let offsets: Vec<usize> = (0..handle_cols).map(|i| num_cols_without_pid - handle_cols + i).collect();
        if offsets.is_empty() {
            vec![num_cols_without_pid - 1]
        } else {
            offsets
        }
    }

    /// Go `extractTaskHandles`.
    fn extract_task_handles(
        &mut self,
        source: &mut dyn Executor,
        chk: &mut Chunk,
        handle_offsets: &[usize],
    ) -> Result<(Vec<Handle>, usize), ExecError> {
        let mut handles = Vec::new();
        // Go `cap(handles)`: made at the first chunk's row count, then
        // grown by `append`.
        let mut handles_cap = 0;
        let max_chunk_size = self.meta.max_chunk_size();
        while handles.len() < self.batch_size {
            let mut required_rows = self.batch_size - handles.len();
            if let Some(limit) = self.pushed_limit {
                if limit.offset + limit.count <= self.scanned_keys {
                    return Ok((handles, handles_cap));
                }
                let left = limit.offset + limit.count - self.scanned_keys;
                required_rows = required_rows.min(usize::try_from(left).unwrap_or(usize::MAX));
            }
            chk.set_required_rows(required_rows as isize, max_chunk_size);
            let started = std::time::Instant::now();
            source.next(chk)?;
            // Go `indexScanBasicStats.Record(time.Since(startTime), chk.NumRows())`.
            if let Some(basic) = self.shared.stats.as_ref().and_then(|stats| stats.index_scan_basic.as_ref()) {
                basic.record_next(started.elapsed(), chk.num_rows() as i64);
            }
            if chk.num_rows() == 0 {
                return Ok((handles, handles_cap));
            }
            if handles_cap == 0 {
                handles_cap = chk.num_rows();
            }
            for i in 0..chk.num_rows() {
                self.scanned_keys += 1;
                if let Some(limit) = self.pushed_limit {
                    if self.scanned_keys <= limit.offset {
                        continue;
                    }
                    if self.scanned_keys > limit.offset + limit.count {
                        return Ok((handles, handles_cap));
                    }
                }
                handles_cap = go_append_cap(handles.len() + 1, handles_cap, SIZEOF_INTERFACE);
                handles.push(get_handle(&self.shared, chk.get_row(i), handle_offsets, GetHandleType::FromIndex)?);
            }
        }
        self.batch_size = (self.batch_size * 2).min(self.max_batch_size);
        Ok((handles, handles_cap))
    }
}

/// Go `IndexLookUpExecutor.getHandle`.
fn get_handle(
    shared: &Shared,
    row: tidb_chunk::row::Row<'_>,
    handle_idx: &[usize],
    tp: GetHandleType,
) -> Result<Handle, ExecError> {
    let is_common_handle = !(shared.handle_cols.len() == 1
        && shared.handle_cols[0].id == tidb_model::column::EXTRA_HANDLE_ID)
        && shared.table_info.is_common_handle;
    let mut handle = if is_common_handle {
        let mut datums: Vec<Datum> = Vec::with_capacity(handle_idx.len());
        for (i, &idx) in handle_idx.iter().enumerate() {
            let mut rtp = shared.handle_cols[i]
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::Null));
            // Go: a version-0 common handle's index entry stores a string
            // column under the binary collation.
            if tidb_datatype::new_collation_enabled()
                && shared.table_info.common_handle_version == 0
                && rtp.eval_type() == tidb_datatype::EvalType::String
                && !tidb_mysql::types::has_binary_flag(rtp.flags() as usize)
                && tp == GetHandleType::FromIndex
            {
                rtp.set_collation_name("binary");
            }
            datums.push(row.get_datum(idx, &rtp));
        }
        if let Some(pk) = &shared.primary_key_index {
            tidb_tablecodec::truncate_index_values(&shared.codec_table_info, pk, &mut datums)
                .map_err(|error| ExecError::Internal(error.to_string().into()))?;
        }
        let zone = tidb_exprctx::Columns::time_zone(&shared.ctx);
        let encoded = tidb_codec::encode_key_in_timezone(&zone, &datums)
            .map_err(|error| ExecError::Internal(error.to_string().into()))?;
        Handle::Common(
            tidb_txnkv::CommonHandle::new(encoded).map_err(|error| ExecError::Internal(error.to_string().into()))?,
        )
    } else {
        let idx = handle_idx.first().copied().unwrap_or(0);
        Handle::Int(tidb_txnkv::IntHandle::new(row.get_int64(idx)))
    };
    let partition_handle = match tp {
        GetHandleType::FromIndex => shared.partition_handle_from_index,
        GetHandleType::FromTable => shared.partition_handle_from_table,
    };
    if partition_handle {
        let pid = row.get_int64(row.len() - 1);
        handle = Handle::Partition(tidb_txnkv::PartitionHandle::new(pid, Some(handle)));
    }
    Ok(handle)
}

/// Go `tableWorker.executeTask`: the rows of `handles` through a table read
/// (`buildTableReaderFromHandles`), sorted back into index order when the
/// read keeps order.
fn execute_table_task(
    shared: &Shared,
    table_request: &tidb_proto::tipb::DagRequest,
    task: LookupTask,
) -> Result<TaskRows, ExecError> {
    let LookupTask { id, mut handles, handles_cap, partition, done: _ } = task;
    // Go `execTableTask`: the task's tracker under `memTracker`.
    let mem_tracker = tidb_memory::Tracker::new(id, -1);
    mem_tracker.attach_to(&shared.mem_tracker);
    let index_order: Option<tidb_txnkv::HandleMap<usize>> = shared.keep_order.then(|| {
        let mut order = tidb_txnkv::HandleMap::new();
        for (i, handle) in handles.iter().enumerate() {
            order.set(handle.clone(), i);
        }
        order
    });
    // Go `buildTableReader`: the task's `TableReaderExecutor` over its
    // handles (`buildTableReaderFromHandles(..., canReorderHandles=true)`).
    let physical_table_id = partition.unwrap_or(shared.physical_table_id);
    let mut reader = crate::table_reader::TableReaderExecutor::build_table_reader_from_handles(
        crate::table_reader::TableReaderForHandles {
            meta: shared.table_reader_meta.clone(),
            ctx: &shared.ctx,
            client: Arc::clone(&shared.client),
            table_plan: shared.table_plans.last().ok_or_else(|| ExecError::Internal("the lookup has no table plans".into()))?,
            plan_ids: shared.table_plan_ids.clone(),
            dag: table_request.clone(),
            physical_table_id,
            is_common_handle: shared.table_info.is_common_handle,
            columns: shared.columns.clone(),
            start_ts: shared.start_ts,
            net_data_size: shared.avg_row_size * handles.len() as f64,
        },
        &mut handles,
    )?;
    // Go `executeTask`'s first charge: `cap(task.handles)`, the table
    // reader's `memUsage()`, each handle, `indexOrder` and `idxRows`.
    let mut mem_usage = (handles_cap * SIZEOF_INTERFACE) as i64
        + reader.mem_usage()
        + handles.iter().map(Handle::mem_usage).sum::<u64>() as i64
        + index_order.as_ref().map_or(0, tidb_txnkv::HandleMap::mem_usage);
    mem_tracker.consume(mem_usage);
    let mut chunks = Vec::new();
    let mut order = Vec::new();
    // Go `task.rows = make([]chunk.Row, 0, handleCnt)`.
    let mut rows_cap = handles.len();
    loop {
        // Go `exec.TryNewCacheChunk(tableReader)` and `exec.Next`.
        let mut chk = shared.table_reader_meta.new_chunk();
        let next = reader.next(&mut chk);
        tidb_executor_internal_exec::ruv2::record_leaf_next(
            shared.table_reader_ruv2.as_ref(),
            chk.num_rows(),
            shared.table_reader_meta.schema().columns.len(),
        );
        if let Err(error) = next {
            let _ = reader.close();
            return Err(error);
        }
        if chk.num_rows() == 0 {
            break;
        }
        // Go: each filled chunk's `MemoryUsage`.
        let chunk_usage = chk.memory_usage();
        mem_usage += chunk_usage;
        mem_tracker.consume(chunk_usage);
        let index = chunks.len();
        order.extend((0..chk.num_rows()).map(|row| (index, row)));
        rows_cap = go_append_cap(order.len(), rows_cap, SIZEOF_CHUNK_ROW);
        chunks.push(chk);
    }
    let _ = reader.close();
    let rows_usage = (rows_cap * SIZEOF_CHUNK_ROW) as i64;
    mem_usage += rows_usage;
    mem_tracker.consume(rows_usage);
    // Go `executeTask`: a handle the table did not answer is a lookup
    // inconsistency (`consistency.Reporter.ReportLookupInconsistent`).
    let handle_cnt = handles.len();
    if handle_cnt != order.len() && !shared.ctx.weak_consistency() && shared.table_plans.len() == 1 {
        return Err(ExecError::from(crate::consistency_errors::ERR_LOOKUP_INCONSISTENT.gen_with_stack_by_args(&[
            shared.table_info.name.original().into(),
            shared.index_name.as_str().into(),
            (handle_cnt as i64).into(),
            (order.len() as i64).into(),
        ])));
    }
    if let Some(index_order) = index_order {
        // Go `task.rowIdx = make([]int, 0, len(task.rows))`.
        let row_idx_usage = (order.len() * SIZEOF_INT) as i64;
        mem_usage += row_idx_usage;
        mem_tracker.consume(row_idx_usage);
        // Go `sort.Sort(task)` by each row's index position.
        let mut keyed = Vec::with_capacity(order.len());
        for (chunk, row) in order {
            let handle = get_handle(
                shared,
                chunks[chunk].get_row(row),
                &shared.handle_idx,
                GetHandleType::FromTable,
            )?;
            let position = index_order.get(&handle).copied().unwrap_or(usize::MAX);
            keyed.push((position, (chunk, row)));
        }
        keyed.sort_by_key(|(position, _)| *position);
        order = keyed.into_iter().map(|(_, at)| at).collect();
    }
    Ok(TaskRows { chunks, order, mem_usage, mem_tracker })
}

/// One index select result as an executor, which the merge reads. Closing
/// it adds its cop tasks of the index scan to the lookup's count (Go
/// `RuntimeStatsColl.GetCopCountAndRows(idxPlans[0].ID())`).
struct SelectResultExec {
    meta: ExecutorMeta,
    result: tidb_distsql::select_response::SelectResponseIter,
    shared: Arc<Shared>,
}

impl SelectResultExec {
    fn new(meta: ExecutorMeta, result: tidb_distsql::select_response::SelectResponseIter, shared: Arc<Shared>) -> Self {
        Self { meta, result, shared }
    }
}

impl Executor for SelectResultExec {
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let required = req.required_rows().max(1);
        if !self
            .result
            .fill_chunk_with_required_rows(required, req)
            .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)))?
        {
            req.reset();
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        let closed = self
            .result
            .close()
            .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)));
        let (requests, rows) = self.result.runtime_stats().get_cop_count_and_rows(self.shared.index_scan_plan_id);
        let mut count = self.shared.index_cop_count_and_rows.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        count.0 += requests;
        count.1 += rows;
        closed
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Go `indexUsageReporter.reportCopIndexUsageForTable(e.table, e.index.ID,
/// e.idxPlans[0].ID())`'s source.
impl tidb_executor_internal_exec::table_access::TableAccess for IndexLookUpExecutor {
    fn cop_count_and_rows(&self) -> Option<(u64, u64)> {
        Some(*self.shared.index_cop_count_and_rows.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
    }
}

impl Executor for IndexLookUpExecutor {
    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        Some(self)
    }

    fn union_scan_reader(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }

    /// Go `IndexLookUpExecutor.Open`: `memTracker` attached to the
    /// statement's, the key ranges built (charging it), then `open`'s reset
    /// and attach again; the workers start at the first `Next`.
    fn open(&mut self) -> Result<(), ExecError> {
        self.shared.mem_tracker.reset();
        self.shared.mem_tracker.attach_to(&self.stmt_mem_tracker);
        self.kv_ranges = self.build_table_key_ranges()?;
        if self.dummy {
            return Ok(());
        }
        self.open_inner()
    }

    /// Go `IndexLookUpExecutor.Next`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.dummy {
            req.reset();
            return Ok(());
        }
        if !self.worker_started {
            self.start_workers(req.required_rows())?;
        }
        req.reset();
        loop {
            if !self.get_result_task()? {
                return Ok(());
            }
            let Some((rows, cursor)) = self.result_curr.as_mut() else {
                return Ok(());
            };
            let num_to_append = (rows.order.len() - *cursor).min(req.required_rows() - req.num_rows());
            for &(chunk, row) in &rows.order[*cursor..*cursor + num_to_append] {
                req.append_row(rows.chunks[chunk].get_row(row));
            }
            *cursor += num_to_append;
            if req.is_full() {
                return Ok(());
            }
        }
    }

    /// Go `IndexLookUpExecutor.Close`.
    fn close(&mut self) -> Result<(), ExecError> {
        self.shared.finished.store(true, Ordering::Release);
        // Go `channel.Clear(e.resultCh)`: drain so the index worker ends.
        if let Some(result_ch) = self.result_ch.take() {
            while result_ch.recv().is_ok() {}
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        self.worker_started = false;
        self.result_curr = None;
        // Go `RegisterStats(e.ID(), e.stats)`.
        if let Some(stats) = self.shared.stats.as_ref() {
            stats.source.register_stats(stats.plan_id, Box::new(stats.stats.snapshot()));
        }
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}
