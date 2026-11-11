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

//! Go `executorBuilder.build`: instantiate a retained physical SELECT tree.
//!
//! Fresh plans and plans rebuilt by `RebuildPlan4CachedPlan` enter the same
//! constructor switch. This module must not inspect the SQL AST to choose
//! access, aggregation, join, sort, or reader policy; the AST is used only
//! for client-visible result-column labels.

use tidb_planner::stmt_ctx::PlannerStmtCtx as _;
use crate::mem_table_retrieval::StmtMemTableRetrieval as _;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::access_path::{
    HandleOutputColumn, HandleSourceExec, IndexJoinLookupExec, IndexRangeSourceExec, LookupObject,
    LookupProbeBoundOp, LookupProbePart, LookupProbeTail, UniqueIndexPointSourceExec,
};
use tidb_executor_join::index_lookup::{LookupProbeBound, SharedIndexJoinProbes};
use crate::apply::NestedLoopApplyExec;
use tidb_executor_internal_exec::index_usage_reporter::{CopIndexUsageExec, PointCommand, PointIndexUsageExec};
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_executor_aggfuncs::aggfuncs::AggFunc;
use tidb_executor_aggfuncs::aggfuncs::AggKind;
use tidb_executor_join::executor::{HashJoinV2Executor, HashJoinV2Plan};
use crate::index_merge_reader::{
    ExecutorPartialHandleSource, HandleRef, IndexMergeReaderExecutor, MergeByItem, PartialHandleBatch,
    PartialHandleColumns, PartialHandleSource, PartitionedHandleSource,
    PushedDownLimit as ExecutorPushedDownLimit,
};
use tidb_executor_join::join::{IndexLookupPlan, IndexLookupSource, IndexProbeKeyDomain, JoinExec, JoinKind};
use tidb_executor_join::joiner::{new_joiner, JoinType as JoinerType, JoinerChunkSizes};
use crate::kv_table::TableScanExec;
use tidb_table::tables::TableHandle;
use tidb_table::tables::RowDecodeContext;
use crate::limit::LimitExec;
use crate::mem_table::MemTableSourceExec;
use crate::physical_cte::{
    CteExec, CteProducer, CteTableReaderExec, SharedCteProducer, SharedCteStorage,
};
use crate::projection::ProjectionExec;
use tidb_distsql::remote_scan::PushdownStatementContext;
use tidb_distsql::remote_scan::PushdownPartialAggregate;
use tidb_distsql::remote_scan::PushdownAggregateKind;
use tidb_distsql::remote_scan::PushdownAggregateFunction;
use tidb_distsql::remote_scan::CopPlanKind;
use tidb_distsql::remote_scan::CopPlanId;
use crate::select_lock::{SelectLockExec, SelectedRecordKey};
use crate::selection::SelectionExec;
use tidb_executor_sortexec::{SortByItem, SortExec};
use tidb_executor_internal_exec::table_access::TableAccess;
use crate::table_dual::TableDualExec;
use crate::table_sample::{SampleOutputColumn, TableSampleExecutor};
use tidb_executor_sortexec::topn::TopNExec;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expression_aggregation::{AggFuncDesc, AggFunctionMode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
#[cfg(test)]
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;
use tidb_hack::GoCaseCompare;
use tidb_planner_core_base::LogicalJoinType;
use tidb_planner_core_operator_physicalop::{
    BatchPointGetPlan, PhysicalIndexScan, PhysicalMemTable, PhysicalPlan, PointGetPlan,
    PhysicalTableSample, PhysicalTableScan,
};

use super::{SessionExtendedInfoSchema, DriverError};

struct CteBuildSlot {
    result: SharedCteStorage,
    iter_in: SharedCteStorage,
    producer: Option<SharedCteProducer>,
}

/// Which view of a table's storage the reader leaves being built read.
///
/// Go's `TableReader`/`IndexReader`/`IndexLookUp`/`IndexMerge` executors send
/// coprocessor requests, which read the statement's snapshot and never the
/// transaction's membuffer; the buffered writes reach a query only through
/// the `UnionScanExec` above such a reader, whose mem reader walks the
/// membuffer over the reader's key ranges.
#[derive(Clone, Debug, Default)]
enum ReaderStorage {
    /// The statement's snapshot alone, where the backend separates it from
    /// the transaction's buffer ([`tidb_kv::storage::TableStorage::snapshot_reader`]).
    #[default]
    Snapshot,
    /// The backend's merged view: below a union scan Go's executor is not
    /// built for (an index-join inner side), whose reader merges the buffered
    /// rows itself.
    Merged,
    /// A `UnionScanExec`'s mem reader: the buffered entries alone.
    MemBuffer(Arc<tidb_kv::mem_buffer::MemBufferSnapshot>),
}

impl ReaderStorage {
    /// The store a reader leaf of this kind reads `table` through, and
    /// whether it was separated from the transaction's buffered writes.
    fn table(
        &self,
        ctx: &crate::StmtContext,
        table: Arc<crate::TableCommon>,
    ) -> (Arc<crate::TableCommon>, Box<dyn tidb_kv::storage::TableStorage>, bool) {
        let statement_store = crate::statement_store(ctx);
        let store = match self {
            ReaderStorage::Snapshot => statement_store.snapshot_reader(),
            ReaderStorage::Merged => None,
            ReaderStorage::MemBuffer(snapshot) => Some(Box::new(
                tidb_store_driver_txn::cluster_storage::MemBufferStorage::new(Arc::clone(snapshot)),
            )
                as Box<dyn tidb_kv::storage::TableStorage>),
        };
        match store {
            Some(store) => (table, store, true),
            None => (table, statement_store, false),
        }
    }

    /// `statement` for a reader leaf over a table [`Self::table`] answered
    /// for.
    fn statement(
        separated: bool,
        statement: tidb_distsql::remote_scan::PushdownStatementContext,
    ) -> tidb_distsql::remote_scan::PushdownStatementContext {
        if separated {
            statement.without_staged_writes()
        } else {
            statement
        }
    }

    /// Whether the leaves are a mem reader, which Go builds no executor for:
    /// no index usage is reported for it.
    const fn is_mem_buffer(&self) -> bool {
        matches!(self, ReaderStorage::MemBuffer(_))
    }
}

#[derive(Default)]
struct BuildState {
    /// See [`ReaderStorage`].
    reader_storage: ReaderStorage,
    /// Building a `UnionScanExec`'s mem reader: Go's mem readers are not
    /// executors, so the copy records no `exec.Next` regions and no runtime
    /// statistics.
    untraced: bool,
    /// Receiver substitutions while building a shuffle worker subtree.
    shuffle_receivers: HashMap<i32, Box<dyn Executor>>,
    cte_slots: HashMap<i32, CteBuildSlot>,
    /// Each index-merge partial's produced handles, which the builder's tests
    /// read to pin how far each partial fetched.
    #[cfg(test)]
    index_merge_partial_rows: HashMap<i32, tidb_executor_internal_exec::RowCount>,
    executor_runtime_stats_source: Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>>,
    /// The coprocessor `Limit offset:o, count:c` a covering `IndexReader`'s
    /// `IndexPlan` carries above its `IndexScan`, as `o + c`. Go pushes it into
    /// the region request; the local cursor has no request, so the scan build
    /// consumes it here. Set only while the reader's own subtree is built.
    index_scan_limit: Option<u64>,
    /// A covering `IndexReader`'s `OutputColumns`, when its `IndexPlan` is the
    /// bare `IndexScan`: Go's `dagReq.OutputOffsets` decode only those, in that
    /// order, so the scan build narrows its source to them. Set only while
    /// the reader's own subtree is built.
    index_output_columns: Option<Vec<tidb_expr::column::Column>>,
    /// The reader's `OutputColumns` when its pushed plan is a Limit over a
    /// pushed Selection: the in-reader Limit narrows its source to them once
    /// the Selection is in, as the request's `OutputOffsets` do.
    index_post_filter_columns: Option<Vec<tidb_expr::column::Column>>,
    /// The reader's pushed plan is a Projection of columns over its scan,
    /// which the request lowers as a coprocessor Projection.
    index_cop_projection: bool,
    /// Go `getPhysicalPlanIDs(plans)` and `rootPlanID` of the reader whose
    /// pushed subtree is being built: the plan IDs its coprocessor
    /// executors report their summaries under.
    cop_chain: Option<CopChain>,
    /// Go `TableReaderExecutor.memTracker` of an index merge's partial table
    /// worker, which its select result charges each response to.
    reader_mem_tracker: Option<Arc<tidb_memory::Tracker>>,
    /// An index merge partial plan is being built: Go's partial worker reads
    /// its request's select result directly, its Limit in the DAG only.
    index_merge_partial: bool,
    reader_net_bytes: Option<f64>,
    /// One task-local replacement for the retained index-merge table scan.
    table_task_source: Option<(i32, Box<dyn Executor>)>,
    /// Reader subplans use coprocessor expression semantics.
    in_reader: bool,
    /// Go `executorStats`' Top SQL registration of the statement being built.
    top_profiling: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Go `execdetails.RUV2MetricsFromContext`: the statement's RU v2
    /// metrics its executors' `exec.Next` records into.
    ruv2_metrics: Option<Arc<tidb_execdetails::ruv2_metrics::RUV2Metrics>>,
    /// Whether a `PhysicalProjection` may run Go's projection workers. Go
    /// keeps every projection serial inside INSERT/UPDATE/DELETE (its
    /// `inUpdateStmt`/`inDeleteStmt`/`inInsertStmt` builder flags, because
    /// those statements write the membuffer the workers would race).
    projection_workers_allowed: bool,
    /// The range group a grouped-range reader is being built for (Go
    /// `GroupedRanges`): the one scan carrying `group_by_col_idxs` reads
    /// these ranges instead of its own.
    range_group: Option<tidb_ranger::types::Ranges>,
    /// Go `executorBuilder.hasLock`: a `SELECT ... FOR UPDATE` below keeps the
    /// projections above it serial for the same reason.
    has_lock: bool,
}

/// Go BasicRuntimeStats' lifecycle counters, which a write's root adds up
/// for its one `exec.Next` record.
#[derive(Clone, Copy, Default)]
struct PhysicalCallStats {
    open: Duration,
    next: Duration,
    close: Duration,
    loops: u64,
}

/// Go `UpdateExec` / `DeleteExec`'s own executor state, which this tier's
/// write drivers carry around the source they stream: the `memTracker` its
/// `Open` creates (`memory.NewTracker(e.ID(), -1)` attached to the statement
/// tracker) and, when runtime stats are collected, the `BasicRuntimeStats`
/// `exec.Open` / `exec.Next` / `exec.Close` record for the plan.
///
/// Building it is Go `Open`'s tracker creation and dropping it `Close`'s
/// deferred `memTracker.ReplaceBytesUsed(0)`; each is timed as that phase.
pub(crate) struct DmlExec {
    pub(crate) mem_tracker: tidb_variable::mem_quota::WriteMemory,
    /// Go `e.MaxChunkSize()`.
    pub(crate) max_chunk_size: usize,
    /// Go `e.AllocPool`, which `newDMLChildChunk` allocates the child chunk
    /// from.
    pub(crate) alloc_pool: Option<Arc<dyn tidb_chunk::alloc::Allocator>>,
    calls: Option<Arc<Mutex<PhysicalCallStats>>>,
    /// Go `exec.Open` / `exec.Next` / `exec.Close`'s `BasicRuntimeStats` of
    /// the root in the statement's `RuntimeStatsColl`, which receives the
    /// one `Next` [`Self::calls`] adds up when the executor is dropped.
    runtime_stats: Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStats>>,
    /// The statement's `RuntimeStatsColl` and the root's plan id, which the
    /// executor's own stats groups register under.
    runtime_stats_source: Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>>,
    plan_id: i64,
    /// Go `InsertValues.stats` of an `InsertExec` / `ReplaceExec` whose
    /// runtime stats are collected (`collectRuntimeStatsEnabled`).
    insert: Option<Arc<Mutex<super::dml::InsertRuntimeStat>>>,
    /// Go `UpdateExec.stats`, likewise.
    update: Option<Arc<super::dml::UpdateRuntimeStats>>,
}

impl DmlExec {
    /// Go `executorBuilder.buildUpdate` / `buildDelete` + `Open`'s tracker:
    /// the executor for the `Update_N` / `Delete_N` root `plan`, its basic
    /// stats registered under the plan when `runtime` collects them.
    pub(crate) fn new(
        plan: &PhysicalPlan,
        ctx: &crate::StmtContext,
    ) -> Self {
        let runtime_stats = ctx
            .executor_runtime_stats_source()
            .and_then(|source| source.for_executor(i64::from(plan.id())));
        let calls = runtime_stats
            .is_some()
            .then(|| Arc::new(Mutex::new(PhysicalCallStats::default())));
        // Go `Open`'s `memory.NewTracker(e.ID(), -1)` + `AttachTo`.
        let start = Instant::now();
        let mem_tracker = ctx.statement_memory().write_accountant(i64::from(plan.id()));
        if let Some(calls) = &calls {
            calls.lock().unwrap().open += start.elapsed();
        }
        Self {
            mem_tracker,
            max_chunk_size: ctx.executor_chunk_sizes().1,
            alloc_pool: ctx.chunk_allocator(),
            calls,
            runtime_stats,
            runtime_stats_source: ctx.executor_runtime_stats_source().cloned(),
            plan_id: i64::from(plan.id()),
            insert: None,
            update: None,
        }
    }

    /// [`Self::new`] for an `InsertExec` / `ReplaceExec`, whose collected
    /// stats also carry Go's `InsertRuntimeStat`.
    pub(crate) fn new_insert(
        plan: &PhysicalPlan,
        ctx: &crate::StmtContext,
    ) -> Self {
        let mut dml = Self::new(plan, ctx);
        // Go `collectRuntimeStatsEnabled`: the statement collects runtime stats.
        if dml.runtime_stats.is_some() {
            dml.insert = Some(Arc::new(Mutex::new(super::dml::InsertRuntimeStat::default())));
        }
        dml
    }

    /// [`Self::new`] for an `UpdateExec`, whose collected stats also carry
    /// Go's `updateRuntimeStats`.
    pub(crate) fn new_update(
        plan: &PhysicalPlan,
        ctx: &crate::StmtContext,
    ) -> Self {
        let mut dml = Self::new(plan, ctx);
        // Go `collectRuntimeStatsEnabled`: the statement collects runtime stats.
        if dml.runtime_stats.is_some() {
            dml.update = Some(Arc::new(super::dml::UpdateRuntimeStats::default()));
        }
        dml
    }

    /// The `updateRuntimeStats` this executor's runtime stats carry.
    pub(crate) fn update_stats(&self) -> Option<&Arc<super::dml::UpdateRuntimeStats>> {
        self.update.as_ref()
    }

    /// The `InsertRuntimeStat` this executor's runtime stats carry.
    pub(crate) fn insert_stats(&self) -> Option<&Arc<Mutex<super::dml::InsertRuntimeStat>>> {
        self.insert.as_ref()
    }

    /// Go `exec.Open(ctx, e)`: the executor's `Open`, which opens its child.
    pub(crate) fn open<T>(&self, open: impl FnOnce() -> T) -> T {
        self.record(open, |calls, elapsed| calls.open += elapsed)
    }

    /// Go `exec.Next(ctx, e, req)`: (part of) the one `Next` that reads the
    /// child and writes every row. A driver that opens or closes the child
    /// inside the region -- one that reads its whole source before writing --
    /// has that time recorded by [`Self::open`] / [`Self::close`], not here.
    pub(crate) fn next<T>(&self, next: impl FnOnce() -> T) -> T {
        let Some(calls) = &self.calls else {
            return next();
        };
        let lifecycle = |calls: &PhysicalCallStats| calls.open + calls.close;
        let before = lifecycle(&calls.lock().unwrap());
        let start = Instant::now();
        let result = next();
        let elapsed = start.elapsed();
        let mut calls = calls.lock().unwrap();
        let nested = lifecycle(&calls).saturating_sub(before);
        calls.next += elapsed.saturating_sub(nested);
        calls.loops = 1;
        result
    }

    /// Go `exec.Close(e)`: the executor's `Close`, which closes its child.
    pub(crate) fn close<T>(&self, close: impl FnOnce() -> T) -> T {
        self.record(close, |calls, elapsed| calls.close += elapsed)
    }

    /// The root's open, its one `Next` (no rows: a write returns none) and
    /// its close, into the statement's `RuntimeStatsColl`, when the executor
    /// is dropped (its close time includes detaching its tracker).
    fn flush_runtime_stats(&self) {
        let (Some(stats), Some(calls)) = (&self.runtime_stats, &self.calls) else {
            return;
        };
        let calls = *calls.lock().unwrap();
        stats.record_open(calls.open);
        stats.record_next(calls.next, 0);
        stats.record_close(calls.close);
        // Go `InsertExec.Close` / `UpdateExec.Close`'s deferred
        // `RuntimeStatsColl.RegisterStats(e.ID(), e.stats)`.
        let Some(source) = self.runtime_stats_source.as_ref() else {
            return;
        };
        if let Some(insert) = &self.insert {
            source.register_stats(
                self.plan_id,
                Box::new(super::dml::RegisteredInsertRuntimeStat {
                    stat: Arc::clone(insert),
                    basic_time: calls.open + calls.next + calls.close,
                }),
            );
        }
        if let Some(update) = &self.update {
            source.register_stats(self.plan_id, Box::new(super::dml::RegisteredUpdateRuntimeStats(Arc::clone(update))));
        }
    }

    fn record<T>(&self, run: impl FnOnce() -> T, add: impl FnOnce(&mut PhysicalCallStats, Duration)) -> T {
        let Some(calls) = &self.calls else {
            return run();
        };
        let start = Instant::now();
        let result = run();
        add(&mut calls.lock().unwrap(), start.elapsed());
        result
    }
}

impl Drop for DmlExec {
    fn drop(&mut self) {
        let start = Instant::now();
        self.mem_tracker.replace_bytes_used(0);
        if let Some(calls) = &self.calls {
            calls.lock().unwrap().close += start.elapsed();
        }
        self.flush_runtime_stats();
    }
}

/// A reader's pushed plan nodes (leaf first) and its select-result root.
type CopChain = (Vec<CopPlanId>, i64);

/// Go `getPhysicalPlanIDs` over `FlattenTreePushDownPlan`: the plan IDs of a
/// reader's pushed subtree in post-order (the order its executors take in the
/// DAG; a `LocalIndexLookUp` follows its index side and its table scan), each
/// with the executor kind the node lowers to.
fn cop_plan_chain(root: &PhysicalPlan) -> Vec<CopPlanId> {
    fn flatten_plan_with_postorder_traversal(plan: &PhysicalPlan, chain: &mut Vec<CopPlanId>) {
        for child in plan.children() {
            flatten_plan_with_postorder_traversal(child, chain);
        }
        let kind = match plan {
            PhysicalPlan::TableScan(_) => CopPlanKind::TableScan,
            PhysicalPlan::IndexScan(_) => CopPlanKind::IndexScan,
            PhysicalPlan::Selection(_) => CopPlanKind::Selection,
            PhysicalPlan::Limit(_) => CopPlanKind::Limit,
            PhysicalPlan::TopN(_) => CopPlanKind::TopN,
            PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_) => CopPlanKind::Aggregation,
            PhysicalPlan::Projection(_) => CopPlanKind::Projection,
            _ => CopPlanKind::Other,
        };
        chain.push(CopPlanId {
            kind,
            id: plan.id() as isize,
        });
    }
    let mut chain = Vec::new();
    flatten_plan_with_postorder_traversal(root, &mut chain);
    chain
}

/// Go `RuntimeStatsColl.RegisterStats(id, &RuntimeStatsWithConcurrencyInfo{..})`
/// with one `name:num` (`name:OFF` for `num == 0`).
fn register_concurrency_info(ctx: &crate::StmtContext, plan: &PhysicalPlan, name: &str, num: usize) {
    let Some(source) = ctx.executor_runtime_stats_source() else {
        return;
    };
    let stats = tidb_execdetails::RuntimeStatsWithConcurrencyInfo::default();
    stats.set_concurrency_info(vec![tidb_execdetails::ConcurrencyInfo::new(
        name,
        i64::try_from(num).unwrap_or(i64::MAX),
    )]);
    source.register_stats(i64::from(plan.id()), Box::new(stats));
}

impl BuildState {
    /// Go `PointGetExecutor.Init` / `buildBatchPointGet`: with
    /// `RuntimeStats() != nil` (EXPLAIN ANALYZE, or execution-info collection),
    /// a point reader owns a `SnapshotRuntimeStats` its snapshot collects
    /// into, registered on the plan's runtime stats.
    fn point_snapshot_runtime_stats(&self) -> Option<Arc<tikv_client::SnapshotRuntimeStats>> {
        self.executor_runtime_stats_source
            .is_some()
            .then(|| Arc::new(tikv_client::SnapshotRuntimeStats::default()))
    }

    /// Go `e.RuntimeStats() != nil`: an apply publishes its cache use when the
    /// statement collects stats.
    fn apply_runtime_sink(&self) -> Option<crate::apply::native::ApplyRuntimeSink> {
        self.executor_runtime_stats_source
            .is_some()
            .then(|| Arc::new(Mutex::new(None)))
    }

    fn meter(
        &mut self,
        plan: &PhysicalPlan,
        executor: Box<dyn Executor>,
        memory: crate::StatementMemory,
    ) -> Box<dyn Executor> {
        let next_region = go_executor_next_region(plan);
        self.meter_as(plan, executor, memory, next_region)
    }

    /// [`Self::meter`] for an executor whose Go `exec.Next` region is
    /// `next_region` (`<Go type>.Next`), `None` for a node Go builds no
    /// executor for.
    fn meter_as(
        &mut self,
        plan: &PhysicalPlan,
        mut executor: Box<dyn Executor>,
        memory: crate::StatementMemory,
        next_region: Option<&'static str>,
    ) -> Box<dyn Executor> {
        // A coprocessor plan's nodes run in TiKV for Go: no TiDB executor
        // and no `exec.Next` region; neither has a union scan's mem reader.
        // Go `exec.Next`'s RU v2 recording, for every executor Go builds.
        let ruv2 = (!self.in_reader)
            .then(|| next_region.or_else(|| go_executor_ruv2_type(plan)))
            .flatten()
            .and_then(|region| {
                tidb_executor_internal_exec::ruv2::Ruv2NextCache::new(
                    region.trim_end_matches(".Next"),
                    self.ruv2_metrics.as_ref(),
                )
            });
        // A coprocessor plan node is no executor of Go's: its `Next` takes no
        // part in `exec.Next`'s accounting.
        let child_count = (!self.in_reader).then(|| plan.children().len());
        let next_region = next_region.filter(|_| !self.in_reader && !self.untraced);
        let plan_id = i64::from(plan.base().base.id());
        // A pushed node runs in TiKV for Go: it reports cop stats only.
        let runtime_stats = (plan_id > 0 && !self.untraced && !self.in_reader)
            .then(|| {
                self.executor_runtime_stats_source
                    .as_ref()
                    .and_then(|source| source.for_executor(plan_id))
            })
            .flatten();
        let trace = next_region.and_then(|_| GoExecutorTrace::for_plan(plan));
        executor = Box::new(SqlKillerCheckExec {
            child: executor,
            memory,
            runtime_stats,
            next_region,
            trace,
            top_profiling: self.top_profiling.clone(),
            ruv2,
            child_count,
        });
        executor
    }
}

/// Applies Go `exec.Next`'s before/after SQL-killer checks to every operator
/// constructed from the retained physical tree. Child executors call their
/// trait methods directly, so each built node needs its own boundary.
struct SqlKillerCheckExec {
    child: Box<dyn Executor>,
    memory: crate::StatementMemory,
    runtime_stats: Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStats>>,
    /// Go `exec.Next`'s `tracing.StartRegionEx(ctx, <type>.Next)` for the
    /// executor Go builds for the node.
    next_region: Option<&'static str>,
    /// The regions and events the node's Go executor adds itself.
    trace: Option<Box<GoExecutorTrace>>,
    /// Go `exec.Next`'s `RegisterSQLAndPlanInExecForTopProfiling`.
    top_profiling: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Go `exec.Next`'s RU v2 recording (`ruv2NextCacheState`).
    ruv2: Option<tidb_executor_internal_exec::ruv2::Ruv2NextCache>,
    /// Go `len(e.AllChildren())`, deciding whether its children report into
    /// a local accumulator; `None` for a node Go builds no executor for,
    /// whose `Next` the accounting passes through.
    child_count: Option<usize>,
}

/// The trace a node's Go executor adds to its own `Open`, `Next` and
/// `Close`.
#[derive(Default)]
struct GoExecutorTrace {
    /// The region its `Open` opens and whether the children's opens run
    /// under it (`StartRegionEx`): `TableReaderExecutor.Open`,
    /// `UnionScanExec.Open`, `TableSampleExecutor.Open`.
    open_region: Option<(&'static str, bool)>,
    /// The region its `Open` opens after its child's: `UnionScanExec.open`'s
    /// `buildMem*Reader` over the child reader. (Here the transaction's
    /// buffered rows are merged by the storage the reader reads.)
    after_open_region: Option<&'static str>,
    /// The `logutil.Eventf` its `Next` logs on every call: a table reader's
    /// `table scan table: <name>, range: <ranges>` (`%v` of its
    /// `[]*ranger.Range`), built only for a traced statement.
    next_event: Option<String>,
    /// `LimitExec.Close`'s `limitExec.Close` span, opened under the span
    /// its `Open` ran in when the close took over a millisecond.
    limit_close: bool,
    open_span: Option<tidb_tracing::Span>,
}

impl GoExecutorTrace {
    fn for_plan(plan: &PhysicalPlan) -> Option<Box<Self>> {
        let trace = match plan {
            PhysicalPlan::TableReader(reader) => Self {
                open_region: Some(("TableReaderExecutor.Open", true)),
                next_event: tidb_tracing::current_span()
                    .and_then(|_| reader.table_scan_plan())
                    .map(|scan| {
                        let ranges = scan
                            .ranges
                            .iter()
                            .map(tidb_ranger::types::Range::to_display_string)
                            .collect::<Vec<_>>()
                            .join(" ");
                        format!(
                            "table scan table: {}, range: [{ranges}]",
                            scan.table_name.to_lowercase()
                        )
                    }),
                ..Self::default()
            },
            PhysicalPlan::UnionScan(_) => {
                // A Selection the builder put over the reader for virtual
                // columns is looked through.
                let mut reader = plan.children().first();
                if let Some(PhysicalPlan::Selection(_)) = reader {
                    reader = reader.and_then(|selection| selection.children().first());
                }
                Self {
                    open_region: Some(("UnionScanExec.Open", true)),
                    after_open_region: Some(match reader {
                        Some(PhysicalPlan::IndexReader(_)) => "buildMemIndexReader",
                        Some(PhysicalPlan::IndexLookUpReader(_)) => "buildMemIndexLookUpReader",
                        Some(PhysicalPlan::IndexMergeReader(_)) => "buildMemIndexMergeReader",
                        _ => "buildMemTableReader",
                    }),
                    ..Self::default()
                }
            }
            PhysicalPlan::TableSample(_) => Self {
                open_region: Some(("TableSampleExecutor.Open", false)),
                ..Self::default()
            },
            PhysicalPlan::Limit(_) => Self {
                limit_close: true,
                ..Self::default()
            },
            _ => return None,
        };
        Some(Box::new(trace))
    }
}

/// Go `reflect.TypeOf(e).String() + ".Next"` for the executor
/// `executorBuilder.build` makes from `plan`; `None` for a node Go builds no
/// executor of its own for.
fn go_executor_next_region(plan: &PhysicalPlan) -> Option<&'static str> {
    Some(match plan {
        PhysicalPlan::Projection(_) => "*executor.ProjectionExec.Next",
        PhysicalPlan::Selection(_) => "*executor.SelectionExec.Next",
        PhysicalPlan::TableDual(_) => "*executor.TableDualExec.Next",
        PhysicalPlan::PointGet(_) => "*executor.PointGetExecutor.Next",
        PhysicalPlan::BatchPointGet(_) => "*executor.BatchPointGetExec.Next",
        PhysicalPlan::TableReader(_) => "*executor.TableReaderExecutor.Next",
        PhysicalPlan::IndexReader(_) => "*executor.IndexReaderExecutor.Next",
        PhysicalPlan::IndexLookUpReader(_) => "*executor.IndexLookUpExecutor.Next",
        PhysicalPlan::IndexMergeReader(_) => "*executor.IndexMergeReaderExecutor.Next",
        PhysicalPlan::Limit(_) => "*executor.LimitExec.Next",
        PhysicalPlan::TopN(_) => "*sortexec.TopNExec.Next",
        PhysicalPlan::Sort(_) => "*sortexec.SortExec.Next",
        PhysicalPlan::HashAgg(_) => "*aggregate.HashAggExec.Next",
        PhysicalPlan::StreamAgg(_) => "*aggregate.StreamAggExec.Next",
        PhysicalPlan::HashJoin(_) => "*join.HashJoinV1Exec.Next",
        PhysicalPlan::MergeJoin(_) => "*join.MergeJoinExec.Next",
        PhysicalPlan::Apply(_) => "*join.NestedLoopApplyExec.Next",
        PhysicalPlan::UnionAll(_) => "*unionexec.UnionExec.Next",
        PhysicalPlan::MaxOneRow(_) => "*executor.MaxOneRowExec.Next",
        PhysicalPlan::Show(_) => "*executor.ShowExec.Next",
        PhysicalPlan::Lock(_) => "*executor.SelectLockExec.Next",
        PhysicalPlan::CTE(_) => "*executor.CTEExec.Next",
        PhysicalPlan::CTETable(_) => "*executor.CTETableReaderExec.Next",
        PhysicalPlan::Sequence(_) => "*executor.SequenceExec.Next",
        PhysicalPlan::UnionScan(_) => "*executor.UnionScanExec.Next",
        PhysicalPlan::TableSample(_) => "*executor.TableSampleExecutor.Next",
        PhysicalPlan::Expand(_) => "*executor.ExpandExec.Next",
        PhysicalPlan::Shuffle(_) => "*executor.ShuffleExec.Next",
        _ => return None,
    })
}

/// The Go executor type of a node [`go_executor_next_region`] leaves out,
/// for its RU v2 recording.
fn go_executor_ruv2_type(plan: &PhysicalPlan) -> Option<&'static str> {
    use tidb_planner_core_operator_physicalop::IndexJoinKind;
    Some(match plan {
        PhysicalPlan::IndexJoin(join) => match join.kind {
            IndexJoinKind::IndexJoin => "*join.IndexLookUpJoin",
            IndexJoinKind::IndexHashJoin => "*join.IndexNestedLoopHashJoin",
            IndexJoinKind::IndexMergeJoin => "*join.IndexLookUpMergeJoin",
        },
        PhysicalPlan::Window(_) => "*windows.WindowExec",
        _ => return None,
    })
}

fn checked_executor(child: Box<dyn Executor>, memory: crate::StatementMemory) -> Box<dyn Executor> {
    checked_executor_with_stats(child, memory, None)
}

fn checked_executor_with_stats(
    child: Box<dyn Executor>,
    memory: crate::StatementMemory,
    runtime_stats: Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStats>>,
) -> Box<dyn Executor> {
    Box::new(SqlKillerCheckExec {
        child,
        memory,
        runtime_stats,
        next_region: None,
        trace: None,
        top_profiling: None,
        ruv2: None,
        child_count: None,
    })
}

impl Executor for SqlKillerCheckExec {
    fn open(&mut self) -> Result<(), crate::ExecError> {
        let trace = self.trace.as_deref_mut();
        let _region = trace
            .as_ref()
            .and_then(|trace| trace.open_region)
            .map(|(name, enter)| {
                if enter {
                    tidb_tracing::start_thread_region_ex(name)
                } else {
                    tidb_tracing::start_thread_region(name)
                }
            });
        let after_open = trace.and_then(|trace| {
            if trace.limit_close {
                trace.open_span = tidb_tracing::current_span();
            }
            trace.after_open_region
        });
        let start = self.runtime_stats.as_ref().map(|_| Instant::now());
        let result = tidb_driver_error::panic_recovery::recover_executor_panic(|| self.child.open());
        if let (Some(stats), Some(start)) = (&self.runtime_stats, start) {
            stats.record_open(start.elapsed());
        }
        if result.is_ok() {
            if let Some(name) = after_open {
                tidb_tracing::start_thread_region(name).end();
            }
        }
        result
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), crate::ExecError> {
        let _region = self
            .next_region
            .map(tidb_tracing::start_thread_region_ex);
        if let Some(event) = self
            .trace
            .as_ref()
            .and_then(|trace| trace.next_event.as_ref())
        {
            tidb_tracing::thread_event(|| event.clone());
        }
        let start = self.runtime_stats.as_ref().map(|_| Instant::now());
        if let Some(register) = &self.top_profiling {
            register();
        }
        let result = tidb_driver_error::panic_recovery::recover_executor_panic(|| {
            self.memory.check().map_err(crate::ExecError::from)?;
            match self.child_count {
                Some(child_count) => tidb_executor_internal_exec::ruv2::next_with_ruv2(
                    self.ruv2.as_ref(),
                    child_count,
                    req,
                    |req| self.child.next(req),
                )?,
                None => self.child.next(req)?,
            }
            self.memory.check().map_err(crate::ExecError::from)
        });
        if let (Some(stats), Some(start)) = (&self.runtime_stats, start) {
            stats.record_next(start.elapsed(), req.num_rows() as i64);
        }
        result
    }

    fn close(&mut self) -> Result<(), crate::ExecError> {
        let close_started = Instant::now();
        let start = self.runtime_stats.as_ref().map(|_| close_started);
        let result = tidb_driver_error::panic_recovery::recover_executor_panic(|| self.child.close());
        if let (Some(stats), Some(start)) = (&self.runtime_stats, start) {
            stats.record_close(start.elapsed());
        }
        // Go `LimitExec.Close`: a close over a millisecond gets its own span
        // under the span its `Open` ran in, started when the close began.
        if let Some(parent) = self
            .trace
            .as_ref()
            .and_then(|trace| trace.open_span.as_ref())
        {
            if close_started.elapsed() > Duration::from_millis(1) {
                let tracer = parent.tracer();
                tracer
                    .start_span_child_of_at(
                        "limitExec.Close",
                        parent.context(),
                        tracer.system_time_of(close_started),
                    )
                    .finish();
            }
        }
        result
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.child.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.child.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.child.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.child.new_chunk()
    }

    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        self.child.table_access()
    }

    fn agg_tree_input_empty(&self) -> bool {
        self.child.agg_tree_input_empty()
    }
}

/// Go `BatchPointGetExec.initialize`'s `slices.SortFunc(e.handles, less)`.
/// Unsigned integer handles compare their stored bits, while signed and
/// common handles use their natural order.
fn sort_handles_for_keep_order(
    handles: &mut [TableHandle],
    desc: bool,
    unsigned_pk_is_handle: bool,
) {
    handles.sort_by(|left, right| {
        let ordering = if unsigned_pk_is_handle {
            match (left, right) {
                (TableHandle::Int(left), TableHandle::Int(right)) => {
                    (*left as u64).cmp(&(*right as u64))
                }
                _ => left.cmp(right),
            }
        } else {
            left.cmp(right)
        };
        if desc {
            ordering.reverse()
        } else {
            ordering
        }
    });
}

/// Go `MaxOneRowExec`: emit one row (NULL when the child is empty) and reject
/// a second row. The retained physical operator caps its child request at two;
/// this executor still performs the second pull because not every child fills
/// a requested chunk.
struct MaxOneRowExec {
    meta: ExecutorMeta,
    child: Box<dyn Executor>,
    evaluated: bool,
}

impl MaxOneRowExec {
    fn new(meta: ExecutorMeta, child: Box<dyn Executor>) -> Self {
        Self {
            meta,
            child,
            evaluated: false,
        }
    }
}

impl Executor for MaxOneRowExec {
    fn open(&mut self) -> Result<(), crate::ExecError> {
        self.evaluated = false;
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), crate::ExecError> {
        req.reset();
        if self.evaluated {
            return Ok(());
        }
        self.evaluated = true;
        self.child.next(req)?;
        match req.num_rows() {
            0 => {
                if self.meta.schema().is_empty() {
                    req.set_num_virtual_rows(1);
                } else {
                    for column in 0..self.meta.schema().len() {
                        req.append_null(column);
                    }
                }
                Ok(())
            }
            1 => {
                let mut extra = self.child.new_chunk();
                self.child.next(&mut extra)?;
                if extra.num_rows() == 0 {
                    Ok(())
                } else {
                    Err(tidb_exeerrors::ERR_SUBQUERY_MORE_THAN1_ROW
                        .gen_with_stack_by_args(&[])
                        .into())
                }
            }
            _ => Err(tidb_exeerrors::ERR_SUBQUERY_MORE_THAN1_ROW
                .gen_with_stack_by_args(&[])
                .into()),
        }
    }

    fn close(&mut self) -> Result<(), crate::ExecError> {
        self.child.close()
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

fn plan_schema(plan: &PhysicalPlan) -> Result<Schema, DriverError> {
    plan.schema()
        .cloned()
        .ok_or_else(|| DriverError::unsupported("a physical operator has no schema"))
}

fn unary_schema(plan: &PhysicalPlan, child: &dyn Executor) -> Schema {
    plan.base()
        .base
        .schema()
        .cloned()
        .unwrap_or_else(|| child.schema().clone())
}

/// Go `retrieveColumnIdxsUsedByChild`: returns the child-column projection
/// needed when a physical schema producer has pruned or reordered its output.
/// A missing column cancels inline projection, matching Go's executor builder.
fn inline_projection_offsets(output: &Schema, child: &Schema) -> Option<Vec<usize>> {
    let mut offsets = Vec::with_capacity(output.len());
    for column in &output.columns {
        let offset = child.column_index(column);
        if offset < 0 {
            return None;
        }
        offsets.push(offset as usize);
    }
    (offsets.len() != child.len()
        || offsets
            .iter()
            .enumerate()
            .any(|(output, child)| output != *child))
    .then_some(offsets)
}

fn meta(ctx: &crate::StmtContext, plan: &PhysicalPlan, schema: Schema) -> ExecutorMeta {
    let (init_cap, max_chunk_size) = ctx.executor_chunk_sizes();
    ExecutorMeta::new(
        schema,
        i64::from(plan.base().base.id()),
        init_cap,
        max_chunk_size,
    )
    .with_alloc_pool(ctx.chunk_allocator())
}

fn only_child(plan: &PhysicalPlan) -> Result<&PhysicalPlan, DriverError> {
    let [child] = plan.children() else {
        return Err(DriverError::unsupported(
            "a unary physical operator has the wrong child count",
        ));
    };
    Ok(child)
}

fn resolve_expression(
    mut expression: Expression,
    schema: &Schema,
) -> Result<Expression, DriverError> {
    super::planner_bridge::materialize_physical_expression(&mut expression);
    resolve_by_identity_or_bound_position(&mut expression, schema)?;
    Ok(expression)
}

/// Go evaluates a column at the `Index` that `ResolveIndices` bound before
/// postOptimize (`physical.resolve_indices()` here). This tier rebinds each
/// column by unique ID against the built child, whose schema may be shaped
/// differently from its plan's. A column whose identity is no longer in the
/// child keeps Go's position: `eliminatePhysicalProjection` drops a strict
/// identity projection whose own columns are new identities over the same
/// positions, as `buildProjection4Union`'s are when aggregation is pushed
/// across UNION ALL, and its parent goes on reading those positions.
fn resolve_by_identity_or_bound_position(
    expression: &mut Expression,
    schema: &Schema,
) -> Result<(), DriverError> {
    match expression {
        Expression::Column(column) => {
            let at = schema.column_index(column);
            if at >= 0 {
                column.index = at as i64;
            } else if usize::try_from(column.index).map_or(true, |at| at >= schema.len()) {
                return Err(DriverError::unsupported(
                    "a physical expression does not resolve in its child",
                ));
            }
            Ok(())
        }
        Expression::ScalarFunction(function) => {
            for arg in &mut function.args {
                resolve_by_identity_or_bound_position(arg, schema)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn resolve_expressions(
    expressions: &[Expression],
    schema: &Schema,
) -> Result<Vec<Expression>, DriverError> {
    expressions
        .iter()
        .cloned()
        .map(|expression| resolve_expression(expression, schema))
        .collect()
}

/// Whether any access condition reads a correlated column.
fn has_correlated_access(conditions: &[Expression]) -> bool {
    conditions
        .iter()
        .any(|condition| !tidb_expr::simple_expr::extract_cor_columns(condition).is_empty())
}

/// Go `SubstituteCorCol2Constant` over each access condition, with the
/// values the Apply bound for the current outer row.
fn substitute_correlated_access(
    conditions: &[Expression],
    ctx: &crate::StmtContext,
) -> Result<Vec<Expression>, tidb_driver_error::ExecError> {
    let builder = tidb_expr::expr_util::builder::RealFunctionBuilder::new(ctx);
    let options = tidb_expr::expr_util::substitute::SubstituteOptions::new(&builder);
    conditions
        .iter()
        .map(|condition| {
            let mut condition = condition.clone();
            super::planner_bridge::materialize_physical_expression(&mut condition);
            tidb_expr::expr_util::substitute::substitute_cor_col_2_constant(
                &condition, ctx, &options,
            )
            .map_err(|error| tidb_driver_error::ExecError::internal(format!("{error:?}")))
        })
        .collect()
}

/// Go `rebuildIndexRanges` (`executor/distsql.go:160`): an index scan whose
/// access conditions read correlated columns rebuilds its ranges from them,
/// with every correlated column replaced by its current value, each time the
/// enclosing Apply reopens it.
fn correlated_index_range_rebuilder(
    scan: &PhysicalIndexScan,
    ctx: &crate::StmtContext,
) -> Option<crate::access_path::RangeRebuilder> {
    let rebuild = scan.range_rebuild.clone()?;
    if !has_correlated_access(&rebuild.access_conditions) {
        return None;
    }
    let ctx = ctx.clone();
    Some(std::sync::Arc::new(move || {
        let access = substitute_correlated_access(&rebuild.access_conditions, &ctx)?;
        let (ranges, _, _) =
            tidb_ranger::detacher::detach_simple_cond_and_build_range_for_index_in(
                &access,
                &rebuild.index_columns,
                &rebuild.index_column_lengths,
                0,
                &tidb_ranger::points::StatementRangerEvaluator(&ctx),
                ctx.ranger_options(),
            )
            .map_err(|error| tidb_driver_error::ExecError::internal(format!("{error:?}")))?;
        Ok(ranges)
    }))
}

/// Go `PhysicalTableScan.ResolveCorrelatedColumns`: the integer handle's
/// range from `BuildTableRange`, a common handle's from
/// `DetachCondAndBuildRangeForIndex`, over the substituted access conditions.
pub(crate) fn correlated_table_range_rebuilder(
    scan: &PhysicalTableScan,
    ctx: &crate::StmtContext,
) -> Option<crate::access_path::RangeRebuilder> {
    let rebuild = scan.range_rebuild.clone()?;
    if !has_correlated_access(&rebuild.access_conditions) {
        return None;
    }
    let ctx = ctx.clone();
    Some(std::sync::Arc::new(move || {
        let access = substitute_correlated_access(&rebuild.access_conditions, &ctx)?;
        let evaluate = tidb_ranger::points::StatementRangerEvaluator(&ctx);
        let ranges = match &rebuild.handle_type {
            Some(handle_type) => {
                tidb_ranger::ranger::build_table_range_in(&access, handle_type, 0, &evaluate)
                    .map_err(|error| tidb_driver_error::ExecError::internal(format!("{error:?}")))?
                    .ranges
            }
            None => {
                tidb_ranger::detacher::detach_cond_and_build_range_for_index_in(
                    &access,
                    &rebuild.common_handle_columns,
                    &rebuild.common_handle_lengths,
                    0,
                    &evaluate,
                    ctx.ranger_options(),
                )
                .map_err(|error| tidb_driver_error::ExecError::internal(format!("{error:?}")))?
                .ranges
            }
        };
        Ok(ranges)
    }))
}

fn table_scan_schema(
    scan: &PhysicalTableScan,
    table: &crate::TableCommon,
) -> Result<(Schema, Vec<usize>, Option<usize>, Option<usize>), DriverError> {
    let output = scan
        .base
        .base
        .schema()
        .ok_or_else(|| DriverError::unsupported("a physical table scan has no schema"))?;
    let mut full = Vec::with_capacity(table.logical_data_source_column_count());
    for (index, column) in table.logical_columns().iter().enumerate() {
        let mut planned = scan
            .cost_columns
            .iter()
            .find(|planned| planned.id == column.read().id)
            .cloned()
            .unwrap_or_else(|| {
                let mut planned = Column::new(column.read().id, column.read().field_type.clone());
                planned.id = column.read().id;
                planned
            });
        planned.index = index as i64;
        full.push(planned);
    }
    let mut keep = Vec::with_capacity(output.columns.len());
    let mut extra_handle_slot = None;
    let mut extra_commit_ts_slot = None;
    for (output_offset, wanted) in output
        .columns
        .iter()
        .filter(|column| column.id != tidb_model::column::EXTRA_PHYS_TBL_ID)
        .enumerate()
    {
        // Go's `DataSource` appends `_tidb_rowid` and then `_tidb_commit_ts`
        // to the stored columns, and the scan fills both at the tail of its
        // output row. A stored column after either extra cannot line up.
        if wanted.id == tidb_model::column::EXTRA_HANDLE_ID {
            if extra_handle_slot.is_some() || extra_commit_ts_slot.is_some() {
                return Err(DriverError::unsupported(
                    "a physical table-scan _tidb_rowid column is not its final output",
                ));
            }
            extra_handle_slot = Some(output_offset);
            continue;
        }
        if wanted.id == tidb_model::column::EXTRA_COMMIT_TS_ID {
            if extra_commit_ts_slot.is_some() {
                return Err(DriverError::unsupported(
                    "a physical table-scan _tidb_commit_ts column is not its final output",
                ));
            }
            extra_commit_ts_slot = Some(output_offset);
            continue;
        }
        if extra_handle_slot.is_some() || extra_commit_ts_slot.is_some() {
            return Err(DriverError::unsupported(
                "a physical table-scan extra column is not its final output",
            ));
        }
        keep.push(
            full.iter()
                .position(|column| {
                    column.unique_id == wanted.unique_id
                        || (wanted.id != 0 && column.id == wanted.id)
                })
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a physical table-scan output column is absent from its table",
                    )
                })?,
        );
    }
    // The source starts in stored-column coordinates. accept_column_prune
    // composes keep into those coordinates and updates its output schema.
    // LogicalSelection has already added every filter input to scan.Schema.
    Ok((
        Schema::new(full),
        keep,
        extra_handle_slot,
        extra_commit_ts_slot,
    ))
}

fn build_table_scan(
    plan: &PhysicalPlan,
    scan: &PhysicalTableScan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    net_bytes: Option<f64>,
    range_group: Option<&tidb_ranger::types::Ranges>,
    storage: &ReaderStorage,
    cop_chain: Option<&CopChain>,
    reader_mem_tracker: Option<Arc<tidb_memory::Tracker>>,
) -> Result<Box<dyn Executor>, DriverError> {
    let (table, store, separated) = storage.table(
        ctx,
        catalog
            .physical_kv_table_by_id(scan.table_id)
            .ok_or_else(|| {
                DriverError::unsupported("physical table ID is absent from the catalog")
            })?,
    );
    let (schema, keep, extra_handle_slot, extra_commit_ts_slot) = table_scan_schema(scan, &table)?;
    let logical_table_id = table.table_id();
    let index_id = tidb_executor_internal_exec::index_usage_reporter::get_cluster_index_id(&table);
    // Go `buildIndexUsageReporter(v, true)`: `LoadTableStats`, then the
    // statement's `UsedStatsInfo` entry the report reads.
    let stats = crate::access_cost::load_table_stats(ctx, catalog, table.stats_physical_id());
    let column_count = table.logical_data_source_column_count();
    let partition_ids = scan
        .dynamic_partition_access
        .as_ref()
        .filter(|access| !access.all_partitions)
        .map(|access| {
            access
                .partitions
                .iter()
                .filter_map(|name| {
                    table.meta().read().get_partition_info().and_then(|pi| {
                        pi.read()
                            .definitions
                            .snapshot()
                            .iter()
                            .find(|definition| definition.name.original().go_lower_eq(name))
                            .map(|definition| definition.id)
                    })
                })
                .collect::<Vec<_>>()
        });
    let mut statement = with_reader_cop_chain(
        crate::remote_scan::pushdown_statement_context(ctx),
        plan,
        None,
        cop_chain,
    );
    statement.select_mem_tracker = reader_mem_tracker;
    let mut source = TableScanExec::new_with_context(
        meta(ctx, plan, schema.clone()),
        std::sync::Arc::unwrap_or_clone(table),
        store,
        RowDecodeContext::for_query(ctx),
        ReaderStorage::statement(
            separated,
            statement
            .with_plan_id(i64::from(scan.base.base.id()))
                .with_estimated_net_bytes(net_bytes.unwrap_or_else(|| {
                    plan.cop_net_data_size(
                        false,
                        tidb_config::tikvcfg::get_global_config()
                            .tikv_client
                            .enable_chunk_rpc,
                    )
                })),
        ),
    );
    // Dynamic prune mode keeps ONE logical scan whose read is restricted to
    // the partitions the planner selected -- the statement's own `PARTITION
    // (p, ...)` list intersected with whatever the WHERE pruned. Without
    // this a named-partition statement would read (or in DELETE/UPDATE,
    // remove) rows living OUTSIDE the named partitions, which Go never
    // does.
    if let Some(ids) = partition_ids {
        if !source.accept_partition_pruning(&ids) {
            return Err(DriverError::unsupported(
                "the physical table scan cannot restrict itself to the named partitions",
            ));
        }
    }
    if keep.len() != column_count || keep.iter().copied().ne(0..column_count) {
        if !source.accept_column_prune(&keep) {
            return Err(DriverError::unsupported(
                "physical scan projection does not match its stored columns",
            ));
        }
    }
    if let Some(slot) = extra_handle_slot {
        if !source.accept_extra_handle(slot) {
            return Err(DriverError::unsupported(
                "the physical table scan cannot emit its _tidb_rowid column",
            ));
        }
    }
    if let Some(slot) = extra_commit_ts_slot {
        if !source.accept_extra_commit_ts(slot) {
            return Err(DriverError::unsupported(
                "the physical table scan cannot emit its _tidb_commit_ts column",
            ));
        }
    }
    if scan.keep_order && !source.accept_keep_order(scan.desc) {
        return Err(DriverError::unsupported(
            "the physical table scan cannot preserve its order",
        ));
    }
    // A scan built from access conditions always plans a non-empty range; it
    // is empty only after a cached plan rebuilt it for parameters that admit
    // no handle (`c1 > 9223372036854775807`), and Go then reads nothing.
    let scan_ranges = range_group
        .filter(|_| !scan.group_by_col_idxs.is_empty())
        .unwrap_or(&scan.ranges);
    if !scan_ranges.is_empty() || scan.range_rebuild.is_some() {
        let ranges = scan_ranges.clone();
        if !source.accept_handle_ranges(&ranges) {
            return Err(DriverError::unsupported(
                "the physical table scan cannot apply its ranges",
            ));
        }
    }
    if let Some(rebuilder) = correlated_table_range_rebuilder(scan, ctx) {
        source.set_handle_range_rebuilder(rebuilder);
    }
    // Go `PhysicalTableScan.StoreType`: the planner assigned this scan to the
    // columnar replica, so the pushdown request names TiFlash and the storage
    // seam lowers it through MPP dispatch instead of the TiKV cop path.
    if scan.store_type == tidb_kv::store_type::StoreType::TiFlash {
        source.set_read_engine(tidb_distsql::remote_scan::PushdownReadEngine::TiFlash);
        source.set_schema_version(catalog.metadata_version());
    }
    if let Some(rows) = scan.base.base.stats_info().map(|stats| stats.row_count()) {
        source.accept_scan_estimate(rows);
    }
    let output = plan_schema(plan)?;
    let source = source.with_physical_schema(output)?;
    if storage.is_mem_buffer() {
        return Ok(Box::new(source));
    }
    let source = Box::new(CopIndexUsageExec::new(
        Box::new(source),
        logical_table_id,
        stats,
        ctx.index_usage_collector().cloned(),
        index_id,
    ));
    Ok(source)
}

fn build_table_sample(
    plan: &PhysicalPlan,
    sample: &PhysicalTableSample,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let physical_ids = if sample.table_sample_info.partition_ids.is_empty() {
        vec![sample.physical_table_id]
    } else {
        sample.table_sample_info.partition_ids.clone()
    };
    // Go samples `GetSnapshot(TxnCtx.StartTS)`, never the membuffer: the
    // table's storage reads its snapshot (`TableStorage::snapshot_first`).
    let mut tables = physical_ids
        .iter()
        .map(|physical_id| {
            catalog
                .physical_kv_table_by_id(*physical_id)
                .ok_or_else(|| {
                    DriverError::unsupported("physical table-sample ID is absent from the catalog")
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    match tables.first().map(|table| table.temp_table_type()) {
        Some(kind) if kind == tidb_model::TempTableType::LOCAL => {
            return Err(DriverError::unsupported(
                "TABLESAMPLE clause can not be applied to local temporary tables",
            ));
        }
        Some(kind) if kind == tidb_model::TempTableType::GLOBAL => {
            tables.clear();
        }
        _ => {}
    }
    if tables.is_empty() {
        return Ok(Box::new(TableSampleExecutor::new(
            meta(ctx, plan, plan_schema(plan)?),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            RowDecodeContext::for_query(ctx),
        )));
    }
    let table = tables
        .first()
        .ok_or_else(|| DriverError::unsupported("a physical table sample has no table"))?;
    let schema = plan_schema(plan)?;
    let output_columns = schema
        .columns
        .iter()
        .map(|column| {
            if column.id == tidb_model::column::EXTRA_HANDLE_ID {
                return Ok(SampleOutputColumn::ExtraHandle);
            }
            if column.id == tidb_model::column::EXTRA_COMMIT_TS_ID {
                return Ok(SampleOutputColumn::ExtraCommitTs);
            }
            table
                .columns()
                .iter()
                .position(|stored| stored.read().id == column.id)
                .map(SampleOutputColumn::Stored)
                .ok_or_else(|| {
                    DriverError::unsupported(format!(
                        "table-sample output column {} is absent from physical table {}",
                        column.id, table.table_id()
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Go `splitTableRanges` + `splitIntoMultiRanges`: every physical
    // table's record range `[t{id}_r, PrefixNext)`, cut at the store's
    // region boundaries, then `sortRanges` by start key.
    let mut ranges = Vec::new();
    for (position, physical_id) in physical_ids.iter().enumerate() {
        let start = tidb_tablecodec::table_key::gen_table_record_prefix(*physical_id);
        let end = tidb_txnkv::Key::from_bytes(start.clone()).prefix_next();
        ranges.extend(
            ctx.region_store()
                .load_regions_in_key_range(&start, end.as_bytes())
                .map(|regions| tidb_kv::region_map::clip_regions(&regions, &start, end.as_bytes()))
                .map_err(|error| DriverError::Mysql(crate::MysqlError::unknown(error)))?
                .into_iter()
                .map(|(start, end)| crate::table_sample::SampleRange {
                    table: position,
                    start,
                    end,
                }),
        );
    }
    ranges.sort_by(|left, right| left.start.cmp(&right.start));
    if sample.desc {
        ranges.reverse();
    }
    let stores = tables
        .iter()
        .map(|_| crate::statement_store(ctx))
        .collect();
    let tables = tables
        .into_iter()
        .map(std::sync::Arc::unwrap_or_clone)
        .collect();
    Ok(Box::new(TableSampleExecutor::new(
        meta(ctx, plan, schema),
        tables,
        stores,
        ranges,
        output_columns,
        RowDecodeContext::for_query(ctx),
    )))
}

/// The declared columns and the rows a memory-table scan reads.
struct MemoryTableRows {
    column_names: Vec<String>,
    source: MemoryTableRowSource,
}

enum MemoryTableRowSource {
    /// The session retrieved them for this scan (Go `memtableRetriever`).
    Retrieved(std::sync::Arc<Vec<Vec<tidb_datatype::Datum>>>),
    /// The scan retrieves them itself when it is first read (Go
    /// `MemTableReaderExec` over its own `memTableRetriever`).
    Owned(
        crate::mem_table_retrieval::MemTableRead,
        std::sync::Arc<dyn crate::mem_table_retrieval::MemTableRetriever>,
    ),
}

/// The rows a memory-table scan shapes, or the retriever that builds them when
/// the scan is first read.
enum MemoryTableRowsOrRetriever<'a> {
    Rows(&'a [Vec<tidb_datatype::Datum>]),
    Owned(
        crate::mem_table_retrieval::MemTableRead,
        std::sync::Arc<dyn crate::mem_table_retrieval::MemTableRetriever>,
    ),
}

impl MemoryTableRows {
    fn rows(&self) -> MemoryTableRowsOrRetriever<'_> {
        match &self.source {
            MemoryTableRowSource::Retrieved(rows) => MemoryTableRowsOrRetriever::Rows(rows),
            MemoryTableRowSource::Owned(read, retriever) => {
                MemoryTableRowsOrRetriever::Owned(read.clone(), std::sync::Arc::clone(retriever))
            }
        }
    }
}

/// The retriever the executor owns for the virtual-table scan `plan`, when
/// the statement's session gave one that serves its table.
fn owned_mem_table_retriever(
    plan: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Option<(
    crate::mem_table_retrieval::MemTableRead,
    std::sync::Arc<dyn crate::mem_table_retrieval::MemTableRetriever>,
)> {
    let read = mem_table_read_of(plan, ctx)?;
    if !matches!(
        catalog.table_in(&read.db_name, &read.table_name),
        Some(super::TableEntry::Kv(_))
    ) {
        return None;
    }
    let retriever = ctx.mem_table_retrieval().retriever_for(&read)?;
    Some((read, retriever))
}

/// Carries a retriever's failure into the executor that runs it.
fn retrieval_error(error: DriverError) -> crate::ExecError {
    crate::ExecError::Retrieval(Box::new(error))
}

/// Go's executors take the plan's `*IndexInfo`; a plan here names its index
/// by id, so a session's hypothetical index (which only a plain EXPLAIN
/// builds) joins the table it indexes for the build.
fn table_with_plan_index(
    mut table: Arc<crate::TableCommon>,
    index_id: Option<i64>,
    ctx: &crate::StmtContext,
) -> Result<Arc<crate::TableCommon>, DriverError> {
    let Some(index_id) = index_id else {
        return Ok(table);
    };
    if table.indexes().iter().any(|index| index.read().id == index_id) {
        return Ok(table);
    }
    let Some(hypo) = ctx.hypo_indexes().and_then(|hypo| hypo.index_by_id(index_id)) else {
        return Ok(table);
    };
    let index = tidb_table::table_from_meta::kv_index(&tidb_model::GoShared::new(hypo.clone()), table.columns())
        .map_err(DriverError::unsupported)?;
    Arc::make_mut(&mut table).add_index(index, false);
    Ok(table)
}

/// Resolves the rows of the memory-table scan `plan_id` over
/// `database`.`table`. A virtual table's rows are not stored: the session
/// retrieves them per scan, so a scan read before that fails with
/// [`DriverError::MemTableRetrievalNeeded`].
fn memory_table_rows(
    plan: &PhysicalPlan,
    database: &str,
    table: &str,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<MemoryTableRows, DriverError> {
    match catalog
        .table_in(database, table)
        .ok_or_else(|| DriverError::unsupported("memory table is absent from the catalog"))?
    {
        super::TableEntry::Kv(virtual_table) => {
            let column_names = virtual_table
                .columns()
                .iter()
                .map(|column| column.read().name.original().to_owned())
                .collect();
            if let Some((read, retriever)) = owned_mem_table_retriever(plan, catalog, ctx) {
                return Ok(MemoryTableRows {
                    column_names,
                    source: MemoryTableRowSource::Owned(read, retriever),
                });
            }
            let rows = match mem_table_read_of(plan, ctx).and_then(|read| ctx.mem_table_retrieval().rows(&read)) {
                Some(rows) => rows,
                // A plain EXPLAIN builds the reader and never reads it.
                None if ctx.in_explain_stmt() && !ctx.in_explain_analyze_stmt() => {
                    std::sync::Arc::new(Vec::new())
                }
                None => return Err(DriverError::MemTableRetrievalNeeded),
            };
            Ok(MemoryTableRows {
                column_names,
                source: MemoryTableRowSource::Retrieved(rows),
            })
        }
        _ => Err(DriverError::unsupported(
            "a memory-table scan resolved to a view or sequence",
        )),
    }
}

fn build_mem_table(
    plan: &PhysicalPlan,
    scan: &PhysicalMemTable,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    if scan.db_name.go_lower_eq("metrics_schema") {
        // Go's `buildMemTable` gives every `metrics_schema` table a
        // `MetricRetriever`, which queries when it is read, not when built.
        let Some(tidb_planner::memtable_predicate_extractor::MemTableExtractor::MetricTable(extractor)) =
            tidb_planner::memtable_predicate_extractor::extractor_of(scan).cloned()
        else {
            return Err(DriverError::unsupported(
                "a metrics_schema scan without its MetricTableExtractor",
            ));
        };
        let sysvar_int = |name: &str, default: i64| {
            tidb_exprctx::Columns::sysvar(ctx, None, name)
                .and_then(|value| value.sql_string().ok())
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };
        let metric_schema_step = sysvar_int(
            tidb_vardef::tidb_vars::TIDB_METRIC_SCHEMA_STEP,
            tidb_vardef::defaults::DEF_TIDB_METRIC_SCHEMA_STEP,
        );
        let metric_schema_range_duration = sysvar_int(
            tidb_vardef::tidb_vars::TIDB_METRIC_SCHEMA_RANGE_DURATION,
            tidb_vardef::defaults::DEF_TIDB_METRIC_SCHEMA_RANGE_DURATION,
        );
        let table = scan.table_name.to_lowercase();
        let columns: Vec<String> = scan.columns.iter().map(|column| column.name.to_lowercase()).collect();
        let meta = meta(ctx, plan, plan_schema(plan)?);
        return Ok(Box::new(MemTableSourceExec::lazy(
            meta,
            Box::new(move || {
                let unknown = |error: String| {
                    retrieval_error(DriverError::Mysql(crate::driver::MysqlError::unknown(error)))
                };
                let rows = crate::metrics_reader::MetricRetriever {
                    table: &table,
                    extractor: &extractor,
                    metric_schema_step,
                    metric_schema_range_duration,
                }
                .retrieve()
                .map_err(unknown)?;
                // A row is in the table's column order (`genColumnInfos`).
                let names = crate::metrics_reader::column_names(&table);
                columns
                    .iter()
                    .map(|column| names.iter().position(|name| name == column))
                    .collect::<Option<Vec<_>>>()
                    .and_then(|offsets| {
                        rows.iter()
                            .map(|row| offsets.iter().map(|offset| row.get(*offset).cloned()).collect())
                            .collect::<Option<Vec<Vec<tidb_datatype::Datum>>>>()
                    })
                    .ok_or_else(|| {
                        retrieval_error(DriverError::unsupported(
                            "a metrics_schema row is not its table's columns",
                        ))
                    })
            }),
        )));
    }
    let table = memory_table_rows(plan, &scan.db_name, &scan.table_name, catalog, ctx)?;

    let offsets = scan
        .columns
        .iter()
        .map(|column| {
            table
                .column_names
                .iter()
                .position(|name| name.eq_ignore_ascii_case(&column.name))
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a physical memory-table output column is absent from its table",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The extractor's claimed predicates left the Selection above this scan;
    // Go's reader requests only what they keep, so the whole materialized
    // table is filtered by them here.
    let column_names = table.column_names.clone();
    let extractor = tidb_planner::memtable_predicate_extractor::extractor_of(scan).cloned();
    let shape = move |rows: &[Vec<tidb_datatype::Datum>]| {
        let names: Vec<&str> = column_names.iter().map(String::as_str).collect();
        rows.iter()
            .filter(|row| {
                extractor
                    .as_ref()
                    .is_none_or(|extractor| extractor.keeps_row(&names, row))
            })
            .map(|row| {
                offsets
                    .iter()
                    .map(|offset| {
                        row.get(*offset).cloned().ok_or_else(|| {
                            DriverError::unsupported(
                                "a memory-table row is shorter than its declared schema",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, DriverError>>()
    };
    let meta = meta(ctx, plan, plan_schema(plan)?);
    // Go's builder gives a `memtableRetriever` table's reader a tracker
    // (`memory.NewTracker(v.ID(), -1)` on the statement's), and the
    // retriever accounts every row it builds (`recordMemoryConsume`).
    let memory = (scan.db_name.go_lower_eq("information_schema")
        && MEMTABLE_RETRIEVER_TABLES
            .iter()
            .any(|name| scan.table_name.go_lower_eq(name)))
    .then(|| ctx.statement_memory().operator_tracker(i64::from(plan.base().base.id())))
    .filter(|_| {
        !MEMTABLE_RETRIEVER_UNCHARGED_TABLES
            .iter()
            .any(|name| scan.table_name.go_lower_eq(name))
    });
    let record_memory_consume = move |rows: &[Vec<tidb_datatype::Datum>]| {
        if let Some(tracker) = &memory {
            tracker.consume(rows.iter().map(|row| tidb_datatype::estimated_mem_usage(row, 1)).sum());
        }
    };
    Ok(match table.rows() {
        MemoryTableRowsOrRetriever::Rows(rows) => {
            let shaped = shape(rows)?;
            let rows = rows.to_vec();
            Box::new(MemTableSourceExec::lazy(
                meta,
                Box::new(move || {
                    record_memory_consume(&rows);
                    Ok(shaped)
                }),
            ))
        }
        MemoryTableRowsOrRetriever::Owned(read, retriever) => {
            let (snapshot, ctx) = (catalog.clone(), ctx.clone());
            Box::new(MemTableSourceExec::lazy(
                meta,
                Box::new(move || {
                    let rows = retriever
                        .retrieve(&read, &snapshot, &ctx)
                        .map_err(retrieval_error)?;
                    record_memory_consume(&rows);
                    shape(&rows).map_err(retrieval_error)
                }),
            ))
        }
    })
}

/// The tables Go's `buildMemTable` serves through a `memtableRetriever`,
/// whose reader carries a memory tracker.
const MEMTABLE_RETRIEVER_TABLES: &[&str] = &[
    "SCHEMATA", "STATISTICS", "TIDB_INDEXES", "VIEWS", "TABLES", "REFERENTIAL_CONSTRAINTS",
    "SEQUENCES", "PARTITIONS", "ENGINES", "COLLATIONS", "ANALYZE_STATUS", "CLUSTER_INFO",
    "PROFILING", "CHARACTER_SETS", "KEY_COLUMN_USAGE", "USER_PRIVILEGES", "METRICS_TABLES",
    "COLLATION_CHARACTER_SET_APPLICABILITY", "PROCESSLIST", "CLUSTER_PROCESSLIST",
    "TIKV_REGION_STATUS", "TIDB_HOT_REGIONS", "TABLE_CONSTRAINTS", "TIFLASH_REPLICA",
    "TIDB_SERVERS_INFO", "TIKV_STORE_STATUS", "CLIENT_ERRORS_SUMMARY_GLOBAL",
    "CLIENT_ERRORS_SUMMARY_BY_USER", "CLIENT_ERRORS_SUMMARY_BY_HOST", "ATTRIBUTES",
    "PLACEMENT_POLICIES", "TRX_SUMMARY", "VARIABLES_INFO", "USER_ATTRIBUTES",
    "CLUSTER_TRX_SUMMARY", "MEMORY_USAGE", "MEMORY_USAGE_OPS_HISTORY", "CLUSTER_MEMORY_USAGE",
    "CLUSTER_MEMORY_USAGE_OPS_HISTORY", "RESOURCE_GROUPS", "RUNAWAY_WATCHES",
    "CHECK_CONSTRAINTS", "TIDB_CHECK_CONSTRAINTS", "KEYWORDS", "TIDB_INDEX_USAGE",
    "TIDB_PLAN_CACHE", "CLUSTER_TIDB_PLAN_CACHE", "CLUSTER_TIDB_INDEX_USAGE", "KEYSPACE_META",
];

/// The `memtableRetriever` tables whose `setData*` never calls
/// `recordMemoryConsume`.
const MEMTABLE_RETRIEVER_UNCHARGED_TABLES: &[&str] =
    &["ANALYZE_STATUS", "USER_PRIVILEGES", "KEYSPACE_META"];

fn aggregate_function(
    descriptor: &AggFuncDesc,
    child_schema: &Schema,
    _ctx: &crate::StmtContext,
) -> Result<AggFunc, DriverError> {
    let upper = descriptor.base.name.to_ascii_uppercase();
    // The planner represents COUNT(*) as a synthetic, id-less Column when
    // the aggregate is pushed below an index reader. That slot is not a
    // stored child column; COUNT(*) must therefore resolve with no argument,
    // just like COUNT(1), instead of trying to bind column id 0 to the scan.
    let count_star_synthetic = upper == "COUNT"
        && descriptor.base.args.len() == 1
        && child_schema.is_empty()
        && matches!(
            &descriptor.base.args[0],
            Expression::Column(column) if column.id == 0 && column.orig_name.is_empty()
        );
    let mut args = if count_star_synthetic {
        Vec::new()
    } else {
        resolve_expressions(&descriptor.base.args, child_schema)?
    };
    let separator = if upper == "GROUP_CONCAT" {
        Some(super::agg_build::separator_text(args.last().ok_or_else(
            || DriverError::unsupported("GROUP_CONCAT has no separator argument"),
        )?)?)
    } else {
        None
    };
    if separator.is_some() {
        args.pop();
    }
    let mut kind = super::agg_build::aggregate_kind(&upper, &args, separator)?;
    // Go merges partial counts in both merging modes: a Partial2 count (a
    // root aggregate under a pushed-down union) sums its children's counts
    // exactly as the Final one above it does.
    if matches!(
        descriptor.mode,
        AggFunctionMode::Final | AggFunctionMode::Partial2
    ) && matches!(kind, AggKind::Count)
    {
        kind = AggKind::FinalCount;
    }
    // Go `buildApproxCountDistinct` picks the signature by mode and result
    // type; a pushed-down Partial1 outputs its serialized sketch.
    if let AggKind::ApproxCountDistinct(sig) = &mut kind {
        let string_result =
            descriptor.ret_type().eval_type() == tidb_datatype::EvalType::String;
        *sig = tidb_executor_aggfuncs::aggfuncs::ApproxCountDistinctSig::of(descriptor.mode, string_result)
            .ok_or_else(|| {
                DriverError::unsupported(format!(
                    "APPROX_COUNT_DISTINCT in {} mode",
                    descriptor.mode.as_str()
                ))
            })?;
    }
    if upper == "APPROX_PERCENTILE" {
        args.truncate(1);
    }
    let arg_orig_name = if upper == "GROUP_CONCAT" {
        args.first()
            .and_then(|expression| match expression {
                Expression::Column(column) if !column.orig_name.is_empty() => {
                    Some(column.orig_name.clone())
                }
                _ => None,
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    let arg = (!args.is_empty()).then(|| args.remove(0));
    // Go's percentile builder ranks every input row even with DISTINCT.
    let distinct = descriptor.has_distinct && !matches!(&kind, AggKind::ApproxPercentile(_));
    Ok(AggFunc {
        kind,
        arg,
        extra_args: args,
        order_by: descriptor
            .order_by_items
            .iter()
            .map(|item| {
                resolve_expression(item.expr.clone(), child_schema)
                    .map(|expression| (expression, item.desc))
            })
            .collect::<Result<Vec<_>, _>>()?,
        distinct,
        arg_orig_name,
    })
}

/// The aggregation a reader has taken over: Go runs it inside the reader's
/// DAG, so the reader decodes the pushed aggregate's rows straight into the
/// caller's chunk (`selectResult.readFromChunk`). This node only carries the
/// aggregation's output schema over that reader.
struct PushedAggregationExec {
    meta: ExecutorMeta,
    child: Box<dyn Executor>,
}

impl Executor for PushedAggregationExec {
    fn open(&mut self) -> Result<(), tidb_driver_error::ExecError> {
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), tidb_driver_error::ExecError> {
        self.child.next(req)
    }

    fn close(&mut self) -> Result<(), tidb_driver_error::ExecError> {
        self.child.close()
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

/// A cop-side partial aggregation its source cannot take, run in this tier
/// with the coprocessor's own aggregation (Go's unistore `aggExec`): the
/// child's rows in, the partial rows a coprocessor would answer out.
struct LocalPartialAggregationExec {
    meta: ExecutorMeta,
    child: Box<dyn Executor>,
    aggregate: PushdownPartialAggregate,
    ctx: crate::StmtContext,
    rows: Option<std::vec::IntoIter<Vec<tidb_datatype::Datum>>>,
}

impl Executor for LocalPartialAggregationExec {
    fn open(&mut self) -> Result<(), tidb_driver_error::ExecError> {
        self.rows = None;
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), tidb_driver_error::ExecError> {
        req.reset();
        if self.rows.is_none() {
            let input_types = self.child.ret_field_types().to_vec();
            let mut chunk = self.child.new_chunk();
            let mut pending: std::collections::VecDeque<Vec<tidb_datatype::Datum>> =
                std::collections::VecDeque::new();
            let child = &mut self.child;
            let rows = crate::local_partial::local_partial_rows(
                &self.aggregate,
                &input_types,
                &self.ctx,
                || {
                    while pending.is_empty() {
                        child.next(&mut chunk)?;
                        if chunk.num_rows() == 0 {
                            return Ok(None);
                        }
                        pending.extend(
                            (0..chunk.num_rows())
                                .map(|row| chunk.get_row(row).get_datum_row(&input_types)),
                        );
                    }
                    Ok(pending.pop_front())
                },
            )?;
            self.rows = Some(rows.into_iter());
        }
        let rows = self.rows.as_mut().expect("partial rows computed");
        while req.num_rows() < self.meta.max_chunk_size() {
            let Some(row) = rows.next() else {
                break;
            };
            for (column, value) in row.iter().enumerate() {
                req.append_datum(column, value);
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), tidb_driver_error::ExecError> {
        self.rows = None;
        self.child.close()
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

fn build_aggregation(
    plan: &PhysicalPlan,
    descriptors: &[AggFuncDesc],
    group_by: &[Expression],
    stream: bool,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
    let register = !state.in_reader && !state.untraced;
    if state.in_reader {
        return build_aggregation_over_child(plan, descriptors, group_by, stream, child, ctx, register);
    }
    build_agg_from_child_exec(plan, descriptors, group_by, stream, child, ctx, register)
}

/// Go `buildStreamAggFromChildExec` / `buildHashAggFromChildExec`: a
/// TiDB-tier aggregate over an already built child.
fn build_agg_from_child_exec(
    plan: &PhysicalPlan,
    descriptors: &[AggFuncDesc],
    group_by: &[Expression],
    stream: bool,
    child: Box<dyn Executor>,
    ctx: &crate::StmtContext,
    register: bool,
) -> Result<Box<dyn Executor>, DriverError> {
    let executor = if stream {
        build_stream_agg_from_child_exec(plan, descriptors, group_by, &child, ctx)?
    } else {
        build_hash_agg_from_child_exec(plan, descriptors, group_by, &child, ctx, register)?
    };
    match executor {
        Some(executor) => Ok(executor(child)),
        None => build_aggregation_over_child(plan, descriptors, group_by, stream, child, ctx, register),
    }
}

/// Go `aggfuncs.Build` of each descriptor over `child_schema`, or `None`
/// where a function has no Go implementation (Go's nil).
fn go_agg_funcs(
    descriptors: &[AggFuncDesc],
    child_schema: &Schema,
    ctx: &crate::StmtContext,
) -> Result<Option<Vec<Box<dyn tidb_executor_aggfuncs::agg_func::AggFunc>>>, DriverError> {
    let Some(resolved) = resolved_descriptors(descriptors, child_schema)? else {
        return Ok(None);
    };
    let mut agg_funcs = Vec::with_capacity(resolved.len());
    for (ordinal, descriptor) in resolved.iter().enumerate() {
        let Some(agg_func) = tidb_executor_aggfuncs::agg_builder::build(ctx, descriptor, ordinal) else {
            return Ok(None);
        };
        agg_funcs.push(agg_func);
    }
    Ok(Some(agg_funcs))
}

/// The descriptors with their arguments and ORDER BY items bound to
/// `child_schema`.
fn resolved_descriptors(descriptors: &[AggFuncDesc], child_schema: &Schema) -> Result<Option<Vec<AggFuncDesc>>, DriverError> {
    let mut resolved_all = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        let mut resolved = descriptor.clone();
        // The planner's COUNT(*) under an index reader is an id-less
        // synthetic column, not a child column: Go's COUNT(1).
        let count_star_synthetic = descriptor.base.name.eq_ignore_ascii_case("count")
            && descriptor.base.args.len() == 1
            && child_schema.is_empty()
            && matches!(
                &descriptor.base.args[0],
                Expression::Column(column) if column.id == 0 && column.orig_name.is_empty()
            );
        resolved.base.args = if count_star_synthetic {
            vec![Expression::Constant(tidb_expr::constant::Constant::new_one())]
        } else {
            resolve_expressions(&descriptor.base.args, child_schema)?
        };
        for item in &mut resolved.order_by_items {
            item.expr = resolve_expression(item.expr.clone(), child_schema)?;
        }
        resolved_all.push(resolved);
    }
    Ok(Some(resolved_all))
}

/// Go `buildHashAggFromChildExec`: the HashAgg Go's TiDB tier runs, under
/// the same conditions as [`build_stream_agg_from_child_exec`]. A parallel
/// one splits each function into its partial and final halves (an AVG's
/// partial outputs two columns), sharing a GROUP_CONCAT's truncation flag.
fn build_hash_agg_from_child_exec(
    plan: &PhysicalPlan,
    descriptors: &[AggFuncDesc],
    group_by: &[Expression],
    child: &Box<dyn Executor>,
    ctx: &crate::StmtContext,
    register_runtime_stats: bool,
) -> Result<Option<Box<dyn FnOnce(Box<dyn Executor>) -> Box<dyn Executor>>>, DriverError> {
    let output_schema = plan_schema(plan)?;
    if output_schema.len() != descriptors.len() {
        return Ok(None);
    }
    let child_schema = child.schema().clone();
    let group_by = resolve_expressions(group_by, &child_schema)?;
    let mut is_unparallel_exec = descriptors.iter().any(|descriptor| !descriptor.order_by_items.is_empty());
    let has_distinct = descriptors.iter().any(|descriptor| descriptor.has_distinct);
    let (partial_concurrency, final_concurrency) = ctx.hashagg_concurrency();
    // Both concurrencies at 1 (or non-positive): no parallel execution.
    if final_concurrency == 0 || partial_concurrency == 0 || (final_concurrency == 1 && partial_concurrency == 1) {
        is_unparallel_exec = true;
    }
    let Some(resolved) = resolved_descriptors(descriptors, &child_schema)? else {
        return Ok(None);
    };
    let mut partial_agg_funcs = Vec::with_capacity(resolved.len());
    let mut final_agg_funcs = Vec::new();
    let mut partial_ordinal = 0;
    for (i, descriptor) in resolved.iter().enumerate() {
        if is_unparallel_exec {
            let Some(agg_func) = tidb_executor_aggfuncs::agg_builder::build(ctx, descriptor, i) else {
                return Ok(None);
            };
            partial_agg_funcs.push(agg_func);
            continue;
        }
        let mut ordinal = vec![partial_ordinal];
        partial_ordinal += 1;
        if descriptor.base.name == "avg" {
            ordinal.push(partial_ordinal + 1);
            partial_ordinal += 1;
        }
        let (partial_desc, final_desc) = descriptor.split(&ordinal);
        let (Some(partial_agg_func), Some(mut final_agg_func)) = (
            tidb_executor_aggfuncs::agg_builder::build(ctx, &partial_desc, i),
            tidb_executor_aggfuncs::agg_builder::build(ctx, &final_desc, i),
        ) else {
            return Ok(None);
        };
        if partial_desc.base.name == "group_concat" {
            // The two halves share one truncation sentinel.
            if let Some(truncated) = partial_agg_func.get_truncated() {
                final_agg_func.set_truncated(truncated);
            }
        }
        partial_agg_funcs.push(partial_agg_func);
        final_agg_funcs.push(final_agg_func);
    }
    let executor_meta = meta(ctx, plan, output_schema);
    let default_val = (group_by.is_empty()
        && !tidb_expression_aggregation::is_all_first_row(descriptors)
        && is_final_agg(descriptors))
    .then(|| {
        // Go `e.DefaultVal = e.AllocPool.Alloc(exec.RetTypes(e), 1, 1)`.
        let mut default_val = executor_meta.new_chunk_with_capacity(executor_meta.ret_field_types(), 1, 1);
        for (i, descriptor) in descriptors.iter().enumerate() {
            default_val.append_datum(i, &descriptor.base.get_default_value());
        }
        default_val
    });
    let track_aggregate_memory_usage = track_aggregate_memory_usage(ctx);
    let enable_parallel_hashagg_spill = tidb_exprctx::Columns::sysvar(ctx, None, "tidb_enable_parallel_hashagg_spill")
        .map_or(tidb_vardef::defaults::DEF_TIDB_ENABLE_PARALLEL_HASHAGG_SPILL, |value| {
            !matches!(value, tidb_datatype::Datum::Int(0) | tidb_datatype::Datum::UInt(0))
        });
    let plan_id = i64::from(plan.id());
    let memory = ctx.statement_memory();
    let runtime_stats = ctx.executor_runtime_stats_source().filter(|_| register_runtime_stats).cloned();
    let ctx = ctx.clone();
    Ok(Some(Box::new(move |child| {
        let mut executor = tidb_executor_aggregate::agg_hash_executor::HashAggExec::new(
            executor_meta,
            child,
            ctx,
            memory,
            tidb_executor_aggregate::agg_hash_executor::HashAggExecBuild {
                partial_agg_funcs,
                final_agg_funcs,
                group_by_items: group_by,
                default_val,
                is_unparallel_exec,
                has_distinct,
                partial_concurrency,
                final_concurrency,
                track_aggregate_memory_usage,
                enable_parallel_hashagg_spill,
            },
        );
        executor.set_runtime_stats(runtime_stats, plan_id);
        Box::new(executor)
    })))
}

/// Go `basePhysicalAgg.IsFinalAgg`: the first function's mode is Final or
/// Complete.
fn is_final_agg(descriptors: &[AggFuncDesc]) -> bool {
    descriptors
        .first()
        .is_some_and(|descriptor| matches!(descriptor.mode, AggFunctionMode::Final | AggFunctionMode::Complete))
}

/// Go `TrackAggregateMemoryUsage`.
fn track_aggregate_memory_usage(ctx: &crate::StmtContext) -> bool {
    tidb_exprctx::Columns::sysvar(ctx, None, "tidb_track_aggregate_memory_usage")
        .map_or(tidb_vardef::defaults::DEF_TIDB_TRACK_AGGREGATE_MEMORY_USAGE, |value| {
            !matches!(value, tidb_datatype::Datum::Int(0) | tidb_datatype::Datum::UInt(0))
        })
}

/// Go `buildStreamAggFromChildExec`: the StreamAgg Go's TiDB tier runs --
/// every function in `Complete` or `Final` mode, its output exactly the
/// functions' results. The executor takes `child` once the caller has it.
fn build_stream_agg_from_child_exec(
    plan: &PhysicalPlan,
    descriptors: &[AggFuncDesc],
    group_by: &[Expression],
    child: &Box<dyn Executor>,
    ctx: &crate::StmtContext,
) -> Result<Option<Box<dyn FnOnce(Box<dyn Executor>) -> Box<dyn Executor>>>, DriverError> {
    let output_schema = plan_schema(plan)?;
    if output_schema.len() != descriptors.len() {
        return Ok(None);
    }
    let child_schema = child.schema().clone();
    let group_by = resolve_expressions(group_by, &child_schema)?;
    let Some(agg_funcs) = go_agg_funcs(descriptors, &child_schema, ctx)? else {
        return Ok(None);
    };
    let executor_meta = meta(ctx, plan, output_schema);
    // Go: only a final global aggregate that is not all FIRSTROW answers
    // a default row over no input (issues #35295, #30923).
    let default_val = (group_by.is_empty()
        && !tidb_expression_aggregation::is_all_first_row(descriptors)
        && is_final_agg(descriptors))
    .then(|| {
        // Go `e.DefaultVal = e.AllocPool.Alloc(exec.RetTypes(e), 1, 1)`.
        let mut default_val = executor_meta.new_chunk_with_capacity(executor_meta.ret_field_types(), 1, 1);
        for (i, descriptor) in descriptors.iter().enumerate() {
            default_val.append_datum(i, &descriptor.base.get_default_value());
        }
        default_val
    });
    let plan_id = i64::from(plan.id());
    // Go attaches the executor's tracker to the statement's when
    // `tidb_track_aggregate_memory_usage` is on.
    let track = track_aggregate_memory_usage(ctx);
    let memory = ctx.statement_memory();
    let mem_tracker = memory.operator_tracker(plan_id);
    let stmt_mem_tracker = track.then(|| std::sync::Arc::clone(memory.stmt_tracker()));
    let group_checker = tidb_executor_internal_vecgroupchecker::VecGroupChecker::new(group_by);
    let ctx = ctx.clone();
    Ok(Some(Box::new(move |child| {
        Box::new(tidb_executor_aggregate::agg_stream_executor::StreamAggExec::new(
            executor_meta,
            child,
            ctx,
            group_checker,
            agg_funcs,
            default_val,
            mem_tracker,
            stmt_mem_tracker,
        ))
    })))
}

fn build_aggregation_over_child(
    plan: &PhysicalPlan,
    descriptors: &[AggFuncDesc],
    group_by_items: &[Expression],
    stream: bool,
    mut child: Box<dyn Executor>,
    ctx: &crate::StmtContext,
    register_runtime_stats: bool,
) -> Result<Box<dyn Executor>, DriverError> {
    let child_schema = child.schema().clone();
    let group_by = resolve_expressions(group_by_items, &child_schema)?;
    let functions = descriptors
        .iter()
        .map(|descriptor| aggregate_function(descriptor, &child_schema, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let output_schema = plan_schema(plan)?;
    if descriptors
        .iter()
        .all(|descriptor| descriptor.mode == AggFunctionMode::Partial1)
    {
        if let Some(partial) = pushed_partial_aggregation(
            descriptors,
            &functions,
            ctx,
            &group_by,
            stream,
            &child_schema,
            &output_schema,
        )? {
            if child
                .table_access()
                .is_some_and(|access| access.accept_partial_aggregate(&partial, ctx))
            {
                return Ok(Box::new(PushedAggregationExec {
                    meta: meta(ctx, plan, output_schema),
                    child,
                }));
            }
            return Ok(Box::new(LocalPartialAggregationExec {
                meta: meta(ctx, plan, output_schema),
                child,
                aggregate: partial,
                ctx: ctx.clone(),
                rows: None,
            }));
        }
    }
    // A cop-side aggregate with no pushed form, or one the coprocessor of
    // this tier stands in for (a TiFlash fragment's), runs as Go's
    // TiDB-tier executor does.
    let executor = if stream {
        build_stream_agg_from_child_exec(plan, descriptors, group_by_items, &child, ctx)?
    } else {
        build_hash_agg_from_child_exec(plan, descriptors, group_by_items, &child, ctx, register_runtime_stats)?
    };
    executor.map(|executor| executor(child)).ok_or_else(|| {
        DriverError::unsupported("an aggregate function has no Go aggfuncs implementation")
    })
}

/// Go `executorBuilder.buildWindow`: `windows.Build(sctx, v, childExec,
/// false)` over the window plan, its expressions resolved against the
/// child's schema (Go `PhysicalWindow.ResolveIndices`).
fn build_window(
    plan: &PhysicalPlan,
    window: &tidb_planner_core_operator_physicalop::PhysicalWindow,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
    let child_schema = child.schema().clone();
    let output_schema = plan_schema(plan)?;
    let resolve_column = |column: &tidb_expr::column::Column| {
        match resolve_expression(Expression::Column(column.clone()), &child_schema)? {
            Expression::Column(column) => Ok(column),
            _ => Err(DriverError::unsupported("a window ORDER BY item is not a column")),
        }
    };
    let partition_by = window
        .partition_by
        .iter()
        .map(|item| resolve_expression(Expression::Column(item.col.clone()), &child_schema))
        .collect::<Result<Vec<_>, _>>()?;
    let order_by = window
        .order_by
        .iter()
        .map(|item| Ok((resolve_column(&item.col)?, item.desc)))
        .collect::<Result<Vec<_>, DriverError>>()?;
    let window_func_descs = window
        .window_func_descs
        .iter()
        .map(|descriptor| {
            let mut descriptor = descriptor.clone();
            descriptor.base.args = resolve_expressions(&descriptor.base.args, &child_schema)?;
            Ok(descriptor)
        })
        .collect::<Result<Vec<_>, DriverError>>()?;
    let frame = window
        .frame
        .clone()
        .map(|mut frame| {
            for bound in frame.start.iter_mut().chain(frame.end.iter_mut()) {
                bound.calc_funcs = resolve_expressions(&bound.calc_funcs, &child_schema)?;
            }
            Ok::<_, DriverError>(frame)
        })
        .transpose()?;
    tidb_executor_windows::build(
        meta(ctx, plan, output_schema),
        ctx.clone(),
        tidb_executor_windows::WindowPlan { partition_by, order_by, window_func_descs, frame },
        child,
        false,
        ctx.enable_pipelined_window_exec(),
    )
    .map_err(DriverError::from)
}

fn pushed_partial_aggregation(
    descriptors: &[AggFuncDesc],
    functions: &[AggFunc],
    ctx: &crate::StmtContext,
    group_by: &[Expression],
    streamed: bool,
    child_schema: &Schema,
    output_schema: &Schema,
) -> Result<Option<PushdownPartialAggregate>, DriverError> {
    let mut pushed_functions = Vec::new();
    let mut output = 0;
    for (descriptor, function) in descriptors.iter().zip(functions) {
        if function.distinct || !function.extra_args.is_empty() || !function.order_by.is_empty() {
            return Ok(None);
        }
        let mut input = function.arg.clone();
        if matches!(function.kind, AggKind::Count)
            && input.as_ref().is_none_or(|argument| {
                matches!(argument, Expression::Constant(constant)
                    if matches!(constant.value, tidb_datatype::Datum::Int(1)
                        | tidb_datatype::Datum::UInt(1)))
            })
        {
            input = None;
        }
        if input.as_ref().is_some_and(|argument| {
            !tidb_executor_internal_exec::predicate_pushdown::pb_condition_exists(argument, ctx)
        }) {
            return Ok(None);
        }
        let kinds: &[PushdownAggregateKind] = match function.kind {
            AggKind::Count => &[PushdownAggregateKind::Count],
            AggKind::Sum => &[PushdownAggregateKind::Sum],
            AggKind::Min => &[PushdownAggregateKind::Min],
            AggKind::Max => &[PushdownAggregateKind::Max],
            AggKind::MinCount => &[PushdownAggregateKind::MinCount],
            AggKind::MaxCount => &[PushdownAggregateKind::MaxCount],
            AggKind::FirstRow => &[PushdownAggregateKind::FirstRow],
            AggKind::Bit(tidb_executor_aggfuncs::aggfuncs::BitOp::And) => &[PushdownAggregateKind::BitAnd],
            AggKind::Bit(tidb_executor_aggfuncs::aggfuncs::BitOp::Or) => &[PushdownAggregateKind::BitOr],
            AggKind::Bit(tidb_executor_aggfuncs::aggfuncs::BitOp::Xor) => &[PushdownAggregateKind::BitXor],
            AggKind::Avg => &[PushdownAggregateKind::Count, PushdownAggregateKind::Sum],
            _ => return Ok(None),
        };
        // Go `BuildFinalModeAggregation`'s partial functions: each a clone
        // of the descriptor, an AVG split into a COUNT typed by `TypeInfer`
        // and a SUM by `TypeInfer4AvgSum(aggFunc.RetTp)`.
        let ret_types = if matches!(function.kind, AggKind::Avg) {
            let mut count = descriptor.base.clone();
            count.name = "count".to_owned();
            count.type_infer(ctx).map_err(|error| DriverError::unsupported(format!("{error:?}")))?;
            let mut sum = descriptor.base.clone();
            sum.name = "sum".to_owned();
            sum.type_infer_4_avg_sum(&descriptor.base.ret_type)
                .map_err(|error| DriverError::unsupported(format!("{error:?}")))?;
            vec![count.ret_type, sum.ret_type]
        } else {
            vec![descriptor.base.ret_type.clone()]
        };
        for (kind, ret_type) in kinds.iter().zip(ret_types) {
            let Some(column) = output_schema.columns.get(output) else {
                return Err(DriverError::unsupported(
                    "a physical partial aggregate output schema is incomplete",
                ));
            };
            pushed_functions.push(PushdownAggregateFunction {
                kind: *kind,
                input: input.clone(),
                output_type: column
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
                ret_type,
            });
            output += 1;
        }
    }
    // Go `Aggregation.GroupBy`: any key the coprocessor can evaluate over
    // the scan row, a column or an expression.
    let mut group_keys = Vec::with_capacity(group_by.len());
    let mut group_types = Vec::with_capacity(group_by.len());
    for expression in group_by {
        if !tidb_executor_internal_exec::predicate_pushdown::pb_condition_exists(expression, ctx)
            || crate::access_path::expression_column_offsets(std::slice::from_ref(expression))
                .iter()
                .any(|offset| *offset >= child_schema.len())
        {
            return Ok(None);
        }
        let Some(field_type) = expression.static_type().cloned() else {
            return Ok(None);
        };
        group_keys.push(expression.clone());
        group_types.push(field_type);
    }
    if output + group_keys.len() != output_schema.len() {
        return Ok(None);
    }
    let partial = if group_keys.is_empty() {
        PushdownPartialAggregate::Global {
            functions: pushed_functions,
            streamed,
        }
    } else {
        PushdownPartialAggregate::Grouped {
            group_by: group_keys,
            group_types,
            functions: pushed_functions,
            streamed,
        }
    };
    Ok(Some(partial))
}

fn build_reader(
    reader_id: i32,
    embedded: Option<&PhysicalPlan>,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let plan =
        embedded.ok_or_else(|| DriverError::unsupported("a physical reader has no pushed plan"))?;
    let previous = std::mem::replace(&mut state.in_reader, true);
    let old_bytes = state.reader_net_bytes.replace(
        plan.cop_net_data_size(
            embedded_index_scan(plan).is_some(),
            tidb_config::tikvcfg::get_global_config()
                .tikv_client
                .enable_chunk_rpc,
        ),
    );
    let old_chain = state
        .cop_chain
        .replace((cop_plan_chain(plan), i64::from(reader_id)));
    let result = build_with_state(plan, catalog, ctx, state);
    state.in_reader = previous;
    state.reader_net_bytes = old_bytes;
    state.cop_chain = old_chain;
    result
}

/// The coprocessor TopN of a reader's pushed `topn` over its child's row:
/// Go `PhysicalTopN.ToPB` lowers each `ByItem`, an expression over that row.
fn remote_topn(
    topn: &tidb_planner_core_operator_physicalop::PhysicalTopN,
    child_schema: &Schema,
) -> Option<tidb_distsql::remote_scan::PushdownTopN> {
    let order_by = topn
        .by_items
        .iter()
        .map(|item| {
            let expr = resolve_expression(item.expr.clone(), child_schema).ok()?;
            Some(tidb_distsql::remote_scan::PushdownTopNOrder { expr, desc: item.desc })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(tidb_distsql::remote_scan::PushdownTopN {
        order_by,
        limit: topn.offset.checked_add(topn.count)?,
    })
}

/// Go's coprocessor TopN evaluates its ordering expressions into a separate
/// key row. Materialize those keys privately when executing a reader locally;
/// the ordinary TopN still receives only columns and constants.
fn materialize_reader_topn_keys(
    plan: &PhysicalPlan,
    child: Box<dyn Executor>,
    by_items: &mut [SortByItem],
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    if by_items
        .iter()
        .all(|item| matches!(item.expr, Expression::Column(_) | Expression::Constant(_)))
    {
        return Ok(child);
    }
    let mut schema = child.schema().clone();
    let mut expressions: Vec<_> = schema
        .columns
        .iter()
        .cloned()
        .map(Expression::Column)
        .collect();
    let mut next_id = schema
        .columns
        .iter()
        .map(|column| column.unique_id)
        .max()
        .unwrap_or(0);
    for item in by_items {
        if matches!(item.expr, Expression::Column(_) | Expression::Constant(_)) {
            continue;
        }
        let field_type =
            item.expr.static_type().cloned().ok_or_else(|| {
                DriverError::unsupported("a coprocessor TopN key has no result type")
            })?;
        next_id = next_id
            .checked_add(1)
            .ok_or_else(|| DriverError::unsupported("coprocessor TopN key column ID overflow"))?;
        // These IDs are local to the private projection and are pruned from
        // TopN output; they never enter the retained physical plan.
        let mut column = Column::new(next_id, field_type);
        column.index = schema.columns.len() as i64;
        schema.columns.push(column.clone());
        expressions.push(std::mem::replace(
            &mut item.expr,
            Expression::Column(column),
        ));
    }
    // The child's columns carry the offsets of the schema they were planned
    // in (a pushed Projection's output keeps its scan offsets), so both the
    // passed-through columns and the moved keys resolve against the child.
    let expressions = resolve_expressions(&expressions, child.schema())?;
    Ok(Box::new(ProjectionExec::new(
        meta(ctx, plan, schema),
        expressions,
        child,
        ctx.clone(),
    )))
}

/// Extracts the partial aggregation carried by an index-lookup table plan.
/// Go's `PhysicalIndexLookUpReader` executes this cop aggregate on the table
/// side before the root final aggregate consumes its partial rows. The Rust
/// reader owns both sides of that plan, so it must hand the descriptor to the
/// index source rather than silently discarding `table_plan`.
fn reader_partial_aggregate(
    table_plan: &PhysicalPlan,
    child_schema: &Schema,
    ctx: &crate::StmtContext,
) -> Result<Option<PushdownPartialAggregate>, DriverError> {
    let Some((descriptors, group_by, stream)) = partial_aggregate_parts(table_plan) else {
        return Ok(None);
    };
    let functions = descriptors
        .iter()
        .map(|descriptor| aggregate_function(descriptor, child_schema, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let group_by = resolve_expressions(group_by, child_schema)?;
    pushed_partial_aggregation(
        descriptors,
        &functions,
        ctx,
        &group_by,
        stream,
        child_schema,
        &plan_schema(table_plan)?,
    )
}

/// The cop partial aggregate (`Partial1` functions, group-by items,
/// streamed) an index-lookup table plan carries at its root.
fn partial_aggregate_parts(table_plan: &PhysicalPlan) -> Option<(&[AggFuncDesc], &[Expression], bool)> {
    let (descriptors, group_by, stream) = match table_plan {
        PhysicalPlan::HashAgg(agg) => (&agg.agg_funcs, &agg.group_by_items, false),
        PhysicalPlan::StreamAgg(agg) => (&agg.agg_funcs, &agg.group_by_items, true),
        _ => return None,
    };
    descriptors
        .iter()
        .all(|descriptor| descriptor.mode == AggFunctionMode::Partial1)
        .then_some((descriptors.as_slice(), group_by.as_slice(), stream))
}

fn embedded_index_scan(plan: &PhysicalPlan) -> Option<&PhysicalIndexScan> {
    match plan {
        PhysicalPlan::IndexScan(scan) => Some(scan),
        _ => plan.children().iter().find_map(embedded_index_scan),
    }
}

fn embedded_table_scan(plan: &PhysicalPlan) -> Option<&PhysicalTableScan> {
    match plan {
        PhysicalPlan::TableScan(scan) => Some(scan),
        _ => plan.children().iter().find_map(embedded_table_scan),
    }
}

/// Go pushes a `Limit offset:o, count:c | cop[tikv]` under the index reader's
/// `IndexPlan`, so the coprocessor index scan itself stops after `o + c`
/// entries. The local byte-level cursor has no region request to carry it, so
/// [`build_index_reader`] hands the same cap to [`IndexRangeSourceExec`].
fn embedded_index_limit(plan: &PhysicalPlan) -> Option<(u64, u64)> {
    match plan {
        PhysicalPlan::Limit(limit) => Some((limit.offset, limit.count)),
        _ => plan.children().iter().find_map(embedded_index_limit),
    }
}

/// The columns a reader's pushed Projection selects when it projects only
/// columns of the IndexScan right under it (Go `PhysicalProjection` pushed to
/// the coprocessor), which is then what the request returns.
fn column_projection_over_index_scan(plan: &PhysicalPlan) -> Option<Vec<tidb_expr::column::Column>> {
    let PhysicalPlan::Projection(projection) = plan else {
        return None;
    };
    if !matches!(plan.children().first(), Some(PhysicalPlan::IndexScan(_))) {
        return None;
    }
    projection
        .exprs
        .iter()
        .map(|expr| match expr {
            Expression::Column(column) => Some(column.clone()),
            _ => None,
        })
        .collect()
}

/// Whether a reader's pushed plan is its IndexScan under zero or more
/// Limits, which output their child's columns as they are.
/// A pushed Limit over pushed Limits and Selections down to the IndexScan,
/// with at least one Selection.
fn limit_over_selection_chain(plan: &PhysicalPlan) -> bool {
    fn chain(plan: &PhysicalPlan, selected: bool) -> bool {
        match plan {
            PhysicalPlan::IndexScan(_) => selected,
            PhysicalPlan::Limit(_) => plan.children().first().is_some_and(|child| chain(child, selected)),
            PhysicalPlan::Selection(_) => plan.children().first().is_some_and(|child| chain(child, true)),
            _ => false,
        }
    }
    matches!(plan, PhysicalPlan::Limit(_)) && chain(plan, false)
}

fn limit_chain_over_index_scan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::IndexScan(_) => true,
        PhysicalPlan::Limit(_) => plan.children().first().is_some_and(limit_chain_over_index_scan),
        _ => false,
    }
}

/// The index-side TopN of a `PhysicalIndexLookUpReader`'s `IndexPlans`
/// (`TopN(Build)`), which Go lowers into the index request's DAG.
fn embedded_index_topn(
    plan: &PhysicalPlan,
) -> Option<&tidb_planner_core_operator_physicalop::PhysicalTopN> {
    match plan {
        PhysicalPlan::TopN(topn) => Some(topn),
        _ => plan.children().iter().find_map(embedded_index_topn),
    }
}

fn embedded_index_prefix_limit(
    plan: &PhysicalPlan,
) -> Option<&tidb_planner_core_operator_physicalop::PhysicalLimit> {
    match plan {
        PhysicalPlan::Limit(limit) if limit.prefix_col.is_some() => Some(limit),
        _ => plan.children().iter().find_map(embedded_index_prefix_limit),
    }
}

fn index_lookup_conditions(plan: &PhysicalPlan, output: &mut Vec<Expression>) {
    for child in plan.children() {
        index_lookup_conditions(child, output);
    }
    if let PhysicalPlan::Selection(selection) = plan {
        output.extend(selection.conditions.iter().cloned());
    }
}

/// Go `AddSelectionConditionForGlobalIndex` pushes `in(_tidb_tid, ids)` into
/// a global index's cop Selection, and TiKV evaluates it over the partition
/// id each entry's value records. The in-process index cursors decode that
/// id while they walk, so the condition lowers to the table's read
/// restriction rather than a row filter: the reader's row schema does not
/// carry `_tidb_tid` (Go's DataSource schema does not either). Returns the
/// remaining conditions and the admitted partition ids (`-1`, Go's
/// impossible id for an empty set, admits none).
fn split_global_partition_filter(
    conditions: Vec<Expression>,
) -> (Vec<Expression>, Option<Vec<i64>>) {
    let mut admitted: Option<Vec<i64>> = None;
    let mut rest = Vec::with_capacity(conditions.len());
    for condition in conditions {
        let ids = match &condition {
            Expression::ScalarFunction(function) if function.func_name.lowercase() == "in" => {
                match function.args.split_first() {
                    Some((Expression::Column(column), values))
                        if column.id == tidb_model::column::EXTRA_PHYS_TBL_ID =>
                    {
                        values
                            .iter()
                            .map(|value| match value {
                                Expression::Constant(constant) => match constant.value {
                                    tidb_datatype::Datum::Int(id) => Some(id),
                                    _ => None,
                                },
                                _ => None,
                            })
                            .collect::<Option<Vec<_>>>()
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        match ids {
            Some(mut ids) => {
                ids.retain(|id| *id >= 0);
                ids.sort_unstable();
                ids.dedup();
                admitted = Some(match admitted {
                    Some(previous) => previous.into_iter().filter(|id| ids.contains(id)).collect(),
                    None => ids,
                });
            }
            None => rest.push(condition),
        }
    }
    (rest, admitted)
}

/// The partition ids a global index's cop Selection admits
/// ([`split_global_partition_filter`]), if it carries that filter.
fn global_index_partition_restriction(index_plan: &PhysicalPlan) -> Option<Vec<i64>> {
    let mut conditions = Vec::new();
    index_lookup_conditions(index_plan, &mut conditions);
    split_global_partition_filter(conditions).1
}

fn lower_index_lookup_selections(
    plan: &PhysicalPlan,
    source: &mut IndexRangeSourceExec,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if let PhysicalPlan::LocalIndexLookUp(_) = plan {
        // The build side is the index subtree TiKV runs before the local
        // lookup: its Selection filters index ENTRIES, below any build-side
        // `Limit` that cuts them (`Limit | Selection(a < 'c') | IndexFullScan`
        // keeps the first three qualifying entries). Only the probe side and
        // the operators above the junction filter looked-up rows.
        let [build, probe] = plan.children() else {
            return Err(DriverError::unsupported(
                "a LocalIndexLookUp needs an index subtree and a table scan",
            ));
        };
        let mut conditions = Vec::new();
        index_lookup_conditions(build, &mut conditions);
        let (conditions, _) = split_global_partition_filter(conditions);
        let conditions = resolve_expressions(&conditions, source.schema())?;
        source.set_index_conditions(conditions, ctx);
        return lower_index_lookup_selections(probe, source, ctx);
    }
    for child in plan.children() {
        lower_index_lookup_selections(child, source, ctx)?;
    }
    let PhysicalPlan::Selection(selection) = plan else {
        return Ok(());
    };
    // The reader applied the global index's partition filter as its read
    // restriction (`global_index_partition_restriction`).
    let (conditions, _) = split_global_partition_filter(selection.conditions.clone());
    if conditions.is_empty() {
        return Ok(());
    }
    let filters = resolve_expressions(&conditions, source.schema())?;
    let pushed =
        tidb_executor_internal_exec::predicate_pushdown::PushedScanFilter::from_physical_conditions(filters, ctx);
    if !source.accept_scan_filter(&pushed, ctx) {
        return Err(DriverError::unsupported(
            "a physical index lookup cannot apply its retained Selection",
        ));
    }
    Ok(())
}

/// Go `ExtraHandleID`'s `_tidb_rowid`, by ID or by its name.
fn is_extra_handle_column(column: &Column) -> bool {
    column.id == tidb_model::column::EXTRA_HANDLE_ID
        || column
            .orig_name
            .rsplit('.')
            .next()
            .is_some_and(|name| name.eq_ignore_ascii_case(tidb_model::column::EXTRA_HANDLE_NAME))
}

fn reader_output_offsets(
    schema: &Schema,
    scan: &PhysicalIndexScan,
    table: &crate::TableCommon,
) -> Result<(Schema, Vec<usize>, Option<usize>), DriverError> {
    let extra_handle = schema.columns.iter().position(is_extra_handle_column);
    let mut source_columns = Vec::with_capacity(schema.columns.len());
    let mut offsets = Vec::with_capacity(schema.columns.len());
    let mut extra_handle_slot = None;
    for (slot, output) in schema.columns.iter().enumerate() {
        if output.id == tidb_model::column::EXTRA_PHYS_TBL_ID {
            // Both local and global index cursors retain the record's route.
            source_columns.push(output.clone());
            continue;
        }
        if Some(slot) == extra_handle {
            if extra_handle_slot.is_some() {
                return Err(DriverError::unsupported(
                    "a physical index reader has multiple _tidb_rowid outputs",
                ));
            }
            extra_handle_slot = Some(source_columns.len());
            // The rowid occupies a schema slot with no stored offset behind
            // it: it must exist in the source schema (the slot index names a
            // real column) while staying out of the kept offsets, which the
            // `read_table_columns` invariant checks. Without this push the
            // schema is never wider than `keep` and column-less selects over
            // handle-forced sources (Go's taobench case, `SELECT 1 FROM t`)
            // trip the width assertion.
            source_columns.push(output.clone());
            continue;
        }
        let source = scan
            .cost_columns
            .iter()
            .find(|column| column.unique_id == output.unique_id)
            .unwrap_or(output);
        let Some(offset) = table
            .columns()
            .iter()
            .position(|column| column.read().id == source.id)
            .or_else(|| {
                source.orig_name.rsplit('.').next().and_then(|name| {
                    table
                        .columns()
                        .iter()
                        .position(|column| column.read().name.original().eq_ignore_ascii_case(name))
                })
            })
        else {
            // A COUNT(*) child can carry a synthetic, id-less aggregate slot
            // in the physical index plan. It is not a stored column and the
            // index source must emit zero data columns so the parent aggregate
            // can count rows. Real unresolved columns remain hard errors.
            if output.id == 0 && source.id == 0 && source.orig_name.is_empty() {
                continue;
            }
            return Err(DriverError::unsupported(
                "a physical index reader output is absent from its table",
            ));
        };
        source_columns.push(output.clone());
        offsets.push(offset);
    }
    Ok((Schema::new(source_columns), offsets, extra_handle_slot))
}

/// Go's no-range reader builders retain one response-width policy for both
/// ordinary reads and rebuilt index-join probes.
/// Binds the plan IDs a reader's coprocessor requests report under. `plan`
/// is the pushed subtree the request reads (Go `idxPlans`); with a
/// `table_lookup_plan` the request is an index lookup's index side and the
/// table side gets its own chain (`tblPlans`). `chain` is the reader's own
/// chain when it is the whole read.
fn with_reader_cop_chain(
    statement: PushdownStatementContext,
    plan: &PhysicalPlan,
    table_lookup_plan: Option<&PhysicalPlan>,
    chain: Option<&CopChain>,
) -> PushdownStatementContext {
    let statement = match (table_lookup_plan, chain) {
        (None, Some((chain, root))) => statement.with_cop_plan_chain(chain.clone(), *root),
        _ => statement.with_cop_plan_chain(cop_plan_chain(plan), i64::from(plan.id())),
    };
    match table_lookup_plan {
        Some(table) => {
            let mut statement =
                statement.with_lookup_cop_plan_chain(cop_plan_chain(table), i64::from(table.id()));
            statement.lookup_table_limit = table_plan_limit(table);
            statement
        }
        None => statement,
    }
}

/// The `offset + count` of the Limit in an index lookup's table plan.
fn table_plan_limit(table_plan: &PhysicalPlan) -> Option<u64> {
    let mut node = Some(table_plan);
    while let Some(plan) = node {
        if let PhysicalPlan::Limit(limit) = plan {
            return limit.offset.checked_add(limit.count);
        }
        node = plan.children().first();
    }
    None
}

fn cop_reader_statement(
    plan: &PhysicalPlan,
    table_lookup_plan: Option<&PhysicalPlan>,
    encoded_key: bool,
    net_bytes: Option<f64>,
    ctx: &crate::StmtContext,
    cop_chain: Option<&CopChain>,
) -> PushdownStatementContext {
    let chunk_rpc = tidb_config::tikvcfg::get_global_config()
        .tikv_client
        .enable_chunk_rpc;
    let average = table_lookup_plan.map(|plan| plan.cop_avg_row_size(false, chunk_rpc));
    let mut statement = with_reader_cop_chain(
        crate::remote_scan::pushdown_statement_context(ctx),
        plan,
        table_lookup_plan,
        cop_chain,
    )
    .with_plan_id(i64::from(plan.id()))
    .with_estimated_net_bytes(
        average
            .or(net_bytes)
            .unwrap_or_else(|| plan.cop_net_data_size(encoded_key, chunk_rpc)),
    );
    statement.lookup_avg_row_bytes = average;
    statement
}

fn build_index_reader(
    plan: &PhysicalPlan,
    index_plan: &PhysicalPlan,
    table_plan: Option<&PhysicalPlan>,
    covering: bool,
    keep_order: bool,
    pushed_limit: Option<tidb_planner_core_operator_physicalop::PushedDownLimit>,
    expect_cnt: Option<u64>,
    lookup_pushdown: bool,
    scan_limit: Option<u64>,
    output_columns: Option<Vec<tidb_expr::column::Column>>,
    cop_projection: bool,
    net_bytes: Option<f64>,
    range_group: Option<&tidb_ranger::types::Ranges>,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    storage: &ReaderStorage,
    cop_chain: Option<&CopChain>,
    reader_mem_tracker: Option<Arc<tidb_memory::Tracker>>,
) -> Result<Box<dyn Executor>, DriverError> {
    let scan = embedded_index_scan(index_plan)
        .ok_or_else(|| DriverError::unsupported("a physical index reader has no index scan"))?;
    // Go's mem readers answer in index order whatever the reader's
    // `keepOrder` (`memIndexLookUpReader.getMemRowsIter` turns the buffered
    // index entries' handles into table ranges in that order), and
    // `UnionScanExec.getOneRow` merges them with the snapshot stream by
    // `usedIndex`; handle order would append the staged rows instead.
    let keep_order = keep_order || storage.is_mem_buffer();
    let (mut table, store, separated) = storage.table(
        ctx,
        catalog
            .physical_kv_table_by_id(scan.table_id)
            .ok_or_else(|| {
                DriverError::unsupported("physical index table ID is absent from the catalog")
            })?,
    );
    // Global index keys share the logical table prefix; the cursor filters
    // their encoded partition IDs using this same read restriction.
    if let Some(access) = &scan.dynamic_partition_access {
        if !access.all_partitions {
            let ids = table.meta().read().get_partition_info().map_or_else(Vec::new, |pi| {
                pi.read()
                    .definitions
                    .snapshot()
                    .iter()
                    .filter(|definition| {
                        access
                            .partitions
                            .iter()
                            .any(|name| definition.name.original().go_lower_eq(name))
                    })
                    .map(|definition| definition.id)
                    .collect()
            });
            std::sync::Arc::make_mut(&mut table).restrict_read_to_partitions(&ids);
        }
    }
    if let Some(ids) = global_index_partition_restriction(index_plan) {
        std::sync::Arc::make_mut(&mut table).restrict_read_to_partitions(&ids);
    }
    let table = table_with_plan_index(table, Some(scan.index_id), ctx)?;
    if !table
        .indexes()
        .iter()
        .any(|index| index.read().id == scan.index_id)
    {
        return Err(DriverError::unsupported(
            "physical index ID is absent from its table",
        ));
    }
    let schema = plan_schema(plan)?;
    // A cop partial aggregate's reader OUTPUT is the aggregate's result, not
    // the table row; the source must still read the aggregate's INPUT columns.
    // When the table plan is that aggregate, derive the kept columns from its
    // child's row schema (the table scan), exactly the schema Go resolves the
    // aggregate's arguments against.
    let cop_row_schema = table_plan
        .filter(|plan| matches!(plan, PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_)))
        .and_then(|plan| plan.children().first())
        .and_then(PhysicalPlan::schema)
        .cloned();
    let offset_schema = cop_row_schema.as_ref().unwrap_or(&schema);
    let (source_schema, keep, extra_handle) = reader_output_offsets(offset_schema, scan, &table)?;
    let scan_ranges = range_group
        .filter(|_| !scan.group_by_col_idxs.is_empty())
        .unwrap_or(&scan.ranges);
    let ranges = if scan_ranges.is_empty() {
        tidb_ranger::points::full_range()
    } else {
        scan_ranges.clone()
    };
    let prefix_limit = embedded_index_prefix_limit(index_plan);
    let prefix_index_offset = prefix_limit
        .map(|limit| {
            let prefix_id = limit
                .prefix_col
                .expect("a prefix Limit has a prefix column");
            let column = scan
                .cost_columns
                .iter()
                .chain(source_schema.columns.iter())
                .find(|column| column.unique_id == prefix_id)
                .ok_or_else(|| {
                    DriverError::unsupported("a prefix Limit column is absent from its scan")
                })?;
            table
                .indexes()
                .iter()
                .find(|index| index.read().id == scan.index_id)
                .and_then(|index| {
                    index
                        .column_offsets
                        .iter()
                        .position(|offset| table.columns()[*offset].read().id == column.id)
                })
                .ok_or_else(|| {
                    DriverError::unsupported("a prefix Limit column is absent from its index")
                })
        })
        .transpose()?;
    let logical_table_id = table.table_id();
    // Go `buildIndexUsageReporter(v, true)`.
    let stats = crate::access_cost::load_table_stats(ctx, catalog, table.stats_physical_id());
    let mut source = IndexRangeSourceExec::new_with_statement(
        meta(ctx, plan, source_schema),
        std::sync::Arc::unwrap_or_clone(table),
        store,
        scan.index_id,
        ranges,
        RowDecodeContext::for_query(ctx),
        ReaderStorage::statement(separated, {
            let mut statement =
                cop_reader_statement(index_plan, table_plan, true, net_bytes, ctx, cop_chain)
                    .with_plan_id(i64::from(scan.base.base.id()));
            statement.select_mem_tracker = reader_mem_tracker;
            statement
        }),
    );
    if let Some(slot) = extra_handle {
        source.read_extra_handle(slot);
    }
    if let Some(rebuilder) = correlated_index_range_rebuilder(scan, ctx) {
        source.set_range_rebuilder(rebuilder);
    }
    source.read_table_columns(keep);
    source.set_lookup_concurrency(ctx.index_lookup_concurrency());
    source.set_lookup_size(ctx.index_lookup_size());
    // Go `IndexLookUpExecutor.memTracker`, attached to the statement's on
    // every run, and `exec.Next`'s RU v2 recording of the
    // `TableReaderExecutor` each of its table tasks builds.
    if table_plan.is_some() && !covering {
        let memory = ctx.statement_memory();
        source.set_lookup_memory(
            memory.operator_tracker(i64::from(plan.id())),
            Arc::clone(memory.stmt_tracker()),
        );
        source.set_table_reader_ruv2(tidb_executor_internal_exec::ruv2::Ruv2NextCache::new(
            "*executor.TableReaderExecutor",
            ctx.ruv2_metrics(),
        ));
    }
    // Go `IndexLookUpExecutor.initRuntimeStats` and `indexScanBasicStats`.
    if let (Some(table_plan), Some(runtime_source), false) =
        (table_plan, ctx.executor_runtime_stats_source(), covering)
    {
        let index_root = i64::from(index_plan.id());
        source.set_lookup_runtime_stats(Some(
            crate::index_lookup_stats::IndexLookUpStatsHandle {
                stats: Arc::new(crate::index_lookup_stats::IndexLookUpRunTimeStats::new(
                    ctx.index_lookup_concurrency(),
                )),
                index_scan_basic: (index_root != i64::from(plan.id()))
                    .then(|| runtime_source.basic_runtime_stats(index_root))
                    .flatten(),
                source: Arc::clone(runtime_source),
                table_root_plan_id: i64::from(table_plan.id()),
                plan_id: i64::from(plan.id()),
                index_end_recorded: false,
            },
        ));
    }
    if lookup_pushdown {
        source.enable_lookup_pushdown();
    }
    if covering {
        source.mark_covering();
        source.answer_in_index_order();
        if let Some(cap) = scan_limit {
            // The covering read answers in index order, so the pushed
            // coprocessor Limit truncates exactly the prefix the `LimitExec`
            // above keeps. A refused cap leaves that executor as the sole
            // authority, which is always correct, only slower.
            let _ = source.accept_scan_limit(cap);
        }
    }
    if (scan.keep_order || keep_order) && !source.accept_keep_order(scan.desc) {
        return Err(DriverError::unsupported(
            "the physical index reader cannot preserve its order",
        ));
    }
    if let Some(rows) = scan.base.base.stats_info().map(|stats| stats.row_count()) {
        source.accept_scan_estimate(rows);
    }
    if lookup_pushdown {
        // A LocalIndexLookUp's rewritten index subtree includes table
        // predicates; those still need full rows from the lookup. Its build
        // side's predicates are index conditions, as below.
        lower_index_lookup_selections(index_plan, &mut source, ctx)?;
    } else {
        let mut conditions = Vec::new();
        index_lookup_conditions(index_plan, &mut conditions);
        let (conditions, _) = split_global_partition_filter(conditions);
        let conditions = resolve_expressions(&conditions, source.schema())?;
        source.set_index_conditions(conditions, ctx);
    }
    if let Some(limit) = prefix_limit {
        source.set_prefix_limit(
            limit.count,
            prefix_index_offset.expect("resolved prefix position"),
        );
    }
    // Go serializes the cloned table subtree through `IndexPlans` after a
    // successful local lookup rewrite. The retained `TablePlans` then exist
    // only to show the TiDB-side zero-row branch, so do not lower them again.
    if let Some(table_plan) = table_plan.filter(|_| !lookup_pushdown) {
        lower_index_lookup_selections(table_plan, &mut source, ctx)?;
        // Go's `tblPlans` TopN (`TopN(Probe)`) is an executor of every table
        // task's request; its keys are expressions over the lookup's row.
        if let Some(topn) = embedded_index_topn(table_plan) {
            if let Some(topn) = remote_topn(topn, source.schema()) {
                source.set_lookup_table_topn(topn);
            }
        }
    }
    // Go `buildIndexLookUpReader`: an index-side TopN is an executor of the
    // index request, so only its handles reach the table lookup. Its keys are
    // expressions over the lookup's row; the root TopN still orders the rows,
    // so a refused offer changes only how many handles are read.
    if let Some(topn) = embedded_index_topn(index_plan).filter(|_| !covering) {
        let order_by = topn
            .by_items
            .iter()
            .map(|item| {
                resolve_expression(item.expr.clone(), source.schema())
                    .ok()
                    .map(|expr| (expr, item.desc))
            })
            .collect::<Option<Vec<_>>>();
        if let (Some(order_by), Some(limit)) = (order_by, topn.offset.checked_add(topn.count)) {
            let _ = source.accept_index_top_n(&order_by, limit);
        }
    }
    // The table side's cop partial aggregate runs in the source when it
    // accepts it, and over the source's rows otherwise; the root's final
    // aggregate reads partial rows either way.
    let mut local_partial_aggregate = None;
    if let Some(table_plan) = table_plan {
        let pushed = match reader_partial_aggregate(table_plan, source.schema(), ctx)? {
            Some(partial) => source.accept_partial_aggregate(&partial, ctx),
            None => false,
        };
        if pushed {
            source.set_partial_output_schema(plan_schema(table_plan)?);
        } else if partial_aggregate_parts(table_plan).is_some() {
            local_partial_aggregate = Some(table_plan);
        }
    }
    if let Some(limit) = pushed_limit {
        if !source.accept_embedded_lookup_limit(limit.offset, limit.count) {
            return Err(DriverError::unsupported(
                "the physical index lookup cannot apply its pushed limit",
            ));
        }
    }
    if let Some(expect_cnt) = expect_cnt.filter(|_| !covering) {
        if !source.accept_lookup_batch_size(expect_cnt) {
            return Err(DriverError::unsupported(
                "the physical index lookup cannot apply its paging size",
            ));
        }
    }
    // Go puts `UnionScanExec` above this reader when the open transaction has
    // written the table, and for an UNORDERED lookup that operator re-merges
    // the handle-ordered snapshot stream with the index-ordered staged stream
    // by `compareExec.compare`. This tier folds that merge into the source;
    // arming it here keeps every other read untouched.
    source.enable_dirty_union_scan_merge();
    // Go `dagReq.OutputOffsets` of a covering `IndexReader`: the request
    // returns its `OutputColumns`, in their order, with no projection above.
    if let Some(output_columns) = output_columns {
        let keep = output_columns
            .iter()
            .map(|output| {
                source
                    .schema()
                    .columns
                    .iter()
                    .position(|column| column.unique_id == output.unique_id)
            })
            .collect::<Option<Vec<_>>>();
        let narrowed = keep.is_some_and(|keep| {
            keep.iter().copied().eq(0..source.schema().columns.len()) || source.accept_column_prune(&keep)
        });
        // Go `PhysicalProjection.ToPB`: a pushed Projection of these columns
        // travels as the request's Projection executor.
        if narrowed && cop_projection {
            source.lower_output_as_projection();
        }
    }
    let source: Box<dyn Executor> = if storage.is_mem_buffer() {
        Box::new(source)
    } else {
        Box::new(CopIndexUsageExec::new(
            Box::new(source),
            logical_table_id,
            stats,
            ctx.index_usage_collector().cloned(),
            Some(scan.index_id),
        ))
    };
    match local_partial_aggregate {
        Some(table_plan) => {
            let (descriptors, group_by, stream) =
                partial_aggregate_parts(table_plan).expect("a partial aggregate table plan");
            build_aggregation_over_child(table_plan, descriptors, group_by, stream, source, ctx, false)
        }
        None => Ok(source),
    }
}

fn join_kind(join_type: LogicalJoinType) -> Result<JoinKind, DriverError> {
    match join_type {
        LogicalJoinType::Inner => Ok(JoinKind::Inner),
        LogicalJoinType::LeftOuter => Ok(JoinKind::Left),
        LogicalJoinType::RightOuter => Ok(JoinKind::Right),
        LogicalJoinType::Semi => Ok(JoinKind::Semi),
        LogicalJoinType::AntiSemi => Ok(JoinKind::AntiSemi),
        LogicalJoinType::LeftOuterSemi => Ok(JoinKind::LeftOuterSemi),
        LogicalJoinType::AntiLeftOuterSemi => Ok(JoinKind::AntiLeftOuterSemi),
    }
}

fn joiner_type(join_type: LogicalJoinType) -> JoinerType {
    match join_type {
        LogicalJoinType::Inner => JoinerType::Inner,
        LogicalJoinType::LeftOuter => JoinerType::LeftOuter,
        LogicalJoinType::RightOuter => JoinerType::RightOuter,
        LogicalJoinType::Semi => JoinerType::SemiJoin,
        LogicalJoinType::AntiSemi => JoinerType::AntiSemiJoin,
        LogicalJoinType::LeftOuterSemi => JoinerType::LeftOuterSemiJoin,
        LogicalJoinType::AntiLeftOuterSemi => JoinerType::AntiLeftOuterSemiJoin,
    }
}

fn filtered_join_child(
    plan: &PhysicalPlan,
    child: Box<dyn Executor>,
    conditions: &[Expression],
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    if conditions.is_empty() {
        return Ok(child);
    }
    let schema = child.schema().clone();
    let filters = resolve_expressions(conditions, &schema)?;
    Ok(Box::new(SelectionExec::new(
        meta(ctx, plan, schema),
        filters,
        child,
        ctx.clone(),
        ctx.statement_memory(),
    )))
}

fn join_equality_with_null(
    left: &Column,
    right: &Column,
    schema: &Schema,
    null_safe: bool,
) -> Result<Expression, DriverError> {
    join_equality_with_collation(left, right, schema, null_safe, None)
}

/// [`join_equality_with_null`] comparing strings under `collation`, which the
/// comparison's result type carries.
fn join_equality_with_collation(
    left: &Column,
    right: &Column,
    schema: &Schema,
    null_safe: bool,
    collation: Option<tidb_charset::Collation>,
) -> Result<Expression, DriverError> {
    let mut expression = tidb_expr::new_function(
        &tidb_exprctx::NoColumns,
        if null_safe { "nulleq" } else { "eq" },
        FieldType::new(FieldTypeCode::Tiny),
        vec![
            Expression::Column(left.clone()),
            Expression::Column(right.clone()),
        ],
    )
    .map_err(|error| DriverError::from(crate::ExecError::Eval(error)))?;
    if let (Some(collation), Expression::ScalarFunction(function)) = (collation, &mut expression) {
        function
            .collation
            .set_charset_and_collation(collation.charset().name(), collation.name());
    }
    resolve_expression(expression, schema)
}

fn join_output_offsets(
    output_schema: &Schema,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Vec<usize>, DriverError> {
    let merged_columns = left_schema
        .columns
        .iter()
        .chain(&right_schema.columns)
        .collect::<Vec<_>>();
    output_schema
        .columns
        .iter()
        .map(|output| {
            usize::try_from(output.index)
                .ok()
                .filter(|index| {
                    merged_columns
                        .get(*index)
                        .is_some_and(|column| column.unique_id == output.unique_id)
                })
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a physical join output is absent from both child schemas",
                    )
                })
        })
        .collect()
}

/// The output offsets of a physical join. A `LeftOuterSemi` join appends its
/// 0/1 marker column AFTER the outer child's columns (`JoinExec`'s semi paths
/// call `append_datum(outer_row.len(), ...)`), so the marker is in neither
/// child schema and `join_output_offsets` cannot place it. Resolve retained
/// outer columns by identity; pruning may remove a prefix of the outer schema.
fn physical_join_output_offsets(
    plan: &PhysicalPlan,
    join_type: LogicalJoinType,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Vec<usize>, DriverError> {
    let schema = plan_schema(plan)?;
    match join_type {
        LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi => {
            let mut offsets = schema.columns[..schema.len().saturating_sub(1)]
                .iter()
                .map(|output| {
                    left_schema
                        .columns
                        .iter()
                        .position(|column| column.unique_id == output.unique_id)
                        .ok_or_else(|| {
                            DriverError::unsupported(
                                "semi join output is absent from its outer schema",
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            offsets.push(left_schema.len());
            Ok(offsets)
        }
        _ => join_output_offsets(&schema, left_schema, right_schema),
    }
}

fn physical_table_schema(plan: &PhysicalPlan, table: &crate::TableCommon) -> Schema {
    fn scan_columns(plan: &PhysicalPlan) -> Option<&[Column]> {
        match plan {
            PhysicalPlan::TableScan(scan) => Some(scan.cost_columns.as_slice()),
            PhysicalPlan::IndexScan(scan) => Some(scan.cost_columns.as_slice()),
            _ => plan.children().iter().find_map(scan_columns),
        }
    }
    let planned_columns = scan_columns(plan);
    let columns = table
        .logical_columns()
        .iter()
        .enumerate()
        .map(|(index, stored)| {
            let mut column = planned_columns
                .and_then(|columns| columns.iter().find(|column| column.id == stored.read().id))
                .cloned()
                .unwrap_or_else(|| Column::new(stored.read().id, stored.read().field_type.clone()));
            column.id = stored.read().id;
            column.index = index as i64;
            column
        })
        .collect();
    Schema::new(columns)
}

fn collect_index_inner_filters(
    plan: &PhysicalPlan,
    schema: &Schema,
    filters: &mut Vec<Expression>,
) -> Result<(), DriverError> {
    for child in plan.children() {
        collect_index_inner_filters(child, schema, filters)?;
    }
    if let PhysicalPlan::Selection(selection) = plan {
        // The reader applied the global index's partition filter as its read
        // restriction.
        let (conditions, _) = split_global_partition_filter(selection.conditions.clone());
        filters.extend(resolve_expressions(&conditions, schema)?);
    }
    Ok(())
}

/// Each reader output column's physical table offset. Go's `_tidb_rowid`
/// (the handle an inner IndexLookUp over a heap table keeps) maps to the slot
/// just past the stored columns, which the lookup fills with the row handle.
fn index_inner_output_offsets(
    schema: &Schema,
    table: &crate::TableCommon,
) -> Result<Vec<usize>, DriverError> {
    schema
        .columns
        .iter()
        .map(|column| {
            if is_extra_handle_column(column) {
                return Ok(table.columns().len());
            }
            // The partition id column sits just past the extra handle slot.
            if column.id == tidb_model::column::EXTRA_PHYS_TBL_ID {
                return Ok(table.columns().len() + 1);
            }
            // `_tidb_commit_ts` sits just past the partition id slot.
            if column.id == tidb_model::column::EXTRA_COMMIT_TS_ID {
                return Ok(table.columns().len() + 2);
            }
            table
                .logical_columns()
                .iter()
                .position(|stored| stored.read().id == column.id)
                .or_else(|| {
                    column.orig_name.rsplit('.').next().and_then(|name| {
                        table
                            .logical_columns()
                            .iter()
                            .position(|stored| stored.read().name.original().eq_ignore_ascii_case(name))
                    })
                })
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "an index-join reader output is absent from its retained table",
                    )
                })
        })
        .collect()
}

fn index_join_probe_shape(
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
) -> Result<(Vec<usize>, Vec<LookupProbePart>), DriverError> {
    let mut dynamic = join
        .key_off2_idx_off
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(key, index)| usize::try_from(index).ok().map(|index| (index, key)))
        .collect::<Vec<_>>();
    // Go `buildIndexJoinInner2TableScan` maps every join key on the handle
    // to it but looks up by the first one only ("Add to newOuterJoinKeys
    // only once"); the join still compares every key pair.
    dynamic.sort_unstable();
    dynamic.dedup_by_key(|(index, _)| *index);
    // Go builds an index join with no join key on the index when its lookup
    // is the constant prefix plus a compare filter on the next column
    // (`t1.c_int in (?, ?)` and `t1.c_str <= t2.c_str`): each outer row then
    // contributes only its filter bounds.
    if dynamic.is_empty() && join.compare_filters.is_none() {
        return Err(DriverError::unsupported(
            "a physical index join retained no usable lookup key",
        ));
    }
    // Go's template width covers the join keys and the EQ/IN columns; a
    // comparison manager or a static next-column range owns the last slot.
    let prefix_len = match &join.compare_filters {
        Some(filters) => filters.target_index_offset,
        None => {
            let width = join.ranges.first().map_or(0, |range| range.low_val.len());
            let width = if index_join_template_has_tail(join) { width - 1 } else { width };
            width.max(dynamic.last().expect("non-empty above").0 + 1)
        }
    };
    if dynamic
        .last()
        .is_some_and(|(index, _)| *index >= prefix_len)
    {
        return Err(DriverError::unsupported(
            "a physical index-join CompareFilters target overlaps a lookup key",
        ));
    }
    let probe_keys = dynamic.iter().map(|(_, key)| *key).collect::<Vec<_>>();
    // A range's exclusion flags describe its last slot only.
    let has_tail = index_join_template_has_tail(join);
    let mut parts = Vec::with_capacity(prefix_len);
    for index in 0..prefix_len {
        if let Some((position, _)) = dynamic
            .iter()
            .enumerate()
            .find(|(_, (object_index, _))| *object_index == index)
        {
            parts.push(LookupProbePart::Dynamic(position));
            continue;
        }
        let values = join
            .ranges
            .iter()
            .map(|range| {
                let low = range.low_val.get(index)?;
                let high = range.high_val.get(index)?;
                (low == high && (has_tail || (!range.low_exclude && !range.high_exclude)))
                    .then(|| low.clone())
            })
            .collect::<Option<Vec<_>>>()
            .filter(|values| !values.is_empty())
            .ok_or_else(|| {
                DriverError::unsupported("an index-join lookup prefix has no fixed range template")
            })?;
        if values.iter().all(|value| value == &values[0]) {
            parts.push(LookupProbePart::Constant(values[0].clone()));
        } else {
            parts.push(LookupProbePart::Alternatives(values));
        }
    }
    Ok((probe_keys, parts))
}

/// Whether Go's template ranges end in a static next-column range
/// (`nextColRange`) rather than a point.
fn index_join_template_has_tail(join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin) -> bool {
    join.compare_filters.is_none()
        && join.ranges.iter().any(|range| {
            range.low_exclude
                || range.high_exclude
                || range.low_val.last() != range.high_val.last()
        })
}

/// The static next-column range of every template, in template order.
fn index_join_probe_tails(join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin) -> Vec<LookupProbeTail> {
    if !index_join_template_has_tail(join) {
        return Vec::new();
    }
    join.ranges
        .iter()
        .filter_map(|range| {
            Some(LookupProbeTail {
                low: range.low_val.last()?.clone(),
                high: range.high_val.last()?.clone(),
                low_exclusive: range.low_exclude,
                high_exclusive: range.high_exclude,
            })
        })
        .collect()
}

fn index_join_probe_key_domains(
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
    probe_parts: &[LookupProbePart],
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Vec<IndexProbeKeyDomain>, DriverError> {
    let table_id = join.inner_access_table_id.ok_or_else(|| {
        DriverError::unsupported("a physical index join has no retained inner table ID")
    })?;
    let table = catalog.physical_kv_table_by_id(table_id).ok_or_else(|| {
        DriverError::unsupported("a physical index-join table ID is absent from the catalog")
    })?;
    let table = table_with_plan_index(table, join.inner_access_index_id, ctx)?;
    let object_types = if let Some(index_id) = join.inner_access_index_id {
        let index = table
            .plan_indexes()
            .find(|index| index.read().id == index_id)
            .ok_or_else(|| {
                DriverError::unsupported("a physical index-join index ID is absent from its table")
            })?;
        let mut types = index
            .column_offsets
            .iter()
            .map(|offset| {
                table
                    .logical_columns()
                    .get(*offset)
                    .map(|column| column.read().field_type.clone())
                    .ok_or_else(|| {
                        DriverError::unsupported(
                            "an index-join key column is absent from its retained table",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Go `fillIndexPath`: a non-unique secondary index on a table without
        // a common handle carries the handle after its own columns
        // (`path.IdxCols = append(path.IdxCols, handleCol)`), which an
        // index join may probe by.
        if !index.read().unique && table.common_handle_offsets().is_empty() {
            types.push(
                table
                    .pk_handle_offset()
                    .and_then(|offset| table.logical_columns().get(offset))
                    .map_or_else(
                        || FieldType::new(FieldTypeCode::LongLong),
                        |column| column.read().field_type.clone(),
                    ),
            );
        }
        types
    } else if !table.common_handle_offsets().is_empty() {
        table
            .common_handle_offsets()
            .iter()
            .map(|offset| {
                table
                    .logical_columns()
                    .get(*offset)
                    .map(|column| column.read().field_type.clone())
                    .ok_or_else(|| {
                        DriverError::unsupported(
                            "a common-handle join key is absent from its retained table",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![table
            .pk_handle_offset()
            .and_then(|offset| table.logical_columns().get(offset))
            .map_or_else(
                || FieldType::new(FieldTypeCode::LongLong),
                |column| column.read().field_type.clone(),
            )]
    };

    let dynamic_count = probe_parts
        .iter()
        .filter(|part| matches!(part, LookupProbePart::Dynamic(_)))
        .count();
    let mut domains = vec![None; dynamic_count];
    for (object_offset, part) in probe_parts.iter().enumerate() {
        let LookupProbePart::Dynamic(dynamic_offset) = part else {
            continue;
        };
        let field_type = object_types.get(object_offset).cloned().ok_or_else(|| {
            DriverError::unsupported("an index-join lookup key exceeds its retained object")
        })?;
        let prefix_length = join
            .idx_col_lens
            .get(object_offset)
            .copied()
            .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH);
        domains[*dynamic_offset] = Some(IndexProbeKeyDomain {
            field_type,
            prefix_length,
        });
    }
    domains
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| DriverError::unsupported("an index-join lookup key domain is incomplete"))
}

const fn lookup_probe_bound_op(
    op: tidb_planner_core_operator_physicalop::IndexJoinCompareOp,
) -> LookupProbeBoundOp {
    match op {
        tidb_planner_core_operator_physicalop::IndexJoinCompareOp::Ge => LookupProbeBoundOp::Ge,
        tidb_planner_core_operator_physicalop::IndexJoinCompareOp::Gt => LookupProbeBoundOp::Gt,
        tidb_planner_core_operator_physicalop::IndexJoinCompareOp::Lt => LookupProbeBoundOp::Lt,
        tidb_planner_core_operator_physicalop::IndexJoinCompareOp::Le => LookupProbeBoundOp::Le,
    }
}

fn index_inner_reader_payload<'a>(plan: &'a PhysicalPlan) -> Option<(&'a PhysicalPlan, bool)> {
    match plan {
        PhysicalPlan::TableReader(reader) => reader.table_plan.as_deref().map(|plan| (plan, false)),
        PhysicalPlan::IndexReader(reader) => reader.index_plan.as_deref().map(|plan| (plan, true)),
        PhysicalPlan::IndexLookUpReader(reader) => reader
            .table_plan
            .as_deref()
            .or(reader.index_plan.as_deref())
            .map(|plan| (plan, false)),
        PhysicalPlan::TableScan(_) => Some((plan, false)),
        PhysicalPlan::IndexScan(_) => Some((plan, true)),
        _ => None,
    }
}

fn index_inner_scan_order(plan: &PhysicalPlan) -> Option<(bool, bool)> {
    match plan {
        PhysicalPlan::TableReader(reader) => reader
            .table_plan
            .as_deref()
            .and_then(index_inner_scan_order),
        PhysicalPlan::IndexReader(reader) => reader
            .index_plan
            .as_deref()
            .and_then(index_inner_scan_order),
        PhysicalPlan::IndexLookUpReader(reader) => {
            let desc = reader
                .index_plan
                .as_deref()
                .and_then(index_inner_scan_order)
                .map(|(_, desc)| desc)
                .unwrap_or(false);
            Some((reader.keep_order, desc))
        }
        PhysicalPlan::TableScan(scan) => Some((scan.keep_order, scan.desc)),
        PhysicalPlan::IndexScan(scan) => Some((scan.keep_order, scan.desc)),
        _ => plan.children().iter().find_map(index_inner_scan_order),
    }
}

/// The cop partial aggregate Go pushes into an index-join inner reader's
/// table plan (`attach2Task4PhysicalHashAgg`). The lookup leaf emits
/// physical-width rows, so the port runs the partial aggregate locally above
/// the leaf instead of inside the lookup source.
fn index_inner_partial_aggregate(
    plan: &PhysicalPlan,
) -> Option<(&PhysicalPlan, &[AggFuncDesc], &[Expression], bool)> {
    let (embedded, _) = index_inner_reader_payload(plan)?;
    match embedded {
        PhysicalPlan::HashAgg(agg) => Some((embedded, &agg.agg_funcs, &agg.group_by_items, false)),
        PhysicalPlan::StreamAgg(agg) => Some((embedded, &agg.agg_funcs, &agg.group_by_items, true)),
        _ => None,
    }
}

/// Go `indexJoinLookupChildIdx`: physical table IDs cannot distinguish aliases.
/// Every retained probe key must belong to exactly one child's logical schema.
fn index_join_lookup_child_idx(plan: &PhysicalPlan, keys: &[Column]) -> Option<usize> {
    let [left, right] = plan.children() else {
        return None;
    };
    if keys.is_empty() {
        return None;
    }
    let contains_keys = |child: &PhysicalPlan| {
        child
            .schema()
            .is_some_and(|schema| keys.iter().all(|key| schema.contains(key)))
    };
    match (contains_keys(left), contains_keys(right)) {
        (true, false) => Some(0),
        (false, true) => Some(1),
        _ => None,
    }
}

fn build_index_inner_reader(
    plan: &PhysicalPlan,
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
    probe_parts: &[LookupProbePart],
    shared: Option<&std::sync::Arc<std::sync::Mutex<SharedIndexJoinProbes>>>,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    offset_schema: Option<&Schema>,
    storage: &ReaderStorage,
) -> Result<IndexJoinLookupExec, DriverError> {
    // Go `buildIndexLookUpReaderForIndexJoin`: `builder.Ti.UseTableLookUp.Store(true)`.
    if let Some((PhysicalPlan::IndexLookUpReader(_), _)) = index_inner_reader_payload(plan) {
        ctx.telemetry_info().use_table_lookup = true;
    }
    let table_id = join.inner_access_table_id.ok_or_else(|| {
        DriverError::unsupported("a physical index join has no retained inner table ID")
    })?;
    let (mut table, store, separated) = storage.table(
        ctx,
        table_with_plan_index(
            catalog.physical_kv_table_by_id(table_id).ok_or_else(|| {
                DriverError::unsupported("a physical index-join table ID is absent from the catalog")
            })?,
            join.inner_access_index_id,
            ctx,
        )?,
    );
    let (embedded, covering) = index_inner_reader_payload(plan).ok_or_else(|| {
        DriverError::unsupported("an index-join inner leaf is not a physical reader")
    })?;
    if retained_table_id(embedded)? != table_id {
        return Err(DriverError::unsupported(
            "an index-join inner reader does not match its retained table ID",
        ));
    }
    let object = if let Some(index_id) = join.inner_access_index_id {
        if !table.plan_indexes().any(|index| index.read().id == index_id) {
            return Err(DriverError::unsupported(
                "a physical index-join index ID is absent from its table",
            ));
        }
        LookupObject::Index(index_id)
    } else if table.common_handle_offsets().is_empty() {
        LookupObject::Handle
    } else {
        LookupObject::CommonHandle
    };
    let schema = plan_schema(plan)?;
    // A cop partial aggregate's reader output is the aggregate's result, not
    // the table row; project the aggregate's INPUT columns instead.
    let row_schema = offset_schema.unwrap_or(&schema);
    let output_offsets = index_inner_output_offsets(row_schema, &table)?;
    let emits_extra_handle = row_schema.columns.iter().any(is_extra_handle_column);
    let emits_physical_table_id = row_schema
        .columns
        .iter()
        .any(|column| column.id == tidb_model::column::EXTRA_PHYS_TBL_ID);
    let emits_commit_ts = row_schema
        .columns
        .iter()
        .any(|column| column.id == tidb_model::column::EXTRA_COMMIT_TS_ID);
    let index_side = match plan {
        PhysicalPlan::IndexLookUpReader(reader) => reader.index_plan.as_deref(),
        _ => Some(embedded),
    };
    if let Some(ids) = index_side.and_then(global_index_partition_restriction) {
        std::sync::Arc::make_mut(&mut table).restrict_read_to_partitions(&ids);
    }
    let filter_schema = physical_table_schema(embedded, &table);
    let mut filters = Vec::new();
    collect_index_inner_filters(embedded, &filter_schema, &mut filters)?;
    let mut index_filters = Vec::new();
    if let PhysicalPlan::IndexLookUpReader(reader) = plan {
        if let Some(index_plan) = reader.index_plan.as_deref() {
            let index_schema = physical_table_schema(index_plan, &table);
            collect_index_inner_filters(index_plan, &index_schema, &mut index_filters)?;
        }
    }
    // Go rebuilds the ordinary no-range reader for each outer task.
    let (network_plan, table_lookup_plan) = match plan {
        PhysicalPlan::IndexLookUpReader(reader) => (
            reader.index_plan.as_deref().ok_or_else(|| {
                DriverError::unsupported("an index-join lookup reader has no index plan")
            })?,
            reader.table_plan.as_deref(),
        ),
        _ => (embedded, None),
    };
    let mut cop_statement =
        cop_reader_statement(network_plan, table_lookup_plan, covering, None, ctx, None);
    // A covering reader's chain is rooted at the reader (Go
    // `getPhysicalPlanIDs` over its pushed plan, registered under it).
    if table_lookup_plan.is_none() {
        cop_statement = cop_statement
            .with_cop_plan_chain(cop_plan_chain(network_plan), i64::from(plan.id()));
    }
    let mut statement = ReaderStorage::statement(separated, cop_statement);
    // Go `buildTableReaderForIndexJoin` -> `buildTableReaderBase`: a table
    // reader built for a task has no memory tracker.
    statement.select_mem_untracked =
        matches!(object, LookupObject::Handle | LookupObject::CommonHandle);
    let double_read = table_lookup_plan.is_some() && !covering && matches!(object, LookupObject::Index(_));
    let mut source = IndexJoinLookupExec::new_with_statement(
        meta(ctx, plan, row_schema.clone()),
        std::sync::Arc::unwrap_or_clone(table),
        store,
        object,
        RowDecodeContext::for_query(ctx),
        statement,
    );
    source.set_probe_parts(probe_parts.to_vec());
    source.set_probe_tails(index_join_probe_tails(join));
    source.set_probe_key_prefix_lengths(join.idx_col_lens.clone());
    if let Some(compare_filters) = &join.compare_filters {
        source.set_probe_bound_ops(
            compare_filters
                .ops
                .iter()
                .copied()
                .map(lookup_probe_bound_op)
                .collect(),
        );
        if let Some(target_type) = &compare_filters.target_col.ret_type {
            source.set_probe_bound_target_type(target_type.clone());
        }
    }
    source.set_filters(filters, ctx.clone());
    source.set_index_filters(index_filters);
    source.set_column_projection(Some(output_offsets), []);
    if emits_extra_handle {
        source.emit_extra_handle();
    }
    if emits_physical_table_id {
        source.emit_physical_table_id();
    }
    if emits_commit_ts {
        source.emit_commit_ts();
    }
    if let Some((true, desc)) = index_inner_scan_order(plan) {
        source.set_keep_order(desc);
    }
    if covering {
        source.mark_covering();
    }
    // Go `buildIndexLookUpReaderForIndexJoin`: a double read is an
    // `IndexLookUpExecutor` built per inner task, its workers started at the
    // inner worker's first `Next` with that chunk's `RequiredRows`.
    if double_read {
        let max_chunk_size = ctx.executor_chunk_sizes().1;
        source.set_inner_lookup(
            crate::access_path::calculate_lookup_batch_size_with_cap(
                embedded_index_scan(network_plan)
                    .and_then(|scan| scan.base.base.stats_info().map(|stats| stats.row_count())),
                max_chunk_size,
                false,
                ctx.index_lookup_size(),
            ),
            ctx.index_lookup_size(),
        );
        // Go `IndexLookUpExecutor.initRuntimeStats` of each task's reader.
        if let (Some(table_plan), Some(runtime_source)) =
            (table_lookup_plan, ctx.executor_runtime_stats_source())
        {
            source.set_inner_lookup_stats(crate::access_path::InnerLookupStatsConfig {
                plan_id: i64::from(plan.id()),
                index_root_plan_id: i64::from(network_plan.id()),
                table_root_plan_id: i64::from(table_plan.id()),
                concurrency: ctx.index_lookup_concurrency(),
                source: std::sync::Arc::clone(runtime_source),
            });
        }
    }
    if let Some(shared) = shared {
        source.set_shared_probes(std::sync::Arc::clone(shared));
    }
    Ok(source)
}

fn build_index_inner_subtree(
    plan: &PhysicalPlan,
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
    probe_parts: &[LookupProbePart],
    shared: &std::sync::Arc<std::sync::Mutex<SharedIndexJoinProbes>>,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    if index_inner_reader_payload(plan).is_some() {
        if let Some((partial_plan, descriptors, group_by, stream)) =
            index_inner_partial_aggregate(plan)
        {
            let row_schema = partial_plan
                .children()
                .first()
                .and_then(PhysicalPlan::schema)
                .cloned()
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "an index-join inner partial aggregate has no input schema",
                    )
                })?;
            let leaf = build_index_inner_reader(
                plan,
                join,
                probe_parts,
                Some(shared),
                catalog,
                ctx,
                Some(&row_schema),
                &state.reader_storage,
            )?;
            return build_aggregation_over_child(
                partial_plan,
                descriptors,
                group_by,
                stream,
                Box::new(leaf),
                ctx,
                !state.in_reader && !state.untraced,
            );
        }
        return Ok(Box::new(build_index_inner_reader(
            plan,
            join,
            probe_parts,
            Some(shared),
            catalog,
            ctx,
            None,
            &state.reader_storage,
        )?));
    }
    match plan {
        PhysicalPlan::Projection(projection) => {
            let child = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            )?;
            let expressions = resolve_expressions(&projection.exprs, child.schema())?;
            Ok(Box::new(ProjectionExec::new(
                meta(ctx, plan, plan_schema(plan)?),
                expressions,
                child,
                ctx.clone(),
            )) as Box<dyn Executor>)
        }
        // Go `buildUnionScanForIndexJoin` builds a union scan over each
        // probe's reader; here the reader below it merges the transaction's
        // own rows instead, so it reads the merged storage.
        PhysicalPlan::UnionScan(_) => {
            let previous = std::mem::replace(&mut state.reader_storage, ReaderStorage::Merged);
            let built = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            );
            state.reader_storage = previous;
            built
        }
        PhysicalPlan::Selection(selection) => {
            let child = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            )?;
            let filters = resolve_expressions(&selection.conditions, child.schema())?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(SelectionExec::new(
                meta(ctx, plan, schema),
                filters,
                child,
                ctx.clone(),
                ctx.statement_memory(),
            )))
        }
        PhysicalPlan::HashAgg(aggregation) => {
            let child = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            )?;
            build_agg_from_child_exec(
                plan,
                &aggregation.agg_funcs,
                &aggregation.group_by_items,
                false,
                child,
                ctx,
                !state.in_reader && !state.untraced,
            )
        }
        PhysicalPlan::StreamAgg(aggregation) => {
            let child = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            )?;
            build_agg_from_child_exec(
                plan,
                &aggregation.agg_funcs,
                &aggregation.group_by_items,
                true,
                child,
                ctx,
                !state.in_reader && !state.untraced,
            )
        }
        PhysicalPlan::HashJoin(hash_join) => {
            let [left_plan, right_plan] = plan.children() else {
                return Err(DriverError::unsupported(
                    "an index-join inner HashJoin has the wrong child count",
                ));
            };
            let lookup_child = index_join_lookup_child_idx(plan, &join.inner_join_keys)
                .ok_or_else(|| DriverError::unsupported(
                    "an index-join inner HashJoin cannot locate its lookup child",
                ))?;
            let left = if lookup_child == 0 {
                build_index_inner_subtree(
                    left_plan,
                    join,
                    probe_parts,
                    shared,
                    catalog,
                    ctx,
                    state,
                )?
            } else {
                build_with_state(left_plan, catalog, ctx, state)?
            };
            let right = if lookup_child == 1 {
                build_index_inner_subtree(
                    right_plan,
                    join,
                    probe_parts,
                    shared,
                    catalog,
                    ctx,
                    state,
                )?
            } else {
                build_with_state(right_plan, catalog, ctx, state)?
            };
            let mut executor = build_join_over_children(
                plan,
                hash_join.join_type,
                &hash_join.left_join_keys,
                &hash_join.right_join_keys,
                &hash_join.is_null_eq,
                &hash_join.left_conditions,
                &hash_join.right_conditions,
                &hash_join.other_conditions,
                left,
                right,
                ctx,
            )?;
            let build_child_idx = if hash_join.use_outer_to_build {
                1usize
                    .checked_sub(hash_join.inner_child_idx)
                    .ok_or_else(|| {
                        DriverError::unsupported(
                            "an index-join inner HashJoin has an invalid inner child",
                        )
                    })?
            } else {
                hash_join.inner_child_idx
            };
            executor.set_hash_build_is_left(build_child_idx == 0);
            executor.set_parallelism(hash_join.concurrency);
            executor
                .set_default_values(hash_join.default_values.clone())
                .map_err(DriverError::Exec)?;
            Ok(Box::new(executor))
        }
        _ => Err(DriverError::unsupported(format!(
            "physical {} cannot be rebuilt as an index-join inner plan",
            plan.tp()
        ))),
    }
}

fn build_index_lookup_source(
    inner: &PhysicalPlan,
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
    probe_parts: &[LookupProbePart],
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<IndexLookupSource, DriverError> {
    if index_inner_reader_payload(inner).is_some() && index_inner_partial_aggregate(inner).is_none()
    {
        let leaf = build_index_inner_reader(
            inner,
            join,
            probe_parts,
            None,
            catalog,
            ctx,
            None,
            &state.reader_storage,
        )?;
        // Go `buildExecutorForIndexJoin` builds the inner reader per task
        // under the reader's plan: its rows, `exec.Next` and `exec.Close`
        // (it is opened directly, so no open time) merge into one stat, and
        // its `exec.Next` records RU v2 like any reader's.
        let ruv2 = go_executor_next_region(inner).and_then(|region| {
            tidb_executor_internal_exec::ruv2::Ruv2NextCache::new(
                region.trim_end_matches(".Next"),
                ctx.ruv2_metrics(),
            )
        });
        let runtime_stats = (!state.untraced)
            .then(|| ctx.executor_runtime_stats_source())
            .flatten()
            .map(|source| IndexJoinInnerStats::new(Arc::clone(source), i64::from(inner.id())));
        if runtime_stats.is_none() && ruv2.is_none() {
            return Ok(IndexLookupSource::Leaf(Box::new(leaf)));
        }
        return Ok(IndexLookupSource::Leaf(Box::new(MeteredIndexJoinLookup {
            inner: Box::new(leaf),
            runtime_stats,
            ruv2,
        })));
    }
    let probes = std::sync::Arc::new(std::sync::Mutex::new(SharedIndexJoinProbes::default()));
    let exec = build_index_inner_subtree(inner, join, probe_parts, &probes, catalog, ctx, state)?;
    Ok(IndexLookupSource::Composite { exec, probes })
}

/// Go's per-task inner reader stats for an index join's leaf
/// ([`build_index_lookup_source`]): every task -- the session's own reader
/// and the ones a lane opens from its fork -- records its `Next`s and
/// `Close` into the reader plan's stats.
/// The inner reader's `BasicRuntimeStats` in the statement's
/// `RuntimeStatsColl`, registered when a task first uses the reader: Go
/// registers them as `buildExecutorForIndexJoin` builds the reader for a
/// task, so an inner side no task reached has none.
#[derive(Clone)]
struct IndexJoinInnerStats {
    source: Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>,
    plan_id: i64,
    stats: Arc<std::sync::OnceLock<Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStats>>>>,
}

impl IndexJoinInnerStats {
    fn new(
        source: Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>,
        plan_id: i64,
    ) -> Self {
        Self {
            source,
            plan_id,
            stats: Arc::default(),
        }
    }

    fn get(&self) -> Option<&Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStats>> {
        self.stats
            .get_or_init(|| self.source.for_executor(self.plan_id))
            .as_ref()
    }
}

struct MeteredIndexJoinLookup {
    inner: Box<dyn tidb_executor_join::index_lookup::IndexJoinLookup>,
    /// Go `exec.Next` / `exec.Close`'s `BasicRuntimeStats` of the inner
    /// reader's plan.
    runtime_stats: Option<IndexJoinInnerStats>,
    ruv2: Option<tidb_executor_internal_exec::ruv2::Ruv2NextCache>,
}

impl Executor for MeteredIndexJoinLookup {
    fn open(&mut self) -> Result<(), crate::ExecError> {
        self.inner.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), crate::ExecError> {
        let start = Instant::now();
        let inner = &mut self.inner;
        let result =
            tidb_executor_internal_exec::ruv2::next_with_ruv2(self.ruv2.as_ref(), 0, req, |req| inner.next(req));
        if let Some(stats) = self.runtime_stats.as_ref().and_then(IndexJoinInnerStats::get) {
            stats.record_next(start.elapsed(), i64::try_from(req.num_rows()).unwrap_or(i64::MAX));
        }
        result
    }

    fn close(&mut self) -> Result<(), crate::ExecError> {
        let start = Instant::now();
        let result = self.inner.close();
        if let Some(stats) = self.runtime_stats.as_ref().and_then(IndexJoinInnerStats::get) {
            stats.record_close(start.elapsed());
        }
        result
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.inner.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.inner.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.inner.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.inner.new_chunk()
    }
}

impl tidb_executor_join::index_lookup::IndexJoinLookup for MeteredIndexJoinLookup {
    fn set_probes(
        &mut self,
        probes: tidb_executor_join::index_lookup::IndexJoinProbes,
    ) -> Result<(), crate::ExecError> {
        self.inner.set_probes(probes)
    }

    fn fork_template(&self) -> Option<Box<dyn tidb_executor_join::index_lookup::IndexJoinLookupFork>> {
        self.inner.fork_template().map(|inner| {
            Box::new(MeteredIndexJoinLookupFork {
                inner,
                runtime_stats: self.runtime_stats.clone(),
                ruv2: self.ruv2.clone(),
            }) as Box<dyn tidb_executor_join::index_lookup::IndexJoinLookupFork>
        })
    }

    fn add_produced(&self, rows: u64) {
        self.inner.add_produced(rows);
    }
}

/// [`MeteredIndexJoinLookup`]'s fork: a lane's task records into the same
/// stats.
struct MeteredIndexJoinLookupFork {
    inner: Box<dyn tidb_executor_join::index_lookup::IndexJoinLookupFork>,
    runtime_stats: Option<IndexJoinInnerStats>,
    ruv2: Option<tidb_executor_internal_exec::ruv2::Ruv2NextCache>,
}

impl tidb_executor_join::index_lookup::IndexJoinLookupFork for MeteredIndexJoinLookupFork {
    fn open(
        &self,
        probes: tidb_executor_join::index_lookup::IndexJoinProbes,
    ) -> Result<
        Result<
            Box<dyn tidb_executor_join::index_lookup::IndexJoinLookup>,
            tidb_executor_join::index_lookup::IndexJoinProbes,
        >,
        crate::ExecError,
    > {
        // Go's task reader is a new executor: its own basic stats under the
        // plan (`initNewExecutorStats`, so tasks after the first make them
        // `total_`). Go opens it directly, so no open time is recorded.
        let opened = self.inner.open(probes)?;
        let runtime_stats = self
            .runtime_stats
            .as_ref()
            .map(|stats| IndexJoinInnerStats::new(Arc::clone(&stats.source), stats.plan_id));
        Ok(opened.map(|inner| {
            Box::new(MeteredIndexJoinLookup {
                inner,
                runtime_stats,
                ruv2: self.ruv2.clone(),
            }) as Box<dyn tidb_executor_join::index_lookup::IndexJoinLookup>
        }))
    }
}

fn build_index_join(
    plan: &PhysicalPlan,
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a physical index join has the wrong child count",
        ));
    };
    if join.inner_child_idx > 1 {
        return Err(DriverError::unsupported(
            "a physical index join has an invalid inner child",
        ));
    }
    let (inner_plan, outer_plan, inner_conditions, outer_conditions) = if join.inner_child_idx == 0
    {
        (
            left_plan,
            right_plan,
            &join.left_conditions,
            &join.right_conditions,
        )
    } else {
        (
            right_plan,
            left_plan,
            &join.right_conditions,
            &join.left_conditions,
        )
    };
    if !inner_conditions.is_empty() {
        return Err(DriverError::unsupported(
            "a physical index join retained a condition on its inner side",
        ));
    }
    // Go `OuterCtx.Filter`: the outer filter only marks `outerMatch`, so an
    // outer row that fails it looks nothing up and reaches the joiner's miss
    // path.
    let outer = build_with_state(outer_plan, catalog, ctx, state)?;
    let outer_filter = resolve_expressions(outer_conditions, outer.schema())?;
    let left_schema = plan_schema(left_plan)?;
    let right_schema = plan_schema(right_plan)?;
    let condition_schema = tidb_expr::schema::merge_schema(Some(&left_schema), Some(&right_schema))
        .ok_or_else(|| DriverError::unsupported("a physical index join has no condition schema"))?;
    let (left_keys, right_keys) = if join.outer_hash_keys.is_empty() {
        (&join.left_join_keys, &join.right_join_keys)
    } else if join.inner_child_idx == 0 {
        (&join.inner_hash_keys, &join.outer_hash_keys)
    } else {
        (&join.outer_hash_keys, &join.inner_hash_keys)
    };
    // Go `buildIndexLookUpJoin`: "Use the probe table's collation" -- the
    // lookup keys and the outer hash types both take the INNER key's
    // collation, so `ascii_bin` 'A' finds and matches `general_ci` 'a'.
    let inner_keys = if join.inner_child_idx == 0 {
        left_keys
    } else {
        right_keys
    };
    let equalities = left_keys
        .iter()
        .zip(right_keys)
        .enumerate()
        .map(|(index, (left, right))| {
            let collation = inner_keys
                .get(index)
                .and_then(|inner| inner.ret_type.as_ref())
                .filter(|field_type| field_type.eval_type() == tidb_datatype::EvalType::String)
                .map(FieldType::collation);
            join_equality_with_collation(
                left,
                right,
                &condition_schema,
                join.is_null_eq.get(index).copied().unwrap_or(false),
                collation,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let other_conditions = resolve_expressions(&join.other_conditions, &condition_schema)?;
    let (probe_keys, probe_parts) = index_join_probe_shape(join)?;
    let probe_key_domains = index_join_probe_key_domains(join, &probe_parts, catalog, ctx)?;
    let probe_bounds = join
        .compare_filters
        .as_ref()
        .map(|filters| {
            filters
                .ops
                .iter()
                .copied()
                .zip(&filters.args)
                .map(|(_, argument)| {
                    resolve_expression(argument.clone(), outer.schema())
                        .map(|arg| LookupProbeBound { arg })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let source = build_index_lookup_source(inner_plan, join, &probe_parts, catalog, ctx, state)?;
    let lookup_is_left = join.inner_child_idx == 0;
    let lookup = IndexLookupPlan {
        lookup_is_left,
        probe_keys,
        probe_key_domains,
        source,
        outer_filter,
        outer_not_null: Vec::new(),
        inner_not_null: Vec::new(),
        probe_bounds,
    };
    let schema_types = |schema: &Schema| -> Vec<FieldType> {
        schema
            .columns
            .iter()
            .map(|column| {
                column
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
            })
            .collect()
    };
    let (left_types, right_types) = (schema_types(&left_schema), schema_types(&right_schema));
    let output_offsets =
        physical_join_output_offsets(plan, join.join_type, &left_schema, &right_schema)?;
    // Go's `IndexLookUpMergeJoin` (and `PhysicalIndexMergeJoin`'s merge
    // fields) is not ported; neither planner builds one on this branch.
    if join.kind == tidb_planner_core_operator_physicalop::IndexJoinKind::IndexMergeJoin {
        return Err(DriverError::unsupported("IndexLookUpMergeJoin is not supported"));
    }
    // Go's joiner appends the left child's columns before the right's; a
    // physical index join's schema is never interleaved.
    let Some(children_used) =
        index_join_children_used(join.join_type, &output_offsets, left_types.len())
    else {
        return Err(DriverError::unsupported(
            "an index join's output interleaves its children's columns",
        ));
    };
    build_index_look_up_join(
        plan,
        join,
        outer,
        lookup,
        equalities,
        other_conditions,
        left_types,
        right_types,
        children_used,
        ctx,
        state,
    )
}

/// Go `markChildrenUsedCols` over a physical join's output offsets into its
/// left-then-right row (a semi join's marker dropped): each child's used
/// columns in output order. `None` when an output interleaves the children,
/// which Go's joiner, appending the left columns before the right, cannot
/// produce.
fn index_join_children_used(
    join_type: LogicalJoinType,
    output_offsets: &[usize],
    left_width: usize,
) -> Option<(Vec<usize>, Vec<usize>)> {
    let offsets = match join_type {
        LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi => {
            &output_offsets[..output_offsets.len().saturating_sub(1)]
        }
        _ => output_offsets,
    };
    let split = offsets.iter().position(|offset| *offset >= left_width).unwrap_or(offsets.len());
    let (left, right) = offsets.split_at(split);
    right.iter().all(|offset| *offset >= left_width).then(|| {
        (left.to_vec(), right.iter().map(|offset| offset - left_width).collect())
    })
}

/// Go `executorBuilder.buildIndexLookUpJoin` (and, for an `IndexHashJoin`,
/// `buildIndexNestedLoopHashJoin`): the executor over Go's `Joiner`
/// (`NewJoiner(..., v.OtherConditions, ..., childrenUsedSchema)`).
#[allow(clippy::too_many_arguments)]
fn build_index_look_up_join(
    plan: &PhysicalPlan,
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
    outer: Box<dyn Executor>,
    lookup: IndexLookupPlan,
    equalities: Vec<Expression>,
    other_conditions: Vec<Expression>,
    left_types: Vec<FieldType>,
    right_types: Vec<FieldType>,
    children_used: (Vec<usize>, Vec<usize>),
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let inner_width = if join.inner_child_idx == 0 {
        left_types.len()
    } else {
        right_types.len()
    };
    let default_values = if join.default_values.is_empty() {
        vec![tidb_datatype::Datum::Null; inner_width]
    } else {
        join.default_values.clone()
    };
    let joiner = new_joiner(
        ctx.clone(),
        joiner_type(join.join_type),
        join.inner_child_idx == 0,
        &default_values,
        other_conditions,
        &left_types,
        &right_types,
        Some(children_used),
        false,
        JoinerChunkSizes {
            vectorized: ctx.enable_vectorized_expression(),
            init_chunk_size: ctx.executor_chunk_sizes().0,
            max_chunk_size: ctx.executor_chunk_sizes().1,
        },
    );
    let build = tidb_executor_join::join::IndexLookUpJoinBuild {
        meta: meta(ctx, plan, plan_schema(plan)?),
        outer,
        plan: lookup,
        equalities,
        left_width: left_types.len(),
        joiner,
        is_outer_join: matches!(
            join.join_type,
            LogicalJoinType::LeftOuter
                | LogicalJoinType::RightOuter
                | LogicalJoinType::LeftOuterSemi
                | LogicalJoinType::AntiLeftOuterSemi
        ),
        ctx: ctx.clone(),
        memory: ctx.statement_memory(),
        vectorized: ctx.enable_vectorized_expression(),
        concurrency: ctx.index_lookup_join_concurrency(),
        max_batch_size: ctx.index_join_batch_size(),
        index_ranges: join.ranges.clone(),
        runtime_stats: ctx
            .executor_runtime_stats_source()
            .filter(|_| !state.untraced)
            .cloned()
            .map(|source| (source, i64::from(plan.id()))),
    };
    // Go `buildIndexNestedLoopHashJoin` wraps the same executor with one
    // joiner per inner worker.
    if join.kind == tidb_planner_core_operator_physicalop::IndexJoinKind::IndexHashJoin {
        return Ok(Box::new(
            tidb_executor_join::join::IndexNestedLoopHashJoin::new(build, join.keep_outer_order)
                .map_err(DriverError::Exec)?,
        ));
    }
    Ok(Box::new(
        tidb_executor_join::join::IndexLookUpJoin::new(build).map_err(DriverError::Exec)?,
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_join(
    plan: &PhysicalPlan,
    join_type: LogicalJoinType,
    left_join_keys: &[Column],
    right_join_keys: &[Column],
    is_null_eq: &[bool],
    left_conditions: &[Expression],
    right_conditions: &[Expression],
    other_conditions: &[Expression],
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<JoinExec<crate::StmtContext>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a physical join has the wrong child count",
        ));
    };
    build_join_over_children(
        plan,
        join_type,
        left_join_keys,
        right_join_keys,
        is_null_eq,
        left_conditions,
        right_conditions,
        other_conditions,
        build_with_state(left_plan, catalog, ctx, state)?,
        build_with_state(right_plan, catalog, ctx, state)?,
        ctx,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_join_over_children(
    plan: &PhysicalPlan,
    join_type: LogicalJoinType,
    left_join_keys: &[Column],
    right_join_keys: &[Column],
    is_null_eq: &[bool],
    left_conditions: &[Expression],
    right_conditions: &[Expression],
    other_conditions: &[Expression],
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    ctx: &crate::StmtContext,
) -> Result<JoinExec<crate::StmtContext>, DriverError> {
    // ON predicates on a preserved side control matching, not whether that
    // row exists. Go installs these as OuterFilter and emits failed rows
    // through the joiner's unmatched path.
    let preserve_left = matches!(
        join_type,
        LogicalJoinType::LeftOuter
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiSemi
            | LogicalJoinType::AntiLeftOuterSemi
    );
    let preserve_right = join_type == LogicalJoinType::RightOuter;
    let left = filtered_join_child(
        plan,
        left,
        if preserve_left { &[] } else { left_conditions },
        ctx,
    )?;
    let right = filtered_join_child(
        plan,
        right,
        if preserve_right {
            &[]
        } else {
            right_conditions
        },
        ctx,
    )?;
    let left_schema = left.schema().clone();
    let right_schema = right.schema().clone();
    let condition_schema =
        tidb_expr::schema::merge_schema(Some(left.schema()), Some(right.schema()))
            .ok_or_else(|| DriverError::unsupported("a physical join has no condition schema"))?;
    let mut conditions = left_join_keys
        .iter()
        .zip(right_join_keys)
        .enumerate()
        .map(|(index, (left, right))| {
            // Go's hash-join builder takes the comparison collation from
            // EqualConditions, not from a freshly constructed boolean type.
            if let PhysicalPlan::HashJoin(join) = plan {
                if let Some(condition) = join.equal_conditions.get(index) {
                    return resolve_expression(
                        Expression::ScalarFunction(condition.clone()),
                        &condition_schema,
                    );
                }
            }
            join_equality_with_null(
                left,
                right,
                &condition_schema,
                is_null_eq.get(index).copied().unwrap_or(false),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Go's null-aware anti-join keeps its `NAEQConditions` on its own field,
    // separate from `EqualConditions`/`OtherConditions`
    // (`buildHashJoinFromChildExecs`, `builder.go`); kept separate here too
    // (`JoinExec::set_na_conditions`, below) rather than folded into the
    // shared `conditions` list, so the executor never has to re-derive
    // which entries came from that field.
    let na_conditions = if let PhysicalPlan::HashJoin(join) = plan {
        join.na_equal_conditions
            .iter()
            .map(|condition| {
                resolve_expression(
                    Expression::ScalarFunction(condition.clone()),
                    &condition_schema,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    conditions.extend(resolve_expressions(other_conditions, &condition_schema)?);
    if preserve_left {
        conditions.extend(resolve_expressions(left_conditions, &condition_schema)?);
    }
    if preserve_right {
        conditions.extend(resolve_expressions(right_conditions, &condition_schema)?);
    }
    let mut executor = JoinExec::new(
        meta(ctx, plan, plan_schema(plan)?),
        join_kind(join_type)?,
        conditions,
        left,
        right,
        ctx.clone(),
        ctx.statement_memory(),
    );
    if !na_conditions.is_empty() {
        executor.set_na_conditions(na_conditions);
    }
    let output_offsets =
        physical_join_output_offsets(plan, join_type, &left_schema, &right_schema)?;
    executor
        .set_output_offsets(output_offsets)
        .map_err(DriverError::Exec)?;
    Ok(executor)
}

/// Go `PhysicalHashJoin.CanUseHashJoinV2`: capability, not query size or workload.
fn can_use_hash_join_v2(join: &tidb_planner_core_operator_physicalop::PhysicalHashJoin) -> bool {
    !join.left_join_keys.is_empty()
        && join.na_equal_conditions.is_empty()
        && !join.is_null_eq.iter().any(|&null_eq| null_eq)
        && (matches!(
            join.join_type,
            LogicalJoinType::Inner
                | LogicalJoinType::LeftOuter
                | LogicalJoinType::RightOuter
                | LogicalJoinType::Semi
                | LogicalJoinType::AntiSemi
        ) || tidb_executor_join_joinversion::use_hash_join_v2_for_non_ga_join())
}

/// Go `physicalop.IsGAForHashJoinV2`.
fn is_ga_for_hash_join_v2(join: &tidb_planner_core_operator_physicalop::PhysicalHashJoin) -> bool {
    join.na_equal_conditions.is_empty()
        && !join.left_join_keys.is_empty()
        && !join.is_null_eq.iter().any(|&null_eq| null_eq)
        && matches!(
            join.join_type,
            LogicalJoinType::Inner
                | LogicalJoinType::LeftOuter
                | LogicalJoinType::RightOuter
                | LogicalJoinType::Semi
                | LogicalJoinType::AntiSemi
        )
}

/// Go `buildHashJoinV2FromChildExecs`: preserve build/probe filters and compare
/// keys using the equality expression's collation and the logical key flags.
fn build_hash_join_v2(
    plan: &PhysicalPlan,
    join: &tidb_planner_core_operator_physicalop::PhysicalHashJoin,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a physical hash join has the wrong child count",
        ));
    };
    if join.inner_child_idx > 1 || join.left_join_keys.len() != join.right_join_keys.len() {
        return Err(DriverError::unsupported(
            "a physical hash join has invalid children or keys",
        ));
    }
    let left = build_with_state(left_plan, catalog, ctx, state)?;
    let right = build_with_state(right_plan, catalog, ctx, state)?;
    let (outer_conditions, outer_schema, inner_conditions) = if join.inner_child_idx == 1 {
        (&join.left_conditions, left.schema(), &join.right_conditions)
    } else {
        (
            &join.right_conditions,
            right.schema(),
            &join.left_conditions,
        )
    };
    if !inner_conditions.is_empty() {
        return Err(DriverError::unsupported(
            "hash join's inner condition should be empty",
        ));
    }
    let outer_filter = resolve_expressions(outer_conditions, outer_schema)?;
    let (build_filter, probe_filter) = if join.use_outer_to_build {
        (outer_filter, vec![])
    } else {
        (vec![], outer_filter)
    };
    let keys = join_key_offsets(
        &join.left_join_keys,
        &join.right_join_keys,
        left.schema(),
        right.schema(),
    )?;
    let mut left_types = Vec::with_capacity(keys.len());
    let mut right_types = Vec::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        let mut left_type = left.ret_field_types()[key.left].clone().with_flags(
            join.left_join_keys[index]
                .ret_type
                .as_ref()
                .ok_or_else(|| DriverError::unsupported("a left hash join key has no type"))?
                .flags(),
        );
        let mut right_type = right.ret_field_types()[key.right].clone().with_flags(
            join.right_join_keys[index]
                .ret_type
                .as_ref()
                .ok_or_else(|| DriverError::unsupported("a right hash join key has no type"))?
                .flags(),
        );
        if let Some(equality) = join.equal_conditions.get(index) {
            let collation = equality.derived_collation();
            left_type = left_type.with_collation(collation);
            right_type = right_type.with_collation(collation);
        }
        left_types.push(left_type);
        right_types.push(right_type);
    }
    let right_as_build_side = (join.inner_child_idx == 1) != join.use_outer_to_build;
    let (build_key_indices, probe_key_indices, build_key_types, probe_key_types) =
        if right_as_build_side {
            (
                keys.iter().map(|key| key.right).collect(),
                keys.iter().map(|key| key.left).collect(),
                right_types,
                left_types,
            )
        } else {
            (
                keys.iter().map(|key| key.left).collect(),
                keys.iter().map(|key| key.right).collect(),
                left_types,
                right_types,
            )
        };
    let condition_schema =
        tidb_expr::schema::merge_schema(Some(left.schema()), Some(right.schema())).ok_or_else(
            || DriverError::unsupported("a physical hash join has no condition schema"),
        )?;
    let other_condition = resolve_expressions(&join.other_conditions, &condition_schema)?;
    let mut l_used_in_other_condition = vec![];
    let mut r_used_in_other_condition = vec![];
    for column in tidb_expr::simple_expr::extract_columns_from_expressions(&other_condition, None) {
        let index = usize::try_from(column.index).map_err(|_| {
            DriverError::unsupported("a hash join condition has a negative column index")
        })?;
        if index < left.schema().len() {
            l_used_in_other_condition.push(index);
        } else {
            r_used_in_other_condition.push(index - left.schema().len());
        }
    }

    let output_schema = plan_schema(plan)?;
    let offsets =
        physical_join_output_offsets(plan, join.join_type, left.schema(), right.schema())?;
    let outer_semi = matches!(
        join.join_type,
        LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi
    );
    let child_output_count = offsets.len() - usize::from(outer_semi);
    // Keep each side's requested order, as Go markChildrenUsedCols does.
    let (left_outputs, right_outputs): (Vec<_>, Vec<_>) = offsets[..child_output_count]
        .iter()
        .copied()
        .enumerate()
        .partition(|&(_, index)| index < left.schema().len());
    let l_used = left_outputs.iter().map(|&(_, index)| index).collect();
    let r_used = right_outputs
        .iter()
        .map(|&(_, index)| index - left.schema().len())
        .collect();
    let mut output_order: Vec<_> = left_outputs
        .iter()
        .chain(&right_outputs)
        .map(|&(index, _)| index)
        .collect();
    if outer_semi {
        output_order.push(child_output_count);
    }
    let native_schema = Schema::new(
        output_order
            .iter()
            .map(|&index| output_schema.columns[index].clone())
            .collect(),
    );
    let mut executor: Box<dyn Executor> = Box::new(HashJoinV2Executor::new(
        meta(ctx, plan, native_schema),
        HashJoinV2Plan {
            concurrency: join.concurrency,
            join_type: joiner_type(join.join_type),
            right_as_build_side,
            build_key_indices,
            probe_key_indices,
            build_key_types,
            probe_key_types,
            l_used,
            r_used,
            l_used_in_other_condition,
            r_used_in_other_condition,
            build_filter,
            probe_filter,
            other_condition,
            vectorized: ctx.enable_vectorized_expression(),
        },
        left,
        right,
        ctx.clone(),
        ctx.statement_memory(),
    )
    .with_runtime_stats(
        ctx.executor_runtime_stats_source()
            .filter(|_| !state.untraced)
            .cloned(),
        is_ga_for_hash_join_v2(join),
    ));
    if output_order
        .iter()
        .enumerate()
        .any(|(index, &source)| index != source)
    {
        let mut expressions = vec![None; output_order.len()];
        for (source, &target) in output_order.iter().enumerate() {
            let mut column = output_schema.columns[target].clone();
            column.index = source as i64;
            expressions[target] = Some(Expression::Column(column));
        }
        executor = Box::new(ProjectionExec::new(
            meta(ctx, plan, output_schema),
            expressions.into_iter().map(Option::unwrap).collect(),
            executor,
            ctx.clone(),
        ));
    }
    Ok(executor)
}

fn build_apply(
    plan: &PhysicalPlan,
    apply: &tidb_planner_core_operator_physicalop::PhysicalApply,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a physical Apply has the wrong child count",
        ));
    };
    if apply.hash_join.inner_child_idx > 1 {
        return Err(DriverError::unsupported(
            "a physical Apply has an invalid inner child",
        ));
    }
    let left = build_with_state(left_plan, catalog, ctx, state)?;
    let right = build_with_state(right_plan, catalog, ctx, state)?;
    let left_types = left.ret_field_types().to_vec();
    let right_types = right.ret_field_types().to_vec();
    let condition_schema =
        tidb_expr::schema::merge_schema(Some(left.schema()), Some(right.schema()))
            .ok_or_else(|| DriverError::unsupported("a physical Apply has no condition schema"))?;
    let mut conditions = apply
        .hash_join
        .equal_conditions
        .iter()
        .chain(&apply.hash_join.na_equal_conditions)
        .cloned()
        .map(Expression::ScalarFunction)
        .map(|condition| resolve_expression(condition, &condition_schema))
        .collect::<Result<Vec<_>, _>>()?;
    conditions.extend(resolve_expressions(
        &apply.hash_join.other_conditions,
        &condition_schema,
    )?);
    let left_filter = resolve_expressions(&apply.hash_join.left_conditions, left.schema())?;
    let right_filter = resolve_expressions(&apply.hash_join.right_conditions, right.schema())?;
    let inner_width = if apply.hash_join.inner_child_idx == 0 {
        left_types.len()
    } else {
        right_types.len()
    };
    let default_values = if apply.hash_join.default_values.is_empty() {
        vec![tidb_datatype::Datum::Null; inner_width]
    } else {
        apply.hash_join.default_values.clone()
    };
    if default_values.len() < inner_width {
        return Err(DriverError::unsupported(
            "a physical Apply has too few default inner values",
        ));
    }
    let worker_conditions = conditions.clone();
    let joiner = new_joiner(
        ctx.clone(),
        joiner_type(apply.hash_join.join_type),
        apply.hash_join.inner_child_idx == 0,
        &default_values,
        conditions,
        &left_types,
        &right_types,
        None,
        false,
        JoinerChunkSizes {
            vectorized: ctx.enable_vectorized_expression(),
            init_chunk_size: ctx.executor_chunk_sizes().0,
            max_chunk_size: ctx.executor_chunk_sizes().1,
        },
    );
    let (outer, inner, outer_filter, inner_filter) = if apply.hash_join.inner_child_idx == 0 {
        (right, left, right_filter, left_filter)
    } else {
        (left, right, left_filter, right_filter)
    };
    if apply.concurrency > 1 {
        use crate::apply::parallel::{
            ApplyWorker, CancellableInner, ParallelNestedLoopApplyExec, WorkerOuter,
        };
        let inner_plan = if apply.hash_join.inner_child_idx == 0 {
            left_plan
        } else {
            right_plan
        };
        // Go `buildApply`: an inner tree `SafeClone` refuses runs serially.
        // Every other runs on the workers, a CTE correlated to the outer row
        // included: the workers share the one producer built from the
        // original inner plan (`CTEStorageMap`), whose correlated cells no
        // worker binds, so Go answers such a CTE from NULL bindings.
        if tidb_planner_core_operator_physicalop::safe_clone(inner_plan).is_ok() {
            let worker_ctx = ctx.clone().with_coprocessor_worker_scope();
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut workers = Vec::with_capacity(apply.concurrency);
            for _ in 0..apply.concurrency {
                let mut cloned = tidb_planner_core_operator_physicalop::clone_for_apply_worker(inner_plan);
                let columns =
                    tidb_planner_core_operator_physicalop::extract_cor_columns_by_schema_4_physical_plan(
                        &mut cloned,
                        outer.schema(),
                    );
                let mut worker_state = BuildState {
                    projection_workers_allowed: state.projection_workers_allowed,
                    has_lock: state.has_lock,
                    executor_runtime_stats_source: state.executor_runtime_stats_source.clone(),
                    ..BuildState::default()
                };
                // Go's CTE storages are statement-wide (`StmtCtx.CTEStorageMap`):
                // every worker's CTE reads the producer the first build set up.
                worker_state.cte_slots = std::mem::take(&mut state.cte_slots);
                let built = build_with_state(&cloned, catalog, &worker_ctx, &mut worker_state);
                state.cte_slots = std::mem::take(&mut worker_state.cte_slots);
                let Ok(worker_inner) = built else {
                    workers.clear();
                    break;
                };
                let feed = Arc::new(Mutex::new((None, false)));
                let worker_outer = WorkerOuter::new(
                    ExecutorMeta::new(
                        outer.schema().clone(),
                        plan.id() as i64,
                        ctx.executor_chunk_sizes().0,
                        ctx.executor_chunk_sizes().1,
                    ),
                    feed.clone(),
                );
                let mut worker_conditions = worker_conditions.clone();
                let mut worker_inner_filter = inner_filter.clone();
                tidb_planner_core_operator_physicalop::rebind_apply_worker_expressions(
                    &mut worker_conditions,
                    &columns,
                );
                tidb_planner_core_operator_physicalop::rebind_apply_worker_expressions(
                    &mut worker_inner_filter,
                    &columns,
                );
                let worker_joiner = new_joiner(
                    worker_ctx.clone(),
                    joiner_type(apply.hash_join.join_type),
                    apply.hash_join.inner_child_idx == 0,
                    &default_values,
                    worker_conditions,
                    &left_types,
                    &right_types,
                    None,
                    false,
                    JoinerChunkSizes {
                        vectorized: ctx.enable_vectorized_expression(),
                        init_chunk_size: ctx.executor_chunk_sizes().0,
                        max_chunk_size: ctx.executor_chunk_sizes().1,
                    },
                );
                let worker = NestedLoopApplyExec::new(
                    meta(ctx, plan, plan_schema(plan)?),
                    Box::new(worker_outer),
                    Box::new(CancellableInner::new(worker_inner, stop.clone())),
                    Vec::new(),
                    worker_inner_filter,
                    columns,
                    worker_joiner,
                    apply.hash_join.join_type != LogicalJoinType::Inner,
                    apply.can_use_cache,
                    worker_ctx.clone(),
                );
                workers.push(ApplyWorker {
                    executor: Arc::new(Mutex::new(worker)),
                    feed,
                });
            }
            if workers.len() == apply.concurrency {
                let sink = state.apply_runtime_sink();
                let executor = Box::new(ParallelNestedLoopApplyExec::new(
                    meta(ctx, plan, plan_schema(plan)?),
                    outer,
                    outer_filter,
                    workers,
                    apply.keep_order,
                    apply.can_use_cache,
                    worker_ctx.clone(),
                    stop,
                    sink.clone(),
                ));
                return Ok(register_apply_stats_on_close(executor, ctx, plan, sink));
            }
        }
    }
    let executor = NestedLoopApplyExec::new(
        meta(ctx, plan, plan_schema(plan)?),
        outer,
        inner,
        outer_filter,
        inner_filter,
        apply.outer_schema.clone(),
        joiner,
        apply.hash_join.join_type != LogicalJoinType::Inner,
        apply.can_use_cache,
        ctx.clone(),
    );
    let sink = state.apply_runtime_sink();
    let executor = Box::new(executor.with_runtime_sink(sink.clone()));
    Ok(register_apply_stats_on_close(executor, ctx, plan, sink))
}

/// Go `NestedLoopApplyExec.Close` / `ParallelNestedLoopApplyExec.Close`'s
/// deferred `RegisterStats(e.ID(), runtimeStats)` of a `joinRuntimeStats`
/// with the apply's concurrency and cache use.
fn register_apply_stats_on_close(
    executor: Box<dyn Executor>,
    ctx: &crate::StmtContext,
    plan: &PhysicalPlan,
    sink: Option<crate::apply::native::ApplyRuntimeSink>,
) -> Box<dyn Executor> {
    let Some(sink) = sink else {
        return executor;
    };
    crate::runtime_stats_groups::RegisterStatsOnClose::wrap(executor, ctx, i64::from(plan.id()), move || {
        let snapshot = (*sink.lock().unwrap_or_else(std::sync::PoisonError::into_inner)).unwrap_or_default();
        Box::new(crate::runtime_stats_groups::ApplyJoinRuntimeStats { snapshot })
    })
}

fn join_key_offsets(
    left_keys: &[Column],
    right_keys: &[Column],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Vec<tidb_executor_join::merge_join_plan::MergeJoinKey>, DriverError> {
    left_keys
        .iter()
        .zip(right_keys)
        .map(|(left_key, right_key)| {
            let left = left_schema
                .columns
                .iter()
                .position(|column| column.unique_id == left_key.unique_id)
                .ok_or_else(|| {
                    DriverError::unsupported("a physical merge key is absent on the left")
                })?;
            let right = right_schema
                .columns
                .iter()
                .position(|column| column.unique_id == right_key.unique_id)
                .ok_or_else(|| {
                    DriverError::unsupported("a physical merge key is absent on the right")
                })?;
            // Go `GetCmpFunction(lhs, rhs)` compares strings under the
            // collation `CheckAndDeriveCollationFromExprs` derives from both.
            let fallback = left_key
                .ret_type
                .as_ref()
                .map_or(tidb_charset::Collation::Binary, |ty| ty.collation());
            let collation = tidb_expr::collation_derive::check_and_derive_collation_from_exprs(
                "",
                tidb_datatype::EvalType::Int,
                &[
                    tidb_expr::expression::Expression::Column(left_key.clone()),
                    tidb_expr::expression::Expression::Column(right_key.clone()),
                ],
            )
            .ok()
            .and_then(|derived| tidb_charset::Collation::from_name(&derived.collation))
            .unwrap_or(fallback);
            Ok(tidb_executor_join::merge_join_plan::MergeJoinKey {
                left,
                right,
                collation,
            })
        })
        .collect()
}

fn retained_table_id(plan: &PhysicalPlan) -> Result<i64, DriverError> {
    fn collect(plan: &PhysicalPlan, table_id: &mut Option<i64>) -> Result<(), DriverError> {
        let current = match plan {
            PhysicalPlan::TableScan(scan) => Some(scan.table_id),
            PhysicalPlan::IndexScan(scan) => Some(scan.table_id),
            _ => None,
        };
        if let Some(current) = current {
            match *table_id {
                Some(expected) if expected != current => {
                    return Err(DriverError::unsupported(
                        "an index-merge partial tree reads more than one table",
                    ));
                }
                None => *table_id = Some(current),
                _ => {}
            }
        }
        for child in plan.children() {
            collect(child, table_id)?;
        }
        Ok(())
    }

    let mut table_id = None;
    collect(plan, &mut table_id)?;
    table_id
        .ok_or_else(|| DriverError::unsupported("an index-merge physical tree has no table access"))
}

fn table_output_columns(
    schema: &Schema,
    table: &crate::TableCommon,
) -> Result<Vec<HandleOutputColumn>, DriverError> {
    schema
        .columns
        .iter()
        .map(|column| {
            if column.id == tidb_model::column::EXTRA_HANDLE_ID {
                return Ok(HandleOutputColumn::ExtraHandle);
            }
            if column.id == tidb_model::column::EXTRA_PHYS_TBL_ID {
                return Ok(HandleOutputColumn::PhysicalTableId);
            }
            if column.id == tidb_model::column::EXTRA_ROW_CHECKSUM_ID {
                return Ok(HandleOutputColumn::RowChecksum);
            }
            table
                .columns()
                .iter()
                .position(|stored| stored.read().id == column.id)
                .map(HandleOutputColumn::Stored)
                .ok_or_else(|| {
                    DriverError::unsupported(format!(
                        "retained table output column {} is absent from physical table {}",
                        column.id, table.table_id()
                    ))
                })
        })
        .collect()
}

fn point_range_values(
    range: &tidb_ranger::types::Range,
) -> Result<&[tidb_datatype::Datum], DriverError> {
    if range.low_exclude || range.high_exclude || range.low_val != range.high_val {
        return Err(DriverError::unsupported(
            "a retained point-get range is not one closed key",
        ));
    }
    Ok(&range.low_val)
}

fn point_handles(
    table: &crate::TableCommon,
    ranges: &tidb_ranger::types::Ranges,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_table::tables::TableHandle>, DriverError> {
    ranges
        .iter()
        .map(|range| {
            let values = point_range_values(range)?;
            if !table.common_handle_offsets().is_empty() {
                if values.len() != table.common_handle_offsets().len()
                    || values.iter().any(tidb_datatype::Datum::is_null)
                {
                    return Err(DriverError::unsupported(
                        "a retained point-get range does not name one common handle",
                    ));
                }
                let encoded = tidb_codec::encode_key_in_timezone(&ctx.session_zone(), values)
                    .map_err(|_| {
                        DriverError::unsupported(
                            "a retained point-get common handle cannot be encoded",
                        )
                    })?;
                let handle = tidb_txnkv::CommonHandle::new(encoded).map_err(|_| {
                    DriverError::unsupported("a retained point-get common handle is invalid")
                })?;
                return Ok(tidb_table::tables::TableHandle::Common(
                    handle.encoded().to_vec(),
                ));
            }
            match values {
                [tidb_datatype::Datum::Int(value)] => Ok(tidb_table::tables::TableHandle::Int(*value)),
                [tidb_datatype::Datum::UInt(value)] => {
                    Ok(tidb_table::tables::TableHandle::Int(*value as i64))
                }
                _ => Err(DriverError::unsupported(
                    "a retained point-get range does not name one integer handle",
                )),
            }
        })
        .collect()
}

fn unique_index_point_values(
    table: &crate::TableCommon,
    index_id: i64,
    ranges: &tidb_ranger::types::Ranges,
    ctx: &crate::StmtContext,
    skip_null: bool,
    keep_order: bool,
    desc: bool,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
    let index = table
        .plan_indexes()
        .find(|index| index.read().id == index_id)
        .ok_or_else(|| DriverError::unsupported("physical point-get index ID is absent"))?;
    if !index.read().unique || index.has_prefix() {
        return Err(DriverError::unsupported(
            "a retained index point-get requires a non-prefix unique index",
        ));
    }
    let mut encoded_values = Vec::with_capacity(ranges.len());
    let mut seen = std::collections::HashSet::with_capacity(ranges.len());
    let mut kept_null = false;
    for range in ranges {
        let values = point_range_values(range)?;
        if values.len() != index.column_offsets.len() {
            return Err(DriverError::unsupported(
                "a retained point-get key has the wrong unique-index width",
            ));
        }
        if values.iter().any(tidb_datatype::Datum::is_null) {
            if !skip_null && !kept_null {
                kept_null = true;
                encoded_values.push((None, values.to_vec()));
            }
            continue;
        }
        let encoded = tidb_codec::Encoder::new(table.use_new_collation())
            .encode_key_in_timezone(&ctx.session_zone(), values)
            .map_err(|_| {
                DriverError::unsupported("a retained unique point-get key cannot be encoded")
            })?;
        if seen.insert(encoded.clone()) {
            encoded_values.push((Some(encoded), values.to_vec()));
        }
    }
    if keep_order {
        encoded_values.sort_by(|left, right| {
            let order = left.0.cmp(&right.0);
            if desc {
                order.reverse()
            } else {
                order
            }
        });
    }
    Ok(encoded_values
        .into_iter()
        .map(|(_, values)| values)
        .collect())
}

fn point_partition_id(
    point: &PointGetPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Option<i64>, DriverError> {
    let Some(routing) = &point.partition else {
        return Ok(Some(point.table_id));
    };
    let table = catalog
        .physical_kv_table_by_id(point.table_id)
        .ok_or_else(|| DriverError::unsupported("point partition logical table is absent"))?;
    table
        .partition()
        .ok_or_else(|| DriverError::unsupported("point partition metadata is absent"))?;
    // A GLOBAL index entry names its row's partition, which the source checks
    // against the PARTITION clause; a LOCAL unique index is routed by its key
    // values, as Go's `PointGetPlan.PrunePartitions` does.
    let local_index = match point.index_id {
        Some(index_id) => {
            let index = table
                .plan_indexes()
                .find(|index| index.read().id == index_id)
                .ok_or_else(|| DriverError::unsupported("point partition index is absent"))?;
            if index.read().global {
                return Ok(Some(point.table_id));
            }
            Some(index.column_offsets.clone())
        }
        None => None,
    };
    let route = if let Some(offsets) = local_index {
        let values = match &point.range_rebuild {
            // Original values, not collation sort keys, are the partition
            // expression's inputs.
            Some(tidb_planner_core_operator_physicalop::range_rebuild::PointRangeRebuild::Index(rebuild)) => {
                let result =
                    tidb_ranger::detacher::detach_cond_and_build_range_for_partition_in(
                        &rebuild.access_conditions,
                        &rebuild.index_columns,
                        &rebuild.index_column_lengths,
                        0,
                        &tidb_ranger::points::StatementRangerEvaluator(ctx),
                        ctx.ranger_options(),
                    )
                    .map_err(|error| {
                        DriverError::unsupported(format!("point partition range: {error:?}"))
                    })?;
                let [range] = result.ranges.as_slice() else {
                    return Err(DriverError::unsupported(
                        "partition point must rebuild one key",
                    ));
                };
                point_range_values(range)?.to_vec()
            }
            _ => {
                let [range] = point.ranges.as_slice() else {
                    return Err(DriverError::unsupported(
                        "partition point must retain one key",
                    ));
                };
                point_range_values(range)?.to_vec()
            }
        };
        if values.len() != offsets.len() {
            return Err(DriverError::unsupported(
                "partition point key width is invalid",
            ));
        }
        let mut row = vec![tidb_datatype::Datum::Null; table.columns().len()];
        for (&offset, value) in offsets.iter().zip(values) {
            row[offset] = value;
        }
        table.row_partition_route(&row, ctx)
    } else if table.common_handle_offsets().is_empty() {
        let handles = point_handles(&table, &point.ranges, ctx)?;
        if handles.len() != 1 {
            return Err(DriverError::unsupported(
                "partition point must retain one handle",
            ));
        }
        table.handle_partition_routes(&handles, &ctx.session_zone(), ctx)[0]
    } else {
        // Go PointGetPlan.PrunePartitions rebuilds original values: a common
        // handle's collation sort keys are not SQL partition expression inputs.
        let Some(tidb_planner_core_operator_physicalop::range_rebuild::PointRangeRebuild::Table(rebuild)) =
            &point.range_rebuild
        else {
            return Err(DriverError::unsupported(
                "partition point lost its access conditions",
            ));
        };
        let result = tidb_ranger::detacher::detach_cond_and_build_range_for_partition_in(
            &rebuild.access_conditions,
            &rebuild.common_handle_columns,
            &rebuild.common_handle_lengths,
            0,
            &tidb_ranger::points::StatementRangerEvaluator(ctx),
            ctx.ranger_options(),
        )
        .map_err(|error| DriverError::unsupported(format!("point partition range: {error:?}")))?;
        let [range] = result.ranges.as_slice() else {
            return Err(DriverError::unsupported(
                "partition point must rebuild one key",
            ));
        };
        let values = point_range_values(range)?;
        if values.len() != table.common_handle_offsets().len() {
            return Err(DriverError::unsupported(
                "partition point key width is invalid",
            ));
        }
        let mut row = vec![tidb_datatype::Datum::Null; table.columns().len()];
        for (&offset, value) in table.common_handle_offsets().iter().zip(values) {
            row[offset] = value.clone();
        }
        table.row_partition_route(&row, ctx)
    };
    Ok(route.and_then(|(ordinal, id)| {
        (routing.names.is_empty()
            || routing
                .names
                .iter()
                .any(|name| {
                    table.meta().read().get_partition_info().is_some_and(|pi| {
                        pi.read()
                            .definitions
                            .get(ordinal)
                            .name
                            .original()
                            .go_lower_eq(name)
                    })
                }))
        .then_some(id)
    }))
}

/// Go `matchPartitionNames`' id set: the physical ids the PARTITION clause
/// names, or `None` when it names none.
fn partition_ids_named(table: &tidb_table::tables::TableCommon, names: &[String]) -> Option<Vec<i64>> {
    if names.is_empty() {
        return None;
    }
    let pi = table.meta().read().get_partition_info()?;
    let definitions = pi.read().definitions.snapshot();
    Some(
        definitions
            .iter()
            .filter(|definition| {
                names.iter().any(|name| definition.name.original().go_lower_eq(name))
            })
            .map(|definition| definition.id)
            .collect(),
    )
}

/// Go `PhysicalLock`'s executor (`SelectLockExec`) around an already built
/// `child`: a locking read inside a transaction locks every row it returns.
/// The lock is a no-op outside one, and a shared lock stays one unless the
/// session promotes it.
fn select_lock_executor(
    lock: &tidb_planner_core_operator_physicalop::PhysicalLock,
    child: Box<dyn Executor>,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let Some(selected) = ctx.selected_lock_keys() else {
        return Ok(child);
    };
    use tidb_planner_core_operator_logicalop::SelectLockType;
    use tidb_txnkv::transaction::LockWaitTime;
    if matches!(
        lock.lock_type,
        SelectLockType::ForShare | SelectLockType::ForShareNoWait
    ) && !selected.shared_lock_promotion()
    {
        return Ok(child);
    }
    let wait = match lock.lock_type {
        SelectLockType::ForUpdateNoWait | SelectLockType::ForShareNoWait => {
            LockWaitTime::NoWait
        }
        SelectLockType::ForUpdateWaitN => {
            LockWaitTime::Timeout(std::time::Duration::from_secs(lock.wait_sec))
        }
        _ => selected.default_wait(),
    };
    let types = child.ret_field_types().to_vec();
    let mut expressions: Vec<SelectedRecordKey> = Vec::new();
    // Go `buildSelectLock` removes the temporary tables from `tblID2Handle`;
    // `SelectLockExec.Next` locks through the statement's transaction.
    let mut locked_table_ids = Vec::new();
    // The statement's snapshot, reached through any non-temporary table.
    let mut lock_store = None;
    for (table_id, columns) in &lock.tbl_id_to_handle_cols {
        let table = catalog.kv_table_by_id(*table_id).ok_or_else(|| {
            DriverError::unsupported("SelectLock table ID is absent from the catalog")
        })?;
        if table.is_temporary() {
            continue;
        }
        locked_table_ids.push(*table_id);
        lock_store.get_or_insert_with(|| crate::statement_store(ctx));
        let width = table.common_handle_offsets().len().max(1);
        if columns.len() % width != 0 {
            return Err(DriverError::unsupported(
                "SelectLock has an incomplete handle",
            ));
        }
        let partition_offset = lock
            .tbl_id_to_phys_tbl_id_col
            .get(table_id)
            .map(|column| {
                usize::try_from(child.schema().column_index(column)).map_err(|_| {
                    DriverError::unsupported("SelectLock lost its physical table ID")
                })
            })
            .transpose()?;
        if table.partition().is_some() && partition_offset.is_none() {
            return Err(DriverError::unsupported(
                "SelectLock requires its physical partition ID",
            ));
        }
        for handle_columns in columns.chunks(width) {
            let offsets = handle_columns
                .iter()
                .map(|column| {
                    usize::try_from(child.schema().column_index(column)).map_err(|_| {
                        DriverError::unsupported("SelectLock lost its record handle")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let table = table.clone();
            let types = types.clone();
            let context = ctx.clone();
            expressions.push(Box::new(move |row| {
                use tidb_datatype::Datum;
                let values = offsets
                    .iter()
                    .map(|offset| row.get_datum(*offset, &types[*offset]))
                    .collect::<Vec<_>>();
                if values.iter().all(Datum::is_null) {
                    return Ok(None);
                }
                let handle = if table.common_handle_offsets().is_empty() {
                    match values[0] {
                        Datum::Int(id) => TableHandle::Int(id),
                        Datum::UInt(id) => TableHandle::Int(id as i64),
                        _ => {
                            return Err(crate::ExecError::internal(
                                "invalid integer lock handle",
                            ));
                        }
                    }
                } else {
                    table
                        .common_handle_from_values(&values, &context.session_zone())
                        .map_err(|error| crate::ExecError::internal(format!("{error:?}")))?
                };
                let physical_id =
                    partition_offset.map_or(table.table_id(), |offset| row.get_int64(offset));
                if physical_id == 0 {
                    return Ok(None);
                }
                Ok(Some(tidb_tablecodec::table_key::encode_row_key_with_handle(
                    physical_id,
                    &handle.record_handle(),
                )))
            }));
        }
    }
    let locker = selected
        .lock_in_execution()
        .then(|| {
            lock_store.map(|store| {
                crate::select_lock::StatementLocker::new(
                    store,
                    locked_table_ids,
                    ctx.clone(),
                    selected.lock_state(),
                )
            })
        })
        .flatten();
    let mut executor = SelectLockExec::new(child, expressions, selected).with_wait(wait);
    if let Some(locker) = locker {
        executor = executor.with_locker(locker);
    }
    Ok(Box::new(executor) as Box<dyn Executor>)
}

/// Go's `PointGetExecutor` / `BatchPointGetExec` with `lock` set: a locking
/// SELECT inside a transaction locks the row each point read returns. The
/// read is built over a schema that also carries the record handle (and the
/// partition id of a partitioned table), `SelectLockExec` takes the locks
/// from those columns, and a projection restores the plan's own columns.
fn with_select_lock(
    plan: &PhysicalPlan,
    select_lock: Option<(tidb_planner_core_operator_logicalop::SelectLockType, u64)>,
    table_id: i64,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    build: impl FnOnce(&PhysicalPlan) -> Result<Box<dyn Executor>, DriverError>,
) -> Result<Box<dyn Executor>, DriverError> {
    use tidb_datatype::{FieldType, FieldTypeCode};
    let Some((lock_type, wait_sec)) = select_lock else {
        return build(plan);
    };
    if ctx.selected_lock_keys().is_none() {
        return build(plan);
    }
    let table = catalog.kv_table_by_id(table_id).ok_or_else(|| {
        DriverError::unsupported("a locking point read's table ID is absent from the catalog")
    })?;
    let schema = plan_schema(plan)?;
    let mut widened = schema.clone();
    let mut next_unique_id = i64::MAX;
    let mut synthetic = |id: i64, field_type: FieldType| {
        let mut column = Column::new(next_unique_id, field_type);
        next_unique_id -= 1;
        column.id = id;
        column
    };
    let mut handle_columns = Vec::new();
    if table.common_handle_offsets().is_empty() {
        handle_columns.push(synthetic(
            tidb_model::column::EXTRA_HANDLE_ID,
            FieldType::new(FieldTypeCode::LongLong),
        ));
    } else {
        for offset in table.common_handle_offsets() {
            let stored = table.columns().get(*offset).ok_or_else(|| {
                DriverError::unsupported("a common handle column is outside its table")
            })?;
            handle_columns.push(synthetic(stored.read().id, stored.read().field_type.clone()));
        }
    }
    let phys_column = table.partition().is_some().then(|| {
        synthetic(
            tidb_model::column::EXTRA_PHYS_TBL_ID,
            FieldType::new(FieldTypeCode::LongLong),
        )
    });
    widened.columns.extend(handle_columns.iter().cloned());
    widened.columns.extend(phys_column.iter().cloned());
    let mut widened_plan = plan.clone();
    widened_plan
        .base_mut()
        .base
        .set_schema(Some(widened.clone()));
    let child = build(&widened_plan)?;
    let lock = tidb_planner_core_operator_physicalop::PhysicalLock {
        base: tidb_planner_core_operator_physicalop::BasePhysicalPlan::default(),
        lock_type,
        wait_sec,
        tbl_id_to_handle_cols: std::collections::BTreeMap::from([(table_id, handle_columns)]),
        tbl_id_to_phys_tbl_id_col: phys_column
            .into_iter()
            .map(|column| (table_id, column))
            .collect(),
    };
    let locked = select_lock_executor(&lock, child, catalog, ctx)?;
    let expressions = resolve_expressions(
        &schema
            .columns
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect::<Vec<_>>(),
        locked.schema(),
    )?;
    // Id 0 keeps the wrapper out of the runtime statistics: they belong to
    // the point read the plan names.
    let (init_cap, max_chunk_size) = ctx.executor_chunk_sizes();
    let restored = ExecutorMeta::new(schema, 0, init_cap, max_chunk_size);
    Ok(Box::new(ProjectionExec::new(restored, expressions, locked, ctx.clone())))
}

/// Go `PointGetExecutor.lock` / `BatchPointGetExec.lock`: the wait a locking
/// point plan locks its record keys with inside `Next`, before reading them.
/// `None` outside a pessimistic transaction's statement (which has no
/// selected-lock owner) and for a plan that does not lock. A locking
/// SELECT's own `NOWAIT` / `WAIT n` choose the wait.
fn point_lock(
    lock: bool,
    select_lock: Option<(tidb_planner_core_operator_logicalop::SelectLockType, u64)>,
    ctx: &crate::StmtContext,
) -> Option<crate::access_path::PointLock> {
    use tidb_planner_core_operator_logicalop::SelectLockType;
    use tidb_txnkv::transaction::LockWaitTime;
    if !lock {
        return None;
    }
    let selected = ctx.selected_lock_keys()?;
    let select_wait = match select_lock {
        None => None,
        Some((SelectLockType::ForUpdate, _)) => None,
        Some((SelectLockType::ForUpdateNoWait, _)) => Some(LockWaitTime::NoWait),
        Some((SelectLockType::ForUpdateWaitN, wait_sec)) => Some(LockWaitTime::Timeout(
            std::time::Duration::from_secs(wait_sec),
        )),
        // Shared locks are taken by the statement's lock step, when at all.
        Some(_) => return None,
    };
    Some(crate::access_path::PointLock {
        wait: select_wait.unwrap_or(selected.default_wait()),
        read_committed: selected.read_committed(),
        lock_state: selected.lock_state(),
    })
}

/// Go `buildPointGet` / `buildBatchPointGet`: a cached table read inside
/// its lease reads through `cacheTableSnapshot`.
fn point_store(
    table: &tidb_table::tables::TableCommon,
    ctx: &crate::StmtContext,
) -> Box<dyn tidb_kv::storage::TableStorage> {
    let store = crate::statement_store(ctx);
    match cached_table_data(table, ctx, false) {
        Some(cache) => store.with_cache_table_snapshot(cache).unwrap_or(store),
        None => store,
    }
}

/// Go `prefetchPointPlanKeys`' `handlePlan`: the unique-index keys and row
/// keys a point plan reads, after its partition pruning (nothing for a plan
/// whose key matches no partition).
pub(super) fn point_plan_prefetch_keys(
    plan: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>), DriverError> {
    let key_error = |error| DriverError::unsupported(format!("unique index key encoding failed: {error:?}"));
    let row_key = |physical_id: i64, handle: &TableHandle| {
        let handle = match handle {
            TableHandle::Int(value) => tidb_tablecodec::table_key::RecordHandle::Int(*value),
            TableHandle::Common(bytes) => tidb_tablecodec::table_key::RecordHandle::Common(bytes.clone()),
        };
        tidb_tablecodec::table_key::encode_row_key_with_handle(physical_id, &handle)
    };
    let mut idx_keys = Vec::new();
    let mut row_keys = Vec::new();
    match plan {
        PhysicalPlan::PointGet(point) => {
            let Some(physical_id) = point_partition_id(point, catalog, ctx)? else {
                return Ok((idx_keys, row_keys));
            };
            let table = catalog
                .physical_kv_table_by_id(physical_id)
                .ok_or_else(|| DriverError::unsupported("physical point-get table ID is absent"))?;
            let table = table_with_plan_index(table, point.index_id, ctx)?;
            if let Some(index_id) = point.index_id {
                for values in unique_index_point_values(&table, index_id, &point.ranges, ctx, false, false, false)? {
                    let keys = table
                        .encode_unique_index_key(index_id, &values, &ctx.session_zone())
                        .map_err(key_error)?;
                    idx_keys.extend(keys.iter().map(|key| key.as_bytes().to_vec()));
                }
            } else {
                for handle in point_handles(&table, &point.ranges, ctx)? {
                    row_keys.push(row_key(physical_id, &handle));
                }
            }
        }
        PhysicalPlan::BatchPointGet(batch) => {
            let table = catalog
                .physical_kv_table_by_id(batch.table_id)
                .ok_or_else(|| DriverError::unsupported("physical batch-point table ID is absent"))?;
            let table = table_with_plan_index(table, batch.index_id, ctx)?;
            if let Some(index_id) = batch.index_id {
                let values = unique_index_point_values(&table, index_id, &batch.ranges, ctx, true, false, false)?;
                let keys = table
                    .unique_batch_index_keys(index_id, &values, &ctx.session_zone(), None)
                    .map_err(key_error)?;
                idx_keys.extend(keys.into_iter().flatten().map(|(key, _)| key.as_bytes().to_vec()));
            } else {
                let handles = point_handles(&table, &batch.ranges, ctx)?;
                for (i, handle) in handles.iter().enumerate() {
                    let physical_id = batch
                        .partition_ids
                        .as_ref()
                        .and_then(|ids| ids.get(i).copied())
                        .unwrap_or(batch.table_id);
                    row_keys.push(row_key(physical_id, handle));
                }
            }
        }
        _ => {}
    }
    Ok((idx_keys, row_keys))
}

fn build_point_get(
    plan: &PhysicalPlan,
    point: &PointGetPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    snapshot_stats: Option<Arc<tikv_client::SnapshotRuntimeStats>>,
) -> Result<Box<dyn Executor>, DriverError> {
    let Some(physical_id) = point_partition_id(point, catalog, ctx)? else {
        return Ok(Box::new(TableDualExec::new(
            meta(ctx, plan, plan_schema(plan)?),
            0,
        )));
    };
    let table = catalog
        .physical_kv_table_by_id(physical_id)
        .ok_or_else(|| DriverError::unsupported("physical point-get table ID is absent"))?;
    let table = table_with_plan_index(table, point.index_id, ctx)?;
    let snapshot_stats =
        configure_point_snapshot_read(ctx, plan.point_avg_row_size(true), snapshot_stats);
    let schema = plan_schema(plan)?;
    let output_columns = table_output_columns(&schema, &table)?;
    let executor_meta = ExecutorMeta::new(schema, i64::from(plan.base().base.id()), 1, 1)
        .with_alloc_pool(ctx.chunk_allocator());
    if let Some(index_id) = point.index_id {
        let index_values =
            unique_index_point_values(&table, index_id, &point.ranges, ctx, false, false, false)?;
        if index_values.len() != 1 {
            return Err(DriverError::unsupported(
                "a physical unique-index PointGet does not retain exactly one key",
            ));
        }
        let allowed = point
            .partition
            .as_ref()
            .and_then(|partition| partition_ids_named(&table, &partition.names));
        let child = Box::new(
            UniqueIndexPointSourceExec::new(
                executor_meta,
                (*table).clone(),
                point_store(&table, ctx),
                index_id,
                index_values,
                output_columns,
                RowDecodeContext::for_query(ctx),
                true,
            )
            .with_allowed_partitions(allowed)
            .with_lock(point_lock(point.lock, point.select_lock, ctx)),
        );
        return Ok(wrap_point_index_usage(
            child,
            std::sync::Arc::unwrap_or_clone(table),
            catalog,
            ctx,
            Some(index_id),
            PointCommand::Get,
            snapshot_stats,
        ));
    }
    let handles = point_handles(&table, &point.ranges, ctx)?;
    if handles.len() != 1 {
        return Err(DriverError::unsupported(
            "a physical PointGet does not retain exactly one key",
        ));
    }
    let child = Box::new(
        HandleSourceExec::new_point_mapped_with_context(
            executor_meta,
            (*table).clone(),
            point_store(&table, ctx),
            handles.into_iter().next().expect("checked one handle"),
            output_columns,
            RowDecodeContext::for_query(ctx),
        )
        .with_lock_before_read(point_lock(point.lock, point.select_lock, ctx)),
    );
    Ok(wrap_point_index_usage(
        child,
        std::sync::Arc::unwrap_or_clone(table),
        catalog,
        ctx,
        None,
        PointCommand::Get,
        snapshot_stats,
    ))
}

fn build_batch_point_get(
    plan: &PhysicalPlan,
    batch: &BatchPointGetPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    snapshot_stats: Option<Arc<tikv_client::SnapshotRuntimeStats>>,
) -> Result<Box<dyn Executor>, DriverError> {
    let table = catalog
        .physical_kv_table_by_id(batch.table_id)
        .ok_or_else(|| DriverError::unsupported("physical batch-point table ID is absent"))?;
    let table = table_with_plan_index(table, batch.index_id, ctx)?;
    let snapshot_stats =
        configure_point_snapshot_read(ctx, plan.point_avg_row_size(true), snapshot_stats);
    let schema = plan_schema(plan)?;
    let output_columns = table_output_columns(&schema, &table)?;
    // Go buildBatchPointGet sizes both capacities to the retained access keys.
    let capacity = batch.ranges.len();
    let executor_meta =
        ExecutorMeta::new(schema, i64::from(plan.base().base.id()), capacity, capacity)
            .with_alloc_pool(ctx.chunk_allocator());
    if let Some(index_id) = batch.index_id {
        let index_values = unique_index_point_values(
            &table,
            index_id,
            &batch.ranges,
            ctx,
            true,
            batch.keep_order,
            batch.desc,
        )?;
        let allowed = partition_ids_named(&table, &batch.partition_names);
        let child = Box::new(
            UniqueIndexPointSourceExec::new(
                executor_meta,
                (*table).clone(),
                point_store(&table, ctx),
                index_id,
                index_values,
                output_columns,
                RowDecodeContext::for_query(ctx),
                false,
            )
            .with_allowed_partitions(allowed)
            .with_lock(point_lock(batch.lock, batch.select_lock, ctx)),
        );
        return Ok(wrap_point_index_usage(
            child,
            std::sync::Arc::unwrap_or_clone(table),
            catalog,
            ctx,
            Some(index_id),
            PointCommand::BatchGet,
            snapshot_stats,
        ));
    }
    let mut handles = point_handles(&table, &batch.ranges, ctx)?;
    let routes = if let Some(ids) = &batch.partition_ids {
        if ids.len() != handles.len() {
            return Err(DriverError::unsupported(
                "batch point partition routes lost alignment",
            ));
        }
        Some(
            handles
                .iter()
                .cloned()
                .zip(ids.iter().copied())
                .collect::<std::collections::HashMap<_, _>>(),
        )
    } else {
        None
    };
    let mut seen = std::collections::HashSet::with_capacity(handles.len());
    handles.retain(|handle| seen.insert(handle.clone()));
    if batch.keep_order {
        let unsigned_pk_is_handle = table
            .pk_handle_offset()
            .and_then(|offset| table.columns().get(offset))
            .is_some_and(|column| column.read().field_type.is_unsigned());
        sort_handles_for_keep_order(&mut handles, batch.desc, unsigned_pk_is_handle);
    }
    let partition_ids = routes.map(|routes| handles.iter().map(|handle| routes[handle]).collect());
    let child = Box::new(
        HandleSourceExec::new_mapped_with_context(
            executor_meta,
            (*table).clone(),
            point_store(&table, ctx),
            handles,
            output_columns,
            RowDecodeContext::for_query(ctx),
        )
        .with_partition_ids(partition_ids)
        .with_lock_before_read(point_lock(batch.lock, batch.select_lock, ctx)),
    );
    Ok(wrap_point_index_usage(
        child,
        std::sync::Arc::unwrap_or_clone(table),
        catalog,
        ctx,
        None,
        PointCommand::BatchGet,
        snapshot_stats,
    ))
}

/// Go `snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)` beside the
/// point reader's other snapshot options; the returned handle keeps the
/// options to restore when the reader closes.
fn configure_point_snapshot_read(
    ctx: &crate::StmtContext,
    avg_row_bytes: f64,
    snapshot_stats: Option<Arc<tikv_client::SnapshotRuntimeStats>>,
) -> Option<tidb_executor_internal_exec::index_usage_reporter::PointSnapshotStats> {
    let options = ctx.snapshot_read_options(avg_row_bytes);
    let store = crate::statement_store(ctx);
    let Some(stats) = snapshot_stats else {
        store.set_snapshot_read_options(options);
        return None;
    };
    store.set_snapshot_read_options(tidb_txnkv::SnapshotReadOptions {
        runtime_stats: Some(tidb_txnkv::SnapshotRuntimeStatsHandle(Arc::clone(&stats))),
        ..options.clone()
    });
    Some(tidb_executor_internal_exec::index_usage_reporter::PointSnapshotStats {
        stats,
        options_without_stats: options,
        sql_cpu_usages: ctx.sql_cpu_usages().cloned(),
    })
}

fn wrap_point_index_usage(
    child: Box<dyn Executor>,
    table: tidb_table::tables::TableCommon,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    index_id: Option<i64>,
    command: PointCommand,
    snapshot_stats: Option<tidb_executor_internal_exec::index_usage_reporter::PointSnapshotStats>,
) -> Box<dyn Executor> {
    // Go `buildIndexUsageReporter(p, false)` for a PointGet loads nothing,
    // so its read counts against `math.MaxInt32` rows; a BatchPointGet's
    // `LoadTableStats` loads the logical table's, whatever partitions it reads.
    let stats = match command {
        PointCommand::Get => None,
        PointCommand::BatchGet => crate::access_cost::load_table_stats(ctx, catalog, table.table_id()),
    };
    let store = crate::statement_store(ctx);
    Box::new(PointIndexUsageExec::new(
        child,
        table,
        store,
        stats,
        ctx.index_usage_collector().cloned(),
        index_id,
        command,
        snapshot_stats,
    ))
}

fn schema_column_slot(schema: &Schema, wanted: &Column) -> Option<usize> {
    schema
        .columns
        .iter()
        .position(|column| column.unique_id == wanted.unique_id)
        .or_else(|| {
            (wanted.id != 0).then(|| {
                schema
                    .columns
                    .iter()
                    .position(|column| column.id == wanted.id)
            })?
        })
}

/// Go `handleCols.GetFieldsTypes()`: the partial index worker's chunk
/// columns (`getRetTpsForIndexScan` with no by-items).
fn index_merge_handle_field_types(
    schema: &Schema,
    table: &crate::TableCommon,
) -> Result<Vec<FieldType>, DriverError> {
    let slots = match partial_handle_columns(schema, table)? {
        PartialHandleColumns::Int(column) => vec![column],
        PartialHandleColumns::Common(columns) => columns,
    };
    slots
        .into_iter()
        .map(|slot| {
            schema.columns[slot].ret_type.clone().ok_or_else(|| {
                DriverError::unsupported("an index-merge handle column has no type")
            })
        })
        .collect()
}

fn partial_handle_columns(
    schema: &Schema,
    table: &crate::TableCommon,
) -> Result<PartialHandleColumns, DriverError> {
    let table_column_slot = |offset: usize| {
        let id = table.columns().get(offset)?.read().id;
        // Common handles are a suffix, even when an index column has the
        // same ID (and may contain only a prefix of the full key value).
        schema.columns.iter().rposition(|column| column.id == id)
    };
    if let Some(offset) = table.pk_handle_offset() {
        return schema
            .columns
            .iter()
            .rposition(|column| column.id == tidb_model::column::EXTRA_HANDLE_ID)
            .or_else(|| table_column_slot(offset))
            .map(PartialHandleColumns::Int)
            .ok_or_else(|| {
                DriverError::unsupported(
                    "an index-merge partial tree does not emit its integer handle",
                )
            });
    }
    if !table.common_handle_offsets().is_empty() {
        return table
            .common_handle_offsets()
            .iter()
            .map(|offset| {
                table_column_slot(*offset).ok_or_else(|| {
                    DriverError::unsupported(
                        "an index-merge partial tree does not emit every common-handle column",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(PartialHandleColumns::Common);
    }
    schema
        .columns
        .iter()
        .position(|column| column.id == tidb_model::column::EXTRA_HANDLE_ID)
        .map(PartialHandleColumns::Int)
        .ok_or_else(|| {
            DriverError::unsupported(
                "an index-merge partial tree over a heap table does not emit _tidb_rowid",
            )
        })
}

fn partial_sort_key_columns(
    schema: &Schema,
    by_items: &[tidb_expression_aggregation::ByItems],
) -> Result<Vec<usize>, DriverError> {
    by_items
        .iter()
        .map(|item| {
            let Expression::Column(column) = &item.expr else {
                return Err(DriverError::unsupported(
                    "an index-merge by-item is not a retained physical column",
                ));
            };
            schema_column_slot(schema, column).ok_or_else(|| {
                DriverError::unsupported(
                    "an index-merge by-item is absent from a partial plan output",
                )
            })
        })
        .collect()
}

/// Go's partial index worker requests handles directly, while retaining the
/// normal scan's runtime counter and close-time index-usage report.
struct IndexPartialHandleSource {
    source: IndexRangeSourceExec,
    partition_indexes: Vec<usize>,
    table: crate::TableCommon,
    stats: Option<tidb_stmtctx::used_stats::UsedStatsInfoForTable>,
    ctx: crate::StmtContext,
    index_id: i64,
    reported: bool,
    initial_batch_size: usize,
    batch_size: usize,
    max_batch_size: usize,
    /// The index merge reader's plan ID: Go's `e.memTracker`, which the
    /// worker charges its chunk to and its request its responses.
    memory_label: i64,
    /// Go `getRetTpsForIndexScan`: the handle columns.
    chunk_field_types: Vec<FieldType>,
    max_chunk_size: usize,
    memory: Option<crate::index_merge_reader::MergeReaderMemory>,
    /// Go `fetchHandles`' `chunk.NewChunkWithCapacity(tps, w.maxChunkSize)`.
    chunk: Option<Chunk>,
}

impl PartialHandleSource for IndexPartialHandleSource {
    fn open(&mut self) -> Result<(), crate::ExecError> {
        self.reported = false;
        self.batch_size = self.initial_batch_size;
        let memory = crate::index_merge_reader::MergeReaderMemory::new(
            &self.ctx.statement_memory(),
            self.memory_label,
        );
        self.source.set_select_mem_tracker(Arc::clone(memory.tracker()));
        self.memory = Some(memory);
        self.chunk = Some(Chunk::new(
            &self.chunk_field_types,
            self.max_chunk_size,
            self.max_chunk_size,
        ));
        self.source.open()
    }

    fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, crate::ExecError> {
        self.next_batch_with_max_handles(usize::MAX)
    }

    fn next_batch_with_max_handles(
        &mut self,
        maximum: usize,
    ) -> Result<Option<PartialHandleBatch>, crate::ExecError> {
        // Go `partialIndexWorker.extractTaskHandles`: each non-empty
        // `idxResult.Next` charges the chunk.
        let target = self.batch_size.min(maximum);
        let memory = self.memory.as_ref().expect("partial source is open");
        let chunk = self.chunk.as_mut().expect("partial source is open");
        let mut handles = Vec::with_capacity(target);
        while handles.len() < target {
            let required = (target - handles.len()).min(self.max_chunk_size);
            chunk.set_required_rows(required as isize, self.max_chunk_size);
            let batch = self.source.next_index_merge_chunk(required, chunk)?;
            if batch.is_empty() {
                break;
            }
            memory.consume_chunk(chunk.memory_usage())?;
            for (handle, partition) in batch {
                let partition_index = *self.partition_indexes.get(partition).ok_or_else(|| {
                    crate::ExecError::internal("index-merge cursor returned an unknown partition")
                })?;
                handles.push(HandleRef {
                    handle,
                    partition_index,
                });
            }
        }
        if handles.len() == self.batch_size {
            self.batch_size = self.batch_size.saturating_mul(2).min(self.max_batch_size);
        }
        Ok((!handles.is_empty()).then_some(PartialHandleBatch {
            handles,
            sort_keys: Default::default(),
        }))
    }

    fn close(&mut self) -> Result<(), crate::ExecError> {
        let result = self.source.close();
        self.memory = None;
        self.chunk = None;
        if !self.reported {
            self.reported = true;
            if let Some(access) = self.source.table_access() {
                let (requests, mut rows) = access.cop_count_and_rows().unwrap_or_default();
                if requests == 0 && rows == 0 {
                    rows = access.local_index_usage_rows().unwrap_or_default();
                }
                tidb_executor_internal_exec::index_usage_reporter::IndexUsageReporter::new(
                    self.ctx.index_usage_collector(),
                )
                .report_cop_index_usage_for_table(
                    &self.table,
                    self.stats.as_ref(),
                    self.index_id,
                    requests,
                    rows,
                );
            }
        }
        result
    }
}

fn build_index_merge_reader(
    plan: &PhysicalPlan,
    reader: &tidb_planner_core_operator_physicalop::PhysicalIndexMergeReader,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let first = reader.partial_plans_raw.first().ok_or_else(|| {
        DriverError::unsupported("a physical index-merge reader has no partial plan")
    })?;
    let table_id = retained_table_id(first)?;
    for partial in &reader.partial_plans_raw[1..] {
        if retained_table_id(partial)? != table_id {
            return Err(DriverError::unsupported(
                "physical index-merge partial plans read different tables",
            ));
        }
    }
    let table_plan = reader.table_plan.as_deref().ok_or_else(|| {
        DriverError::unsupported("a physical index-merge reader has no final table plan")
    })?;
    if retained_table_id(table_plan)? != table_id {
        return Err(DriverError::unsupported(
            "a physical index-merge final table plan reads a different table",
        ));
    }
    let (table, store, separated) =
        state
            .reader_storage
            .table(ctx, catalog.physical_kv_table_by_id(table_id).ok_or_else(|| {
                DriverError::unsupported("physical index-merge table ID is absent from the catalog")
            })?);
    let physical_ids = table.record_physical_ids();
    // Go starts one worker goroutine per partial in plan order, all racing to
    // push handle tasks into one fetch channel. The lighter index requests
    // consistently complete first, so go's ARRIVAL order -- the union's
    // emission order -- is the index partials first, then the table partials,
    // whatever the DNF order the plans carry (go itself flips under load;
    // that residual race is nondeterministic there). This port is
    // deterministic, so it drains in go's modal order: build every source,
    // then let the index-partial sources overtake the table ones while
    // keeping their relative order inside each wave.
    let mut built: Vec<(bool, Box<dyn PartialHandleSource>)> = Vec::new();
    // The partial plan root of each built source whose basic stats the
    // reader records, in `built` order: an executor-built partial records
    // its own through the operator wrapper.
    let mut built_ids: Vec<Option<i64>> = Vec::new();
    for partial in &reader.partial_plans_raw {
        let partial_schema = plan_schema(partial)?;
        let selected = index_merge_partition_indexes(partial, &table)?;
        if let PhysicalPlan::IndexScan(scan) = partial {
            if reader.by_items.is_empty() {
                // Go buildIndexScanOutputOffsets returns only handles here.
                // Do not turn a partial index scan into a table double read.
                let mut partial_table = (*table).clone();
                let ids: Vec<_> = selected.iter().map(|index| physical_ids[*index]).collect();
                partial_table.restrict_read_to_partitions(&ids);
                // Go builds the worker's key ranges from the scan's Ranges:
                // a partial whose ranges came out empty (`c1 >= 3 and
                // c1 <= 1`) reads no keys.
                let ranges = scan.ranges.clone();
                let chunk_field_types = index_merge_handle_field_types(&partial_schema, &table)?;
                let mut source = IndexRangeSourceExec::new_with_statement(
                    meta(ctx, partial, partial_schema),
                    partial_table.clone(),
                    store.clone(),
                    scan.index_id,
                    ranges,
                    RowDecodeContext::for_query(ctx),
                    ReaderStorage::statement(
                        separated,
                        crate::remote_scan::pushdown_statement_context(ctx)
                            .with_cop_plan_chain(cop_plan_chain(partial), i64::from(partial.id()))
                            .with_plan_id(i64::from(scan.base.base.id()))
                            .with_estimated_net_bytes(
                                partial.cop_net_data_size(
                                    true,
                                    tidb_config::tikvcfg::get_global_config()
                                        .tikv_client
                                        .enable_chunk_rpc,
                                ),
                            ),
                    ),
                );
                source.set_lookup_concurrency(ctx.index_lookup_concurrency());
                source.set_lookup_size(ctx.index_lookup_size());
                #[cfg(test)]
                state.index_merge_partial_rows.insert(partial.id(), source.produced_rows());
                built_ids.push(Some(i64::from(partial.id())));
                built.push((
                    reader.by_items.is_empty(),
                    Box::new(IndexPartialHandleSource {
                        source,
                        partition_indexes: selected,
                        stats: crate::access_cost::load_table_stats(
                            ctx,
                            catalog,
                            partial_table.stats_physical_id(),
                        ),
                        table: partial_table,
                        ctx: ctx.clone(),
                        index_id: scan.index_id,
                        reported: false,
                        initial_batch_size:
                            crate::access_path::calculate_lookup_batch_size_with_cap(
                                partial.stats_count(),
                                ctx.executor_chunk_sizes().1,
                                false,
                                ctx.index_lookup_size(),
                            ),
                        batch_size: 0,
                        max_batch_size: ctx.index_lookup_size(),
                        memory_label: i64::from(plan.id()),
                        chunk_field_types,
                        max_chunk_size: ctx.executor_chunk_sizes().1,
                        memory: None,
                        chunk: None,
                    }),
                ));
                continue;
            }
        }
        // Go's partial table worker drains one result per pruned partition.
        // Keep those results under one path for intersection membership.
        let mut partitions: Vec<Box<dyn PartialHandleSource>> = Vec::new();
        // Go builds the worker's key ranges from the scan's Ranges, so an
        // empty set (`a between 2 and 1`) reads no keys; a table scan built
        // here with none would read every handle, as a lookup's table side
        // does.
        let reads_no_keys = embedded_index_scan(partial).is_none()
            && embedded_table_scan(partial).is_some_and(|scan| scan.ranges.is_empty());
        let selected = if reads_no_keys { Vec::new() } else { selected };
        // Go `buildPartialWorkerKVRanges`: a partial whose ranges pin the
        // group-by columns reads every (range group, partition) pair as its
        // own ordered stream, merged below like the partitions are.
        let range_groups: Vec<Option<tidb_ranger::types::Ranges>> =
            match partial_range_groups(partial)? {
                Some(groups) => groups.into_iter().map(Some).collect(),
                None => vec![None],
            };
        // Go's partial table worker opens one `TableReaderExecutor`, its
        // tracker labelled with the partial plan, for every partition.
        let reader_tracker = tidb_memory::Tracker::new(i64::from(partial.id()), -1);
        let streams = selected
            .into_iter()
            .flat_map(|partition_index| {
                range_groups
                    .iter()
                    .map(move |group| (partition_index, group))
            })
            .collect::<Vec<_>>();
        for (partition_index, group) in streams {
            let mut partition_plan = partial.clone();
            set_index_merge_partition(&mut partition_plan, physical_ids[partition_index]);
            if let Some(group) = group {
                narrow_scan_ranges(&mut partition_plan, group);
            }
            let old_chain = state
                .cop_chain
                .replace((cop_plan_chain(partial), i64::from(partial.id())));
            // Go's partial index worker sends its requests with the
            // reader's own tracker (`SetMemTracker(e.memTracker)`).
            let old_tracker = state.reader_mem_tracker.replace(if embedded_index_scan(partial).is_none() {
                Arc::clone(&reader_tracker)
            } else {
                ctx.statement_memory().persistent_operator_tracker(i64::from(plan.id()))
            });
            let old_partial = std::mem::replace(&mut state.index_merge_partial, true);
            let executor = build_with_state(&partition_plan, catalog, ctx, state);
            state.index_merge_partial = old_partial;
            state.cop_chain = old_chain;
            state.reader_mem_tracker = old_tracker;
            let executor = executor?;
            if executor.schema().len() != partial_schema.len() {
                return Err(DriverError::unsupported(
                    "an index-merge partial executor changed its retained output width",
                ));
            }
            let mut source = ExecutorPartialHandleSource::new(
                executor,
                (*table).clone(),
                RowDecodeContext::for_query(ctx),
                partial_handle_columns(&partial_schema, &table)?,
                partial_sort_key_columns(&partial_schema, &reader.by_items)?,
            )
            .with_batch_sizes(
                crate::access_path::calculate_lookup_batch_size_with_cap(
                    embedded_index_scan(partial).and_then(|scan| {
                        scan.base.base.stats_info().map(|stats| stats.row_count())
                    }),
                    ctx.executor_chunk_sizes().1,
                    false,
                    ctx.index_lookup_size(),
                ),
                ctx.index_lookup_size(),
            )
            .with_partition_index(partition_index)
            .with_memory_label(i64::from(plan.id()));
            if embedded_index_scan(partial).is_none() {
                source = source.with_reader_tracker(Arc::clone(&reader_tracker));
            }
            if embedded_index_scan(partial).is_some() {
                source = source.with_index_row_layout(
                    partial_schema
                        .columns
                        .iter()
                        .position(|column| column.id == tidb_model::column::EXTRA_PHYS_TBL_ID),
                );
            }
            partitions.push(Box::new(source));
        }
        let source = PartitionedHandleSource::new(partitions).with_order(
            reader
                .by_items
                .iter()
                .map(|item| MergeByItem {
                    collation: tidb_expr::collation_derive::collation_of_node(&item.expr),
                    desc: item.desc,
                })
                .collect(),
            ctx.executor_chunk_sizes().1,
        );
        #[cfg(test)]
        state.index_merge_partial_rows.insert(partial.id(), source.produced_rows());
        built_ids.push(None);
        built.push((false, Box::new(source)));
    }
    // The drain order: index partials first, as below.
    let partial_plan_ids: Vec<Option<i64>> = built
        .iter()
        .zip(&built_ids)
        .filter(|((index_first, _), _)| *index_first)
        .chain(
            built
                .iter()
                .zip(&built_ids)
                .filter(|((index_first, _), _)| !*index_first),
        )
        .map(|(_, id)| *id)
        .collect();
    let (index_first, rest): (Vec<_>, Vec<_>) =
        built.into_iter().partition(|(index_first, _)| *index_first);
    let mut partials: Vec<Box<dyn PartialHandleSource>> =
        index_first.into_iter().map(|(_, source)| source).collect();
    partials.extend(rest.into_iter().map(|(_, source)| source));

    let schema = plan_schema(plan)?;
    let mut table_scan = table_plan;
    while !matches!(table_scan, PhysicalPlan::TableScan(_)) {
        table_scan = only_child(table_scan)?;
    }
    let source_schema = plan_schema(table_scan)?;
    let output_columns = table_output_columns(&source_schema, &table)?;
    let scan_id = table_scan.id();
    let retained_table_plan = table_plan.clone();
    let task_catalog = catalog.clone();
    let task_context = ctx.clone();
    let table_task_builder = Box::new(move |source: Box<dyn Executor>| {
        let mut state = BuildState {
            table_task_source: Some((scan_id, source)),
            in_reader: true,
            ..Default::default()
        };
        build_with_state(
            &retained_table_plan,
            &task_catalog,
            &task_context,
            &mut state,
        )
        .map_err(|error| match error {
            DriverError::Exec(error) => error,
            other => crate::ExecError::Mysql(other.to_mysql_error()),
        })
    });
    let by_items = reader
        .by_items
        .iter()
        .map(|item| MergeByItem {
            collation: tidb_expr::collation_derive::collation_of_node(&item.expr),
            desc: item.desc,
        })
        .collect();
    let mut executor = IndexMergeReaderExecutor::new(
        meta(ctx, plan, schema.clone()),
        (*table).clone(),
        store,
        RowDecodeContext::for_query(ctx),
        partials,
        reader.is_intersection_type,
    )
    .with_table_statement(ReaderStorage::statement(
        separated,
        cop_reader_statement(table_scan, Some(table_plan), false, None, ctx, None)
            .with_cop_plan_chain(cop_plan_chain(table_plan), i64::from(table_plan.id())),
    ))
    .with_batch_size(ctx.index_lookup_size())
    .with_output_columns(output_columns)
    .with_by_items(by_items)
    .with_table_plan(
        source_schema,
        table_task_builder,
        matches!(table_plan, PhysicalPlan::TableScan(_)),
    )
    .with_table_workers(ctx.index_lookup_concurrency());
    // Go `IndexMergeReaderExecutor`'s `stats` (`Concurrency:
    // IndexLookupConcurrency`) and its partial and table readers' basic stats.
    if let Some(runtime_source) = ctx.executor_runtime_stats_source().filter(|_| !state.untraced) {
        executor = executor.with_runtime_stats(Some(
            crate::index_lookup_stats::IndexMergeStatsHandle {
                stats: Arc::new(crate::index_lookup_stats::IndexMergeRuntimeStat::new(
                    ctx.index_lookup_concurrency(),
                )),
                partial_basic: partial_plan_ids
                    .iter()
                    .map(|id| id.and_then(|id| runtime_source.basic_runtime_stats(id)))
                    .collect(),
                source: Arc::clone(runtime_source),
                table_root_plan_id: i64::from(table_plan.id()),
                plan_id: i64::from(plan.id()),
            },
        ));
    }
    if let Some(limit) = reader.pushed_limit {
        executor = executor.with_pushed_limit(ExecutorPushedDownLimit {
            offset: limit.offset,
            count: limit.count,
        });
    }
    Ok(Box::new(executor))
}

/// The range groups of a merge partial whose scan carries Go's
/// `GroupByColIdxs`, or `None` when it reads its ranges as one stream.
fn partial_range_groups(
    partial: &PhysicalPlan,
) -> Result<Option<Vec<tidb_ranger::types::Ranges>>, DriverError> {
    let grouped = embedded_index_scan(partial)
        .map(|scan| (&scan.group_by_col_idxs, &scan.ranges))
        .or_else(|| embedded_table_scan(partial).map(|scan| (&scan.group_by_col_idxs, &scan.ranges)));
    match grouped {
        Some((idxs, ranges)) if !idxs.is_empty() => {
            Ok(Some(group_ranges_by_cols(ranges, idxs)?))
        }
        _ => Ok(None),
    }
}

/// Points the grouped scan of a cloned plan at one range group.
fn narrow_scan_ranges(plan: &mut PhysicalPlan, ranges: &tidb_ranger::types::Ranges) {
    match plan {
        PhysicalPlan::TableScan(scan) if !scan.group_by_col_idxs.is_empty() => {
            scan.ranges = ranges.clone();
            scan.group_by_col_idxs.clear();
        }
        PhysicalPlan::IndexScan(scan) if !scan.group_by_col_idxs.is_empty() => {
            scan.ranges = ranges.clone();
            scan.group_by_col_idxs.clear();
        }
        _ => {
            for child in plan.base_mut().children_mut() {
                narrow_scan_ranges(child, ranges);
            }
        }
    }
}

fn index_merge_partition_indexes(
    plan: &PhysicalPlan,
    table: &crate::TableCommon,
) -> Result<Vec<usize>, DriverError> {
    let access = match plan {
        PhysicalPlan::TableScan(scan) => scan.dynamic_partition_access.as_ref(),
        PhysicalPlan::IndexScan(scan) => scan.dynamic_partition_access.as_ref(),
        _ => {
            return plan
                .children()
                .first()
                .ok_or_else(|| DriverError::unsupported("index-merge partial has no scan"))
                .and_then(|child| index_merge_partition_indexes(child, table));
        }
    };
    Ok(table
        .record_physical_ids()
        .iter()
        .enumerate()
        .filter_map(|(index, id)| {
            let selected = access.is_none_or(|access| {
                access.all_partitions
                    || table.meta().read().get_partition_info().is_some_and(|pi| {
                        pi.read().definitions.snapshot().iter().any(|definition| {
                            definition.id == *id
                                && access
                                    .partitions
                                    .iter()
                                    .any(|name| name.go_lower_eq(definition.name.original()))
                        })
                    })
            });
            selected.then_some(index)
        })
        .collect())
}

fn set_index_merge_partition(plan: &mut PhysicalPlan, physical_id: i64) {
    match plan {
        PhysicalPlan::TableScan(scan) => {
            scan.table_id = physical_id;
            scan.dynamic_partition_access = None;
        }
        PhysicalPlan::IndexScan(scan) => {
            scan.table_id = physical_id;
            scan.dynamic_partition_access = None;
        }
        _ => {
            for child in plan.base_mut().children_mut() {
                set_index_merge_partition(child, physical_id);
            }
        }
    }
}

/// Refresh statement-dependent routing before execution and plan publication.
///
/// Also states what the plan's memory-table scans need retrieved (Go builds a
/// `MemTableReaderExec` per `PhysicalMemTable`); a plan with scans whose rows
/// the statement does not hold yet is refused with
/// [`DriverError::MemTableRetrievalNeeded`] so the session retrieves them.
pub(crate) fn prepare_execution_plan(
    plan: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    // Go keeps ONE warning buffer in order of occurrence, and planning ends
    // here: what the constant fold raised joins the statement's list before
    // any executor -- on this thread or a worker's -- appends to it.
    ctx.drain_fold_warnings();
    record_tiflash_contained(plan, ctx);
    prepare_execution_plan_node(plan, catalog, ctx, true)?;
    if ctx.mem_table_retrieval().has_pending() {
        return Err(DriverError::MemTableRetrievalNeeded);
    }
    Ok(())
}

/// [`prepare_execution_plan`] for a plan that is only reported, never run
/// (plain `EXPLAIN`): no memory-table retrieval is requested, as Go's
/// `MemTableReaderExec` retrieves only when it is read.
pub(crate) fn prepare_plan_for_explain(
    plan: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    record_tiflash_contained(plan, ctx);
    prepare_execution_plan_node(plan, catalog, ctx, false)
}

/// Go `buildExplain`'s `b.build(v.TargetPlan)` for a plain EXPLAIN: the
/// executor tree is built (its checks and side effects happen) and dropped
/// unopened. A DML root's own executor is the write the session drives; its
/// source plan is what is built here.
pub(crate) fn build_for_explain(
    plan: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let target = match plan {
        PhysicalPlan::Dml(root) => match root.select_plan.as_deref() {
            Some(select) => select,
            None => return Ok(()),
        },
        _ => plan,
    };
    let mut state = BuildState {
        projection_workers_allowed: true,
        ..BuildState::default()
    };
    build_with_state(target, catalog, ctx, &mut state).map(drop)
}

/// Go `ExecuteStmt`'s `plannercore.IsTiFlashContained(stmt.Plan)`, recorded
/// for the session's telemetry count.
fn record_tiflash_contained(plan: &PhysicalPlan, ctx: &crate::StmtContext) {
    let (push_down, exchange_push_down) = tidb_planner::telemetry::is_tiflash_contained(plan);
    let mut ti = ctx.telemetry_info();
    ti.tiflash_push_down = push_down;
    ti.tiflash_exchange_push_down = exchange_push_down;
}

/// What the memory-table scan `plan` hands its retriever (Go's
/// `PhysicalMemTable` fields; for a cluster table's TiDB-store scan, its
/// columns), or `None` for any other node.
fn mem_table_read_of(
    plan: &PhysicalPlan,
    ctx: &crate::StmtContext,
) -> Option<crate::mem_table_retrieval::MemTableRead> {
    if let PhysicalPlan::TableReader(reader) = plan {
        return cluster_table_reader_read(plan, reader, ctx).ok().flatten();
    }
    let (db_name, table_name, columns, column_ids, extractor, query_time_range, is_cluster_scan) =
        match plan {
            PhysicalPlan::MemTable(scan) => (
                &scan.db_name,
                &scan.table_name,
                scan.columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                Vec::new(),
                tidb_planner::memtable_predicate_extractor::extractor_of(scan).cloned(),
                scan.query_time_range.clone(),
                false,
            ),
            // A CLUSTER_* table's scan is read by its reader's coprocessor
            // request (`cluster_table_reader_read`).
            PhysicalPlan::TableScan(scan)
                if scan.store_type == tidb_kv::store_type::StoreType::TiDb
                    && tidb_infoschema::infoschema_meta::cluster_table_source(&scan.table_name)
                        .is_none() =>
            {
                (
                    &scan.db_name,
                    &scan.table_name,
                    Vec::new(),
                    plan.schema()
                        .map(|schema| schema.columns.iter().map(|column| column.id).collect())
                        .unwrap_or_default(),
                    None,
                    tidb_planner_core_operator_logicalop::mem_table::QueryTimeRange::default(),
                    true,
                )
            }
            _ => return None,
        };
    Some(crate::mem_table_retrieval::MemTableRead {
        plan_id: plan.id(),
        db_name: db_name.clone(),
        table_name: table_name.clone(),
        columns,
        column_ids,
        extractor,
        query_time_range,
        is_cluster_scan,
        cop_request: None,
    })
}

/// The read of a CLUSTER_* table's reader on the `kv.TiDB` store: Go's
/// `buildTableReader` builds its DAG (`ConstructDAGReq` over the plans the
/// planner pushed) and key ranges, and every TiDB answers them. `None` for
/// any other reader.
fn cluster_table_reader_read(
    plan: &PhysicalPlan,
    reader: &tidb_planner_core_operator_physicalop::PhysicalTableReader,
    ctx: &crate::StmtContext,
) -> Result<Option<crate::mem_table_retrieval::MemTableRead>, DriverError> {
    if reader.store_type != tidb_kv::store_type::StoreType::TiDb {
        return Ok(None);
    }
    let Some(scan) = reader.table_scans().into_iter().next() else {
        return Ok(None);
    };
    if !tidb_infoschema::infoschema_meta::is_cluster_table_by_name(&scan.table_name) {
        return Ok(None);
    }
    let table_plan = reader
        .table_plan
        .as_deref()
        .ok_or_else(|| DriverError::unsupported("a TiDB-store reader has no pushed plan"))?;
    let request = crate::builder_utils::build_tidb_cop_request(
        ctx,
        plan,
        table_plan,
        scan,
        reader.is_common_handle(),
    )?;
    Ok(Some(crate::mem_table_retrieval::MemTableRead {
        plan_id: plan.id(),
        db_name: scan.db_name.clone(),
        table_name: scan.table_name.clone(),
        columns: Vec::new(),
        column_ids: Vec::new(),
        extractor: None,
        query_time_range: tidb_planner_core_operator_logicalop::mem_table::QueryTimeRange::default(),
        is_cluster_scan: true,
        cop_request: Some(std::sync::Arc::new(request)),
    }))
}

/// Go `TableReaderExecutor` for a CLUSTER_* table on the `kv.TiDB` store:
/// its rows are what the TiDBs answer its coprocessor request with, already
/// shaped by the plans pushed into it.
fn build_cluster_table_reader(
    plan: &PhysicalPlan,
    read: crate::mem_table_retrieval::MemTableRead,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let meta = meta(ctx, plan, plan_schema(plan)?);
    if let Some(retriever) = ctx.mem_table_retrieval().retriever_for(&read) {
        if let (Some(reader), Some(request)) =
            (retriever.tidb_store_reader(&read, catalog, ctx), read.cop_request.clone())
        {
            // Go `buildTableReader`'s `getSnapshotTS`.
            let start_ts = ctx.txn_store().map_or(0, |store| store.start_ts());
            return Ok(Box::new(crate::table_reader::TableReaderExecutor::new(
                meta,
                ctx.clone(),
                request,
                reader,
                start_ts,
            )));
        }
        let (snapshot, ctx) = (catalog.clone(), ctx.clone());
        return Ok(Box::new(MemTableSourceExec::lazy(
            meta,
            Box::new(move || retriever.retrieve(&read, &snapshot, &ctx).map_err(retrieval_error)),
        )));
    }
    let rows = match ctx.mem_table_retrieval().rows(&read) {
        Some(rows) => rows.as_ref().clone(),
        // A plain EXPLAIN builds the reader and never reads it.
        None if ctx.in_explain_stmt() && !ctx.in_explain_analyze_stmt() => Vec::new(),
        None => return Err(DriverError::MemTableRetrievalNeeded),
    };
    Ok(Box::new(MemTableSourceExec::new(meta, rows)))
}

/// Records the retrieval a virtual-table scan at `plan` needs: the rows of an
/// `information_schema` / `performance_schema` table are not stored, so the
/// session produces them per scan. A table the catalog stores as materialized
/// rows (a fixture, `SHOW ... WHERE`'s selection) already holds its own.
fn request_memory_table_rows(plan: &PhysicalPlan, catalog: &SessionExtendedInfoSchema, ctx: &crate::StmtContext) {
    let Some(read) = mem_table_read_of(plan, ctx) else {
        return;
    };
    // Go's metric tables query metric storage, which the node does not have;
    // `build_mem_table` answers them.
    if read.db_name.go_lower_eq(super::metricsschema::METRICS_SCHEMA) {
        return;
    }
    if !matches!(
        catalog.table_in(&read.db_name, &read.table_name),
        Some(super::TableEntry::Kv(_))
    ) || ctx.mem_table_retrieval().rows(&read).is_some()
        || ctx.mem_table_retrieval().retriever_for(&read).is_some()
    {
        return;
    }
    ctx.mem_table_retrieval().request(read);
}

fn prepare_execution_plan_node(
    plan: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    retrieve: bool,
) -> Result<(), DriverError> {
    if retrieve {
        request_memory_table_rows(plan, catalog, ctx);
    }
    if let PhysicalPlan::PointGet(point) = plan {
        let physical_id = point_partition_id(point, catalog, ctx)?;
        if let Some(partition) = &mut point.partition {
            partition.physical_table_id = physical_id;
        }
    }
    if let PhysicalPlan::Apply(apply) = plan {
        let inner_index = apply.hash_join.inner_child_idx;
        if inner_index > 1 {
            return Err(DriverError::unsupported(
                "a physical Apply has an invalid inner child",
            ));
        }
        let children = apply.hash_join.base.children_mut();
        if children.len() != 2 {
            return Err(DriverError::unsupported(
                "a physical Apply has the wrong child count",
            ));
        }
        let (left, right) = children.split_at_mut(1);
        let (inner, outer) = if inner_index == 0 {
            (&mut left[0], &right[0])
        } else {
            (&mut right[0], &left[0])
        };
        let outer_schema = plan_schema(outer)?;
        apply.outer_schema =
            tidb_planner_core_operator_physicalop::extract_cor_columns_by_schema_4_physical_plan(
                inner,
                &outer_schema,
            );
    }

    match plan {
        PhysicalPlan::ShuffleReceiver(receiver) => {
            prepare_execution_plan_node(&mut receiver.data_source, catalog, ctx, retrieve)?;
        }
        PhysicalPlan::TableReader(reader) => {
            if let Some(plan) = reader.table_plan.as_deref_mut() {
                prepare_execution_plan_node(plan, catalog, ctx, retrieve)?;
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(plan) = reader.index_plan.as_deref_mut() {
                prepare_execution_plan_node(plan, catalog, ctx, retrieve)?;
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(plan) = reader.index_plan.as_deref_mut() {
                prepare_execution_plan_node(plan, catalog, ctx, retrieve)?;
            }
            if let Some(plan) = reader.table_plan.as_deref_mut() {
                prepare_execution_plan_node(plan, catalog, ctx, retrieve)?;
            }
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            for partial in &mut reader.partial_plans_raw {
                prepare_execution_plan_node(partial, catalog, ctx, retrieve)?;
            }
            if let Some(plan) = reader.table_plan.as_deref_mut() {
                prepare_execution_plan_node(plan, catalog, ctx, retrieve)?;
            }
        }
        PhysicalPlan::Dml(root) => {
            if let Some(plan) = root.select_plan.as_deref_mut() {
                prepare_execution_plan_node(plan, catalog, ctx, retrieve)?;
            }
        }
        PhysicalPlan::CTE(cte) => {
            prepare_execution_plan_node(&mut cte.seed_plan, catalog, ctx, retrieve)?;
            if let Some(plan) = cte.recursive_plan.as_deref_mut() {
                prepare_execution_plan_node(plan, catalog, ctx, retrieve)?;
            }
        }
        _ => {}
    }
    for child in plan.base_mut().children_mut() {
        prepare_execution_plan_node(child, catalog, ctx, retrieve)?;
    }
    Ok(())
}

fn cte_reader(
    plan: &PhysicalPlan,
    cte: &tidb_planner_core_operator_physicalop::PhysicalCTE,
    producer: SharedCteProducer,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    Ok(Box::new(CteExec::new(
        meta(ctx, plan, plan_schema(plan)?),
        producer,
        cte.has_limit,
        cte.limit_beg,
        cte.limit_end,
    )))
}

fn build_cte(
    plan: &PhysicalPlan,
    cte: &tidb_planner_core_operator_physicalop::PhysicalCTE,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    // Go `buildCTE`'s telemetry, under `withStmtCtxLock`.
    {
        let mut ti = ctx.telemetry_info();
        ti.use_non_recursive = true;
        if cte.recursive_plan.is_some() {
            ti.use_recursive = true;
        }
    }
    if let Some(producer) = state
        .cte_slots
        .get(&cte.id_for_storage)
        .and_then(|slot| slot.producer.as_ref())
        .cloned()
    {
        return cte_reader(plan, cte, producer, ctx);
    }
    if state.cte_slots.contains_key(&cte.id_for_storage) {
        return Err(DriverError::unsupported(format!(
            "producer should already be set up by CTEExec(id: {})",
            cte.id_for_storage
        )));
    }

    let mut cor_cols = tidb_planner_core_operator_physicalop::extract_outer_apply_correlated_cols(&cte.seed_plan);
    let seed = build_with_state(&cte.seed_plan, catalog, ctx, state)?;
    let output_types = seed.ret_field_types().to_vec();
    // Go `cteProducer.openProducer`: `memory.NewTracker(cteExec.ID(), -1)`
    // and its disk twin, which every storage of the CTE attaches to.
    let storage_parents = (
        ctx.statement_memory().operator_tracker(i64::from(plan.id())),
        ctx.statement_memory().operator_disk_tracker(i64::from(plan.id())),
    );
    let mut result_storage = tidb_cteutil::CteStorage::new(
        output_types.clone(),
        ctx.executor_chunk_sizes().1,
        ctx.statement_memory(),
    )
    .with_parents(storage_parents.clone());
    result_storage.open_and_ref()?;
    let result = Arc::new(Mutex::new(result_storage));
    let mut iter_in_storage = tidb_cteutil::CteStorage::new(
        output_types.clone(),
        ctx.executor_chunk_sizes().1,
        ctx.statement_memory(),
    )
    .with_parents(storage_parents.clone());
    if let Err(error) = iter_in_storage.open_and_ref() {
        result.lock().unwrap().deref_and_close()?;
        return Err(error.into());
    }
    let iter_in = Arc::new(Mutex::new(iter_in_storage));
    state.cte_slots.insert(
        cte.id_for_storage,
        CteBuildSlot {
            result: Arc::clone(&result),
            iter_in: Arc::clone(&iter_in),
            producer: None,
        },
    );

    let recursive = match cte.recursive_plan.as_deref() {
        Some(recursive) => match build_with_state(recursive, catalog, ctx, state) {
            Ok(executor) => Some(executor),
            Err(error) => {
                state.cte_slots.remove(&cte.id_for_storage);
                return Err(error);
            }
        },
        None => None,
    };
    if let Some(recursive) = cte.recursive_plan.as_deref() {
        cor_cols.extend(tidb_planner_core_operator_physicalop::extract_outer_apply_correlated_cols(recursive));
    }
    let slot = state
        .cte_slots
        .get(&cte.id_for_storage)
        .expect("CTE slot inserted before recursive executor build");
    let result = Arc::clone(&slot.result);
    let iter_in = Arc::clone(&slot.iter_in);
    let producer = Arc::new(Mutex::new(CteProducer::new(
        seed,
        recursive,
        result,
        iter_in,
        cte.is_distinct,
        cte.has_limit,
        cte.limit_end,
        ctx.clone(),
        output_types,
        ctx.executor_chunk_sizes().1,
        cor_cols,
    )
    .with_storage_parents(storage_parents)));
    state
        .cte_slots
        .get_mut(&cte.id_for_storage)
        .expect("CTE slot inserted before recursive executor build")
        .producer = Some(Arc::clone(&producer));
    cte_reader(plan, cte, producer, ctx)
}

fn build_cte_table(
    plan: &PhysicalPlan,
    table: &tidb_planner_core_operator_physicalop::PhysicalCTETable,
    state: &BuildState,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let slot = state.cte_slots.get(&table.id_for_storage).ok_or_else(|| {
        DriverError::unsupported(format!(
            "iterInTbl should already be set up by CTEExec(id: {})",
            table.id_for_storage
        ))
    })?;
    Ok(Box::new(CteTableReaderExec::new(
        meta(ctx, plan, plan_schema(plan)?),
        Arc::clone(&slot.iter_in),
    )))
}

/// Recursively instantiates one retained physical operator tree.
fn build_shuffle(
    plan: &PhysicalPlan,
    shuffle: &tidb_planner_core_operator_physicalop::PhysicalShuffle,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    use crate::shuffle::{
        FinishFlag, InboxHandle, PartitionHashSplitter, PartitionRangeSplitter, PartitionSplitter,
        ShuffleExec, ShuffleReceiver, ShuffleWorker,
    };
    use tidb_planner_core_operator_physicalop::shuffle::PartitionSplitterType;
    let mut sources = Vec::with_capacity(shuffle.data_sources.len());
    let mut splitters: Vec<Box<dyn PartitionSplitter<crate::StmtContext>>> = Vec::new();
    let mut source_plans = Vec::new();
    let mut receiver_plans = Vec::new();
    for (i, id) in shuffle.data_sources.iter().enumerate() {
        let source = shuffle
            .referenced_plan(*id)
            .map_err(|error| DriverError::unsupported(format!("{error:?}")))?;
        let schema = plan_schema(source)?;
        let items = shuffle
            .by_item_arrays
            .get(i)
            .ok_or_else(|| DriverError::unsupported("shuffle partition expressions are absent"))?
            .iter()
            .cloned()
            .map(|item| resolve_expression(item, &schema))
            .collect::<Result<Vec<_>, _>>()?;
        splitters.push(match shuffle.splitter_type {
            PartitionSplitterType::Hash => {
                Box::new(PartitionHashSplitter::new(shuffle.concurrency, items))
            }
            PartitionSplitterType::Range => {
                Box::new(PartitionRangeSplitter::new(shuffle.concurrency, items))
            }
        });
        sources.push(build_with_state(source, catalog, ctx, state)?);
        source_plans.push(source);
        let tail = shuffle
            .referenced_plan(shuffle.tails[i])
            .map_err(|error| DriverError::unsupported(format!("{error:?}")))?;
        let receiver = only_child(tail)?;
        if !matches!(receiver, PhysicalPlan::ShuffleReceiver(_)) {
            return Err(DriverError::unsupported(
                "shuffle receiver boundary was not prepared",
            ));
        }
        receiver_plans.push(receiver);
    }
    let finish = FinishFlag::new();
    let mut workers = Vec::with_capacity(shuffle.concurrency);
    for _ in 0..shuffle.concurrency {
        let inboxes: Vec<_> = source_plans.iter().map(|_| InboxHandle::new()).collect();
        let saved_receivers = std::mem::take(&mut state.shuffle_receivers);
        let result = (|| {
            for (receiver_plan, inbox) in receiver_plans.iter().zip(&inboxes) {
                let receiver = ShuffleReceiver::new(
                    meta(ctx, receiver_plan, plan_schema(receiver_plan)?),
                    inbox.clone(),
                    finish.clone(),
                );
                state
                    .shuffle_receivers
                    .insert(receiver_plan.id(), Box::new(receiver));
            }
            build_with_state(only_child(plan)?, catalog, ctx, state)
        })();
        state.shuffle_receivers = saved_receivers;
        workers.push(ShuffleWorker::new(result?, inboxes, finish.clone()));
    }
    Ok(Box::new(ShuffleExec::new(
        meta(ctx, plan, plan_schema(plan)?),
        ctx.clone(),
        workers,
        splitters,
        sources,
        finish,
    )))
}

/// Builds one reader (or index scan lookup) as the plan describes it, without
/// the per-node metering [`build_with_state`] adds.
fn build_reader_node(
    plan: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    match plan {
        PhysicalPlan::TableReader(reader) => {
            // Go `buildTableReader`: `if useMPPExecution(b.ctx, v) { return
            // b.buildMPPGather(v) }`.
            if crate::mpp_gather::use_mpp_execution(ctx, reader) {
                // Go `buildUnionScanFromReader`'s `MPPGather` arm: a union
                // scan reads the buffer through its own mem table reader over
                // the gather's single table scan, never through the gather.
                if state.reader_storage.is_mem_buffer() {
                    let scan = reader
                        .table_plan
                        .as_deref()
                        .and_then(embedded_table_scan)
                        .ok_or_else(|| DriverError::unsupported("a union scan's MPP reader has no table scan"))?;
                    let scan = PhysicalPlan::TableScan(scan.clone());
                    return build_reader(plan.id(), Some(&scan), catalog, ctx, state);
                }
                let gather = crate::mpp_gather::MppGather::build(meta(ctx, plan, plan_schema(plan)?), ctx, catalog, reader)
                    .map_err(DriverError::from)?;
                ctx.mark_physical_table_reader();
                return Ok(Box::new(gather));
            }
            let executor = build_reader(plan.id(), reader.table_plan.as_deref(), catalog, ctx, state)?;
            ctx.mark_physical_table_reader();
            Ok(executor)
        }
        PhysicalPlan::IndexReader(reader) => {
            // Go's `IndexPlan` carries the pushed coprocessor Limit above the
            // IndexScan; the generic walk below builds it as a `LimitExec`,
            // and the scan itself needs the same cap so the local cursor stops
            // early. The reader's subtree is built synchronously, so the slot
            // cannot leak into a sibling.
            state.index_scan_limit = reader
                .index_plan
                .as_deref()
                .and_then(embedded_index_limit)
                .map(|(offset, count)| offset.saturating_add(count));
            // Go `dagReq.OutputOffsets`: the request returns the reader's
            // `OutputColumns` of what its pushed plan outputs. A pushed Limit
            // passes its child's columns through, so a Limit chain over the
            // scan narrows like the bare scan.
            let pushed_projection = reader
                .index_plan
                .as_deref()
                .and_then(column_projection_over_index_scan);
            state.index_cop_projection = pushed_projection.is_some();
            state.index_output_columns = pushed_projection.or_else(|| {
                reader
                    .index_plan
                    .as_deref()
                    .is_some_and(limit_chain_over_index_scan)
                    .then(|| reader.output_columns.clone())
            });
            state.index_post_filter_columns = (state.index_output_columns.is_none()
                && reader.index_plan.as_deref().is_some_and(limit_over_selection_chain))
                .then(|| reader.output_columns.clone());
            let built = build_reader(plan.id(), reader.index_plan.as_deref(), catalog, ctx, state);
            state.index_scan_limit = None;
            state.index_output_columns = None;
            state.index_post_filter_columns = None;
            state.index_cop_projection = false;
            let mut child = built?;
            // Go `dagReq.OutputOffsets` over a pushed Selection: the request
            // returns the reader's `OutputColumns` of the rows it admitted.
            let output_offsets = reader
                .output_columns
                .iter()
                .map(|output| {
                    child
                        .schema()
                        .columns
                        .iter()
                        .position(|column| column.unique_id == output.unique_id)
                })
                .collect::<Option<Vec<_>>>();
            if let Some(offsets) = output_offsets {
                let output = Schema::new(reader.output_columns.clone());
                if child
                    .table_access()
                    .is_some_and(|access| access.accept_post_filter_projection(&offsets, &output))
                {
                    return Ok(child);
                }
            }
            // The physical index row is ordered by index keys and handles.
            // Go's reader OutputColumns restores the datasource's output.
            if child.schema().columns.len() == reader.output_columns.len()
                && child
                    .schema()
                    .columns
                    .iter()
                    .zip(&reader.output_columns)
                    .all(|(input, output)| input.unique_id == output.unique_id)
            {
                Ok(child)
            } else {
                let expressions = reader
                    .output_columns
                    .iter()
                    .cloned()
                    .map(Expression::Column)
                    .collect::<Vec<_>>();
                let expressions = resolve_expressions(&expressions, child.schema())?;
                Ok(Box::new(ProjectionExec::new(
                    meta(ctx, plan, plan_schema(plan)?),
                    expressions,
                    child,
                    ctx.clone(),
                )) as Box<dyn Executor>)
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            let executor = build_index_reader(
                plan,
                reader.index_plan.as_deref().ok_or_else(|| {
                    DriverError::unsupported("a physical index lookup has no index plan")
                })?,
                reader.table_plan.as_deref(),
                false,
                reader.keep_order,
                reader.pushed_limit,
                reader.paging.then_some(reader.expect_cnt),
                reader.index_lookup_push_down,
                None,
                None,
                false,
                None,
                state.range_group.as_ref(),
                catalog,
                ctx,
                &state.reader_storage,
                None,
                None,
            )?;
            ctx.mark_physical_table_reader();
            Ok(executor)
        }
        _ => Err(DriverError::unsupported(
            "a reader node was expected",
        )),
    }
}

/// Go `GroupRangesByCols`: ranges that agree on the group-by columns form one
/// group. Go collects a map's values, so its group order is unspecified; the
/// first-appearance order used here is one valid order.
fn group_ranges_by_cols(
    ranges: &tidb_ranger::types::Ranges,
    group_by_col_idxs: &[usize],
) -> Result<Vec<tidb_ranger::types::Ranges>, DriverError> {
    let mut positions: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut groups: Vec<tidb_ranger::types::Ranges> = Vec::new();
    for range in ranges {
        let datums = group_by_col_idxs
            .iter()
            .map(|index| {
                range.low_val.get(*index).cloned().ok_or_else(|| {
                    DriverError::unsupported("a grouped range is narrower than its group columns")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key = tidb_codec::encode_value(&datums)
            .map_err(|error| DriverError::unsupported(format!("{error:?}")))?;
        let position = *positions.entry(key).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[position].push(range.clone());
    }
    Ok(groups)
}

/// The scan of a reader that carries Go's `GroupByColIdxs`, with the property
/// the planner built it under.
fn range_grouped_scan(
    plan: &PhysicalPlan,
) -> Option<(&[usize], &tidb_ranger::types::Ranges, Option<&tidb_planner_property::PhysicalProperty>)> {
    let leaf = match plan {
        PhysicalPlan::TableReader(reader) => reader.table_plan.as_deref().and_then(|plan| {
            embedded_table_scan(plan).map(|scan| {
                (&scan.group_by_col_idxs[..], &scan.ranges, scan.prop.as_deref())
            })
        }),
        PhysicalPlan::IndexReader(reader) => reader.index_plan.as_deref().and_then(|plan| {
            embedded_index_scan(plan).map(|scan| {
                (&scan.group_by_col_idxs[..], &scan.ranges, scan.prop.as_deref())
            })
        }),
        PhysicalPlan::IndexLookUpReader(reader) => reader.index_plan.as_deref().and_then(|plan| {
            embedded_index_scan(plan).map(|scan| {
                (&scan.group_by_col_idxs[..], &scan.ranges, scan.prop.as_deref())
            })
        }),
        _ => None,
    }?;
    (!leaf.0.is_empty()).then_some(leaf)
}

/// Go `TableReaderExecutor.buildRespForGroupedRanges` and the index readers'
/// equivalents: a reader whose ranges pin the group-by columns reads each
/// range group as its own request, and the group results merge-sort by the
/// scan property's items (`NewSortedSelectResults`).
fn build_range_grouped_reader(
    plan: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let Some((group_by_col_idxs, ranges, prop)) = range_grouped_scan(plan) else {
        return build_reader_node(plan, catalog, ctx, state);
    };
    let groups = group_ranges_by_cols(ranges, group_by_col_idxs)?;
    if groups.len() <= 1 {
        // One group is read in range order, which is already the property's.
        return build_reader_node(plan, catalog, ctx, state);
    }
    let sort_items = prop
        .map(|prop| prop.sort_items.clone())
        .filter(|items| !items.is_empty())
        .ok_or_else(|| {
            DriverError::unsupported("a grouped-range reader has no order to merge by")
        })?;
    let previous = state.range_group.take();
    let mut children = Vec::with_capacity(groups.len());
    let mut result = Ok(());
    for group in groups {
        state.range_group = Some(group);
        match build_reader_node(plan, catalog, ctx, state) {
            Ok(child) => children.push(child),
            Err(error) => {
                result = Err(error);
                break;
            }
        }
    }
    state.range_group = previous;
    result?;
    let schema = unary_schema(plan, children[0].as_ref());
    let by_items = sort_items
        .iter()
        .map(|item| {
            // The property's column carries identity only; the merge reads
            // the reader's own typed output column.
            let schema = children[0].schema();
            let position = usize::try_from(schema.column_index(&item.col)).map_err(|_| {
                DriverError::unsupported("a grouped-range sort column is absent from its reader")
            })?;
            let mut column = schema.columns[position].clone();
            column.index = i64::try_from(position).unwrap_or(i64::MAX);
            Ok::<SortByItem, DriverError>(SortByItem {
                expr: Expression::Column(column),
                desc: item.desc,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Box::new(
        crate::range_group_merge::SortedSelectResults::new(
            meta(ctx, plan, schema),
            by_items,
            children,
            ctx.clone(),
        )
        .map_err(|error| DriverError::unsupported(format!("{error:?}")))?,
    ))
}

/// A cluster table's scan (`PhysicalTableScan` on the TiDB store): Go sends a
/// coprocessor request to every TiDB, each of which answers from its own
/// memory table. This node materializes the same rows the session generated
/// for the statement and the operators the planner pushed into the reader
/// run above it as ordinary executors, which is what the cop handler does.
fn build_cluster_table_scan(
    plan: &PhysicalPlan,
    scan: &PhysicalTableScan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let table = memory_table_rows(plan, &scan.db_name, &scan.table_name, catalog, ctx)?;
    let schema = plan_schema(plan)?;
    // Memory-table column ids are offset + 1 (`planner_schema_view`).
    let offsets = schema
        .columns
        .iter()
        .map(|column| {
            usize::try_from(column.id - 1)
                .ok()
                .filter(|offset| *offset < table.column_names.len())
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a cluster table scan column is absent from its table",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // `pushLimitDownToTiDBCop` keeps the scan in handle order for its pushed
    // Limit: Go's slow-query retriever then reads its files forwards, or
    // backwards for a descending scan (`SlowQueryExtractor.Desc`).
    let handle_position = scan
        .keep_order
        .then(|| {
            scan.handle_col_unique_id.and_then(|unique_id| {
                schema
                    .columns
                    .iter()
                    .position(|column| column.unique_id == unique_id)
            })
        })
        .flatten();
    let desc = scan.desc;
    let shape = move |rows: &[Vec<tidb_datatype::Datum>]| {
        let mut rows = rows
            .iter()
            .map(|row| {
                offsets
                    .iter()
                    .map(|offset| {
                        row.get(*offset).cloned().ok_or_else(|| {
                            DriverError::unsupported(
                                "a memory-table row is shorter than its declared schema",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, DriverError>>()?;
        if let Some(position) = handle_position {
            rows.sort_by(|left, right| {
                let order = left[position]
                    .compare(&right[position], tidb_charset::Collation::Binary)
                    .unwrap_or(std::cmp::Ordering::Equal);
                if desc {
                    order.reverse()
                } else {
                    order
                }
            });
        }
        Ok::<_, DriverError>(rows)
    };
    let meta = meta(ctx, plan, schema);
    Ok(match table.rows() {
        MemoryTableRowsOrRetriever::Rows(rows) => {
            Box::new(MemTableSourceExec::new(meta, shape(rows)?))
        }
        MemoryTableRowsOrRetriever::Owned(read, retriever) => {
            let (snapshot, ctx) = (catalog.clone(), ctx.clone());
            Box::new(MemTableSourceExec::lazy(
                meta,
                Box::new(move || {
                    let rows = retriever
                        .retrieve(&read, &snapshot, &ctx)
                        .map_err(retrieval_error)?;
                    shape(&rows).map_err(retrieval_error)
                }),
            ))
        }
    })
}

/// Go `PointGetExecutor.Close` / `BatchPointGetExec.Close`'s
/// `RuntimeStatsColl.RegisterStats(e.ID(), e.stats)` of the reader's
/// snapshot stats.
fn register_snapshot_stats_on_close(
    executor: Box<dyn Executor>,
    ctx: &crate::StmtContext,
    plan: &PhysicalPlan,
    snapshot_stats: Option<&Arc<tikv_client::SnapshotRuntimeStats>>,
) -> Box<dyn Executor> {
    let Some(snapshot_stats) = snapshot_stats.cloned() else {
        return executor;
    };
    crate::runtime_stats_groups::RegisterStatsOnClose::wrap(executor, ctx, i64::from(plan.id()), move || {
        Box::new(crate::runtime_stats_groups::RuntimeStatsWithSnapshot::new(&snapshot_stats))
    })
}

/// Go `executorBuilder.getSnapshotTS` / `getSnapshot`: building a reader
/// takes the statement's read timestamp, which activates its transaction
/// (`GetStmtReadTS` -> `ActivateTxn`) -- under a plain EXPLAIN too, whose
/// executors are built and never read. A timestamp that fails to arrive is
/// `0` here; the reader's first read reports the error.
fn get_snapshot_ts(ctx: &crate::StmtContext) {
    if let Some(store) = ctx.txn_store() {
        store.start_ts();
    }
}

fn build_with_state(
    plan: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    if state.ruv2_metrics.is_none() {
        state.ruv2_metrics = ctx.ruv2_metrics().cloned();
    }
    if state.top_profiling.is_none() {
        state.top_profiling = ctx.top_profiling_registration();
    }
    if matches!(plan, PhysicalPlan::TableScan(_))
        && state
            .table_task_source
            .as_ref()
            .is_some_and(|(id, _)| *id == plan.id())
    {
        return Ok(state.table_task_source.take().unwrap().1);
    }
    if state.executor_runtime_stats_source.is_none() && !state.untraced {
        state.executor_runtime_stats_source = ctx.executor_runtime_stats_source().cloned();
    }
    if let Some(receiver) = state.shuffle_receivers.remove(&plan.id()) {
        return Ok(state.meter(plan, receiver, ctx.statement_memory()));
    }
    let executor: Box<dyn Executor> = match plan {
        PhysicalPlan::Shuffle(shuffle) => {
            // Go `ShuffleExec.Open` registers its concurrency.
            if !state.untraced {
                register_concurrency_info(
                    ctx,
                    plan,
                    "ShuffleConcurrency",
                    usize::try_from(shuffle.concurrency).unwrap_or(0),
                );
            }
            build_shuffle(plan, shuffle, catalog, ctx, state)
        }
        PhysicalPlan::ShuffleReceiver(_) => Err(DriverError::unsupported(
            "shuffle receiver has no owning worker",
        )),
        PhysicalPlan::MemTable(scan) => build_mem_table(plan, scan, catalog, ctx),
        PhysicalPlan::TableScan(scan)
            if scan.store_type == tidb_kv::store_type::StoreType::TiDb =>
        {
            build_cluster_table_scan(plan, scan, catalog, ctx)
        }
        // A CLUSTER_* table's reader sends its pushed plan to every TiDB.
        // A cluster table's reader (Go `IsClusterTable`: the CLUSTER_*
        // tables and `TIFLASH_REPLICA`) sends its pushed plan to the TiDBs.
        PhysicalPlan::TableReader(reader)
            if reader.store_type == tidb_kv::store_type::StoreType::TiDb =>
        {
            let read = cluster_table_reader_read(plan, reader, ctx)?
                .ok_or_else(|| DriverError::unsupported("a cluster table reader has no read"))?;
            build_cluster_table_reader(plan, read, catalog, ctx)
        }
        PhysicalPlan::TableScan(scan) => build_table_scan(
            plan,
            scan,
            catalog,
            ctx,
            state.reader_net_bytes,
            state.range_group.as_ref(),
            &state.reader_storage,
            state.cop_chain.as_ref(),
            state.reader_mem_tracker.clone(),
        ),
        PhysicalPlan::TableSample(sample) => {
            get_snapshot_ts(ctx);
            build_table_sample(plan, sample, catalog, ctx)
        }
        PhysicalPlan::IndexScan(scan) => build_index_reader(
            plan,
            plan,
            None,
            true,
            scan.keep_order,
            None,
            None,
            false,
            state.index_scan_limit.take(),
            state.index_output_columns.take(),
            std::mem::take(&mut state.index_cop_projection),
            state.reader_net_bytes,
            state.range_group.as_ref(),
            catalog,
            ctx,
            &state.reader_storage,
            state.cop_chain.as_ref(),
            state.reader_mem_tracker.clone(),
        ),
        PhysicalPlan::TableReader(_) | PhysicalPlan::IndexReader(_) => {
            get_snapshot_ts(ctx);
            build_range_grouped_reader(plan, catalog, ctx, state)
        }
        PhysicalPlan::IndexLookUpReader(_) => {
            // Go `buildIndexLookUpReader`: `b.Ti.UseTableLookUp.Store(true)`.
            ctx.telemetry_info().use_table_lookup = true;
            get_snapshot_ts(ctx);
            build_range_grouped_reader(plan, catalog, ctx, state)
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            // Go `buildIndexMergeReader`.
            {
                let mut ti = ctx.telemetry_info();
                ti.use_index_merge = true;
                ti.use_table_lookup = true;
            }
            get_snapshot_ts(ctx);
            let executor = build_index_merge_reader(plan, reader, catalog, ctx, state)?;
            ctx.mark_physical_table_reader();
            Ok(executor)
        }
        PhysicalPlan::PointGet(point) => {
            // Go `buildPointGet`'s `b.getSnapshot()`.
            get_snapshot_ts(ctx);
            let snapshot_stats = state.point_snapshot_runtime_stats();
            with_select_lock(
                plan,
                point.select_lock,
                point.table_id,
                catalog,
                ctx,
                |plan| {
                    let executor = build_point_get(plan, point, catalog, ctx, snapshot_stats.clone())?;
                    Ok(register_snapshot_stats_on_close(executor, ctx, plan, snapshot_stats.as_ref()))
                },
            )
        }
        PhysicalPlan::BatchPointGet(batch) => {
            // Go `buildBatchPointGet`'s `b.getSnapshot()`.
            get_snapshot_ts(ctx);
            let snapshot_stats = state.point_snapshot_runtime_stats();
            with_select_lock(
                plan,
                batch.select_lock,
                batch.table_id,
                catalog,
                ctx,
                |plan| {
                    let executor = build_batch_point_get(plan, batch, catalog, ctx, snapshot_stats.clone())?;
                    Ok(register_snapshot_stats_on_close(executor, ctx, plan, snapshot_stats.as_ref()))
                },
            )
        }
        PhysicalPlan::Lock(lock) => {
            state.has_lock = true;
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            select_lock_executor(lock, child, catalog, ctx)
        }
        PhysicalPlan::Projection(projection) => {
            let mut child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            // Go keeps a direct-column cop projection inside the reader and
            // sends it as `DAGRequest.output_offsets`; a computed projection
            // stays as a local executor. Folding it here is what makes the
            // remote row narrow at the region instead of at the root.
            let direct_offsets = projection
                .exprs
                .iter()
                .map(|expression| {
                    let column = expression.as_column()?;
                    usize::try_from(child.schema().column_index(column)).ok()
                })
                .collect::<Option<Vec<usize>>>();
            if let Some(offsets) = direct_offsets {
                let output = plan_schema(plan)?;
                let accepted = child.table_access().is_some_and(|access| {
                    let accepted = access.accept_post_filter_projection(&offsets, &output);
                    // Go `PhysicalProjection.ToPB`: a Projection pushed into
                    // the reader is the request's last executor.
                    if accepted && state.in_reader {
                        access.lower_output_as_projection();
                    }
                    accepted
                });
                if accepted {
                    return Ok(child);
                }
            }
            let expressions = resolve_expressions(&projection.exprs, child.schema())?;
            // Go `PhysicalProjection.ToPB`: a Projection the planner pushed
            // into the reader computes in the coprocessor too.
            if state.in_reader {
                let output = plan_schema(plan)?;
                if child
                    .table_access()
                    .is_some_and(|access| access.accept_computed_projection(&expressions, &output, ctx))
                {
                    return Ok(child);
                }
            }
            let meta = meta(ctx, plan, plan_schema(plan)?);
            // Go `newProjectionExec`: `tidb_projection_concurrency` workers,
            // none when the estimated row count is under one chunk ("to
            // reduce the goroutine overhead"), inside a writing statement or
            // above a lock.
            let workers = if state.projection_workers_allowed
                && !state.in_reader
                && !state.has_lock
                && plan
                    .stats_count()
                    .is_some_and(|rows| rows >= meta.max_chunk_size() as f64)
            {
                ctx.projection_concurrency()
            } else {
                0
            };
            // Go `newProjectionExec`: `NewEvaluatorSuite(v.Exprs,
            // v.AvoidColumnEvaluator)`.
            let mut projection = ProjectionExec::new_projection_exec(
                meta,
                expressions,
                child,
                ctx.clone(),
                projection.avoid_column_evaluator,
            )
            .with_workers(workers)
            .in_coprocessor(state.in_reader);
            if !state.in_reader && !state.untraced {
                projection = projection.with_mem_tracker(
                    ctx.statement_memory().operator_tracker(i64::from(plan.id())),
                );
            }
            let executor: Box<dyn Executor> = Box::new(projection);
            if state.in_reader || state.untraced {
                return Ok(executor);
            }
            // Go `ProjectionExec.Close` registers its concurrency
            // (`Concurrency:OFF` for the serial path).
            Ok(crate::runtime_stats_groups::RegisterStatsOnClose::wrap(
                executor,
                ctx,
                i64::from(plan.id()),
                move || {
                    let stats = tidb_execdetails::RuntimeStatsWithConcurrencyInfo::default();
                    stats.set_concurrency_info(vec![tidb_execdetails::ConcurrencyInfo::new(
                        "Concurrency",
                        i64::try_from(workers).unwrap_or(i64::MAX),
                    )]);
                    Box::new(stats)
                },
            ))
        }
        PhysicalPlan::Selection(selection) => {
            let mut child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let filters = resolve_expressions(&selection.conditions, child.schema())?;
            // Go decides push-down in the planner: a `PhysicalSelection` still
            // standing above a reader is exactly what `PredicatePushDown` LEFT
            // there. The driver must not re-decide, because fusing a refused
            // condition skips Go's `SelectionExec` -- including its statement
            // memory accounting -- and hands the cop task a predicate the
            // planner never admitted (`oct(a) > '0'`, `RAND()` without a
            // seed). Only a condition the shared admission would have pushed
            // may be offered to the source.
            let pushed = tidb_executor_internal_exec::predicate_pushdown::PushedScanFilter::from_physical_conditions(
                filters.clone(),
                ctx,
            );
            if tidb_expr::infer_pushdown::can_exprs_push_down_tikv(&filters)
                && child
                    .table_access()
                    .is_some_and(|access| access.accept_scan_filter(&pushed, ctx))
            {
                let output = unary_schema(plan, child.as_ref());
                if output.len() != child.schema().len() {
                    let expressions = output
                        .columns
                        .iter()
                        .map(|column| {
                            let offset = child.schema().column_index(column);
                            (offset >= 0).then(|| {
                                Expression::Column(child.schema().columns[offset as usize].clone())
                            })
                        })
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| {
                            DriverError::unsupported("selection output column is absent from child")
                        })?;
                    return Ok(Box::new(ProjectionExec::new(
                        meta(ctx, plan, output),
                        expressions,
                        child,
                        ctx.clone(),
                    )));
                }
                Ok(child)
            } else {
                let schema = unary_schema(plan, child.as_ref());
                Ok::<Box<dyn Executor>, DriverError>(Box::new(SelectionExec::new(
                    meta(ctx, plan, schema),
                    filters,
                    child,
                    ctx.clone(),
                    ctx.statement_memory(),
                )))
            }
        }
        PhysicalPlan::Limit(limit) => {
            let child_plan = only_child(plan)?;
            let mut child = build_with_state(child_plan, catalog, ctx, state)?;
            // Go puts a cop-side Limit below the reader's scan; the local
            // LimitExec still applies the offset. A refused cap leaves that
            // executor as the sole authority, which is always correct, only
            // slower.
            let capped = limit.offset.checked_add(limit.count).is_some_and(|cap| {
                child
                    .table_access()
                    .is_some_and(|access| access.accept_scan_limit(cap))
            });
            // Go `dagReq.OutputOffsets` over `Limit | Selection | IndexScan`:
            // the request returns the reader's columns of what the Selection
            // admitted, so the source narrows below this Limit.
            if state.in_reader {
                if let Some(outputs) = state.index_post_filter_columns.take() {
                    let offsets = outputs
                        .iter()
                        .map(|output| {
                            child.schema().columns.iter().position(|column| column.unique_id == output.unique_id)
                        })
                        .collect::<Option<Vec<_>>>();
                    if let Some(offsets) = offsets {
                        let output = Schema::new(outputs);
                        let _ = child
                            .table_access()
                            .is_some_and(|access| access.accept_post_filter_projection(&offsets, &output));
                    }
                }
            }
            if state.index_merge_partial && capped && limit.offset == 0 {
                return Ok(child);
            }
            // A coprocessor Limit outputs its child's columns; the reader's
            // `OutputColumns` may already have narrowed them at the source.
            let schema = if state.in_reader {
                child.schema().clone()
            } else {
                unary_schema(plan, child.as_ref())
            };
            let max_chunk_size = ctx.executor_chunk_sizes().1;
            let init_cap = limit.count.min(max_chunk_size as u64) as usize;
            let executor = LimitExec::new(
                ExecutorMeta::new(
                    schema,
                    i64::from(plan.base().base.id()),
                    init_cap,
                    max_chunk_size,
                )
                .with_alloc_pool(ctx.chunk_allocator()),
                limit.offset,
                limit.count,
                child,
            );
            Ok(Box::new(executor) as Box<dyn Executor>)
        }
        PhysicalPlan::Sort(sort) => {
            // Go buildSort consumes the positions bound before postOptimize.
            // An eliminated identity projection can leave different column IDs.
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let by_items = sort
                .by_items
                .iter()
                .map(|item| {
                    Ok::<SortByItem, DriverError>(SortByItem {
                        expr: {
                            let mut expr = item.expr.clone();
                            super::planner_bridge::materialize_physical_expression(&mut expr);
                            expr
                        },
                        desc: item.desc,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(SortExec::new(
                meta(ctx, plan, schema),
                by_items,
                child,
                ctx.clone(),
                ctx.statement_memory(),
            )) as Box<dyn Executor>)
        }
        PhysicalPlan::TopN(topn) => {
            let mut child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            // Go `(*PhysicalTopN).ToPB`: a TopN inside a reader is an executor
            // of the reader's DAG, so the coprocessor returns only its rows.
            // The local TopN below still runs over them, so a refusal changes
            // where rows are reduced, never the answer.
            if state.in_reader {
                if let Some(remote) = remote_topn(topn, child.schema()) {
                    let _ = child
                        .table_access()
                        .is_some_and(|access| access.accept_remote_topn(&remote));
                }
            }
            let mut by_items = topn
                .by_items
                .iter()
                .map(|item| {
                    let mut expr = item.expr.clone();
                    super::planner_bridge::materialize_physical_expression(&mut expr);
                    Ok::<_, DriverError>(SortByItem {
                        expr,
                        desc: item.desc,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = unary_schema(plan, child.as_ref());
            let child = if state.in_reader {
                materialize_reader_topn_keys(plan, child, &mut by_items, ctx)?
            } else {
                child
            };
            let output_offsets = inline_projection_offsets(&schema, child.schema());
            let child_schema = child.schema().clone();
            let child_field_types = child.ret_field_types().to_vec();
            let mut executor = TopNExec::new(
                meta(ctx, plan, schema),
                by_items,
                child,
                ctx.clone(),
                topn.offset,
                topn.count,
                ctx.statement_memory(),
            )
            .with_output_offsets(output_offsets)
            // A TopN INSIDE a reader is the cop-layer executor: go runs that
            // one through the coprocessor's push-built heap, not `heap.Init`
            // -- its tie order differs (oracle g-group).
            .with_cop_heap(state.in_reader)
            .with_parallelism(ctx.executor_concurrency());
            if let Some(prefix_col) = topn.prefix_col {
                let index = child_schema
                    .columns
                    .iter()
                    .position(|column| column.unique_id == prefix_col)
                    .ok_or_else(|| {
                        DriverError::unsupported(
                            "a physical TopN prefix column is absent from its child schema",
                        )
                    })?;
                let field_type = child_field_types.get(index).cloned().ok_or_else(|| {
                    DriverError::unsupported(
                        "a physical TopN prefix column has no child field type",
                    )
                })?;
                executor = executor.with_rank_prefix(index, topn.prefix_len as i64, field_type);
            }
            Ok(Box::new(executor) as Box<dyn Executor>)
        }
        PhysicalPlan::HashAgg(aggregation) => build_aggregation(
            plan,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            false,
            catalog,
            ctx,
            state,
        ),
        PhysicalPlan::StreamAgg(aggregation) => build_aggregation(
            plan,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            true,
            catalog,
            ctx,
            state,
        ),
        PhysicalPlan::Expand(expand) => {
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            // Go buildExpand consumes positions resolved before postOptimize.
            let mut levels = expand.level_exprs.clone();
            for expression in levels.iter_mut().flatten() {
                super::planner_bridge::materialize_physical_expression(expression);
            }
            Ok(Box::new(crate::expand::ExpandExec::new(
                meta(ctx, plan, plan_schema(plan)?),
                levels,
                child,
                ctx.clone(),
                ctx.statement_memory(),
            )) as Box<dyn Executor>)
        }
        PhysicalPlan::Window(window) => build_window(plan, window, catalog, ctx, state),
        PhysicalPlan::HashJoin(join) => {
            if ctx.optimizer_cost_env().session.use_hash_join_v2
                && tidb_executor_join_joinversion::is_hash_join_v2_supported()
                && can_use_hash_join_v2(join)
            {
                return build_hash_join_v2(plan, join, catalog, ctx, state).map(|executor| {
                    state.meter_as(
                        plan,
                        executor,
                        ctx.statement_memory(),
                        Some("*join.HashJoinV2Exec.Next"),
                    )
                });
            }
            let mut executor = build_join(
                plan,
                join.join_type,
                &join.left_join_keys,
                &join.right_join_keys,
                &join.is_null_eq,
                &join.left_conditions,
                &join.right_conditions,
                &join.other_conditions,
                catalog,
                ctx,
                state,
            )?;
            let build_child_idx = if join.use_outer_to_build {
                1usize.checked_sub(join.inner_child_idx).ok_or_else(|| {
                    DriverError::unsupported("a physical hash join has an invalid inner child")
                })?
            } else {
                join.inner_child_idx
            };
            executor.set_hash_build_is_left(build_child_idx == 0);
            executor.set_parallelism(join.concurrency);
            executor
                .set_default_values(join.default_values.clone())
                .map_err(DriverError::Exec)?;
            Ok(Box::new(executor) as Box<dyn Executor>)
        }
        PhysicalPlan::MergeJoin(join) => {
            let [left, right] = plan.children() else {
                return Err(DriverError::unsupported(
                    "a physical merge join has the wrong child count",
                ));
            };
            let keys = join_key_offsets(
                &join.left_join_keys,
                &join.right_join_keys,
                &plan_schema(left)?,
                &plan_schema(right)?,
            )?;
            let mut executor = build_join(
                plan,
                join.join_type,
                &join.left_join_keys,
                &join.right_join_keys,
                &join.is_null_eq,
                &join.left_conditions,
                &join.right_conditions,
                &join.other_conditions,
                catalog,
                ctx,
                state,
            )?;
            executor.set_merge_plan(tidb_executor_join::merge_join_plan::MergeJoinPlan {
                keys,
                desc: join.desc,
            });
            executor
                .set_default_values(join.default_values.clone())
                .map_err(DriverError::Exec)?;
            Ok(Box::new(executor) as Box<dyn Executor>)
        }
        PhysicalPlan::IndexJoin(join) => {
            // Go `buildIndexLookUpJoin` -> `newDataReaderBuilder`'s
            // `getSnapshotTS`, for the inner reader it builds per task.
            get_snapshot_ts(ctx);
            build_index_join(plan, join, catalog, ctx, state)
        }
        PhysicalPlan::Apply(apply) => build_apply(plan, apply, catalog, ctx, state),
        PhysicalPlan::TableDual(dual) => Ok(Box::new(TableDualExec::new(
            ExecutorMeta::new(
                plan.base()
                    .base
                    .schema()
                    .cloned()
                    .unwrap_or_else(|| Schema::new(Vec::new())),
                i64::from(plan.base().base.id()),
                dual.row_count,
                ctx.executor_chunk_sizes().1,
            ),
            dual.row_count,
        )) as Box<dyn Executor>),
        PhysicalPlan::UnionAll(_) => {
            let children = plan
                .children()
                .iter()
                .map(|child| build_with_state(child, catalog, ctx, state))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Box::new(crate::union_all::UnionExec::new(
                meta(ctx, plan, plan_schema(plan)?),
                children,
                // Go `UnionConcurrency()` is the executor concurrency.
                ctx.executor_concurrency(),
            )) as Box<dyn Executor>)
        }
        PhysicalPlan::MaxOneRow(_) => {
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let executor_meta =
                ExecutorMeta::new(plan_schema(plan)?, i64::from(plan.base().base.id()), 2, 2);
            Ok(Box::new(MaxOneRowExec::new(executor_meta, child)) as Box<dyn Executor>)
        }
        PhysicalPlan::NominalSort(_) => build_with_state(only_child(plan)?, catalog, ctx, state),
        PhysicalPlan::UnionScan(union_scan) => {
            build_union_scan(plan, union_scan, catalog, ctx, state)
        }
        PhysicalPlan::CTE(cte) => build_cte(plan, cte, catalog, ctx, state),
        PhysicalPlan::CTETable(table) => build_cte_table(plan, table, state, ctx),
        // Go `executorBuilder.buildShow` / `buildShowDDLJobs`: a `ShowExec` /
        // `ShowDDLJobsExec` whose rows the session supplied for this node.
        PhysicalPlan::Show(_) | PhysicalPlan::ShowDDLJobs(_) => {
            let rows = ctx
                .show_retrieval()
                .rows(plan.base().base.id())
                .ok_or_else(|| DriverError::unsupported("SHOW rows were not fetched"))?;
            Ok(Box::new(crate::mem_table::MemTableSourceExec::new(
                meta(ctx, plan, plan_schema(plan)?),
                rows.as_ref().clone(),
            )) as Box<dyn Executor>)
        }
        _ => Err(DriverError::unsupported(format!(
            "physical executor construction for {} is not implemented",
            plan.base().base.tp()
        ))),
    }?;
    Ok(state.meter(plan, executor, ctx.statement_memory()))
}

/// The reader a `UnionScanExec` merges with the transaction's buffered rows:
/// Go `buildUnionScanFromReader`'s `reader.(type)` arms.
struct UnionScanReader {
    /// The table the reader scans (a dynamic partition read names the
    /// partitioned table; its rows carry `_tidb_tid`).
    table_id: i64,
    /// The index an index reader or lookup scans.
    index_id: Option<i64>,
    /// Go `keepOrder`.
    keep_order: bool,
    /// Go `desc`.
    desc: bool,
    /// Go `collectColIdxFromByItems`' input: the plan column ids of the
    /// reader's `byItems`, whose rows the union scan then sorts by
    /// (`needExtraSorting`). Empty when the reader has none.
    by_item_columns: Vec<i64>,
}

/// Go `collectColIdxFromByItems`'s input: the plan column identity
/// (`unique_id`) of each item. A
/// non-column item is Go's "Not support non-column in orderBy pushed down".
fn by_item_column_ids(
    items: &[tidb_expression_aggregation::ByItems],
) -> Result<Vec<i64>, DriverError> {
    items
        .iter()
        .map(|item| match &item.expr {
            tidb_expr::expression::Expression::Column(column) => Ok(column.unique_id),
            _ => Err(DriverError::unsupported(
                "Not support non-column in orderBy pushed down",
            )),
        })
        .collect()
}

fn union_scan_reader(plan: &PhysicalPlan) -> Result<Option<UnionScanReader>, DriverError> {
    Ok(match plan {
        PhysicalPlan::TableReader(reader)
            if reader.store_type != tidb_kv::store_type::StoreType::TiDb =>
        {
            let Some(scan) = reader.table_plan.as_deref().and_then(embedded_table_scan) else {
                return Ok(None);
            };
            Some(UnionScanReader {
                table_id: scan.table_id,
                index_id: None,
                keep_order: scan.keep_order,
                desc: scan.desc,
                by_item_columns: by_item_column_ids(&scan.by_items)?,
            })
        }
        PhysicalPlan::IndexReader(reader) => {
            let Some(scan) = reader.index_plan.as_deref().and_then(embedded_index_scan) else {
                return Ok(None);
            };
            Some(UnionScanReader {
                table_id: scan.table_id,
                index_id: Some(scan.index_id),
                keep_order: scan.keep_order,
                desc: scan.desc,
                by_item_columns: by_item_column_ids(&scan.by_items)?,
            })
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            let Some(scan) = reader.index_plan.as_deref().and_then(embedded_index_scan) else {
                return Ok(None);
            };
            Some(UnionScanReader {
                table_id: scan.table_id,
                index_id: Some(scan.index_id),
                keep_order: reader.keep_order,
                desc: scan.desc,
                by_item_columns: by_item_column_ids(&scan.by_items)?,
            })
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            let Some(scan) = reader.table_plan.as_deref().and_then(embedded_table_scan) else {
                return Ok(None);
            };
            // Go `buildUnionScanFromReader`: an index merge with `byItems`
            // keeps `keepOrder`, takes `desc` from the first item and sorts
            // by the items' columns.
            let mut keep_order = false;
            let mut desc = false;
            let mut by_item_columns = Vec::new();
            if let Some(first) = reader.by_items.first() {
                keep_order = reader.keep_order;
                desc = first.desc;
                by_item_columns = by_item_column_ids(&reader.by_items)?;
            }
            Some(UnionScanReader {
                table_id: scan.table_id,
                index_id: None,
                keep_order,
                desc,
                by_item_columns,
            })
        }
        _ => None,
    })
}

/// Go `plannerutil.HandleCols` as `UnionScanExec` uses it: the handle of a
/// row, built from the union scan's handle columns the way the table writes
/// its record keys.
struct UnionScanHandleCols {
    /// The handle columns' offsets in the row, in the handle's order.
    offsets: Vec<usize>,
    /// The row's types.
    types: Vec<FieldType>,
    /// The table, for a common handle (`CommonHandleCols.BuildHandle`);
    /// `None` for an integer handle (`IntHandleCols`).
    common: Option<Arc<crate::TableCommon>>,
    zone: tidb_datatype::SessionTimeZone,
}

impl crate::union_scan::HandleCols for UnionScanHandleCols {
    fn build_handle(
        &self,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<tidb_table::tables::TableHandle, crate::ExecError> {
        match &self.common {
            None => Ok(tidb_table::tables::TableHandle::Int(
                row.get_int64(self.offsets[0]),
            )),
            Some(table) => {
                let values = self
                    .offsets
                    .iter()
                    .map(|&offset| row.get_datum(offset, &self.types[offset]))
                    .collect::<Vec<_>>();
                table
                    .common_handle_of_values(&values, &self.zone)
                    .map_err(|error| crate::ExecError::internal(format!("{error:?}")))
            }
        }
    }

    fn compare_handles(
        &self,
        left: &[tidb_datatype::Datum],
        right: &[tidb_datatype::Datum],
        collators: &[tidb_charset::Collation],
    ) -> Result<std::cmp::Ordering, crate::ExecError> {
        for &offset in &self.offsets {
            // Go `IntHandleCols.Compare` compares with the binary collator,
            // `CommonHandleCols.Compare` with each column's own.
            let collation = match self.common {
                None => tidb_charset::Collation::Binary,
                Some(_) => collators
                    .get(offset)
                    .copied()
                    .unwrap_or(tidb_charset::Collation::Binary),
            };
            let ordering = left[offset]
                .compare(&right[offset], collation)
                .map_err(|error| crate::ExecError::internal(format!("{error:?}")))?;
            if ordering != std::cmp::Ordering::Equal {
                return Ok(ordering);
            }
        }
        Ok(std::cmp::Ordering::Equal)
    }
}

/// Go `memRowsIter`: a union scan's mem reader -- here the reader itself over
/// the buffered entries -- pulled one row at a time. It opens on the first
/// pull and closes with the union scan, so a re-opened union scan (an
/// Apply's inner side) reads the buffered rows again.
struct ReaderMemRowsIter {
    reader: Box<dyn Executor>,
    chunk: Option<Chunk>,
    position: usize,
    opened: bool,
}

impl crate::mem_reader::MemRowsIter for ReaderMemRowsIter {
    fn next_row(
        &mut self,
    ) -> Result<Option<Vec<tidb_datatype::Datum>>, crate::mem_reader::MemReaderError> {
        if !self.opened {
            self.reader.open()?;
            self.opened = true;
            self.chunk = Some(self.reader.new_chunk());
            self.position = 0;
        }
        let Some(chunk) = self.chunk.as_mut() else {
            return Ok(None);
        };
        if self.position >= chunk.num_rows() {
            self.reader.next(chunk)?;
            self.position = 0;
            if chunk.num_rows() == 0 {
                self.chunk = None;
                return Ok(None);
            }
        }
        let row = chunk
            .get_row(self.position)
            .get_datum_row(self.reader.ret_field_types());
        self.position += 1;
        Ok(Some(row))
    }

    fn close(&mut self) {
        if self.opened {
            self.opened = false;
            self.chunk = None;
            if let Err(error) = self.reader.close() {
                eprintln!("union scan mem reader close: {error:?}");
            }
        }
    }
}

/// Go `buildUnionScanExec` / `buildUnionScanFromReader`: the reader below
/// reads the statement's snapshot, and `UnionScanExec` merges it with the
/// transaction's buffered rows of the same key ranges. Go's mem readers
/// (`buildMemTableReader` and its index siblings) decode the membuffer
/// entries as the reader decodes coprocessor rows; this tier builds that mem
/// reader as the same reader over the buffered entries, copied when the
/// statement starts ([`tidb_kv::mem_buffer::MemBufferSnapshot`]).
///
/// A point read, a dual or a sample is returned as is (Go: they handle the
/// buffer themselves or read disk only). A backend without a separate
/// snapshot keeps its merged reader, as does a reader shape Go's builder has
/// no arm for.
/// Go `executorBuilder.getCacheTable` / `UnionScanExec.handleCachedTable`:
/// the rows of a cached table whose lease covers the statement's read ts. A
/// read outside it takes the read lock (and loads the copy) in the
/// background, unless the statement writes the table -- `lock_in_insert`
/// false is `getCacheTable`'s INSERT exception -- or only explains.
fn cached_table_data(
    table: &tidb_table::tables::TableCommon,
    ctx: &crate::StmtContext,
    lock_in_insert: bool,
) -> Option<Arc<tidb_kv::mem_buffer::MemBufferSnapshot>> {
    if table.cache_status() != tidb_model::TableCacheStatusType::ENABLE {
        return None;
    }
    let cached = table.cached_table()?;
    let store = crate::statement_store(ctx);
    let start_ts = store.start_ts();
    let lease_duration = std::time::Duration::from_secs(
        tidb_vardef::TABLE_CACHE_LEASE
            .load(std::sync::atomic::Ordering::SeqCst)
            .max(0) as u64,
    );
    let (data, loading) = cached.try_read_from_cache(start_ts, lease_duration);
    if let Some(data) = data {
        ctx.set_read_from_table_cache();
        return Some(data);
    }
    let writes_table = match ctx.statement_class() {
        crate::StatementClass::Update
        | crate::StatementClass::Delete => true,
        crate::StatementClass::Insert => lock_in_insert,
        _ => false,
    };
    if !loading && !writes_table && !ctx.in_explain_stmt() {
        if let Some(kv_storage) = store.kv_storage() {
            cached.update_lock_for_read(kv_storage, start_ts, lease_duration);
        }
    }
    None
}

/// Go `UnionIter(memBuffer, cacheTable)`: the buffer's entries over the
/// cached rows, its tombstones hiding theirs.
fn union_cache_under_buffer(
    cache: &tidb_kv::mem_buffer::MemBufferSnapshot,
    buffer: &tidb_kv::mem_buffer::MemBufferSnapshot,
) -> Arc<tidb_kv::mem_buffer::MemBufferSnapshot> {
    let mut entries = cache.entries.clone();
    entries.extend(buffer.entries.iter().map(|(key, value)| (key.clone(), value.clone())));
    Arc::new(tidb_kv::mem_buffer::MemBufferSnapshot { entries })
}

fn build_union_scan(
    plan: &PhysicalPlan,
    union_scan: &tidb_planner_core_operator_physicalop::PhysicalUnionScan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let child_plan = only_child(plan)?;
    // Go: "If the push-downed condition contains virtual column, we may
    // build a selection upon reader."
    let reader_plan = match child_plan {
        PhysicalPlan::Selection(_) => only_child(child_plan)?,
        other => other,
    };
    let merged = |state: &mut BuildState| {
        let previous = match state.reader_storage {
            ReaderStorage::Snapshot => Some(std::mem::replace(
                &mut state.reader_storage,
                ReaderStorage::Merged,
            )),
            _ => None,
        };
        let built = build_with_state(child_plan, catalog, ctx, state);
        if let Some(previous) = previous {
            state.reader_storage = previous;
        }
        built
    };
    if matches!(
        reader_plan,
        PhysicalPlan::PointGet(_)
            | PhysicalPlan::BatchPointGet(_)
            | PhysicalPlan::TableDual(_)
            | PhysicalPlan::TableSample(_)
    ) {
        return build_with_state(child_plan, catalog, ctx, state);
    }
    let Some(reader) = union_scan_reader(reader_plan)? else {
        return merged(state);
    };
    let table = catalog
        .physical_kv_table_by_id(reader.table_id)
        .ok_or_else(|| {
            DriverError::unsupported("a union scan's table ID is absent from the catalog")
        })?;
    let schema = plan_schema(plan)?;
    let handle_offsets = union_scan
        .handle_cols
        .iter()
        .map(|column| usize::try_from(schema.column_index(column)).ok())
        .collect::<Option<Vec<_>>>();
    let (Some(handle_offsets), ReaderStorage::Snapshot) = (handle_offsets, &state.reader_storage)
    else {
        return merged(state);
    };
    if handle_offsets.is_empty() || crate::statement_store(ctx).snapshot_reader().is_none() {
        return merged(state);
    }
    // The buffered entries of every physical table the reader can reach,
    // and of the logical table a global index is keyed under.
    let mut physical_ids = table.record_physical_ids();
    physical_ids.push(table.table_id());
    physical_ids.push(reader.table_id);
    physical_ids.sort_unstable();
    physical_ids.dedup();
    let ranges = physical_ids
        .iter()
        .map(|id| {
            (
                tidb_txnkv::Key::from_bytes(tidb_tablecodec::table_key::encode_table_prefix(*id)),
                tidb_txnkv::Key::from_bytes(tidb_tablecodec::table_key::encode_table_prefix(
                    id.saturating_add(1),
                )),
            )
        })
        .collect::<Vec<_>>();
    let snapshot = crate::statement_store(ctx).mem_buffer_snapshot(&ranges).ok_or_else(|| {
        DriverError::unsupported("a union scan's storage has no membuffer to read")
    })?;
    // Go `handleCachedTable`: a cached table read inside its lease reads the
    // in-memory copy, which `getSnapIter` unions under the buffer, and its
    // reader is a dummy (`setDummy`).
    let cache_table = cached_table_data(&table, ctx, true);
    let (child, mem_snapshot) = match &cache_table {
        Some(cache) => (
            Box::new(crate::table_dual::TableDualExec::new(
                meta(ctx, child_plan, plan_schema(child_plan)?),
                0,
            )) as Box<dyn Executor>,
            union_cache_under_buffer(cache, &snapshot),
        ),
        None => (
            build_with_state(child_plan, catalog, ctx, state)?,
            Arc::clone(&snapshot),
        ),
    };
    let mut mem_state = BuildState {
        reader_storage: ReaderStorage::MemBuffer(mem_snapshot),
        untraced: true,
        has_lock: state.has_lock,
        ..BuildState::default()
    };
    let mem_reader = build_with_state(child_plan, catalog, ctx, &mut mem_state)?;
    let schema = child.schema().clone();
    if schema.len() != plan_schema(plan)?.len() || mem_reader.schema().len() != schema.len() {
        return Err(DriverError::unsupported(
            "a union scan's reader does not produce the union scan's columns",
        ));
    }
    let types = child.ret_field_types().to_vec();
    let collators = types
        .iter()
        .map(|field_type| field_type.collation())
        .collect::<Vec<_>>();
    // Go `usedIndex`: an index reader's or lookup's rows are ordered by its
    // index columns, located in the reader's columns by name; a table read
    // by its handle alone (Go's `byItems` arms have no counterpart here).
    let by_item_used_index = reader
        .by_item_columns
        .iter()
        .filter_map(|id| {
            schema
                .columns
                .iter()
                .position(|column| column.unique_id == *id)
        })
        .collect::<Vec<_>>();
    let need_extra_sorting = !reader.by_item_columns.is_empty();
    let used_index = reader
        .index_id
        .and_then(|index_id| table.indexes().iter().find(|index| index.read().id == index_id))
        .map(|index| {
            index
                .column_offsets
                .iter()
                .filter_map(|offset| {
                    let name = table.columns().get(*offset)?.read().name.original().to_owned();
                    schema.columns.iter().position(|column| {
                        column
                            .orig_name
                            .rsplit('.')
                            .next()
                            .is_some_and(|column_name| column_name.go_lower_eq(&name))
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    // Go `usedIndex = colIdxes` for an index merge with `byItems`.
    let used_index = if need_extra_sorting {
        by_item_used_index
    } else {
        used_index
    };
    let handle_cols = UnionScanHandleCols {
        offsets: handle_offsets,
        types: types.clone(),
        common: (!table.common_handle_offsets().is_empty()).then(|| Arc::clone(&table)),
        zone: ctx.session_zone(),
    };
    let conditions_with_vir_col = union_scan
        .conditions
        .iter()
        .filter(|condition| {
            tidb_expr::expr_util::predicates::contain_virtual_column(std::slice::from_ref(
                condition,
            ))
        })
        .cloned()
        .collect::<Vec<_>>();
    let conditions_with_vir_col = resolve_expressions(&conditions_with_vir_col, &schema)?;
    let columns = schema
        .columns
        .iter()
        .zip(&types)
        .map(|(column, field_type)| crate::union_scan::UnionScanColumn {
            id: column.id,
            name: column
                .orig_name
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .to_owned(),
            field_type: field_type.clone(),
        })
        .collect();
    Ok(Box::new(crate::union_scan::UnionScanExec::new(
        crate::union_scan::UnionScanSpec {
            meta: meta(ctx, plan, schema),
            child,
            mem_buf_snap: Box::new(snapshot),
            added_rows_iter: Box::new(ReaderMemRowsIter {
                reader: mem_reader,
                chunk: None,
                position: 0,
                opened: false,
            }),
            conditions_with_vir_col,
            columns,
            table_record_id: reader.table_id,
            // The readers on both sides fill the virtual columns already.
            virtual_column_index: Vec::new(),
            cache_table: cache_table.is_some(),
            partition_id_map: std::collections::BTreeSet::new(),
            keep_order: reader.keep_order,
            compare_exec: crate::union_scan::CompareExec {
                collators,
                used_index,
                desc: reader.desc,
                need_extra_sorting,
                handle_cols: Box::new(handle_cols),
            },
            ctx: ctx.clone(),
            stmt: ctx.clone(),
        },
    )))
}

pub(super) fn build(
    plan: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let mut state = BuildState {
        projection_workers_allowed: true,
        ..BuildState::default()
    };
    let child = build_with_state(plan, catalog, ctx, &mut state)?;
    let output = plan_schema(plan)?;
    if output.len() != child.schema().len() {
        let expressions = output
            .columns
            .iter()
            .map(|column| {
                let offset = child.schema().column_index(column);
                (offset >= 0)
                    .then(|| Expression::Column(child.schema().columns[offset as usize].clone()))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DriverError::unsupported("root output column is absent from executor")
            })?;
        return Ok(checked_executor(
            Box::new(ProjectionExec::new(
                meta(ctx, plan, output),
                expressions,
                child,
                ctx.clone(),
            )),
            ctx.statement_memory(),
        ));
    }
    Ok(child)
}

fn schema_column_name(column: &Column, ordinal: usize) -> String {
    column
        .orig_name
        .rsplit('.')
        .find(|part| !part.is_empty())
        .map_or_else(|| format!("Column#{}", ordinal + 1), str::to_owned)
}

fn result_columns(
    select: &tidb_ast::SelectStmt,
    schema: &Schema,
    output_names: &[tidb_datatype::FieldName],
) -> Vec<(String, FieldType)> {
    let expression_names = select
        .fields
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| match field {
            tidb_ast::SelectField::Expr { expr, alias } => {
                Some(alias.clone().unwrap_or_else(|| {
                    super::default_field_display_name(&select.fields, index, expr)
                }))
            }
            tidb_ast::SelectField::Wildcard(_) => None,
        })
        .collect::<Vec<_>>();
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let name = if expression_names.len() == schema.len() {
                expression_names[index].clone()
            } else {
                // A wildcard's name is not in the AST: it belongs to the
                // relation the `*` expands to. Go reads it from the logical
                // plan's `OutputNames` captured before optimization
                // (`planner/optimize.go:525`), because projection elimination
                // can leave the physical root without the naming projection.
                output_names
                    .get(index)
                    .map(|field| field.names.column.original.clone())
                    .filter(|name| !name.is_empty())
                    .unwrap_or_else(|| schema_column_name(column, index))
            };
            let field_type = column
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            (name, field_type)
        })
        .collect()
}

/// Go's `Plan.OutputNames`: a node that produces no names of its own (a
/// `Sort`, `TopN`, `Limit` or `Selection` above a set operation) answers its
/// first child's, so the result columns keep what the query wrote.
fn forwarded_output_names(plan: &PhysicalPlan) -> &[tidb_datatype::FieldName] {
    let mut node = plan;
    loop {
        let names = node.base().base.output_names();
        match node.children().first() {
            Some(child) if names.is_empty() => node = child,
            _ => return names,
        }
    }
}

fn physical_result_columns(plan: &PhysicalPlan, schema: &Schema) -> Vec<(String, FieldType)> {
    let names = forwarded_output_names(plan);
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let name = names.get(index).map_or_else(
                || schema_column_name(column, index),
                |field| {
                    if field.names.column.original.is_empty() {
                        if field.names.column.lower.is_empty() {
                            schema_column_name(column, index)
                        } else {
                            field.names.column.lower.clone()
                        }
                    } else {
                        field.names.column.original.clone()
                    }
                },
            );
            let field_type = column
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            (name, field_type)
        })
        .collect()
}

/// Derives the client result metadata from the selected physical root without
/// constructing or draining an executor. Go keeps this metadata on the same
/// `ExecStmt.Plan` used by `buildExecutor`; planning-only consumers must not
/// enter a second AST execution pipeline to rediscover it.
pub(super) fn planned_result_columns(
    select: &tidb_ast::SelectStmt,
    physical: &PhysicalPlan,
) -> Result<Vec<(String, FieldType)>, DriverError> {
    Ok(result_columns(
        select,
        &plan_schema(physical)?,
        physical.base().base.output_names(),
    ))
}

/// Go's common query execution seam: both SELECT and set-operation logical
/// roots are already physical plans before executor construction begins.
pub(super) fn open_query(
    query: &tidb_ast::QueryStmt,
    physical: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<super::RecordSet, DriverError> {
    // Go `runStmt` -> `ExecStmt.Exec`: build the executor and open it --
    // or, for a prepared point get in autocommit (`stmt.PsStmt`, set when
    // `IsSafeToReusePointGetExecutor` holds and the plan is a
    // `PointGetPlan`), `ExecStmt.PointGet`.
    if matches!(physical, PhysicalPlan::PointGet(_))
        && ctx.is_prepared_execution()
        && !ctx.pessimistic_transaction()
        && !ctx.optimistic_transaction()
        && !ctx.is_staleness()
    {
        super::begin_point_get_region();
    } else {
        super::begin_run_stmt_region();
    }
    prepare_execution_plan(physical, catalog, ctx)?;
    crate::isolation_advise::advise_optimize_with_plan(ctx, physical, false);
    crate::explain::publish_physical_process_info(ctx, physical, catalog)?;
    let root = build(physical, catalog, ctx)?;
    let columns = match query {
        tidb_ast::QueryStmt::Select(select) => {
            result_columns(select, root.schema(), physical.base().base.output_names())
        }
        tidb_ast::QueryStmt::SetOpr(_) => physical_result_columns(physical, root.schema()),
    };
    // Go `recordSet.Fields()` reads the names captured before optimization.
    let output_names = forwarded_output_names(physical)
        .iter()
        .map(|name| name.names.clone())
        .collect();
    let current_db = ctx.current_database_name().unwrap_or_default();
    crate::notify_before_executor_first_run(ctx);
    let detachable = crate::detach::detach(physical);
    super::RecordSet::open(root, columns, ctx.statement_memory()).map(|record_set| {
        record_set
            .with_output_names(output_names, current_db)
            .with_detachable(detachable)
    })
}

/// Go `runStmt` -> `ExecStmt.Exec` for a planned SHOW: the executor built
/// over the rows the session supplied, and opened.
pub(super) fn open_show(
    physical: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<super::RecordSet, DriverError> {
    super::begin_run_stmt_region();
    prepare_execution_plan(physical, catalog, ctx)?;
    let root = build(physical, catalog, ctx)?;
    let columns = physical_result_columns(physical, root.schema());
    let output_names = forwarded_output_names(physical)
        .iter()
        .map(|name| name.names.clone())
        .collect();
    let current_db = ctx.current_database_name().unwrap_or_default();
    crate::notify_before_executor_first_run(ctx);
    super::RecordSet::open(root, columns, ctx.statement_memory())
        .map(|record_set| record_set.with_output_names(output_names, current_db))
}

/// Builds and drains the target of `EXPLAIN ANALYZE` through the same
/// physical executor switch used by ordinary and cached execution; its
/// executors record into the statement's `RuntimeStatsColl`.
pub(crate) fn execute_for_explain(
    physical: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    explain_fields: &[tidb_datatype::FieldType],
) -> Result<(), DriverError> {
    prepare_execution_plan(physical, catalog, ctx)?;
    let mut state = BuildState {
        projection_workers_allowed: true,
        ..BuildState::default()
    };
    let mut root = build_with_state(physical, catalog, ctx, &mut state)?;
    crate::notify_before_executor_first_run(ctx);
    let result = (|| {
        // Go `ExplainExec.Open` opens the analyzed tree; the connection then
        // takes its write chunk (`rs.NewChunk`) before `Next` runs the tree.
        root.open()?;
        ctx.new_result_write_chunk(explain_fields);
        let mut req = root.new_chunk();
        loop {
            super::next_executor(root.as_mut(), &mut req, &ctx.statement_memory())?;
            if req.num_rows() == 0 {
                break;
            }
        }
        Ok::<(), DriverError>(())
    })();
    // Go `executeAnalyzeExec` registers the RU before its deferred `Close`.
    crate::explain::register_ru_runtime_stats(ctx, i64::from(physical.id()));
    let close_result = root.close().map_err(DriverError::from);
    result?;
    close_result?;
    Ok(())
}

/// Go `executor.EvalSubqueryFirstRow` (`pkg/executor/select.go:598`): build
/// the separately evaluated child, pull ONE chunk, and hand back its first
/// row together with whether any row came back.
///
/// The child is a scalar subquery's `MaxOneRow` (one NULL row when the inner
/// query matched nothing, 1242 when it matched more than one) or an `EXISTS`
/// child whose row count is the whole answer. Nothing is retained: the caller
/// keeps the physical tree for the EXPLAIN root, so this borrows it.
pub(crate) fn eval_subquery_first_row(
    physical: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<(Vec<tidb_datatype::Datum>, bool), DriverError> {
    prepare_execution_plan(physical, catalog, ctx)?;
    let mut root = build(physical, catalog, ctx)?;
    let types = root.ret_field_types().to_vec();
    let result: Result<(Vec<tidb_datatype::Datum>, bool), DriverError> = (|| {
        root.open()?;
        let mut req = root.new_chunk();
        super::next_executor(root.as_mut(), &mut req, &ctx.statement_memory())?;
        if req.num_rows() == 0 {
            return Ok((Vec::new(), false));
        }
        Ok((req.get_row(0).get_datum_row(&types), true))
    })();
    let close_result = root.close().map_err(DriverError::from);
    let row = result?;
    close_result?;
    Ok(row)
}

/// Builds and visits the retained child of an UPDATE or DELETE. Go's DML
/// executors consume this same physical child to obtain both the row values
/// and the handle columns used by the write. Runtime counters are collected
/// only for EXPLAIN ANALYZE; ordinary and cached execution use the identical
/// builder without the metering wrappers.
pub(crate) fn execute_dml_source(
    physical: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    dml: Option<&DmlExec>,
    charge_chunks: bool,
    visit: impl FnMut(Vec<tidb_datatype::Datum>) -> Result<(), DriverError>,
) -> Result<(), DriverError> {
    prepare_execution_plan(physical, catalog, ctx)?;
    let mut state = BuildState::default();
    let root = build_with_state(physical, catalog, ctx, &mut state)?;
    crate::notify_before_executor_first_run(ctx);
    let field_types = root.ret_field_types().to_vec();
    super::visit_executor_row_chunks(
        root,
        &field_types,
        &ctx.statement_memory(),
        dml,
        charge_chunks,
        visit,
    )
}

/// [`execute_dml_source`] for a statement that writes each row as the source
/// delivers it (Go `UpdateExec.updateRows`, `DeleteExec.deleteSingleTableByChunk`):
/// `visit` receives the catalog the write mutates. The source executor owns
/// what it read from the catalog while it was built, so the catalog is free
/// again for the rows' writes.
pub(crate) fn execute_dml_source_writing(
    physical: &mut PhysicalPlan,
    catalog: &mut SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    dml: &DmlExec,
    visit: impl FnMut(&mut SessionExtendedInfoSchema, Vec<tidb_datatype::Datum>) -> Result<(), DriverError>,
) -> Result<(), DriverError> {
    prepare_execution_plan(physical, catalog, ctx)?;
    let mut state = BuildState::default();
    let root = build_with_state(physical, catalog, ctx, &mut state)?;
    crate::notify_before_executor_first_run(ctx);
    let field_types = root.ret_field_types().to_vec();
    super::visit_executor_rows_writing(
        root,
        &field_types,
        &ctx.statement_memory(),
        dml,
        catalog,
        visit,
    )
}

/// Builds a write statement's source executor for its driver to read
/// chunk by chunk (Go `insertRowsFromSelect`'s `selectExec`).
pub(crate) fn build_dml_source(
    physical: &mut PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    prepare_execution_plan(physical, catalog, ctx)?;
    let mut state = BuildState::default();
    let root = build_with_state(physical, catalog, ctx, &mut state)?;
    crate::notify_before_executor_first_run(ctx);
    Ok(root)
}

#[cfg(test)]
mod tests {
    use crate::KvTableDdl;
use super::*;
    use tidb_datatype::Datum;
    use tidb_planner_core_operator_physicalop::{BasePhysicalPlan, BatchPointGetPlan, PhysicalCTE, PhysicalCTETable, PhysicalIndexMergeReader, PhysicalMaxOneRow, PointGetPlan, PhysicalSelection, PhysicalTableDual, PhysicalTableReader, PhysicalUnionAll};

    /// Go `LimitExec.Close`: a close that takes over a millisecond records
    /// `limitExec.Close` from when the close began, under the span the
    /// executor was opened in; a quick close records none.
    #[test]
    fn a_slow_limit_close_records_its_span() {
        struct SlowClose {
            meta: tidb_executor_internal_exec::ExecutorMeta,
            close_for: Duration,
        }
        impl Executor for SlowClose {
            fn open(&mut self) -> Result<(), crate::ExecError> {
                Ok(())
            }
            fn next(&mut self, req: &mut Chunk) -> Result<(), crate::ExecError> {
                req.reset();
                Ok(())
            }
            fn close(&mut self) -> Result<(), crate::ExecError> {
                std::thread::sleep(self.close_for);
                Ok(())
            }
            fn schema(&self) -> &Schema {
                self.meta.schema()
            }
            fn ret_field_types(&self) -> &[FieldType] {
                self.meta.ret_field_types()
            }
            fn init_cap(&self) -> usize {
                1
            }
            fn max_chunk_size(&self) -> usize {
                1
            }
            fn new_chunk(&self) -> Chunk {
                self.meta.new_chunk()
            }
        }
        let spans = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&spans);
        let tracer = Arc::new(tidb_tracing::Tracer::new(Arc::new(move |raw| {
            sink.lock().unwrap().push(raw);
        })));
        let root = tracer.start_span("trace");
        let entered = tidb_tracing::with_thread_span(Some(root.clone()));
        for close_for in [Duration::from_millis(3), Duration::ZERO] {
            let mut limit = SqlKillerCheckExec {
                ruv2: None,
                child_count: None,
                child: Box::new(SlowClose {
                    meta: tidb_executor_internal_exec::ExecutorMeta::new(Schema::new(Vec::new()), 1, 1, 1),
                    close_for,
                }),
                memory: crate::StatementMemory::default(),
                runtime_stats: None,
                next_region: Some("*executor.LimitExec.Next"),
                top_profiling: None,
                trace: Some(Box::new(GoExecutorTrace {
                    limit_close: true,
                    ..GoExecutorTrace::default()
                })),
            };
            limit.open().unwrap();
            limit.close().unwrap();
        }
        drop(entered);
        root.finish();
        let spans = spans.lock().unwrap();
        let names = spans
            .iter()
            .map(|span| span.operation.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["limitExec.Close", "trace"]);
        assert_eq!(spans[0].parent_span_id, root.context().span_id);
        assert!(spans[0].start >= spans[1].start);
    }

    /// Go's write executor (`UpdateExec` / `DeleteExec` / `InsertExec`):
    /// `Open`, its one `Next` and `Close` are timed apart, so a driver that
    /// opens and closes the child inside its `Next` region still counts that
    /// time once, as open and close; `loops` is the one `Next` however many
    /// regions it was timed in; and the `memTracker` is labelled by the plan
    /// and released by `Close`, keeping its peak.
    #[test]
    fn a_write_executor_times_open_next_and_close_apart() {
        let plan = dual(7, 0);
        let coll = Arc::new(tidb_execdetails::runtime_stats::RuntimeStatsColl::new(None));
        let ctx = crate::StmtContext::default().with_executor_runtime_stats_source(Some(coll.clone()));
        let pause = Duration::from_millis(30);
        let tracker = {
            let dml = DmlExec::new(&plan, &ctx);
            dml.next(|| {
                dml.open(|| std::thread::sleep(pause));
                dml.close(|| std::thread::sleep(pause));
            });
            dml.next(|| ());
            dml.mem_tracker.consume_checked(100).unwrap();
            let tracker = ctx
                .statement_memory()
                .stmt_tracker()
                .search_tracker_without_lock(7)
                .expect("labelled by the plan ID");
            assert_eq!(tracker.bytes_consumed(), 100);
            tracker
        };
        assert_eq!((tracker.bytes_consumed(), tracker.max_consumed()), (0, 100));
        // The collector got one `Next` (Go `exec.Next`'s one record).
        let basic = basic_stats_of(&coll, 7);
        assert!(basic.open >= pause && basic.close >= pause, "{basic:?}");
        // The printed durations are rounded apart, so their difference may
        // fall a rounding step below zero.
        assert!(basic.time.saturating_sub(basic.open + basic.close) < pause, "{basic:?}");
        assert_eq!(basic.loops, 1);
    }

    /// `BasicRuntimeStats.String`'s fields, which Go exposes no getters for.
    #[derive(Debug)]
    struct BasicStatsText {
        time: Duration,
        open: Duration,
        close: Duration,
        loops: u64,
    }

    fn basic_stats_of(coll: &tidb_execdetails::runtime_stats::RuntimeStatsColl, plan_id: i64) -> BasicStatsText {
        use tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource as _;
        let info = coll.root_runtime_info(plan_id).expect("the plan recorded stats");
        let field = |name: &str| {
            info.split(", ")
                .find_map(|part| part.strip_prefix(&format!("{name}:")))
                .unwrap_or_else(|| panic!("no {name} in {info}"))
                .to_owned()
        };
        let duration = |text: String| {
            let (number, unit) = text
                .find(|c: char| !(c.is_ascii_digit() || c == '.'))
                .map(|at| text.split_at(at))
                .unwrap_or((&text, "s"));
            let value: f64 = number.parse().unwrap();
            let nanos = match unit {
                "ns" => value,
                "µs" => value * 1e3,
                "ms" => value * 1e6,
                "s" => value * 1e9,
                "m" => value * 6e10,
                other => panic!("unit {other} in {info}"),
            };
            Duration::from_nanos(nanos as u64)
        };
        BasicStatsText {
            time: duration(field("time")),
            open: duration(field("open")),
            close: duration(field("close")),
            loops: field("loops").parse().unwrap(),
        }
    }

    /// Go `ExplainExec.executeAnalyzeExec` registers `RURuntimeStats` on the
    /// target plan after the run and before its executors' `Close` register
    /// theirs, so `RootRuntimeStats.String` prints it right after the basic
    /// stats and before the close-registered groups.
    #[test]
    fn ru_runtime_stats_follow_basic_stats_in_execution_info() {
        use tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource as _;
        let coll = tidb_execdetails::runtime_stats::RuntimeStatsColl::new(None);
        let basic = coll.for_executor(1).unwrap();
        basic.record_next(Duration::ZERO, 0);
        basic.record_next(Duration::ZERO, 0);
        coll.register_stats(
            1,
            Box::new(tidb_execdetails::ruv2_metrics::RuRuntimeStats {
                ru_details: Some(tikv_client::RuDetails::new_with(
                    1.5,
                    1.33,
                    std::time::Duration::ZERO,
                )),
                ..Default::default()
            }),
        );
        // An apply's group, registered from its `Close`.
        coll.register_stats(
            1,
            Box::new(crate::runtime_stats_groups::ApplyJoinRuntimeStats {
                snapshot: crate::apply::native::ApplyRuntimeSnapshot::default(),
            }),
        );
        assert_eq!(
            coll.root_runtime_info(1).unwrap(),
            "time:0s, open:0s, close:0s, loops:2, RU:2.83, concurrency:OFF, cache:OFF"
        );
        // No RU details registers nothing visible.
        coll.register_stats(2, Box::new(tidb_execdetails::ruv2_metrics::RuRuntimeStats::default()));
        assert_eq!(coll.root_runtime_info(2).unwrap_or_default(), "");
    }

    #[derive(Default)]
    struct RecordedExecutorRuntimeStats {
        opens: std::sync::atomic::AtomicUsize,
        nexts: std::sync::atomic::AtomicUsize,
        rows: std::sync::atomic::AtomicI64,
        closes: std::sync::atomic::AtomicUsize,
    }

    impl tidb_execdetails::runtime_stats::ExecutorRuntimeStats for RecordedExecutorRuntimeStats {
        fn record_open(&self, _elapsed: Duration) {
            self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        fn record_next(&self, _elapsed: Duration, rows: i64) {
            self.nexts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.rows
                .fetch_add(rows, std::sync::atomic::Ordering::SeqCst);
        }

        fn record_close(&self, _elapsed: Duration) {
            self.closes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct RecordingExecutorRuntimeStatsSource {
        plan_ids: Mutex<Vec<i64>>,
        stats: Arc<RecordedExecutorRuntimeStats>,
    }

    impl tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource for RecordingExecutorRuntimeStatsSource {
        fn for_executor(
            &self,
            plan_id: i64,
        ) -> Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStats>> {
            self.plan_ids.lock().unwrap().push(plan_id);
            let stats: Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStats> = self.stats.clone();
            Some(stats)
        }
    }

    #[test]
    fn ordinary_executor_records_runtime_stats_when_collection_is_enabled() {
        let store = crate::test_store::store();
        let source = Arc::new(RecordingExecutorRuntimeStatsSource::default());
        let source_handle: Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource> = source.clone();
        let ctx =
            crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())).with_executor_runtime_stats_source(Some(source_handle));
        let plan = dual(17, 1);
        let mut executor = build(&plan, &tidb_infoschema_perfschema::mock_catalog(), &ctx).unwrap();

        executor.open().unwrap();
        let mut req = executor.new_chunk();
        executor.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 1);
        req.reset();
        executor.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        executor.close().unwrap();

        assert_eq!(*source.plan_ids.lock().unwrap(), [17]);
        assert_eq!(
            source.stats.opens.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            source.stats.nexts.load(std::sync::atomic::Ordering::SeqCst),
            2
        );
        assert_eq!(
            source.stats.rows.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            source
                .stats
                .closes
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn built_executor_checks_sql_killer_before_next() {
        let store = crate::test_store::store();
        let memory = crate::StatementMemory::default();
        let ctx = crate::StmtContext::for_query_with_memory(memory.clone()).with_txn_store(Box::new(store.clone()));
        let mut executor = build(&dual(1, 1), &tidb_infoschema_perfschema::mock_catalog(), &ctx).unwrap();
        executor.open().unwrap();
        memory
            .sql_killer()
            .send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);

        let mut req = executor.new_chunk();
        assert!(matches!(
            executor.next(&mut req),
            Err(crate::ExecError::Killed(_))
        ));
        assert_eq!(req.num_rows(), 0);
        executor.close().unwrap();
    }

    #[test]
    fn metered_executor_records_sql_killer_precheck_failures() {
        let memory = crate::StatementMemory::default();
        let plan = dual(1, 1);
        let coll = Arc::new(tidb_execdetails::runtime_stats::RuntimeStatsColl::new(None));
        let mut state = BuildState {
            executor_runtime_stats_source: Some(coll.clone()),
            ..BuildState::default()
        };
        let child = PanickingExec {
            meta: ExecutorMeta::new(empty_schema(), 1, 1, 1),
            panic_at: PanicAt::Close,
        };
        let mut executor = state.meter(&plan, Box::new(child), memory.clone());
        executor.open().unwrap();
        memory
            .sql_killer()
            .send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
        let mut req = executor.new_chunk();
        assert!(matches!(
            executor.next(&mut req),
            Err(crate::ExecError::Killed(_))
        ));

        // Go `exec.Next` records the call before its killer check fails.
        executor.close().ok();
        assert_eq!(basic_stats_of(&coll, 1).loops, 1);
    }

    struct KillOnNextExec {
        meta: ExecutorMeta,
        memory: crate::StatementMemory,
    }

    impl Executor for KillOnNextExec {
        fn open(&mut self) -> Result<(), crate::ExecError> {
            Ok(())
        }

        fn next(&mut self, _req: &mut Chunk) -> Result<(), crate::ExecError> {
            self.memory
                .sql_killer()
                .send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
            Ok(())
        }

        fn close(&mut self) -> Result<(), crate::ExecError> {
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

    #[test]
    fn executor_rechecks_sql_killer_after_next() {
        let memory = crate::StatementMemory::default();
        let child = KillOnNextExec {
            meta: ExecutorMeta::new(empty_schema(), 1, 1, 1),
            memory: memory.clone(),
        };
        let mut executor = checked_executor(Box::new(child), memory);
        executor.open().unwrap();

        let mut req = executor.new_chunk();
        assert!(matches!(
            executor.next(&mut req),
            Err(crate::ExecError::Killed(_))
        ));
        executor.close().unwrap();
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PanicAt {
        Open,
        Next,
        Close,
    }

    struct PanickingExec {
        meta: ExecutorMeta,
        panic_at: PanicAt,
    }

    impl PanickingExec {
        fn panic_if(&self, point: PanicAt) {
            if self.panic_at == point {
                std::thread::sleep(Duration::from_millis(1));
                panic!("executor panic at {point:?}");
            }
        }
    }

    impl Executor for PanickingExec {
        fn open(&mut self) -> Result<(), crate::ExecError> {
            self.panic_if(PanicAt::Open);
            Ok(())
        }

        fn next(&mut self, _req: &mut Chunk) -> Result<(), crate::ExecError> {
            self.panic_if(PanicAt::Next);
            Ok(())
        }

        fn close(&mut self) -> Result<(), crate::ExecError> {
            self.panic_if(PanicAt::Close);
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

    #[test]
    fn built_executor_recovers_open_next_and_close_panics() {
        let memory = crate::StatementMemory::default();
        let build = |panic_at| {
            checked_executor(
                Box::new(PanickingExec {
                    meta: ExecutorMeta::new(empty_schema(), 1, 1, 1),
                    panic_at,
                }),
                memory.clone(),
            )
        };

        let mut open = build(PanicAt::Open);
        assert!(matches!(
            open.open(),
            Err(crate::ExecError::Internal(message)) if message == "executor panic at Open"
        ));

        let mut next = build(PanicAt::Next);
        next.open().unwrap();
        let mut req = next.new_chunk();
        assert!(matches!(
            next.next(&mut req),
            Err(crate::ExecError::Internal(message)) if message == "executor panic at Next"
        ));
        next.close().unwrap();

        let mut close = build(PanicAt::Close);
        close.open().unwrap();
        assert!(matches!(
            close.close(),
            Err(crate::ExecError::Internal(message)) if message == "executor panic at Close"
        ));
    }

    #[test]
    fn metered_executor_records_lifecycle_time_when_child_panics() {
        let build = |panic_at| {
            let coll = Arc::new(tidb_execdetails::runtime_stats::RuntimeStatsColl::new(None));
            let mut state = BuildState {
                executor_runtime_stats_source: Some(coll.clone()),
                ..BuildState::default()
            };
            let child = PanickingExec {
                meta: ExecutorMeta::new(empty_schema(), 1, 1, 1),
                panic_at,
            };
            let executor = state.meter(&dual(1, 1), Box::new(child), crate::StatementMemory::default());
            (executor, coll)
        };

        let (mut open, open_coll) = build(PanicAt::Open);
        assert!(open.open().is_err());
        assert!(basic_stats_of(&open_coll, 1).open >= Duration::from_millis(1));

        let (mut next, next_coll) = build(PanicAt::Next);
        next.open().unwrap();
        let mut req = next.new_chunk();
        assert!(next.next(&mut req).is_err());
        let basic = basic_stats_of(&next_coll, 1);
        assert_eq!(basic.loops, 1);
        assert!(basic.time - basic.open >= Duration::from_millis(1), "{basic:?}");

        let (mut close, close_coll) = build(PanicAt::Close);
        close.open().unwrap();
        assert!(close.close().is_err());
        assert!(basic_stats_of(&close_coll, 1).close >= Duration::from_millis(1));
    }

    fn int_handles(values: &[i64]) -> Vec<TableHandle> {
        values.iter().copied().map(TableHandle::Int).collect()
    }

    fn int_values(handles: &[TableHandle]) -> Vec<i64> {
        handles
            .iter()
            .map(|handle| handle.int_value().expect("int handle"))
            .collect()
    }

    #[test]
    fn physical_shuffle_builds_worker_receivers_and_reads_each_source_once() {
        let store = crate::test_store::store();
        use tidb_planner_core_operator_physicalop::PhysicalShuffle;
        let mut projection_base = BasePhysicalPlan::with_id(7, "Projection", 0);
        projection_base.base.set_schema(Some(empty_schema()));
        projection_base.set_children(vec![dual(1, 1)]);
        let projected = PhysicalPlan::Projection(tidb_planner_core_operator_physicalop::PhysicalProjection {
            base: projection_base,
            ..Default::default()
        });
        let mut tails = Vec::new();
        for (id, source) in [(5, projected), (6, dual(2, 1))] {
            let mut base = BasePhysicalPlan::with_id(id, "Selection", 0);
            base.set_children(vec![source]);
            tails.push(PhysicalPlan::Selection(PhysicalSelection {
                base,
                ..Default::default()
            }));
        }
        let child = union(3, tails);
        let mut base = BasePhysicalPlan::with_id(4, "Shuffle", 0);
        base.set_children(vec![child]);
        let plan = PhysicalPlan::Shuffle(PhysicalShuffle {
            base,
            concurrency: 3,
            tails: vec![5, 6],
            data_sources: vec![7, 2],
            by_item_arrays: vec![vec![], vec![]],
            ..Default::default()
        });
        let mut plan = tidb_planner::post_optimize::eliminate_physical_projection(plan);
        let ids = tidb_planner_core_base::PlanIdAllocator::new();
        for _ in 0..7 {
            ids.alloc();
        }
        tidb_planner_core_operator_physicalop::shuffle::install_receivers(&mut plan, &ids).unwrap();
        let mut exec = build(&plan, &tidb_infoschema_perfschema::mock_catalog(), &crate::StmtContext::default().with_txn_store(Box::new(store.clone()))).unwrap();
        for _ in 0..2 {
            exec.open().unwrap();
            let mut rows = 0;
            loop {
                let mut chunk = exec.new_chunk();
                exec.next(&mut chunk).unwrap();
                if chunk.num_rows() == 0 {
                    break;
                }
                rows += chunk.num_rows();
            }
            exec.close().unwrap();
            assert_eq!(rows, 2);
        }
    }

    #[test]
    fn retained_batch_point_handle_order_matches_go() {
        let mut signed = int_handles(&[5, -3, 1]);
        sort_handles_for_keep_order(&mut signed, false, false);
        assert_eq!(int_values(&signed), vec![-3, 1, 5]);
        sort_handles_for_keep_order(&mut signed, true, false);
        assert_eq!(int_values(&signed), vec![5, 1, -3]);

        let mut unsigned = int_handles(&[-1, 1, i64::MIN]);
        sort_handles_for_keep_order(&mut unsigned, false, true);
        assert_eq!(int_values(&unsigned), vec![1, i64::MIN, -1]);

        let mut common = vec![
            TableHandle::Common(vec![2, 0]),
            TableHandle::Common(vec![1, 9]),
        ];
        sort_handles_for_keep_order(&mut common, false, false);
        assert_eq!(common[0], TableHandle::Common(vec![1, 9]));
    }

    fn empty_schema() -> Schema {
        Schema::new(Vec::new())
    }

    fn dual(id: i32, rows: usize) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "TableDual", 0);
        base.base.set_schema(Some(empty_schema()));
        PhysicalPlan::TableDual(PhysicalTableDual {
            base,
            row_count: rows,
        })
    }

    fn union(id: i32, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "UnionAll", 0);
        base.base.set_schema(Some(empty_schema()));
        base.set_children(children);
        PhysicalPlan::UnionAll(PhysicalUnionAll { base, mpp: false })
    }

    fn cte_table(id: i32, storage_id: i32) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "CTETable", 0);
        base.base.set_schema(Some(empty_schema()));
        PhysicalPlan::CTETable(PhysicalCTETable {
            base,
            id_for_storage: storage_id,
        })
    }

    fn cte(
        id: i32,
        storage_id: i32,
        seed: PhysicalPlan,
        recursive: Option<PhysicalPlan>,
        distinct: bool,
        limit: Option<(u64, u64)>,
    ) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "CTE", 0);
        base.base.set_schema(Some(empty_schema()));
        PhysicalPlan::CTE(PhysicalCTE {
            base,
            seed_plan: Box::new(seed),
            recursive_plan: recursive.map(Box::new),
            id_for_storage: storage_id,
            is_distinct: distinct,
            has_limit: limit.is_some(),
            limit_beg: limit.map_or(0, |limit| limit.0),
            limit_end: limit.map_or(0, |limit| limit.1),
            cte_as_name: "r".to_owned(),
            cte_name: "r".to_owned(),
        })
    }

    fn drain_row_count(mut executor: Box<dyn Executor>) -> Result<usize, crate::ExecError> {
        executor.open()?;
        let mut rows = 0;
        loop {
            let mut output = executor.new_chunk();
            executor.next(&mut output)?;
            if output.num_rows() == 0 {
                break;
            }
            rows += output.num_rows();
        }
        executor.close()?;
        Ok(rows)
    }

    #[test]
    fn physical_cte_uses_the_recursive_delta_and_go_limit_window() {
        let store = crate::test_store::store();
        let plan = cte(
            3,
            41,
            dual(1, 1),
            Some(cte_table(2, 41)),
            false,
            Some((1, 4)),
        );
        let executor = build(&plan, &tidb_infoschema_perfschema::mock_catalog(), &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())))
            .expect("build recursive physical CTE");
        assert_eq!(drain_row_count(executor).expect("run recursive CTE"), 3);
    }

    #[test]
    fn cached_physical_union_and_max_one_row_build_directly() {
        let store = crate::test_store::store();
        let catalog = tidb_infoschema_perfschema::mock_catalog();
        let ctx = crate::StmtContext::default().with_txn_store(Box::new(store.clone()));

        let mut union_executor = build(&union(3, vec![dual(1, 1), dual(2, 1)]), &catalog, &ctx)
            .expect("build cached union");
        union_executor.open().expect("open cached union");
        let mut output = union_executor.new_chunk();
        union_executor.next(&mut output).expect("first union row");
        assert_eq!(output.num_rows(), 1);
        union_executor.next(&mut output).expect("second union row");
        assert_eq!(output.num_rows(), 1);
        union_executor.next(&mut output).expect("union eof");
        assert_eq!(output.num_rows(), 0);

        let union = union(6, vec![dual(4, 1), dual(5, 1)]);
        let mut base = BasePhysicalPlan::with_id(7, "MaxOneRow", 0);
        base.base.set_schema(Some(empty_schema()));
        base.set_children(vec![union]);
        let max_one = PhysicalPlan::MaxOneRow(PhysicalMaxOneRow { base });
        let mut max_executor = build(&max_one, &catalog, &ctx).expect("build cached max-one-row");
        max_executor.open().expect("open cached max-one-row");
        let error = max_executor
            .next(&mut max_executor.new_chunk())
            .expect_err("two union rows violate MaxOneRow");
        assert!(matches!(
            error,
            crate::ExecError::Mysql(ref error)
                if error.equal(&tidb_exeerrors::ERR_SUBQUERY_MORE_THAN1_ROW)
        ));
    }

    fn long_column(name: &str, id: i64) -> tidb_table::column::Column {
        tidb_table::column::Column {
            info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                id: id,
                name: tidb_ast::CiString::new(name.to_owned()),
                field_type: FieldType::new(FieldTypeCode::LongLong),
                version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::ColumnInfo::default()
            }),
            generated: None,
            default_value: None,
            origin_default: None,
        }
    }

    fn two_long_schema() -> Schema {
        Schema::new(
            [1, 2]
                .into_iter()
                .enumerate()
                .map(|(index, id)| {
                    let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
                    column.id = id;
                    column.index = index as i64;
                    column
                })
                .collect(),
        )
    }

    fn table_scan(id: i32, table_id: i64, low: i64, high: i64) -> PhysicalPlan {
        let schema = two_long_schema();
        let mut base = BasePhysicalPlan::with_id(id, "TableScan", 0);
        base.base.set_schema(Some(schema.clone()));
        PhysicalPlan::TableScan(PhysicalTableScan {
            base,
            table_id,
            cost_columns: schema.columns,
            ranges: vec![tidb_ranger::types::Range {
                low_val: vec![tidb_datatype::Datum::Int(low)],
                high_val: vec![tidb_datatype::Datum::Int(high)],
                collators: vec![tidb_charset::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }],
            ..PhysicalTableScan::default()
        })
    }

    #[test]
    fn index_probe_hash_join_distinguishes_aliases_of_one_table() {
        let store = crate::test_store::store();
        use tidb_planner_core_operator_physicalop::{PhysicalHashJoin, PhysicalIndexJoin};
        for right_table in [40, 41] {
            let mut catalog = tidb_infoschema_perfschema::mock_catalog();
            catalog.register_kv(
                "t",
                crate::TableCommon::new(40, vec![long_column("id", 1), long_column("value", 2)]),
            );
            catalog.register_kv(
                "other",
                crate::TableCommon::new(41, vec![long_column("id", 1), long_column("value", 2)]),
            );
            let left = table_scan(1, 40, 1, 3);
            let mut right = table_scan(2, right_table, 1, 3);
            let mut right_schema = right.schema().unwrap().clone();
            for column in &mut right_schema.columns {
                column.unique_id += 10;
            }
            right.base_mut().base.set_schema(Some(right_schema.clone()));
            if let PhysicalPlan::TableScan(scan) = &mut right {
                scan.cost_columns = right_schema.columns.clone();
            }
            let key = left.schema().unwrap().columns[0].clone();
            let mut base = BasePhysicalPlan::with_id(3, "HashJoin", 0);
            let mut output = left.schema().unwrap().columns.clone();
            output.extend(right_schema.columns.clone());
            for (index, column) in output.iter_mut().enumerate() {
                column.index = index as i64;
            }
            base.base.set_schema(Some(Schema::new(output)));
            base.set_children(vec![left, right]);
            let inner = PhysicalPlan::HashJoin(PhysicalHashJoin {
                base,
                left_join_keys: vec![key.clone()],
                right_join_keys: vec![right_schema.columns[0].clone()],
                store_tp: Default::default(),
                mpp_shuffle_join: false,
                ..Default::default()
            });
            let other_key = right_schema.columns[0].clone();
            assert_eq!(index_join_lookup_child_idx(&inner, &[]), None);
            assert_eq!(
                index_join_lookup_child_idx(&inner, &[key.clone(), other_key.clone()]),
                None
            );
            for (expected_child, probe_key) in [key, other_key].into_iter().enumerate() {
                assert_eq!(
                    index_join_lookup_child_idx(&inner, std::slice::from_ref(&probe_key)),
                    Some(expected_child)
                );
                let join = PhysicalIndexJoin {
                    inner_access_table_id: Some(if expected_child == 0 { 40 } else { right_table }),
                    inner_join_keys: vec![probe_key],
                    ..Default::default()
                };
                let probes = Arc::new(Mutex::new(SharedIndexJoinProbes::default()));
                let result = build_index_inner_subtree(
                    &inner,
                    &join,
                    &[LookupProbePart::Dynamic(0)],
                    &probes,
                    &catalog,
                    &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
                    &mut BuildState::default(),
                );
                assert!(
                    result.is_ok(),
                    "the probe key identifies one alias: {:?}",
                    result.err()
                );
            }
        }
    }

    #[test]
    fn table_reader_build_records_go_stmtctx_table_access() {
        let store = crate::test_store::store();
        let table_id = 40;
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv(
            "reader",
            crate::TableCommon::new(
                table_id,
                vec![long_column("id", 1), long_column("value", 2)],
            ),
        );
        let scan = table_scan(1, table_id, 1, 1);
        let mut base = BasePhysicalPlan::with_id(2, "TableReader", 0);
        base.base.set_schema(scan.schema().cloned());
        let plan = PhysicalPlan::TableReader(PhysicalTableReader {
            base,
            table_plan: Some(Box::new(scan)),
            store_type: tidb_kv::store_type::StoreType::TiKv,
            is_common_handle: false,
            read_req_type: tidb_planner_core_operator_physicalop::physical_table_reader::ReadReqType::Cop,
            plan_part_info: None,
        });

        let session = crate::SessionMemory::new(-1, crate::OomAction::Cancel, 7);
        let cancellation = session.begin_query_cancellation();
        cancellation.cancel();
        let ctx = crate::StmtContext::for_query_with_memory(session.statement()).with_txn_store(Box::new(store.clone()));
        let _executor = build(&plan, &catalog, &ctx).expect("build physical table reader");

        assert!(tidb_exprctx::Columns::sleep_for(
            &ctx,
            std::time::Duration::from_secs(1)
        ));
        assert!(tidb_exprctx::Columns::sleep_for(
            &ctx,
            std::time::Duration::from_secs(1)
        ));
    }

    #[test]
    fn cached_physical_table_scan_emits_only_extra_handle() {
        let mut table = crate::TableCommon::new(41, vec![long_column("value", 1)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        for (row_id, value) in [(11, 100), (13, 200)] {
            table
                .insert_row_with_row_id(&mut store, &[Datum::Int(value)],
                    Some(row_id),
                    0,
                    &tidb_exprctx::NoColumns,
                )
                .expect("seed heap row");
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("heap", table);

        let mut handle = Column::new(
            tidb_model::column::EXTRA_HANDLE_ID,
            FieldType::new(FieldTypeCode::LongLong),
        );
        handle.id = tidb_model::column::EXTRA_HANDLE_ID;
        handle.index = 0;
        let mut base = BasePhysicalPlan::with_id(9, "TableScan", 0);
        base.base
            .set_schema(Some(Schema::new(vec![handle.clone()])));
        let plan = PhysicalPlan::TableScan(PhysicalTableScan {
            base,
            table_id: 41,
            cost_columns: vec![handle],
            ..PhysicalTableScan::default()
        });

        let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
        let mut executor = build(&plan, &catalog, &ctx).expect("build cached table scan");
        executor.open().expect("open cached table scan");
        let mut output = executor.new_chunk();
        executor.next(&mut output).expect("read cached table scan");
        assert_eq!(output.num_cols(), 1);
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 11);
        assert_eq!(output.get_row(1).get_int64(0), 13);
    }

    #[test]
    fn cached_physical_point_get_builds_direct_handle_read() {
        let mut table =
            crate::TableCommon::new(43, vec![long_column("id", 1), long_column("value", 2)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        table.set_pk_handle_offset(0);
        for value in 1..=3 {
            table
                .insert_row(&mut store, &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_exprctx::NoColumns,
                )
                .expect("seed clustered row");
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("point", table);

        let mut base = BasePhysicalPlan::with_id(10, "PointGet", 0);
        base.base.set_schema(Some(two_long_schema()));
        let plan = PhysicalPlan::PointGet(PointGetPlan {
            base,
            table_id: 43,
            partition: None,
            index_id: None,
            lock: false,
            access_cols: None,
            ranges: vec![tidb_ranger::types::Range {
                low_val: vec![Datum::Int(2)],
                high_val: vec![Datum::Int(2)],
                collators: vec![tidb_charset::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }],
            range_rebuild: None,
            select_lock: None,
        });

        let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
        let mut executor = build(&plan, &catalog, &ctx).expect("build cached point get");
        assert_eq!(executor.init_cap(), 1);
        assert_eq!(executor.max_chunk_size(), 1);
        executor.open().expect("open cached point get");
        let mut output = executor.new_chunk();
        executor.next(&mut output).expect("read cached point get");
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.get_row(0).get_int64(0), 2);
        assert_eq!(output.get_row(0).get_int64(1), 20);
        executor.next(&mut output).expect("point get eof");
        assert_eq!(output.num_rows(), 0);
    }

    #[test]
    fn cached_physical_point_get_resolves_unique_index_before_common_handle() {
        let mut table =
            crate::TableCommon::new(46, vec![long_column("id", 1), long_column("value", 2)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        table.set_common_handle_offsets(vec![0, 1]);
        table
            .create_index_with_context(&mut store, tidb_table::tables::Index {
                    info: tidb_model::GoShared::new(tidb_model::IndexInfo {
                        id: 8,
                        name: tidb_ast::CiString::new("value_uidx"),
                        unique: true,
                        columns: (vec![1])
                            .iter()
                            .enumerate()
                            .map(|(position, _)| tidb_model::IndexColumn {
                                length: (vec![tidb_datatype::UNSPECIFIED_LENGTH]).get(position).copied().unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                                ..tidb_model::IndexColumn::default()
                            })
                            .collect::<Vec<_>>()
                            .into(),
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::IndexInfo::default()
                    }),
                    column_offsets: vec![1],
                    clustered_primary: false,
                },
                &crate::StmtContext::for_query(),
            )
            .expect("create unique index");
        for value in 1..=3 {
            table
                .insert_row(&mut store, &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_exprctx::NoColumns,
                )
                .expect("seed common-handle row");
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("unique_point", table);

        let mut base = BasePhysicalPlan::with_id(13, "PointGet", 0);
        base.base.set_schema(Some(two_long_schema()));
        let plan = PhysicalPlan::PointGet(PointGetPlan {
            base,
            table_id: 46,
            partition: None,
            index_id: Some(8),
            lock: false,
            access_cols: None,
            ranges: vec![tidb_ranger::types::Range {
                low_val: vec![Datum::Int(20)],
                high_val: vec![Datum::Int(20)],
                collators: vec![tidb_charset::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }],
            range_rebuild: None,
            select_lock: None,
        });

        let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
        let mut executor = build(&plan, &catalog, &ctx).expect("build unique point get");
        executor.open().expect("open unique point get");
        let mut output = executor.new_chunk();
        executor.next(&mut output).expect("read unique point get");
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.get_row(0).get_int64(0), 2);
        assert_eq!(output.get_row(0).get_int64(1), 20);
        executor.next(&mut output).expect("unique point get eof");
        assert_eq!(output.num_rows(), 0);
    }

    #[test]
    fn cached_physical_batch_point_get_deduplicates_and_orders_handles() {
        let mut table =
            crate::TableCommon::new(44, vec![long_column("id", 1), long_column("value", 2)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        table.set_pk_handle_offset(0);
        for value in 1..=3 {
            table
                .insert_row(&mut store, &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_exprctx::NoColumns,
                )
                .expect("seed clustered row");
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("batch", table);

        let mut base = BasePhysicalPlan::with_id(11, "BatchPointGet", 0);
        base.base.set_schema(Some(two_long_schema()));
        let point_range = |value| tidb_ranger::types::Range {
            low_val: vec![Datum::Int(value)],
            high_val: vec![Datum::Int(value)],
            collators: vec![tidb_charset::Collation::Binary],
            low_exclude: false,
            high_exclude: false,
        };
        let plan = PhysicalPlan::BatchPointGet(BatchPointGetPlan {
            base,
            table_id: 44,
            index_id: None,
            access_cols: None,
            unsigned_handle: true,
            ranges: [3, 1, 3, 9].into_iter().map(point_range).collect(),
            partition_ids: None,
            partition_names: Vec::new(),
            range_rebuild: None,
            keep_order: true,
            desc: false,
            select_lock: None,
            lock: false,
});

        let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
        let mut executor = build(&plan, &catalog, &ctx).expect("build cached batch point get");
        executor.open().expect("open cached batch point get");
        let mut output = executor.new_chunk();
        executor
            .next(&mut output)
            .expect("read cached batch point get");
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 1);
        assert_eq!(output.get_row(1).get_int64(0), 3);
    }

    #[test]
    fn cached_physical_batch_point_get_uses_batched_unique_index_lookup() {
        let mut table =
            crate::TableCommon::new(45, vec![long_column("id", 1), long_column("value", 2)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        table.set_pk_handle_offset(0);
        table
            .create_index_with_context(&mut store, tidb_table::tables::Index {
                    info: tidb_model::GoShared::new(tidb_model::IndexInfo {
                        id: 8,
                        name: tidb_ast::CiString::new("value_uidx"),
                        unique: true,
                        columns: (vec![1])
                            .iter()
                            .enumerate()
                            .map(|(position, _)| tidb_model::IndexColumn {
                                length: (vec![tidb_datatype::UNSPECIFIED_LENGTH]).get(position).copied().unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                                ..tidb_model::IndexColumn::default()
                            })
                            .collect::<Vec<_>>()
                            .into(),
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::IndexInfo::default()
                    }),
                    column_offsets: vec![1],
                    clustered_primary: false,
                },
                &crate::StmtContext::for_query(),
            )
            .expect("create unique index");
        for value in 1..=3 {
            table
                .insert_row(&mut store, &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_exprctx::NoColumns,
                )
                .expect("seed unique-index row");
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("unique_batch", table);

        let mut base = BasePhysicalPlan::with_id(12, "BatchPointGet", 0);
        base.base.set_schema(Some(two_long_schema()));
        let point_range = |value| tidb_ranger::types::Range {
            low_val: vec![Datum::Int(value)],
            high_val: vec![Datum::Int(value)],
            collators: vec![tidb_charset::Collation::Binary],
            low_exclude: false,
            high_exclude: false,
        };
        let plan = PhysicalPlan::BatchPointGet(BatchPointGetPlan {
            base,
            table_id: 45,
            index_id: Some(8),
            access_cols: None,
            unsigned_handle: false,
            ranges: [30, 10, 30, 90].into_iter().map(point_range).collect(),
            partition_ids: None,
            partition_names: Vec::new(),
            range_rebuild: None,
            keep_order: true,
            desc: false,
            select_lock: None,
            lock: false,
});

        let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
        let mut executor = build(&plan, &catalog, &ctx).expect("build unique batch point get");
        executor.open().expect("open unique batch point get");
        let mut output = executor.new_chunk();
        executor
            .next(&mut output)
            .expect("read unique batch point get");
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 1);
        assert_eq!(output.get_row(1).get_int64(0), 3);
    }

    fn index_scan(id: i32, table_id: i64, index_id: i64, low: i64, high: i64) -> PhysicalPlan {
        let schema = two_long_schema();
        let mut base = BasePhysicalPlan::with_id(id, "IndexScan", 0);
        base.base.set_schema(Some(schema.clone()));
        PhysicalPlan::IndexScan(PhysicalIndexScan {
            base,
            table_id,
            cost_columns: schema.columns,
            index_id,
            index_name: "value_idx".to_owned(),
            ranges: vec![tidb_ranger::types::Range {
                low_val: vec![tidb_datatype::Datum::Int(low)],
                high_val: vec![tidb_datatype::Datum::Int(high)],
                collators: vec![tidb_charset::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }],
            ..PhysicalIndexScan::default()
        })
    }

    fn table_selection(id: i32, child: PhysicalPlan, minimum_value: i64) -> PhysicalPlan {
        let schema = two_long_schema();
        let mut value = schema.columns[1].clone();
        value.index = 1;
        let condition = tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "gt",
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(value),
                Expression::Constant(tidb_expr::constant::Constant::new(
                    Datum::Int(minimum_value),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        ).unwrap();
        let mut base = BasePhysicalPlan::with_id(id, "Selection", 0);
        base.base.set_schema(Some(schema));
        base.set_children(vec![child]);
        PhysicalPlan::Selection(PhysicalSelection {
            base,
            conditions: vec![condition],
            from_data_source: true,
        })
    }

    #[test]
    fn index_merge_partial_batches_grow_across_chunks_and_reset() {
        let mut table =
            crate::TableCommon::new(42, vec![long_column("id", 1), long_column("value", 2)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        table.set_pk_handle_offset(0);
        for id in 1..=19 {
            table
                .insert_row_with_row_id(&mut store, &[Datum::Int(id), Datum::Int(id * 10)],
                    None,
                    0,
                    &tidb_exprctx::NoColumns,
                )
                .unwrap();
        }
        table
            .create_index_with_context(&mut store, tidb_table::tables::Index {
                    info: tidb_model::GoShared::new(tidb_model::IndexInfo {
                        id: 7,
                        name: tidb_ast::CiString::new("value_idx"),
                        columns: (vec![1])
                            .iter()
                            .enumerate()
                            .map(|(position, _)| tidb_model::IndexColumn {
                                length: (vec![tidb_datatype::UNSPECIFIED_LENGTH]).get(position).copied().unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                                ..tidb_model::IndexColumn::default()
                            })
                            .collect::<Vec<_>>()
                            .into(),
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::IndexInfo::default()
                    }),
                    column_offsets: vec![1],
                    clustered_primary: false,
                },
                &crate::StmtContext::for_query(),
            )
            .unwrap();
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("im", table.clone());
        let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())).with_executor_chunk_sizes(2, 2);
        for quota in [1, -1] {
            let memory = crate::StatementMemory::new(quota, crate::OomAction::Cancel, 7);
            let memory_ctx = crate::StmtContext::for_query_with_memory(memory.clone())
                .with_txn_store(Box::new(store.clone()))
                .with_executor_chunk_sizes(2, 2);
            let executor = build(&table_scan(10, 42, 1, 19), &catalog, &memory_ctx).unwrap();
            let scratch_bytes = executor.new_chunk().memory_usage();
            let mut source = ExecutorPartialHandleSource::new(
                executor,
                table.clone(),
                RowDecodeContext::for_query(&memory_ctx),
                PartialHandleColumns::Int(0),
                vec![1],
            )
            .with_batch_sizes(5, 5);
            source.open().unwrap();
            let batch = source.next_batch();
            if quota == 1 {
                assert!(
                    matches!(
                        batch,
                        Err(crate::ExecError::MemoryExceedForQuery { conn_id: 7 })
                    ),
                    "{batch:?}"
                );
            } else {
                assert_eq!(batch.unwrap().unwrap().handles.len(), 5);
                assert_eq!(memory.stmt_tracker().max_consumed(), 3 * scratch_bytes);
                // Go's zero-valued deferred release keeps each non-empty
                // Next's chunk charged: one per two-row chunk.
                let mut chunks = 3;
                while let Some(batch) = source.next_batch().unwrap() {
                    chunks += batch.handles.len().div_ceil(2) as i64;
                    assert_eq!(memory.stmt_tracker().bytes_consumed(), chunks * scratch_bytes);
                }
                assert_eq!(chunks, 11);
            }
            if quota == 1 {
                assert_eq!(memory.stmt_tracker().max_consumed(), scratch_bytes);
            }
            source.close().unwrap();
        }
        // An ORDER BY handle still occupies a separate key slot before the
        // handle suffix in Go's index task. Table tasks prune that suffix.
        for index_layout in [false, true] {
            let executor = build(&table_scan(10, 42, 1, 19), &catalog, &ctx).unwrap();
            let mut source = ExecutorPartialHandleSource::new(
                executor,
                table.clone(),
                RowDecodeContext::for_query(&ctx),
                PartialHandleColumns::Int(0),
                vec![0],
            )
            .with_batch_sizes(5, 5);
            if index_layout {
                source = source.with_index_row_layout(None);
            }
            source.open().unwrap();
            let batch = source.next_batch_with_max_handles(3).unwrap().unwrap();
            let crate::index_merge_reader::MergeSortKeys::Typed { chunk, fields } =
                &batch.sort_keys
            else {
                panic!("expected typed task rows");
            };
            assert_eq!(chunk.capacity(), 5);
            assert_eq!(chunk.num_cols(), if index_layout { 2 } else { 1 });
            assert_eq!(fields.len(), chunk.num_cols());
            for row in 0..3 {
                let value = Datum::Int(row as i64 + 1);
                assert_eq!(batch.sort_keys.values(row), vec![value; chunk.num_cols()]);
            }
            source.close().unwrap();
        }
        for (initial, expected) in [(2, vec![2, 4, 5, 5, 3]), (5, vec![5, 5, 5, 4])] {
            let executor = build(&table_scan(10, 42, 1, 19), &catalog, &ctx).unwrap();
            let source = ExecutorPartialHandleSource::new(
                executor,
                table.clone(),
                RowDecodeContext::for_query(&ctx),
                PartialHandleColumns::Int(0),
                vec![1],
            )
            .with_batch_sizes(initial, 5);
            let index_source = IndexPartialHandleSource {
                source: IndexRangeSourceExec::new_with_statement(
                    ExecutorMeta::new(two_long_schema(), 10, 2, 2),
                    table.clone(),
                    Box::new(store.clone()),
                    7,
                    tidb_ranger::points::full_range(),
                    RowDecodeContext::for_query(&ctx),
                    crate::remote_scan::pushdown_statement_context(&ctx),
                ),
                partition_indexes: vec![0],
                table: table.clone(),
                stats: None,
                ctx: ctx.clone(),
                index_id: 7,
                reported: false,
                initial_batch_size: initial,
                batch_size: 0,
                max_batch_size: 5,
                memory_label: 1,
                chunk_field_types: vec![FieldType::new(FieldTypeCode::LongLong)],
                max_chunk_size: 1024,
                memory: None,
                chunk: None,
            };
            let sources: Vec<(Box<dyn PartialHandleSource>, bool)> =
                vec![(Box::new(source), true), (Box::new(index_source), false)];
            for (mut source, ordered) in sources {
                for _ in 0..2 {
                    source.open().unwrap();
                    let mut sizes = Vec::new();
                    let mut ids = Vec::new();
                    while let Some(batch) = source.next_batch().unwrap() {
                        sizes.push(batch.handles.len());
                        for (index, handle) in batch.handles.iter().enumerate() {
                            let id = handle.handle.int_value().unwrap();
                            if ordered {
                                assert_eq!(
                                    batch.sort_keys.values(index),
                                    vec![Datum::Int(id * 10)]
                                );
                            } else {
                                assert!(batch.sort_keys.is_empty());
                            }
                            ids.push(id);
                        }
                    }
                    assert_eq!(sizes, expected);
                    assert_eq!(ids, (1..=19).collect::<Vec<_>>());
                    source.close().unwrap();
                }
                source.open().unwrap();
                let batch = source.next_batch_with_max_handles(3).unwrap().unwrap();
                assert_eq!(batch.handles.len(), initial.min(3));
                if ordered {
                    let crate::index_merge_reader::MergeSortKeys::Typed { chunk, fields } =
                        &batch.sort_keys
                    else {
                        panic!("partial source discarded typed key storage");
                    };
                    // Go allocates retChk at the adaptive task capacity,
                    // even when the pushed budget requests fewer rows.
                    assert_eq!(chunk.capacity(), initial);
                    assert_eq!(chunk.num_rows(), batch.handles.len());
                    assert_eq!(fields.len(), 1);
                }
                assert_eq!(
                    batch
                        .handles
                        .iter()
                        .map(|handle| handle.handle.int_value().unwrap())
                        .collect::<Vec<_>>(),
                    (1..=initial.min(3) as i64).collect::<Vec<_>>()
                );
                assert!(source.next_batch_with_max_handles(0).unwrap().is_none());
                source.close().unwrap();
            }
        }
    }

    #[test]
    fn index_merge_partial_limit_counts_handles_before_union_dedup_only() {
        let mut table =
            crate::TableCommon::new(42, vec![long_column("id", 1), long_column("value", 2)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        table.set_pk_handle_offset(0);
        for id in 1..=19 {
            table
                .insert_row_with_row_id(&mut store, &[Datum::Int(id), Datum::Int(id * 10)],
                    None,
                    0,
                    &tidb_exprctx::NoColumns,
                )
                .unwrap();
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("im", table);
        for (intersection, ordered) in [(false, false), (false, true), (true, false)] {
            let partials = vec![table_scan(10, 42, 1, 19), table_scan(11, 42, 1, 19)];
            let mut base = BasePhysicalPlan::with_id(30, "IndexMerge", 0);
            base.base.set_schema(Some(two_long_schema()));
            let plan = PhysicalPlan::IndexMergeReader(PhysicalIndexMergeReader {
                base,
                partial_plans_raw: partials,
                table_plan: Some(Box::new(table_scan(20, 42, 1, 19))),
                is_intersection_type: intersection,
                by_items: if ordered {
                    vec![tidb_expression_aggregation::ByItems::new(
                        Expression::Column(two_long_schema().columns[1].clone()),
                        false,
                    )]
                } else {
                    vec![]
                },
                pushed_limit: Some(tidb_planner_core_operator_physicalop::PushedDownLimit {
                    offset: 3,
                    count: 2,
                }),
                ..Default::default()
            });
            let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())).with_executor_chunk_sizes(6, 6);
            let mut state = BuildState::default();
            let mut executor = build_with_state(&plan, &catalog, &ctx, &mut state).unwrap();
            executor.open().unwrap();
            let mut ids = Vec::new();
            loop {
                let mut chunk = executor.new_chunk();
                executor.next(&mut chunk).unwrap();
                if chunk.num_rows() == 0 {
                    break;
                }
                ids.extend((0..chunk.num_rows()).map(|row| chunk.get_row(row).get_int64(0)));
            }
            executor.close().unwrap();
            if intersection {
                assert_eq!(ids.len(), 2);
                assert!(ids.iter().all(|id| (1..=19).contains(id)));
                assert_ne!(ids[0], ids[1]);
            } else {
                assert_eq!(ids, vec![4, 5]);
            }
            let PhysicalPlan::IndexMergeReader(reader) = &plan else {
                unreachable!()
            };
            for (index, partial) in reader.partial_plans_raw.iter().enumerate() {
                let rows = state.index_merge_partial_rows[&partial.id()].get();
                let expected = if intersection {
                    19
                } else if ordered || index == 0 {
                    5
                } else {
                    0
                };
                assert_eq!(
                    rows, expected,
                    "intersection={intersection}, ordered={ordered}, partial={index}"
                );
            }
        }
    }

    #[test]
    fn union_merge_runs_ahead_of_its_table_tasks_like_go() {
        let mut table =
            crate::TableCommon::new(42, vec![long_column("id", 1), long_column("value", 2)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        table.set_pk_handle_offset(0);
        for id in 1..=19 {
            table
                .insert_row_with_row_id(&mut store, &[Datum::Int(id), Datum::Int(id * 10)],
                    None,
                    0,
                    &tidb_exprctx::NoColumns,
                )
                .unwrap();
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("im", table);
        let partials = vec![table_scan(10, 42, 1, 19), table_scan(11, 42, 1, 19)];
        let keys = partials.iter().map(PhysicalPlan::id).collect::<Vec<_>>();
        let mut base = BasePhysicalPlan::with_id(30, "IndexMerge", 0);
        base.base.set_schema(Some(two_long_schema()));
        let merge = PhysicalPlan::IndexMergeReader(PhysicalIndexMergeReader {
            base,
            partial_plans_raw: partials,
            table_plan: Some(Box::new(table_selection(21, table_scan(20, 42, 1, 19), 20))),
            ..Default::default()
        });
        let mut base = BasePhysicalPlan::with_id(31, "Limit", 0);
        base.base.set_schema(Some(two_long_schema()));
        base.set_children(vec![merge]);
        let plan = PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
            base,
            count: 1,
            ..Default::default()
        });
        let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())).with_executor_chunk_sizes(6, 6);
        let mut state = BuildState::default();
        let mut executor = build_with_state(&plan, &catalog, &ctx, &mut state).unwrap();
        for _ in 0..2 {
            executor.open().unwrap();
            let mut chunk = executor.new_chunk();
            executor.next(&mut chunk).unwrap();
            assert_eq!(chunk.num_rows(), 1);
            assert_eq!(chunk.get_row(0).get_int64(0), 3);
            executor.close().unwrap();
            let counters = &state.index_merge_partial_rows;
            // Go's process worker runs ahead of the table workers by their
            // count plus `workCh`'s one slot, so even under an outer LIMIT 1
            // both partials' few tasks are fetched before the first table
            // task runs.
            assert_eq!(counters[&keys[0]].get(), 19);
            assert_eq!(counters[&keys[1]].get(), 19);
        }
    }

    #[test]
    fn index_merge_table_tasks_execute_retained_operator_order() {
        let mut table =
            crate::TableCommon::new(42, vec![long_column("id", 1), long_column("value", 2)]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        table.set_pk_handle_offset(0);
        for id in 1..=6 {
            table
                .insert_row_with_row_id(&mut store, &[Datum::Int(id), Datum::Int(id * 10)],
                    None,
                    0,
                    &tidb_exprctx::NoColumns,
                )
                .unwrap();
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("im", table);
        for batch_size in [3, 20_000] {
            for topn in [false, true] {
                for limit_above_filter in [false, true] {
                    let scan = table_scan(20, 42, i64::MIN, i64::MAX);
                    let child = if limit_above_filter {
                        table_selection(21, scan, 20)
                    } else {
                        scan
                    };
                    let mut base = BasePhysicalPlan::with_id(22, "Limit", 0);
                    base.base.set_schema(Some(two_long_schema()));
                    base.set_children(vec![child]);
                    let limit = if topn {
                        let mut value = two_long_schema().columns[1].clone();
                        value.index = 1;
                        PhysicalPlan::TopN(tidb_planner_core_operator_physicalop::PhysicalTopN {
                            base,
                            offset: 1,
                            count: 2,
                            by_items: vec![tidb_expression_aggregation::ByItems::new(
                                Expression::Column(value),
                                true,
                            )],
                            ..Default::default()
                        })
                    } else {
                        PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
                            base,
                            offset: 0,
                            count: 2,
                            ..Default::default()
                        })
                    };
                    let table_plan = if limit_above_filter {
                        limit
                    } else {
                        table_selection(21, limit, 20)
                    };
                    let mut partial = table_scan(10, 42, 1, 6);
                    partial
                        .base_mut()
                        .base
                        .set_schema(Some(Schema::new(
                            vec![two_long_schema().columns[0].clone()],
                        )));
                    let mut base = BasePhysicalPlan::with_id(30, "IndexMerge", 0);
                    base.base.set_schema(Some(two_long_schema()));
                    let plan = PhysicalPlan::IndexMergeReader(PhysicalIndexMergeReader {
                        base,
                        partial_plans_raw: vec![partial],
                        table_plan: Some(Box::new(table_plan)),
                        ..Default::default()
                    });
                    let mut env = tidb_planner::find_best_task::coster::CostEnv::default();
                    env.session.index_lookup_size = batch_size as f64;
                    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()))
                        .with_optimizer_cost_env(env)
                        .with_executor_chunk_sizes(6, 6);
                    let mut executor = build(&plan, &catalog, &ctx).unwrap();
                    executor.open().unwrap();
                    let mut ids = Vec::new();
                    loop {
                        let mut chunk = executor.new_chunk();
                        executor.next(&mut chunk).unwrap();
                        if chunk.num_rows() == 0 {
                            break;
                        }
                        ids.extend(
                            (0..chunk.num_rows()).map(|row| chunk.get_row(row).get_int64(0)),
                        );
                    }
                    executor.close().unwrap();
                    let expected = match (topn, limit_above_filter, batch_size) {
                        (true, _, _) => vec![5, 4],
                        (false, true, 3) => vec![3, 4, 5],
                        (false, true, _) => vec![3, 4],
                        (false, false, 3) => vec![4, 5],
                        _ => vec![],
                    };
                    assert_eq!(
                        ids, expected,
                        "TopN={topn}, above_filter={limit_above_filter}, batch={batch_size}"
                    );
                }
            }
        }
    }

    #[test]
    fn cached_physical_index_merge_builds_from_retained_partial_trees() {
        // Go TestIdexMerge: exercise the retained reader, not a cost-selected
        // table scan that can accidentally hide a broken index-merge builder.
        for (partitioned, global) in [(false, false), (true, false), (true, true)] {
            for handle_kind in ["integer", "common", "heap"] {
                let mut table =
                    crate::TableCommon::new(42, vec![long_column("id", 1), long_column("value", 2)]);
                let mut store = tidb_kv::storage::MemTableStorage::new();
                match handle_kind {
                    "integer" => table.set_pk_handle_offset(0),
                    "common" => table.set_common_handle_offsets(vec![0]),
                    _ => {}
                }
                if partitioned {
                    table.set_partition(tidb_table::partition_routing::PartitionExpr {
                        kind: tidb_table::partition_routing::PartitionKind::Hash,
                        expr: Expression::Column(two_long_schema().columns[0].clone()),
                        dependencies: vec!["id".to_owned()],
                    });
                    table.meta().write().partition = Some(tidb_model::GoShared::new(
                        tidb_model::partition::PartitionInfo {
                            enable: true,
                            definitions: (0..2)
                            .map(|ordinal| tidb_model::partition::PartitionDefinition {
                                id: 101 + ordinal,
                                name: tidb_ast::CiString::new(format!("p{ordinal}")),
                                ..Default::default()
                            })
                            .collect::<Vec<_>>()
                                .into(),
                            ..Default::default()
                        },
                    ));
                }
                table
                    .create_index_with_context(&mut store, tidb_table::tables::Index {
                            info: tidb_model::GoShared::new(tidb_model::IndexInfo {
                                id: 7,
                                name: tidb_ast::CiString::new("value_idx"),
                                global: global,
                                columns: (vec![1])
                                    .iter()
                                    .enumerate()
                                    .map(|(position, _)| tidb_model::IndexColumn {
                                        length: (vec![tidb_datatype::UNSPECIFIED_LENGTH]).get(position).copied().unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                                        ..tidb_model::IndexColumn::default()
                                    })
                                    .collect::<Vec<_>>()
                                    .into(),
                                state: tidb_model::SchemaState::PUBLIC,
                                ..tidb_model::IndexInfo::default()
                            }),
                            column_offsets: vec![1],
                            clustered_primary: false,
                        },
                        &crate::StmtContext::for_query(),
                    )
                    .expect("create secondary index");
                for value in 1..=4 {
                    table
                        .insert_row_with_row_id(&mut store, &[Datum::Int(value), Datum::Int(value * 10)],
                            // The same hidden handle in different partitions
                            // must survive dedup AND fetch the correct record.
                            (partitioned && handle_kind == "heap").then_some((value + 1) / 2),
                            0,
                            &tidb_exprctx::NoColumns,
                        )
                        .expect("seed row");
                }
                let mut catalog = tidb_infoschema_perfschema::mock_catalog();
                catalog.register_kv("im", table);

                for table_partial in [false, true] {
                    let mut base = BasePhysicalPlan::with_id(13, "IndexMerge", 0);
                    base.base.set_schema(Some(two_long_schema()));
                    let mut partial_plans_raw =
                        vec![index_scan(10, 42, 7, 10, 30), index_scan(11, 42, 7, 30, 40)];
                    for partial in &mut partial_plans_raw {
                        let mut columns = vec![two_long_schema().columns[1].clone()];
                        let handle = if handle_kind == "heap" {
                            let mut column =
                                Column::new(3, FieldType::new(FieldTypeCode::LongLong));
                            column.id = tidb_model::column::EXTRA_HANDLE_ID;
                            column
                        } else {
                            two_long_schema().columns[0].clone()
                        };
                        columns.push(handle);
                        if global {
                            let mut column =
                                Column::new(4, FieldType::new(FieldTypeCode::LongLong));
                            column.id = tidb_model::column::EXTRA_PHYS_TBL_ID;
                            columns.push(column);
                        }
                        partial
                            .base_mut()
                            .base
                            .set_schema(Some(Schema::new(columns)));
                    }
                    if table_partial {
                        let handle = partial_plans_raw[1].schema().unwrap().columns[1].clone();
                        // A full handle range: a partial table scan with no
                        // ranges reads no keys, as Go's partial worker does.
                        let mut partial = table_scan(11, 42, i64::MIN, i64::MAX);
                        if let PhysicalPlan::TableScan(scan) = &mut partial {
                            scan.base.base.set_schema(Some(Schema::new(vec![handle])));
                        }
                        partial_plans_raw[1] = partial;
                    }
                    let mut plan = PhysicalPlan::IndexMergeReader(PhysicalIndexMergeReader {
                        base,
                        partial_plans_raw,
                        table_plan: Some(Box::new(table_selection(
                            12,
                            table_scan(14, 42, i64::MIN, i64::MAX),
                            20,
                        ))),
                        ..PhysicalIndexMergeReader::default()
                    });

                    for order in [None, Some(false), Some(true)] {
                        let ordered = order.is_some();
                        if ordered && table_partial {
                            continue;
                        }
                        if let PhysicalPlan::IndexMergeReader(reader) = &mut plan {
                            reader.by_items = if ordered {
                                vec![tidb_expression_aggregation::ByItems::new(
                                    Expression::Column(two_long_schema().columns[1].clone()),
                                    order.unwrap(),
                                )]
                            } else {
                                Vec::new()
                            };
                            let mut output = two_long_schema();
                            if ordered && partitioned {
                                // Go BuildIndexMergeTableScan adds this to the
                                // final fetch as well as the partial workers.
                                let mut column =
                                    Column::new(4, FieldType::new(FieldTypeCode::LongLong));
                                column.id = tidb_model::column::EXTRA_PHYS_TBL_ID;
                                output.columns.push(column);
                            }
                            reader.base.base.set_schema(Some(output.clone()));
                            // A Selection preserves its scan's columns. Keep
                            // the partition-ID slot in both retained schemas.
                            let table_plan = reader.table_plan.as_mut().unwrap();
                            table_plan.base_mut().base.set_schema(Some(output.clone()));
                            table_plan.base_mut().children_mut()[0]
                                .base_mut()
                                .base
                                .set_schema(Some(output));
                            for partial in &mut reader.partial_plans_raw {
                                if let PhysicalPlan::IndexScan(scan) = partial {
                                    scan.keep_order = ordered;
                                    scan.desc = order.unwrap_or(false);
                                }
                            }
                        }
                        for selection in [None, Some("p1"), Some("absent")] {
                            if selection.is_some() && !partitioned {
                                continue;
                            }
                            if let PhysicalPlan::IndexMergeReader(reader) = &mut plan {
                                for partial in &mut reader.partial_plans_raw {
                                    let access = selection.map(|name| {
                                        tidb_planner_core_access::DynamicPartitionAccessObject {
                                            partitions: vec![name.into()],
                                            ..Default::default()
                                        }
                                    });
                                    match partial {
                                        PhysicalPlan::IndexScan(scan) => {
                                            scan.dynamic_partition_access = access
                                        }
                                        PhysicalPlan::TableScan(scan) => {
                                            scan.dynamic_partition_access = access
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                            }
                            let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
                            let mut state = BuildState::default();
                            let mut executor = build_with_state(&plan, &catalog, &ctx, &mut state)
                                .expect("build cached index merge");
                            executor.open().expect("open cached index merge");
                            let mut rows = Vec::new();
                            loop {
                                let mut output = executor.new_chunk();
                                executor.next(&mut output).expect("read cached index merge");
                                if output.num_rows() == 0 {
                                    break;
                                }
                                for row in 0..output.num_rows() {
                                    rows.push((
                                        output.get_row(row).get_int64(0),
                                        output.get_row(row).get_int64(1),
                                        if ordered && partitioned {
                                            output.get_row(row).get_int64(2)
                                        } else {
                                            0
                                        },
                                    ));
                                }
                            }
                            if !ordered {
                                rows.sort_unstable();
                            }
                            let expected = match selection {
                                None => vec![(3, 30), (4, 40)],
                                Some("p1") => vec![(3, 30)],
                                _ => Vec::new(),
                            };
                            let mut expected: Vec<_> = expected
                                .into_iter()
                                .map(|(id, value)| {
                                    (
                                        id,
                                        value,
                                        if ordered && partitioned {
                                            101 + id % 2
                                        } else {
                                            0
                                        },
                                    )
                                })
                                .collect();
                            if order == Some(true) {
                                expected.reverse();
                            }
                            assert_eq!(
                                rows, expected,
                                "{handle_kind}, partitioned={partitioned}, global={global}, selection={selection:?}"
                            );
                            executor.close().unwrap();
                            if let PhysicalPlan::IndexMergeReader(reader) = &plan {
                                for partial in &reader.partial_plans_raw {
                                    let count = state.index_merge_partial_rows[&partial.id()].get();
                                    assert_eq!(count > 0, selection != Some("absent"));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
