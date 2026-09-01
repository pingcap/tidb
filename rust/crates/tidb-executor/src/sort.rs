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

//! `pkg/executor/sortexec` `SortExec`: the `ORDER BY` operator.
//!
//! Go's default parallel path: the first `Next` fetches child chunks into
//! bounded persistent-pool worker lanes, each worker sorts batches of at most
//! `maxChunkSize * 30` rows and locally K-way merges them, and the result path
//! heap-merges one run per worker. OOM coordinates whole-worker spill rounds
//! through [`crate::parallel_sort_spill_helper`]. The explicit serial path is
//! retained for Go's `IsUnparallel` tests and uses [`crate::sort_partition`].
//!
//! Null ordering matches Go `chunk.cmpNull`: NULL compares below every
//! non-NULL value, and a descending by-item negates the whole comparison --
//! so NULLs come first ascending and last descending.
//!
//! The worker/fetcher pipeline, spill-to-disk partitions, K-way mergers, and
//! memory/disk trackers are active. Go failpoints and the random worker-fault
//! injection hooks remain test-harness-only and are not production behavior.
//!
//! Row comparison is `tidb_expr::compare_datums` — the shared,
//! collation-aware datum comparator (Go `types/datum.go` `Datum.Compare`
//! via `pkg/util/chunk/compare.go` `GetCompareFunc`). A comparison error
//! (an unorderable key kind) is captured during the sort and returned from
//! `Next`, as Go returns it from `Next`.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::compare::ColumnCompareFunc;
use tidb_chunk::row::{OwnedRow, Row};
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_util::memory::{ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY};

use crate::mem_quota::StatementMemory;
use crate::parallel_sort_spill_helper::{LocalSortWorker, ParallelSortSpillHelper};
use crate::sort_partition::{spill_action, SortPartition, SPILL_CHUNK_SIZE};

/// Go `planner/util.ByItems`: one `ORDER BY` item -- the key expression and
/// its direction.
#[derive(Clone)]
pub struct SortByItem {
    /// Go `ByItems.Expr`.
    pub expr: Expression,
    /// Go `ByItems.Desc`.
    pub desc: bool,
}

#[derive(Clone)]
struct MergeHead {
    partition_id: usize,
    key: Vec<Datum>,
}

/// Go `parallelSortWorker`: one bounded input lane, its locally sorted
/// batches, and the memory charged by the fetcher on its behalf.
struct ParallelSortWorker<C: Columns> {
    field_types: Vec<FieldType>,
    detached_tracker: Arc<Tracker>,
    spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
    spill_chunk_size: usize,
    by_items: Vec<SortByItem>,
    compare_funcs: Vec<Option<ColumnCompareFunc>>,
    ctx: C,
    batches: Vec<SortPartition>,
    current: SortPartition,
    max_sorted_rows: usize,
    total_memory_usage: i64,
}

impl<C> ParallelSortWorker<C>
where
    C: Columns,
{
    fn new(
        field_types: Vec<FieldType>,
        spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
        spill_chunk_size: usize,
        by_items: Vec<SortByItem>,
        ctx: C,
        max_chunk_size: usize,
    ) -> Self {
        // The fetcher charges the sort tracker before dispatch, exactly as Go
        // does. Worker partitions therefore account below a detached tracker
        // solely to keep their own release bookkeeping intact.
        let detached_tracker = Tracker::new(0, -1);
        let mut current = SortPartition::new(
            field_types.clone(),
            &detached_tracker,
            Arc::clone(&spill_storage),
        );
        current.set_spill_chunk_size(spill_chunk_size);
        let compare_funcs = compile_compare_funcs(&by_items);
        Self {
            field_types,
            detached_tracker,
            spill_storage,
            spill_chunk_size,
            by_items,
            compare_funcs,
            ctx,
            batches: Vec::new(),
            current,
            max_sorted_rows: max_chunk_size.saturating_mul(30).max(1),
            total_memory_usage: 0,
        }
    }

    fn fresh_partition(&self) -> SortPartition {
        let mut partition = SortPartition::new(
            self.field_types.clone(),
            &self.detached_tracker,
            Arc::clone(&self.spill_storage),
        );
        partition.set_spill_chunk_size(self.spill_chunk_size);
        partition
    }

    fn finish_batch(&mut self) -> Result<(), ExecError> {
        if self.current.num_rows() == 0 {
            return Ok(());
        }
        self.current
            .sort(&self.by_items, &self.compare_funcs, &self.ctx)?;
        let next = self.fresh_partition();
        self.batches
            .push(std::mem::replace(&mut self.current, next));
        Ok(())
    }

    fn add_chunk(&mut self, chunk: Chunk, memory_usage: i64) -> Result<(), ExecError> {
        self.total_memory_usage = self.total_memory_usage.saturating_add(memory_usage);
        self.current.add(chunk);
        if self.current.num_rows() >= self.max_sorted_rows {
            self.finish_batch()?;
        }
        Ok(())
    }

    fn sort_local_rows(&mut self) -> Result<Vec<OwnedRow>, ExecError> {
        let Some(mut run) = self.sort_local_partition()? else {
            return Ok(Vec::new());
        };
        Ok(run.take_sorted_owned_rows())
    }

    fn sort_local_partition(&mut self) -> Result<Option<SortPartition>, ExecError> {
        self.finish_batch()?;
        SortPartition::merge_sorted_in_memory(
            std::mem::take(&mut self.batches),
            &self.by_items,
            &self.compare_funcs,
            &self.ctx,
        )
    }

    fn take_total_memory_usage(&mut self) -> i64 {
        std::mem::take(&mut self.total_memory_usage)
    }
}

/// Shared pointer shape of Go's `*parallelSortWorker`. Worker-pool jobs and
/// the coordinated spill helper never operate on the same worker at once:
/// the fetcher joins all in-flight lanes before initiating a spill round.
struct SharedParallelSortWorker<C: Columns>(Arc<Mutex<ParallelSortWorker<C>>>);

impl<C: Columns> Clone for SharedParallelSortWorker<C> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<C: Columns> SharedParallelSortWorker<C> {
    fn sort_local_partition(&mut self) -> Result<Option<SortPartition>, ExecError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sort_local_partition()
    }
}

impl<C> LocalSortWorker for SharedParallelSortWorker<C>
where
    C: Columns + Send + 'static,
{
    fn sort_local_rows(&mut self) -> Result<Vec<OwnedRow>, ExecError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sort_local_rows()
    }

    fn take_total_memory_usage(&mut self) -> i64 {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_total_memory_usage()
    }
}

/// Go `parallelSortSpillAction`: request one coordinated spill only when the
/// sort itself owns at least a tenth of the statement quota; otherwise defer
/// to the previous OOM action.
struct ParallelSortSpillAction {
    base: BaseOomAction,
    need_spill: Arc<AtomicBool>,
    sort_tracker: Arc<Tracker>,
    spill_limit: i64,
}

impl ActionOnExceed for ParallelSortSpillAction {
    fn action(&self, tracker: &Arc<Tracker>) {
        if self.need_spill.load(SeqCst) {
            return;
        }
        if self.sort_tracker.bytes_consumed() > self.spill_limit {
            self.need_spill.store(true, SeqCst);
            return;
        }
        if tracker.check_exceed() {
            if let Some(fallback) = self.get_fallback() {
                fallback.action(tracker);
            }
        }
    }

    fn set_fallback(&self, action: Option<ArcAction>) {
        self.base.set_fallback(action);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    fn get_priority(&self) -> i64 {
        DEF_SPILL_PRIORITY
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

/// Evaluates every by-item against `row`, producing an owned merge-head key.
///
/// The in-memory sort does not call this: like Go, it compares cells in the
/// retained chunks directly. Owned keys are needed only while merging run
/// heads, including spilled rows whose source chunk can be reloaded.
pub fn eval_sort_key<C: Columns>(
    by_items: &[SortByItem],
    ctx: &C,
    row: Row<'_>,
) -> Result<Vec<Datum>, ExecError> {
    let mut key = Vec::with_capacity(by_items.len());
    for item in by_items {
        key.push(item.expr.eval(ctx, row)?);
    }
    Ok(key)
}

/// Go `lessRow`: the first non-equal by-item decides, and `Desc` negates it.
///
/// Each key compares under ITS OWN derived collation (Go builds `keyCmpFuncs`
/// from the by-item's `RetType`): `ORDER BY ci_col` orders `a, A, b, B`, not
/// the byte order `A, B, a, b`.
pub fn less_by_items(
    by_items: &[SortByItem],
    a: &[Datum],
    b: &[Datum],
) -> Result<Ordering, ExecError> {
    for (i, item) in by_items.iter().enumerate() {
        let mut cmp = tidb_expr::compare_datums_with_collation(
            &a[i],
            &b[i],
            tidb_expr::collation_derive::collation_of_node(&item.expr),
        )?;
        if item.desc {
            cmp = cmp.reverse();
        }
        if cmp != Ordering::Equal {
            return Ok(cmp);
        }
    }
    Ok(Ordering::Equal)
}

/// Compiles Go `keyCmpFuncs` once for direct-column by-items.
///
/// Go's physical Sort accepts columns and constants. The Rust executor also
/// accepts scalar expressions, so those retain the evaluated-Datum fallback;
/// the common physical-column path is allocation-free just like Go's.
fn compile_compare_funcs(by_items: &[SortByItem]) -> Vec<Option<ColumnCompareFunc>> {
    by_items
        .iter()
        .map(|item| {
            item.expr
                .as_column()
                .and_then(|column| column.get_static_type())
                .and_then(tidb_chunk::compare::get_column_compare_func)
        })
        .collect()
}

/// Go `lessRow`: compares two retained chunk rows without allocating keys.
pub(crate) fn compare_rows<C: Columns>(
    by_items: &[SortByItem],
    compare_funcs: &[Option<ColumnCompareFunc>],
    ctx: &C,
    left: Row<'_>,
    right: Row<'_>,
) -> Result<Ordering, ExecError> {
    for (index, item) in by_items.iter().enumerate() {
        let mut ordering = match (&item.expr, &compare_funcs[index]) {
            (Expression::Column(column), Some(compare)) if column.index >= 0 => {
                let column = usize::try_from(column.index).unwrap_or(usize::MAX);
                if column < left.len() && column < right.len() {
                    let left_column = left
                        .chunk()
                        .expect("a non-empty row has a chunk")
                        .column(column);
                    let right_column = right
                        .chunk()
                        .expect("a non-empty row has a chunk")
                        .column(column);
                    compare(&left_column, left.idx(), &right_column, right.idx())
                } else {
                    let left = item.expr.eval(ctx, left)?;
                    let right = item.expr.eval(ctx, right)?;
                    tidb_expr::compare_datums_with_collation(
                        &left,
                        &right,
                        tidb_expr::collation_derive::collation_of_node(&item.expr),
                    )?
                }
            }
            // A constant has the same value for every input row and cannot
            // affect their order. Go omits it from `keyColumns`.
            (Expression::Constant(_), _) => Ordering::Equal,
            _ => {
                let left = item.expr.eval(ctx, left)?;
                let right = item.expr.eval(ctx, right)?;
                tidb_expr::compare_datums_with_collation(
                    &left,
                    &right,
                    tidb_expr::collation_derive::collation_of_node(&item.expr),
                )?
            }
        };
        if item.desc {
            ordering = ordering.reverse();
        }
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

/// Go `SortExec` (unparallel, external): one or more sorted runs, merged.
pub struct SortExec<C: Columns> {
    meta: ExecutorMeta,
    /// Go `ByItems`.
    by_items: Vec<SortByItem>,
    /// Go `keyCmpFuncs`, compiled once instead of allocating a Datum key for
    /// every retained row.
    compare_funcs: Vec<Option<ColumnCompareFunc>>,
    child: Box<dyn Executor>,
    ctx: C,
    /// Go `fetched`: whether the child has been drained and sorted.
    fetched: bool,
    /// Go `Unparallel.sortPartitions`: the sorted runs, in creation order.
    /// One entry, unspilled, is the common in-memory case.
    partitions: Vec<SortPartition>,
    /// Go `multiWayMergeImpl.elements`: one current head per live run.
    merge_heads: Vec<MergeHead>,
    /// Whether [`Self::merge_heads`] has been initialized from the runs.
    merge_initialized: bool,
    /// The statement's memory budget, which this operator's tracker hangs off
    /// and whose quota it checks after each `Consume`.
    memory: StatementMemory,
    /// Go `SortExec.memTracker` = `memory.NewTracker(e.ID(), -1)` attached to
    /// `StmtCtx.MemTracker`: this operator's own node in the tracker tree, so
    /// `SHOW`-style tree dumps attribute the bytes to the sort.
    tracker: Arc<Tracker>,
    /// Go `SortExec.diskTracker`.
    disk_tracker: Arc<tidb_util::disk::Tracker>,
    /// Go `enableTmpStorageOnOOM` = `vardef.EnableTmpStorageOnOOM.Load()`:
    /// `tidb_enable_tmp_storage_on_oom`. With it OFF the sort registers no
    /// spill action, so an overrun goes straight to the 8175 cancellation --
    /// which is exactly what this executor did before spilling existed.
    enable_tmp_storage_on_oom: bool,
    /// Go `spillLimit` = `MemTracker.GetBytesLimit() / 10`.
    spill_limit: i64,
    /// Raised by the current partition's spill action; see
    /// `crate::sort_partition`'s module doc for why a flag stands in for
    /// Go's spill goroutine.
    need_spill: Arc<AtomicBool>,
    /// The action currently registered on the session tracker, kept so
    /// `close` can unbind it.
    registered_action: Option<ArcAction>,
    /// Go `spillChunkSize` (a package var so tests can shrink it).
    spill_chunk_size: usize,
    /// Go `SessionVars.ExecutorConcurrency`, resolved for this statement.
    /// The default constructor stays serial for executor-level unit tests;
    /// SQL planning installs the statement value with
    /// [`Self::with_parallelism`].
    parallelism: usize,
}

impl<C> SortExec<C>
where
    C: Columns + Clone + Send + Sync + 'static,
{
    /// Builds a sort of `child`'s rows by `by_items`, evaluated with `ctx`.
    /// `memory` is the statement's budget (Go: the `StmtCtx.MemTracker` the
    /// operator attaches to). It is a required argument rather than an
    /// optional one so a new call site cannot produce an UNACCOUNTED sort by
    /// omitting it.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        by_items: Vec<SortByItem>,
        child: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let compare_funcs = compile_compare_funcs(&by_items);
        let tracker = memory.operator_tracker(meta.id());
        let disk_tracker = memory.operator_disk_tracker(meta.id());
        let spill_limit = memory.quota() / 10;
        let enable_tmp_storage_on_oom = memory.tmp_storage_on_oom();
        SortExec {
            meta,
            by_items,
            compare_funcs,
            child,
            ctx,
            fetched: false,
            partitions: Vec::new(),
            merge_heads: Vec::new(),
            merge_initialized: false,
            memory,
            tracker,
            disk_tracker,
            enable_tmp_storage_on_oom,
            spill_limit,
            need_spill: Arc::new(AtomicBool::new(false)),
            registered_action: None,
            spill_chunk_size: SPILL_CHUNK_SIZE,
            parallelism: 1,
        }
    }

    /// Selects Go's default parallel sort worker count for this statement.
    #[must_use]
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism.max(1);
        self
    }

    /// Go `SetSmallSpillChunkSizeForTest`: shrink the spill chunk so a test
    /// can produce many spilled chunks without a large data set.
    pub fn set_spill_chunk_size_for_test(&mut self, size: usize) {
        self.spill_chunk_size = size;
    }

    /// Bytes this sort has written to spill files (Go `SortExec.diskTracker`).
    #[must_use]
    pub fn bytes_in_disk(&self) -> i64 {
        self.disk_tracker.bytes_consumed()
    }

    /// How many sorted runs the sort produced; more than one means the sort
    /// spilled. For tests and diagnostics.
    #[must_use]
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Runs that actually hold rows. A spill that fires while the child is
    /// already exhausted leaves a trailing EMPTY run, so `num_partitions`
    /// alone does not prove the merge had anything to merge.
    #[must_use]
    pub fn num_non_empty_partitions(&self) -> usize {
        self.partitions
            .iter()
            .filter(|partition| partition.num_rows() > 0)
            .count()
    }

    /// Go `switchToNewSortPartition`: start a fresh run and point the spill
    /// action at it.
    fn new_partition(&mut self, fields: &[FieldType]) -> SortPartition {
        let mut partition =
            SortPartition::new(fields.to_vec(), &self.tracker, self.memory.spill_storage());
        partition.set_spill_chunk_size(self.spill_chunk_size);
        if self.enable_tmp_storage_on_oom {
            partition.disk_tracker().attach_to(&self.disk_tracker);
            let (action, need_spill) = spill_action(&partition, self.spill_limit);
            self.need_spill = need_spill;
            let action: ArcAction = action;
            self.memory
                .session_tracker()
                .fallback_old_and_set_new_action(Arc::clone(&action));
            self.registered_action = Some(action);
        }
        partition
    }

    /// Go `fetchChunksUnparallel` + `storeChunk`: drain the child into sorted
    /// runs, spilling whenever the memory action says to.
    fn fetch_and_sort(&mut self) -> Result<(), ExecError> {
        if self.parallelism > 1 {
            return self.fetch_and_sort_parallel();
        }

        let fields: Vec<FieldType> = self.meta.ret_field_types().to_vec();
        let mut current = self.new_partition(&fields);

        loop {
            let mut chunk = self.child.new_chunk();
            self.child.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            // Accounting happens INSIDE the loop, which is what makes a query
            // over a large table spill (or stop) early instead of first
            // materializing everything and only then noticing.
            current.add(chunk);

            if self.need_spill.swap(false, SeqCst) {
                current.spill_to_disk(&self.by_items, &self.compare_funcs, &self.ctx)?;
                self.partitions.push(current);
                current = self.new_partition(&fields);
            }
            // With tmp storage off (or with a partition too small to be worth
            // a file), the action fell through to the cancellation, and this
            // is where the statement stops with 8175.
            self.memory.check()?;
        }

        current.sort(&self.by_items, &self.compare_funcs, &self.ctx)?;
        self.partitions.push(current);
        Ok(())
    }

    /// Go `fetchChunksParallel`: the fetcher owns child access, dispatches
    /// bounded work to persistent worker-pool lanes while it continues
    /// fetching, coordinates whole-worker spill rounds, and finally exposes
    /// one sorted run per worker (or per spill round) to the result merger.
    fn fetch_and_sort_parallel(&mut self) -> Result<(), ExecError> {
        let fields = self.meta.ret_field_types().to_vec();
        let spill_storage = self.memory.spill_storage();
        let worker_count = self.parallelism;
        let mut workers = (0..worker_count)
            .map(|_| {
                SharedParallelSortWorker(Arc::new(Mutex::new(ParallelSortWorker::new(
                    fields.clone(),
                    Arc::clone(&spill_storage),
                    self.spill_chunk_size,
                    self.by_items.clone(),
                    self.ctx.clone(),
                    self.meta.max_chunk_size(),
                ))))
            })
            .collect::<Vec<_>>();

        let finish = Arc::new(AtomicBool::new(false));
        let spill_by_items = Arc::new(self.by_items.clone());
        let spill_compare_funcs = Arc::new(compile_compare_funcs(&spill_by_items));
        let spill_ctx = self.ctx.clone();
        let (error_sender, _error_receiver) = std::sync::mpsc::channel();
        let mut spill_helper = ParallelSortSpillHelper::new(
            workers.clone(),
            Arc::clone(&self.tracker),
            Arc::clone(&self.disk_tracker),
            Arc::clone(&spill_storage),
            fields.clone(),
            Arc::clone(&finish),
            move |left: &OwnedRow, right: &OwnedRow| {
                compare_rows(
                    &spill_by_items,
                    &spill_compare_funcs,
                    &spill_ctx,
                    left.as_row(),
                    right.as_row(),
                )
            },
            error_sender,
            "",
        );

        let need_spill = Arc::new(AtomicBool::new(false));
        self.need_spill = Arc::clone(&need_spill);
        if self.enable_tmp_storage_on_oom {
            let action: ArcAction = Arc::new(ParallelSortSpillAction {
                base: BaseOomAction::default(),
                need_spill: Arc::clone(&need_spill),
                sort_tracker: Arc::clone(&self.tracker),
                spill_limit: self.spill_limit,
            });
            self.memory
                .session_tracker()
                .fallback_old_and_set_new_action(Arc::clone(&action));
            self.registered_action = Some(action);
        }

        let mut pending = (0..worker_count).map(|_| None).collect::<Vec<_>>();
        let join_lane = |lane: &mut Option<std::sync::mpsc::Receiver<Result<(), ExecError>>>| {
            if let Some(result) = lane.take() {
                result.recv().map_err(|_| {
                    ExecError::internal("parallel sort worker dropped its result")
                })??;
            }
            Ok::<(), ExecError>(())
        };
        let join_all =
            |pending: &mut Vec<Option<std::sync::mpsc::Receiver<Result<(), ExecError>>>>| {
                for lane in pending {
                    join_lane(lane)?;
                }
                Ok::<(), ExecError>(())
            };

        let result = (|| -> Result<(), ExecError> {
            let mut next_worker = 0usize;
            loop {
                let mut chunk = self.child.new_chunk();
                self.child.next(&mut chunk)?;
                if chunk.num_rows() == 0 {
                    break;
                }

                let lane = next_worker;
                next_worker = (next_worker + 1) % worker_count;
                join_lane(&mut pending[lane])?;

                let rows = i64::try_from(chunk.num_rows()).unwrap_or(i64::MAX);
                let memory_usage = chunk
                    .memory_usage()
                    .saturating_add(tidb_chunk::row::ROW_SIZE.saturating_mul(rows));
                self.tracker.consume(memory_usage);
                let worker = workers[lane].clone();
                pending[lane] = Some(crate::worker_pool::spawn(move || {
                    worker
                        .0
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .add_chunk(chunk, memory_usage)
                }));

                if need_spill.swap(false, SeqCst) {
                    join_all(&mut pending)?;
                    spill_helper.set_bytes_info(
                        self.memory.session_tracker().bytes_consumed(),
                        self.memory.session_tracker().get_bytes_limit(),
                    );
                    spill_helper.set_need_spill();
                    spill_helper.spill()?;
                }
                self.memory.check()?;
            }

            join_all(&mut pending)?;
            if spill_helper.is_spill_triggered() {
                // Go spills the workers' final partial batches too, so the result
                // source is wholly on disk after the first spill round.
                spill_helper.spill()?;
                for run in spill_helper.take_sorted_rows_in_disk() {
                    self.partitions.push(SortPartition::from_spilled(
                        fields.clone(),
                        &self.tracker,
                        Arc::clone(&spill_storage),
                        run,
                    ));
                }
            } else {
                // No spill: each worker locally merges its sorted batches into
                // one run, then the SortExec heap merges those worker runs.
                for worker in &mut workers {
                    let mut run = worker.sort_local_partition()?;
                    let released = LocalSortWorker::take_total_memory_usage(worker);
                    self.tracker.consume(-released);
                    if let Some(run) = &mut run {
                        run.attach_memory_to(&self.tracker);
                    }
                    if let Some(run) = run {
                        self.partitions.push(run);
                    }
                }
            }
            Ok(())
        })();
        if result.is_err() {
            // A lane error must not leave sibling jobs or spill files behind.
            // The executor may not be closed immediately after `Next` fails.
            let _ = join_all(&mut pending);
            self.tracker.replace_bytes_used(0);
        }
        finish.store(true, SeqCst);
        spill_helper.close();
        result
    }
}

impl<C> Executor for SortExec<C>
where
    C: Columns + Clone + Send + Sync + 'static,
{
    /// Go `Open`: resets the fetched state and opens the child.
    fn open(&mut self) -> Result<(), ExecError> {
        self.fetched = false;
        for partition in &mut self.partitions {
            partition.close();
        }
        self.partitions.clear();
        self.merge_heads.clear();
        self.merge_initialized = false;
        // Go `SortExec.Open`: `e.memTracker.ReplaceBytesUsed(0)` -- a re-opened
        // sort (an Apply's inner side re-runs per outer row) must not keep
        // charging for rows it has just dropped.
        self.tracker.replace_bytes_used(0);
        self.need_spill.store(false, SeqCst);
        self.child.open()
    }

    /// Go `Next`: the first call drains and sorts; every call then appends
    /// sorted rows until the chunk-size bound or exhaustion.
    ///
    /// With one run this is Go's `onePartitionSorting`; with several it is
    /// `externalSorting`, the multi-way merge over the runs.
    ///
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !self.fetched {
            self.fetch_and_sort()?;
            self.fetched = true;
        }

        let batch = self.meta.max_chunk_size();
        if self.partitions.len() == 1 {
            return self.partitions[0].append_sorted_rows_into(req, batch);
        }
        let mut partitions = std::mem::take(&mut self.partitions);
        let result = (|| -> Result<(), ExecError> {
            if !self.merge_initialized {
                self.merge_heads.clear();
                for (partition_id, partition) in partitions.iter_mut().enumerate() {
                    partition.load_head(&self.by_items, &self.ctx)?;
                    if let Some(key) = partition.head_key() {
                        self.merge_heads.push(MergeHead {
                            partition_id,
                            key: key.to_vec(),
                        });
                    }
                }
                let mut compare_error = None;
                crate::topn_chunk_heap::go_heap::init(&mut self.merge_heads, &mut |left, right| {
                    match less_by_items(&self.by_items, &left.key, &right.key) {
                        Ok(ordering) => ordering == Ordering::Less,
                        Err(error) => {
                            if compare_error.is_none() {
                                compare_error = Some(error);
                            }
                            false
                        }
                    }
                });
                if let Some(error) = compare_error {
                    return Err(error);
                }
                self.merge_initialized = true;
            }
            while req.num_rows() < batch {
                let Some(head) = self.merge_heads.first() else {
                    break;
                };
                let partition_id = head.partition_id;
                partitions[partition_id].take_head_into(req);
                partitions[partition_id].load_head(&self.by_items, &self.ctx)?;
                if let Some(key) = partitions[partition_id].head_key() {
                    self.merge_heads[0].key = key.to_vec();
                    let mut compare_error = None;
                    crate::topn_chunk_heap::go_heap::fix(
                        &mut self.merge_heads,
                        0,
                        &mut |left, right| match less_by_items(
                            &self.by_items,
                            &left.key,
                            &right.key,
                        ) {
                            Ok(ordering) => ordering == Ordering::Less,
                            Err(error) => {
                                if compare_error.is_none() {
                                    compare_error = Some(error);
                                }
                                false
                            }
                        },
                    );
                    if let Some(error) = compare_error {
                        return Err(error);
                    }
                } else {
                    let mut compare_error = None;
                    crate::topn_chunk_heap::go_heap::remove(
                        &mut self.merge_heads,
                        0,
                        &mut |left, right| match less_by_items(
                            &self.by_items,
                            &left.key,
                            &right.key,
                        ) {
                            Ok(ordering) => ordering == Ordering::Less,
                            Err(error) => {
                                if compare_error.is_none() {
                                    compare_error = Some(error);
                                }
                                false
                            }
                        },
                    );
                    if let Some(error) = compare_error {
                        return Err(error);
                    }
                }
            }
            Ok(())
        })();
        self.partitions = partitions;
        result
    }

    /// Go `Close`: drops the runs and their spill files, unbinds the spill
    /// action, and gives the bytes back to the statement's budget.
    fn close(&mut self) -> Result<(), ExecError> {
        for partition in &mut self.partitions {
            partition.close();
        }
        self.partitions.clear();
        self.merge_heads.clear();
        self.merge_initialized = false;
        if let Some(action) = self.registered_action.take() {
            self.memory
                .session_tracker()
                .unbind_action_from_hard_limit(&action);
        }
        self.tracker.replace_bytes_used(0);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem_quota::OomAction;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A test-only source that emits one prebuilt chunk, then EOF (same
    /// helper pattern as the limit/selection tests).
    struct OneChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
    }

    impl Executor for OneChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
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

    /// A test-only source that emits `rows` in chunks of `chunk_size`, so a
    /// sort sees several child chunks -- which is what lets a spill produce
    /// more than one NON-EMPTY sorted run. A single-chunk source cannot: the
    /// spill fires after the only chunk is in, leaving one full run and one
    /// empty one, and a merge over that is not a merge at all.
    struct ManyChunkSource {
        meta: ExecutorMeta,
        rows: Vec<Vec<Option<i64>>>,
        emitted: usize,
        chunk_size: usize,
    }

    impl Executor for ManyChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.emitted = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.emitted + self.chunk_size).min(self.rows.len());
            for row in &self.rows[self.emitted..end] {
                for (c, v) in row.iter().enumerate() {
                    match v {
                        Some(v) => req.append_int64(c, *v),
                        None => req.append_null(c),
                    }
                }
            }
            self.emitted = end;
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
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

    /// A sort over a MULTI-CHUNK source, which is what a spilling sort needs.
    fn multi_chunk_sorter(
        rows: &[Vec<Option<i64>>],
        by: Vec<SortByItem>,
        chunk_size: usize,
        memory: StatementMemory,
    ) -> SortExec<NoColumns> {
        let n_cols = rows.first().map_or(1, Vec::len);
        let source = ManyChunkSource {
            meta: ExecutorMeta::new(schema_of(n_cols), 0, 4, chunk_size),
            rows: rows.to_vec(),
            emitted: 0,
            chunk_size,
        };
        SortExec::new(
            ExecutorMeta::new(schema_of(n_cols), 1, 4, 1024),
            by,
            Box::new(source),
            NoColumns,
            memory,
        )
    }

    fn schema_of(n_cols: usize) -> Schema {
        let cols = (0..n_cols)
            .map(|i| {
                let mut c = Column::new(i as i64 + 1, long());
                c.index = i as i64;
                c
            })
            .collect();
        Schema::new(cols)
    }

    fn col_expr(idx: usize) -> Expression {
        let mut c = Column::new(idx as i64 + 1, long());
        c.index = idx as i64;
        Expression::Column(c)
    }

    /// Builds a sort over one chunk whose rows are given per column as
    /// `Option<i64>` (None = NULL).
    fn sort_over(rows: &[Vec<Option<i64>>], by: Vec<SortByItem>) -> SortExec<NoColumns> {
        let n_cols = rows.first().map_or(1, Vec::len);
        let fields: Vec<FieldType> = (0..n_cols).map(|_| long()).collect();
        let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
        for row in rows {
            for (c, v) in row.iter().enumerate() {
                match v {
                    Some(v) => data.append_int64(c, *v),
                    None => data.append_null(c),
                }
            }
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(schema_of(n_cols), 0, 4, 1024),
            data: Some(data),
        };
        SortExec::new(
            ExecutorMeta::new(schema_of(n_cols), 1, 4, 1024),
            by,
            Box::new(source),
            NoColumns,
            StatementMemory::default(),
        )
    }

    /// Same as [`sorter`] but with a caller-chosen budget, so a test can pick
    /// a quota the sort must cross.
    fn sorter_with_memory(
        n_cols: usize,
        rows: &[Vec<Option<i64>>],
        by: Vec<SortByItem>,
        memory: StatementMemory,
    ) -> SortExec<NoColumns> {
        let fields: Vec<FieldType> = (0..n_cols).map(|_| long()).collect();
        let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
        for row in rows {
            for (c, v) in row.iter().enumerate() {
                match v {
                    Some(v) => data.append_int64(c, *v),
                    None => data.append_null(c),
                }
            }
        }
        let source = OneChunkSource {
            meta: ExecutorMeta::new(schema_of(n_cols), 0, 4, 1024),
            data: Some(data),
        };
        SortExec::new(
            ExecutorMeta::new(schema_of(n_cols), 1, 4, 1024),
            by,
            Box::new(source),
            NoColumns,
            memory,
        )
    }

    fn one_col_rows(n: i64) -> Vec<Vec<Option<i64>>> {
        (0..n).rev().map(|v| vec![Some(v)]).collect()
    }

    #[test]
    fn a_sort_accounts_its_materialized_rows_against_the_statement() {
        let memory = StatementMemory::default();
        let mut exec = sorter_with_memory(
            1,
            &one_col_rows(64),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
            memory.clone(),
        );
        assert_eq!(memory.bytes_consumed(), 0, "nothing before the fetch");
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        exec.next(&mut req).unwrap();
        let held = memory.bytes_consumed();
        // At least the retained chunk bytes plus one row cursor per row.
        assert!(
            held > tidb_chunk::row::ROW_SIZE * 64,
            "accounted only {held} bytes for 64 retained rows"
        );
        // Go `Close` releases the partition: the statement's budget must come
        // back down, or a session would leak its quota statement by statement.
        exec.close().unwrap();
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn crossing_the_quota_fails_the_sort_with_8175_under_cancel() {
        // A quota far below what 4096 retained rows need, with spilling OFF
        // (`tidb_enable_tmp_storage_on_oom = 0`). With it ON the same sort
        // spills and completes -- see
        // `a_sort_over_the_quota_spills_to_disk_and_returns_every_row`.
        let memory =
            StatementMemory::new(2048, OomAction::Cancel, 42).with_tmp_storage_on_oom(false);
        let mut exec = sorter_with_memory(
            1,
            &one_col_rows(4096),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
            memory.clone(),
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        match exec.next(&mut req) {
            Err(ExecError::MemoryExceedForQuery { conn_id }) => assert_eq!(conn_id, 42),
            other => panic!("expected the quota to be enforced, got {other:?}"),
        }
    }

    #[test]
    fn the_same_sort_completes_under_log_however_far_it_overruns() {
        let memory = StatementMemory::new(2048, OomAction::Log, 42).with_tmp_storage_on_oom(false);
        let mut exec = sorter_with_memory(
            1,
            &one_col_rows(4096),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
            memory.clone(),
        );
        let out = collect(&mut exec);
        assert_eq!(out.len(), 4096);
        assert_eq!(out[0], vec![Some(0)]);
        assert_eq!(out[4095], vec![Some(4095)]);
    }

    fn collect(exec: &mut SortExec<NoColumns>) -> Vec<Vec<Option<i64>>> {
        exec.open().unwrap();
        let mut out = Vec::new();
        let mut req = exec.new_chunk();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..exec.ret_field_types().len())
                        .map(|c| {
                            if row.is_null(c) {
                                None
                            } else {
                                Some(row.get_int64(c))
                            }
                        })
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        out
    }

    fn rows1(vals: &[Option<i64>]) -> Vec<Vec<Option<i64>>> {
        vals.iter().map(|v| vec![*v]).collect()
    }

    #[test]
    fn ascending_int_sort() {
        let mut e = sort_over(
            &rows1(&[Some(3), Some(1), Some(2)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
        );
        assert_eq!(collect(&mut e), rows1(&[Some(1), Some(2), Some(3)]));
    }

    #[test]
    fn descending_int_sort() {
        let mut e = sort_over(
            &rows1(&[Some(3), Some(1), Some(2)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: true,
            }],
        );
        assert_eq!(collect(&mut e), rows1(&[Some(3), Some(2), Some(1)]));
    }

    #[test]
    fn multi_key_ties_broken_by_second_key() {
        // (col0 asc, col1 desc): col0 ties resolved by larger col1 first.
        let mut e = sort_over(
            &[
                vec![Some(2), Some(1)],
                vec![Some(1), Some(5)],
                vec![Some(2), Some(9)],
                vec![Some(1), Some(7)],
            ],
            vec![
                SortByItem {
                    expr: col_expr(0),
                    desc: false,
                },
                SortByItem {
                    expr: col_expr(1),
                    desc: true,
                },
            ],
        );
        assert_eq!(
            collect(&mut e),
            vec![
                vec![Some(1), Some(7)],
                vec![Some(1), Some(5)],
                vec![Some(2), Some(9)],
                vec![Some(2), Some(1)],
            ]
        );
    }

    #[test]
    fn nulls_first_ascending() {
        // Go chunk.cmpNull: NULL is below every value.
        let mut e = sort_over(
            &rows1(&[Some(2), None, Some(1)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
        );
        assert_eq!(collect(&mut e), rows1(&[None, Some(1), Some(2)]));
    }

    #[test]
    fn nulls_last_descending() {
        // Desc negates the whole comparison, so NULLs move to the end.
        let mut e = sort_over(
            &rows1(&[Some(2), None, Some(1)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: true,
            }],
        );
        assert_eq!(collect(&mut e), rows1(&[Some(2), Some(1), None]));
    }

    #[test]
    fn eof_after_emission() {
        let mut e = sort_over(
            &rows1(&[Some(2), Some(1)]),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
        );
        e.open().unwrap();
        let mut req = e.new_chunk();
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 2);
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        e.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
        e.close().unwrap();
    }

    #[test]
    fn empty_child_is_empty() {
        let mut e = sort_over(
            &[],
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
        );
        assert_eq!(collect(&mut e), Vec::<Vec<Option<i64>>>::new());
    }

    /// `tmp-storage-path` is process-global, so the tests that redirect it
    /// must not run at the same time inside one test binary -- and that
    /// includes the aggregation's and the TopN's spill tests, which is why the
    /// lock is the CRATE's rather than this module's.
    use crate::test_temp_storage::{scratch_dir as scratch_temp_dir, storage as test_storage};

    fn spill_files_in(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.contains("ChunkDataInDiskByChunks"))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drains an executor into a flat list of first-column values.
    fn drain_first_col(exec: &mut SortExec<NoColumns>) -> Vec<i64> {
        let mut out = Vec::new();
        loop {
            let mut req = exec.new_chunk();
            exec.next(&mut req).expect("sort must not fail");
            if req.num_rows() == 0 {
                return out;
            }
            for r in 0..req.num_rows() {
                out.push(req.get_row(r).get_int64(0));
            }
        }
    }

    fn asc() -> Vec<SortByItem> {
        vec![SortByItem {
            expr: col_expr(0),
            desc: false,
        }]
    }

    /// Go source of truth: `sortexec.TestSortInParallel`.
    #[test]
    fn parallel_sort_workers_share_input_and_heap_merge_their_runs() {
        let n = 4096i64;
        let rows: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![Some((i * 4051) % n)]).collect();
        let mut expected: Vec<i64> = rows.iter().map(|row| row[0].unwrap()).collect();
        expected.sort_unstable();

        let mut exec =
            multi_chunk_sorter(&rows, asc(), 64, StatementMemory::default()).with_parallelism(4);
        exec.open().unwrap();
        assert_eq!(drain_first_col(&mut exec), expected);
        assert_eq!(
            exec.num_partitions(),
            4,
            "the statement concurrency must create four independently sorted worker runs"
        );
        assert_eq!(
            exec.partitions
                .iter()
                .map(SortPartition::in_memory_chunk_count)
                .sum::<usize>(),
            64,
            "Go retains the 64 fetched chunks and sorts lightweight row cursors instead of copying every row into new chunks"
        );
        exec.close().unwrap();
    }

    /// Go `parallelSortWorker.multiWayMergeLocalSortedRows`: crossing the
    /// worker's `maxChunkSize * 30` boundary creates more than one locally
    /// sorted batch, then merges their row cursors without replacing the
    /// fetched chunks.
    #[test]
    fn parallel_worker_merges_multiple_batches_without_copying_chunks() {
        let fields = vec![long()];
        let memory = StatementMemory::default();
        let mut worker = ParallelSortWorker::new(
            fields.clone(),
            memory.spill_storage(),
            SPILL_CHUNK_SIZE,
            asc(),
            NoColumns,
            2,
        );
        for batch in 0..3i64 {
            let mut chunk = Chunk::new_with_capacity(&fields, 32);
            for row in 0..32i64 {
                chunk.append_int64(0, 95 - (batch * 32 + row));
            }
            let rows = i64::try_from(chunk.num_rows()).unwrap();
            let memory_usage = chunk.memory_usage() + tidb_chunk::row::ROW_SIZE * rows;
            worker.add_chunk(chunk, memory_usage).unwrap();
        }

        let mut run = worker
            .sort_local_partition()
            .unwrap()
            .expect("the worker received rows");
        assert_eq!(run.in_memory_chunk_count(), 3);
        let mut output = Chunk::new_with_capacity(&fields, 96);
        run.append_sorted_rows_into(&mut output, 96).unwrap();
        assert_eq!(
            (0..output.num_rows())
                .map(|row| output.get_row(row).get_int64(0))
                .collect::<Vec<_>>(),
            (0..96).collect::<Vec<_>>()
        );
    }

    /// Go source of truth: `sortexec.TestParallelSortSpillDisk`.
    ///
    /// Once a parallel spill is triggered, Go's fetcher coordinates a whole
    /// worker spill round and spills the final partial batches as well. The
    /// result must therefore be backed entirely by the helper's disk runs,
    /// not by the old serial-fetch path followed by an in-memory repartition.
    #[test]
    fn parallel_sort_spills_worker_rounds_and_final_batches() {
        let dir = scratch_temp_dir("parallel-sortexec");
        let n = 12_345i64;
        let rows: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![Some((i * 12_341) % n)]).collect();
        let mut expected = rows
            .iter()
            .map(|row| row[0].expect("no nulls"))
            .collect::<Vec<_>>();
        expected.sort_unstable();

        let memory = StatementMemory::new(1 << 16, OomAction::Cancel, 43)
            .with_spill_storage(test_storage(&dir));
        let mut exec = multi_chunk_sorter(&rows, asc(), 193, memory).with_parallelism(4);
        exec.open().unwrap();
        assert_eq!(drain_first_col(&mut exec), expected);
        assert!(exec.bytes_in_disk() > 0, "parallel sort did not spill");
        assert!(
            exec.partitions.iter().all(SortPartition::is_spilled),
            "after its first spill Go writes every final worker batch to disk"
        );
        assert!(
            exec.num_non_empty_partitions() > 1,
            "the input must exercise more than one coordinated spill round"
        );
        exec.close().unwrap();
        assert!(spill_files_in(&dir).is_empty());
        drop(exec);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go source of truth: `sortexec.TestUnparallelSortSpillDisk`.
    ///
    /// This is the serial executor equivalent of that test's in-memory and
    /// multi-partition spill cases. A quota the sort cannot hold its rows
    /// within, with `tidb_enable_tmp_storage_on_oom` ON, must spill (proved by
    /// a spill file existing while it runs and by the disk tracker), produce
    /// several non-empty sorted runs, and return the same rows as an
    /// unspilled reference execution.
    ///
    /// The input values are shuffled by a stride so that consecutive runs
    /// cover OVERLAPPING ranges. That is what makes the multi-way merge load
    /// bearing: a merge that drained run 0 and then run 1 would emit an
    /// unsorted sequence, and so would one that picked the wrong end.
    #[test]
    fn test_unparallel_sort_spill_disk() {
        let dir = scratch_temp_dir("sortexec");

        let n = 8192i64;
        let rows: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![Some((i * 7919) % n)]).collect();
        let mut expected: Vec<i64> = rows.iter().map(|r| r[0].expect("no nulls")).collect();
        expected.sort_unstable();

        // The unspilled reference: a quota this sort fits inside.
        let mut reference = multi_chunk_sorter(&rows, asc(), 256, StatementMemory::default());
        reference.open().unwrap();
        assert_eq!(drain_first_col(&mut reference), expected);
        assert_eq!(reference.num_partitions(), 1);
        assert_eq!(reference.bytes_in_disk(), 0, "the reference must not spill");
        reference.close().unwrap();

        // Now the same sort under a quota it cannot hold, spilling enabled.
        let memory = StatementMemory::new(1 << 16, OomAction::Cancel, 42)
            .with_spill_storage(test_storage(&dir));
        let mut exec = multi_chunk_sorter(&rows, asc(), 256, memory);
        // Small spill chunks so each run becomes many spilled chunks, the
        // shape Go's `SetSmallSpillChunkSizeForTest` produces.
        exec.set_spill_chunk_size_for_test(64);
        exec.open().unwrap();

        let mut got = Vec::new();
        let mut saw_spill_file = false;
        loop {
            let mut req = exec.new_chunk();
            exec.next(&mut req).expect("a spilling sort must not fail");
            if req.num_rows() == 0 {
                break;
            }
            // DISK WAS ACTUALLY USED: a spill file exists while the sort is
            // still producing rows.
            saw_spill_file |= !spill_files_in(&dir).is_empty();
            for r in 0..req.num_rows() {
                got.push(req.get_row(r).get_int64(0));
            }
        }

        assert!(
            saw_spill_file,
            "no spill file was ever created -- this test proved nothing"
        );
        assert!(
            exec.bytes_in_disk() > 0,
            "the disk tracker must have counted the spilled bytes"
        );
        assert!(
            exec.num_non_empty_partitions() > 1,
            "a spilling sort must produce more than one NON-EMPTY sorted run, got {}",
            exec.num_non_empty_partitions()
        );
        assert_eq!(got, expected, "spilled sort must return the same rows");

        exec.close().unwrap();
        assert!(
            spill_files_in(&dir).is_empty(),
            "close must remove every spill file"
        );
        drop(exec);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A DESCENDING spilled sort, so the merge's direction is exercised in
    /// both directions rather than only the one the ascending test pins.
    #[test]
    fn a_spilled_descending_sort_returns_every_row_in_order() {
        let dir = scratch_temp_dir("sortdesc");

        let n = 8192i64;
        let rows: Vec<Vec<Option<i64>>> = (0..n).map(|i| vec![Some((i * 7919) % n)]).collect();
        let mut expected: Vec<i64> = rows.iter().map(|r| r[0].expect("no nulls")).collect();
        expected.sort_unstable_by(|a, b| b.cmp(a));

        let memory = StatementMemory::new(1 << 16, OomAction::Cancel, 42)
            .with_spill_storage(test_storage(&dir));
        let mut exec = multi_chunk_sorter(
            &rows,
            vec![SortByItem {
                expr: col_expr(0),
                desc: true,
            }],
            256,
            memory,
        );
        exec.set_spill_chunk_size_for_test(64);
        exec.open().unwrap();
        let got = drain_first_col(&mut exec);
        assert!(exec.num_non_empty_partitions() > 1, "this test needs runs");
        assert_eq!(got, expected);
        exec.close().unwrap();
        drop(exec);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The gate: with `tidb_enable_tmp_storage_on_oom = 0` the SAME sort under
    /// the SAME quota raises 8175 instead of spilling, and leaves no file.
    #[test]
    fn the_same_sort_raises_8175_when_tmp_storage_is_disabled() {
        let dir = scratch_temp_dir("sortgate");

        let memory = StatementMemory::new(1 << 15, OomAction::Cancel, 42)
            .with_spill_storage(test_storage(&dir))
            .with_tmp_storage_on_oom(false);
        let mut exec = sorter_with_memory(
            1,
            &one_col_rows(4096),
            vec![SortByItem {
                expr: col_expr(0),
                desc: false,
            }],
            memory,
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        match exec.next(&mut req) {
            Err(ExecError::MemoryExceedForQuery { conn_id }) => assert_eq!(conn_id, 42),
            other => panic!("expected 8175 with tmp storage disabled, got {other:?}"),
        }
        assert!(spill_files_in(&dir).is_empty(), "no file may be written");
        drop(exec);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
