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

//! `pkg/executor/join/index_lookup_join.go`: Go's `IndexLookUpJoin`.
//!
//! An outer worker reads the outer child into tasks (`buildTask`) and hands
//! each to the inner workers and, in order, to the result channel; an inner
//! worker builds the task's lookup contents, reads the inner rows they reach
//! and builds the task's lookup map (`handleTask`); `Next` takes the tasks in
//! order and joins each outer row with the inner rows its encoded key finds
//! through the [`Joiner`].
//!
//! Go's goroutines are threads here. Go's `IndexJoinExecutorBuilder` builds a
//! fresh inner executor per task; this port's inner side is one
//! [`IndexLookupSource`] re-seeded per task, so a task either opens a fork of
//! it on its worker (a common-handle prefix lookup) or holds the shared
//! source for its read.

use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Mutex;
use std::thread::JoinHandle;

use tidb_chunk::iterator::{Iterator4RowPtr, LendingIterator};

use crate::join_stats::IndexLookUpJoinRuntimeStats;
use crate::joiner::{Joiner, NAAJType};

/// Go `lookUpJoinTask` once its inner worker has handled it.
struct LookUpJoinTask {
    outer: OuterBatch,
    /// Go `task.memTracker`'s charge for `outerResult`, released when the
    /// task is consumed (`task.memTracker.Detach()`).
    _outer_charge: TrackerCharge,
    inner: PreparedIndexInner,
}

/// Go `task.doneCh`.
type TaskDone = Receiver<Result<LookUpJoinTask, ExecError>>;

/// What the outer worker hands an inner worker.
type InnerTask = (OuterBatch, TrackerCharge, SyncSender<Result<LookUpJoinTask, ExecError>>);

/// Go `IndexLookUpJoin`.
pub struct IndexLookUpJoin<C: Columns + Clone + Send + Sync + 'static> {
    meta: ExecutorMeta,
    /// Go `Children(0)`, lent to the outer worker while it runs.
    outer_child: Option<Box<dyn Executor>>,
    outer_worker: Option<JoinHandle<Box<dyn Executor>>>,
    inner_workers: Vec<JoinHandle<()>>,
    source: Arc<Mutex<IndexLookupSource>>,
    shared: Arc<IndexTaskShared<C>>,
    outer_filter: Vec<Expression>,
    outer_not_null: Vec<usize>,
    /// Go `resultCh`.
    result_ch: Option<Receiver<TaskDone>>,
    task: Option<LookUpJoinTask>,
    /// Go `task.cursor`, over the task's visited outer rows.
    cursor: usize,
    /// Go `innerIter` over `task.matchedInners`: its rows and cursor.
    inner_iter: Option<(Vec<RowPtr>, usize)>,
    /// Go `task.hasMatch` / `task.hasNull` for the row at `cursor`.
    has_match: bool,
    has_null: bool,
    joiner: Box<dyn Joiner>,
    is_outer_join: bool,
    /// Go `requiredRows`, read by the outer worker of an outer join.
    required_rows: Arc<AtomicUsize>,
    /// Go `Finished`.
    finished: Arc<AtomicBool>,
    concurrency: usize,
    max_batch_size: usize,
    /// See [`JoinExec::set_index_ranges`].
    worker_ranges_bytes: i64,
    /// Go `StmtCtx.RuntimeStatsColl` and the plan the stats register on.
    runtime_stats: Option<(Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>, i64)>,
    stats: Option<Arc<IndexLookUpJoinRuntimeStats>>,
    prepared: bool,
}

/// What [`IndexLookUpJoin::new`] takes from the physical join.
pub struct IndexLookUpJoinBuild<C: Columns> {
    pub meta: ExecutorMeta,
    pub outer: Box<dyn Executor>,
    pub plan: IndexLookupPlan,
    /// The join's equalities over the left-then-right row, one per hash key
    /// (Go `OuterHashKeys` / `InnerHashKeys`).
    pub equalities: Vec<Expression>,
    /// The left child's width in that row.
    pub left_width: usize,
    pub joiner: Box<dyn Joiner>,
    pub is_outer_join: bool,
    pub ctx: C,
    pub memory: StatementMemory,
    pub vectorized: bool,
    /// Go `IndexLookupJoinConcurrency()`.
    pub concurrency: usize,
    /// Go `SessionVars.IndexJoinBatchSize`.
    pub max_batch_size: usize,
    /// Go `IndexRanges`, of which every inner worker keeps a copy.
    pub index_ranges: Vec<tidb_ranger::types::Range>,
    pub runtime_stats: Option<(Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>, i64)>,
}

/// The pieces both index lookup joins build from an [`IndexLookUpJoinBuild`].
pub(super) struct IndexLookUpJoinParts<C: Columns> {
    pub(super) meta: ExecutorMeta,
    pub(super) outer: Box<dyn Executor>,
    pub(super) source: IndexLookupSource,
    pub(super) shared: Arc<IndexTaskShared<C>>,
    pub(super) outer_filter: Vec<Expression>,
    pub(super) outer_not_null: Vec<usize>,
    pub(super) joiner: Box<dyn Joiner>,
    pub(super) is_outer_join: bool,
    pub(super) concurrency: usize,
    pub(super) max_batch_size: usize,
    pub(super) worker_ranges_bytes: i64,
    pub(super) runtime_stats: Option<(Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>, i64)>,
    pub(super) stats: Option<Arc<IndexLookUpJoinRuntimeStats>>,
}

impl<C: Columns + Clone + Send + Sync + 'static> IndexLookUpJoinParts<C> {
    /// `build_lookup_map`: whether tasks build Go's inner lookup map
    /// (`IndexLookUpJoin`), not the hash join's outer one.
    pub(super) fn new(build: IndexLookUpJoinBuild<C>, build_lookup_map: bool) -> Result<Self, ExecError> {
        let IndexLookUpJoinBuild {
            meta,
            outer,
            plan,
            equalities,
            left_width,
            joiner,
            is_outer_join,
            ctx,
            memory,
            vectorized,
            concurrency,
            max_batch_size,
            index_ranges,
            runtime_stats,
        } = build;
        let concurrency = concurrency.max(1);
        let keys = crate::hash_join::split_equi(&equalities, left_width).keys;
        if keys.len() != equalities.len() {
            return Err(ExecError::unsupported(
                "an index lookup join key has no comparable encoding",
            ));
        }
        let tracker = memory.operator_tracker(meta.id());
        let stats = runtime_stats
            .as_ref()
            .map(|_| Arc::new(IndexLookUpJoinRuntimeStats::new(concurrency)));
        let shared = Arc::new(IndexTaskShared {
            build_lookup_map,
            lookup_charges: true,
            vectorized,
            probe_plan: IndexProbePlan::new(
                &keys,
                plan.probe_keys.clone(),
                plan.probe_key_domains.clone(),
                plan.probe_bounds.clone(),
            )?,
            outer_is_left: !plan.lookup_is_left,
            keys,
            ctx,
            outer_types: outer.ret_field_types().to_vec(),
            inner_types: plan.source.ret_field_types().to_vec(),
            inner_not_null: plan.inner_not_null.clone(),
            init_cap: meta.init_cap(),
            max_chunk_size: meta.max_chunk_size(),
            tracker,
            memory,
            stats: stats.clone(),
            inner_opened: std::sync::atomic::AtomicBool::new(false),
        });
        let worker_ranges_bytes = index_ranges
            .first()
            .map_or(0, |range| 2 * tidb_datatype::estimated_mem_usage(&range.low_val, index_ranges.len()));
        Ok(Self {
            meta,
            outer,
            source: plan.source,
            shared,
            outer_filter: plan.outer_filter,
            outer_not_null: plan.outer_not_null,
            joiner,
            is_outer_join,
            concurrency,
            max_batch_size: max_batch_size.max(1),
            worker_ranges_bytes,
            runtime_stats,
            stats,
        })
    }
}

impl<C: Columns + Clone + Send + Sync + 'static> IndexLookUpJoin<C> {
    /// Go `executorBuilder.buildIndexLookUpJoin`'s executor.
    pub fn new(build: IndexLookUpJoinBuild<C>) -> Result<Self, ExecError> {
        let parts = IndexLookUpJoinParts::new(build, true)?;
        Ok(Self {
            meta: parts.meta,
            outer_child: Some(parts.outer),
            outer_worker: None,
            inner_workers: Vec::new(),
            source: Arc::new(Mutex::new(parts.source)),
            shared: parts.shared,
            outer_filter: parts.outer_filter,
            outer_not_null: parts.outer_not_null,
            result_ch: None,
            task: None,
            cursor: 0,
            inner_iter: None,
            has_match: false,
            has_null: false,
            joiner: parts.joiner,
            is_outer_join: parts.is_outer_join,
            required_rows: Arc::new(AtomicUsize::new(0)),
            finished: Arc::new(AtomicBool::new(false)),
            concurrency: parts.concurrency,
            max_batch_size: parts.max_batch_size,
            worker_ranges_bytes: parts.worker_ranges_bytes,
            runtime_stats: parts.runtime_stats,
            stats: parts.stats,
            prepared: false,
        })
    }

    /// Go `startWorkers`: one outer worker and `concurrency` inner workers.
    fn start_workers(&mut self, init_batch_size: usize) -> Result<(), ExecError> {
        let concurrency = self.concurrency;
        let (result_tx, result_rx) = sync_channel::<TaskDone>(concurrency);
        let (inner_tx, inner_rx) = crossbeam_channel::bounded::<InnerTask>(concurrency);
        self.result_ch = Some(result_rx);
        let mut child = self
            .outer_child
            .take()
            .ok_or_else(|| ExecError::internal("index lookup join has no outer child"))?;
        let outer = OuterWorker {
            shared: Arc::clone(&self.shared),
            outer_filter: self.outer_filter.clone(),
            outer_not_null: self.outer_not_null.clone(),
            batch_size: init_batch_size.min(self.max_batch_size),
            max_batch_size: self.max_batch_size,
            is_outer_join: self.is_outer_join,
            required_rows: Arc::clone(&self.required_rows),
            finished: Arc::clone(&self.finished),
        };
        let span = tidb_tracing::current_span();
        let acc = tidb_executor_internal_exec::ruv2::current_next_io_acc();
        self.outer_worker = Some(
            std::thread::Builder::new()
                .name("IndexLookupJoinOuterWorker".to_owned())
                .spawn(move || {
                    let _span = tidb_tracing::with_thread_span(span);
                    let _acc = tidb_executor_internal_exec::ruv2::with_next_io_acc(acc);
                    outer.run(child.as_mut(), &inner_tx, &result_tx);
                    child
                })
                .map_err(|error| ExecError::internal(format!("index lookup join outer worker: {error}")))?,
        );
        for _ in 0..concurrency {
            // Go `newInnerWorker`: each worker's copy of the ranges is
            // charged to the statement for as long as the statement runs.
            self.shared
                .memory
                .stmt_tracker()
                .consume(self.worker_ranges_bytes);
            let inner_rx = inner_rx.clone();
            let shared = Arc::clone(&self.shared);
            let source = Arc::clone(&self.source);
            let finished = Arc::clone(&self.finished);
            let span = tidb_tracing::current_span();
            let acc = tidb_executor_internal_exec::ruv2::current_next_io_acc();
            self.inner_workers.push(
                std::thread::Builder::new()
                    .name("IndexLookupJoinInnerWorker".to_owned())
                    .spawn(move || {
                        let _span = tidb_tracing::with_thread_span(span);
                        let _acc = tidb_executor_internal_exec::ruv2::with_next_io_acc(acc);
                        inner_worker_run(&shared, &source, &finished, &inner_rx);
                    })
                    .map_err(|error| ExecError::internal(format!("index lookup join inner worker: {error}")))?,
            );
        }
        Ok(())
    }

    /// Go `getFinishedTask`: the current task while it has outer rows left,
    /// else the next one in order once its inner worker is done.
    fn get_finished_task(&mut self) -> Result<bool, ExecError> {
        if self.task.as_ref().is_some_and(|task| self.cursor < task.outer.len()) {
            return Ok(true);
        }
        // The previous task has been processed, so release its memory.
        self.task = None;
        let Some(result_ch) = &self.result_ch else {
            return Ok(false);
        };
        let Ok(done) = result_ch.recv() else {
            return Ok(false);
        };
        match done.recv() {
            Ok(Ok(task)) => {
                self.task = Some(task);
                self.cursor = 0;
                Ok(true)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(ExecError::internal(
                "the index lookup join inner worker stopped without a result",
            )),
        }
    }
}

/// Go `outerWorker`.
struct OuterWorker<C: Columns> {
    shared: Arc<IndexTaskShared<C>>,
    outer_filter: Vec<Expression>,
    outer_not_null: Vec<usize>,
    batch_size: usize,
    max_batch_size: usize,
    is_outer_join: bool,
    required_rows: Arc<AtomicUsize>,
    finished: Arc<AtomicBool>,
}

impl<C: Columns> OuterWorker<C> {
    /// Go `outerWorker.run`: a recovered panic or a build error reaches the
    /// main thread as a failed task; closing both channels ends the workers.
    fn run(
        mut self,
        child: &mut dyn Executor,
        inner_ch: &crossbeam_channel::Sender<InnerTask>,
        result_ch: &SyncSender<TaskDone>,
    ) {
        let outcome = catch_unwind(AssertUnwindSafe(|| self.run_tasks(child, inner_ch, result_ch)));
        let error = match outcome {
            Ok(Ok(())) => return,
            Ok(Err(error)) => error,
            Err(panic) => {
                self.finished.store(true, Ordering::SeqCst);
                ExecError::internal(format!("outerWorker panicked: {}", panic_message(&panic)))
            }
        };
        let (done_tx, done_rx) = sync_channel(1);
        let _ = done_tx.send(Err(error));
        let _ = result_ch.send(done_rx);
    }

    fn run_tasks(
        &mut self,
        child: &mut dyn Executor,
        inner_ch: &crossbeam_channel::Sender<InnerTask>,
        result_ch: &SyncSender<TaskDone>,
    ) -> Result<(), ExecError> {
        loop {
            if self.finished.load(Ordering::SeqCst) {
                return Ok(());
            }
            let parent_required = if self.is_outer_join {
                self.required_rows.load(Ordering::SeqCst)
            } else {
                0
            };
            let outer = build_outer_task(
                child,
                &self.shared.outer_types,
                &self.outer_filter,
                &self.outer_not_null,
                &self.shared.ctx,
                self.shared.vectorized,
                &mut self.batch_size,
                self.max_batch_size,
                parent_required,
            )?;
            if outer.rows.len() == 0 {
                return Ok(());
            }
            let charge = TrackerCharge::new(&self.shared.tracker, outer.bytes);
            self.shared.memory.check().map_err(ExecError::from)?;
            let (done_tx, done_rx) = sync_channel(1);
            if inner_ch.send((outer, charge, done_tx)).is_err() {
                return Ok(());
            }
            if result_ch.send(done_rx).is_err() {
                return Ok(());
            }
        }
    }
}

/// Go `innerWorker.run`.
fn inner_worker_run<C: Columns>(
    shared: &IndexTaskShared<C>,
    source: &Mutex<IndexLookupSource>,
    finished: &AtomicBool,
    task_ch: &crossbeam_channel::Receiver<InnerTask>,
) {
    for (outer, charge, done) in task_ch.iter() {
        if finished.load(Ordering::SeqCst) {
            return;
        }
        let result = catch_unwind(AssertUnwindSafe(|| handle_task(shared, source, outer, charge)))
            .unwrap_or_else(|panic| {
                finished.store(true, Ordering::SeqCst);
                Err(ExecError::internal(format!("innerWorker panicked: {}", panic_message(&panic))))
            });
        let _ = done.send(result);
    }
}

/// Go `innerWorker.handleTask`: the task's lookup contents, the inner rows
/// they reach and the lookup map over them; the inner worker's charges end
/// with the task.
fn handle_task<C: Columns>(
    shared: &IndexTaskShared<C>,
    source: &Mutex<IndexLookupSource>,
    mut outer: OuterBatch,
    charge: TrackerCharge,
) -> Result<LookUpJoinTask, ExecError> {
    let inner = handle_lookup(shared, source, &mut outer, None, |_, _, _| Ok(()))?;
    Ok(LookUpJoinTask {
        outer,
        _outer_charge: charge,
        inner,
    })
}

/// The lookup half of Go's `handleTask`: `constructLookupContent`, then
/// `fetchInnerResults` -- in windows of `max_fetch_rows` when set, each
/// handed to `on_window` with whether the inner side is done -- through a
/// fork of the inner reader or, holding it, the shared one. The inner
/// worker's charges are reset when the lookup ends.
pub(super) fn handle_lookup<C: Columns>(
    shared: &IndexTaskShared<C>,
    source: &Mutex<IndexLookupSource>,
    outer: &mut OuterBatch,
    max_fetch_rows: Option<usize>,
    mut on_window: impl FnMut(&OuterBatch, &PreparedIndexInner, bool) -> Result<(), ExecError>,
) -> Result<PreparedIndexInner, ExecError> {
    let result = (|| {
        let probes = construct_index_task(shared, outer)?;
        let mem_tracker = outer.take_kv_range_tracker(false);
        let mut fetch = |reader: &mut dyn Executor, outer: &OuterBatch| {
            let mut inner = None;
            loop {
                let (window, rows, done) = fetch_index_inner(shared, reader, max_fetch_rows, inner.take())?;
                on_window(outer, &window, done)?;
                if done {
                    return Ok::<_, ExecError>((window, rows));
                }
                inner = Some(window);
            }
        };
        // Go `fetchInnerResults`: the task's inner executor, built for its
        // lookup contents and read to the end (which closes it).
        let IndexLookupSource::Builder(builder) = &*source.lock().unwrap();
        let builder = Arc::clone(builder);
        let (keys, bound_values) = probes.into_iter().map(|probe| (probe.key, probe.bounds)).unzip();
        let mut inner_exec = builder.build_executor_for_index_join(crate::index_lookup::IndexJoinProbes {
            keys,
            bound_values,
            mem_tracker,
        })?;
        Ok(fetch(inner_exec.as_mut(), outer)?.0)
    })();
    outer.inner_worker_tracker = None;
    result
}

pub(super) fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
        .unwrap_or_else(|| "unknown panic".to_owned())
}

impl<C: Columns + Clone + Send + Sync + 'static> Executor for IndexLookUpJoin<C> {
    /// Go `Open`: only the outer child; the inner side is read per task.
    fn open(&mut self) -> Result<(), ExecError> {
        self.outer_child
            .as_mut()
            .ok_or_else(|| ExecError::internal("index lookup join has no outer child"))?
            .open()?;
        self.shared.inner_opened.store(false, Ordering::SeqCst);
        self.finished.store(false, Ordering::SeqCst);
        self.prepared = false;
        Ok(())
    }

    /// Go `IndexLookUpJoin.Next`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if !self.prepared {
            self.start_workers(req.required_rows())?;
            self.prepared = true;
        }
        if self.is_outer_join {
            self.required_rows.store(req.required_rows(), Ordering::SeqCst);
        }
        req.reset();
        loop {
            if !self.get_finished_task()? {
                return Ok(());
            }
            let probe_started = std::time::Instant::now();
            let Self {
                task,
                cursor,
                inner_iter,
                has_match,
                has_null,
                joiner,
                ..
            } = self;
            let task = task.as_ref().expect("a finished task");
            let at_end = |iter: &Option<(Vec<RowPtr>, usize)>| {
                iter.as_ref()
                    .is_none_or(|(rows, at)| *at == 0 || *at > rows.len())
            };
            if at_end(inner_iter) {
                // Go `lookUpMatchedInners`.
                let matched = task
                    .outer
                    .encoded_look_up_key(*cursor)
                    .and_then(|key| task.inner.matched.get(&key).cloned())
                    .unwrap_or_default();
                let start = usize::from(!matched.is_empty());
                *inner_iter = Some((matched, start));
            }
            let (rows, at) = inner_iter.take().expect("an inner iterator");
            let mut iter = LendingIterator::RowPtr(Iterator4RowPtr::resume(&task.inner.rows, rows, at));
            let outer_row = task.outer.row(*cursor);
            if iter.current().is_some() {
                let (matched, is_null) =
                    joiner.try_to_match_inners(outer_row, &mut iter, req, NAAJType::Unknown)?;
                *has_match |= matched;
                *has_null |= is_null;
            }
            let exhausted = iter.current().is_none();
            let LendingIterator::RowPtr(iter) = iter else {
                unreachable!("built as a row-pointer iterator")
            };
            *inner_iter = Some(iter.into_parts());
            if exhausted {
                if !*has_match {
                    joiner.on_miss_match(*has_null, outer_row, req);
                }
                *cursor += 1;
                *has_match = false;
                *has_null = false;
            }
            if let Some(stats) = &self.stats {
                stats.record_probe(probe_started.elapsed());
            }
            if req.is_full() {
                return Ok(());
            }
        }
    }

    /// Go `IndexLookUpJoin.Close`: registers the stats, stops and waits for
    /// the workers, then closes the outer child.
    fn close(&mut self) -> Result<(), ExecError> {
        if let (Some((source, plan_id)), Some(stats)) = (&self.runtime_stats, &self.stats) {
            source.register_stats(*plan_id, Box::new(stats.snapshot()));
        }
        self.finished.store(true, Ordering::SeqCst);
        self.result_ch = None;
        self.task = None;
        self.inner_iter = None;
        if let Some(worker) = self.outer_worker.take() {
            match worker.join() {
                Ok(child) => self.outer_child = Some(child),
                Err(_) => return Err(ExecError::internal("the index lookup join outer worker panicked")),
            }
        }
        for worker in self.inner_workers.drain(..) {
            let _ = worker.join();
        }
        self.prepared = false;
        let closed = if self.shared.inner_opened.swap(false, Ordering::SeqCst) {
            self.source.lock().unwrap().close()
        } else {
            Ok(())
        };
        if let Some(child) = self.outer_child.as_mut() {
            child.close()?;
        }
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
