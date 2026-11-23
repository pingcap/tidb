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

//! `pkg/executor/sortexec/parallel_sort_worker.go`,
//! `parallel_sort_spill_helper.go`, the `parallelSortSpillAction` half of
//! `sort_spill.go` and the parallel half of `sort.go` (`fetchChunksParallel`,
//! `fetchChunksFromChild`, `checkSpillAndExecute`,
//! `spillRemainingRowsWhenNeeded`).
//!
//! The chunk fetcher runs on the thread that calls `Next`, as Go's fetcher
//! goroutine runs while `Next` waits on `resultChannel` for its first row;
//! the workers are threads fed through a channel bounded at the executor
//! concurrency. Go's result generator (`generateResult`) becomes
//! `SortExec::next` pulling from the merged run or the spilled runs.

use std::sync::atomic::{AtomicI64, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use crossbeam_channel::{Receiver, Sender};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::compare::ColumnCompareFunc;
use tidb_datatype::FieldType;
use tidb_driver_error::panic_recovery::{recover_executor_panic, recover_worker_panic};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::Executor;
use tidb_exprctx::Columns;
use tidb_memory::{ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY};
use tidb_spill_storage::SpillStorage;
use tidb_variable::mem_quota::StatementMemory;

use crate::sort_partition::SortPartition;
use tidb_expression_aggregation::ByItems;

/// Go `SignalCheckpointForSort`: comparator calls between SQL-killer polls in
/// a parallel sort worker.
pub const SIGNAL_CHECKPOINT_FOR_PARALLEL_SORT: u64 = 20_000;

const FP_PARALLEL_SORT_RANDOM_FAIL: &str =
    "github.com/pingcap/tidb/pkg/executor/sortexec/ParallelSortRandomFail";
const FP_SLOW_SOME_WORKERS: &str = "github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers";
const FP_ISSUE_59655: &str = "github.com/pingcap/tidb/pkg/executor/sortexec/Issue59655";
const FP_ISSUE_63216: &str = "github.com/pingcap/tidb/pkg/executor/sortexec/Issue63216";

/// Go `injectParallelSortRandomFail`.
pub(crate) fn inject_parallel_sort_random_fail(trigger_factor: i32) {
    tidb_failpoint::inject(FP_PARALLEL_SORT_RANDOM_FAIL, |value| {
        if value.bool() && tidb_go_rand::int31n(10000) < trigger_factor {
            panic!("panic is triggered by random fail");
        }
    });
}

/// Go `injectErrorForIssue59655`.
fn inject_error_for_issue_59655(trigger_factor: i32) -> Result<(), ExecError> {
    let mut result = Ok(());
    tidb_failpoint::inject(FP_ISSUE_59655, |value| {
        if value.bool() && tidb_go_rand::int31n(1000) < trigger_factor {
            result = Err(ExecError::internal("issue 59655 error"));
        }
    });
    result
}

/// Go `injectPanicForIssue63216`.
fn inject_panic_for_issue_63216() {
    tidb_failpoint::inject(FP_ISSUE_63216, |value| {
        if value.bool() {
            panic!("issue 63216 panic");
        }
    });
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a sort worker and the spill helper read: Go's `lessRowFunc` (the
/// by-items with their key comparators), the expression context, the
/// statement's killer, and how a new run is built.
pub(crate) struct SortKeys<'a, C> {
    pub by_items: &'a [ByItems],
    pub compare_funcs: &'a [Option<ColumnCompareFunc>],
    pub ctx: &'a C,
    pub memory: &'a StatementMemory,
    pub field_types: &'a [FieldType],
    pub spill_storage: &'a Arc<SpillStorage>,
}

impl<C: Columns> SortKeys<'_, C> {
    fn new_run(&self) -> SortPartition {
        SortPartition::new_detached(self.field_types.to_vec(), Arc::clone(self.spill_storage))
    }

    /// Go `newMultiWayMerger(&memorySource{...}, lessRow)` drained into one
    /// run; `check` runs before each merged row.
    pub(crate) fn merge(
        &self,
        runs: Vec<SortPartition>,
        check: impl FnMut(u64) -> Result<(), ExecError>,
    ) -> Result<SortPartition, ExecError> {
        SortPartition::merge_sorted_runs(
            self.field_types.to_vec(),
            Arc::clone(self.spill_storage),
            runs,
            self.by_items,
            self.compare_funcs,
            self.ctx,
            check,
        )
    }
}

/// Go `parallelSortWorker`.
pub(crate) struct ParallelSortWorker {
    worker_id_for_test: usize,
    /// Go `chunkIters` + `rowNumInChunkIters`: the chunks received since the
    /// last batch sort.
    batch: SortPartition,
    /// Go `localSortedRows`.
    local_sorted_rows: Vec<SortPartition>,
    /// Go `maxSortedRowsLimit` = `maxChunkSize * 30`.
    max_sorted_rows_limit: usize,
    /// Go `totalMemoryUsage`: what the fetcher charged for the chunks this
    /// worker holds.
    total_memory_usage: i64,
    /// Go `sortedRowsIter`: the worker's merged run after its channel closes.
    sorted_rows: Option<SortPartition>,
}

impl ParallelSortWorker {
    fn new<C: Columns>(worker_id_for_test: usize, keys: &SortKeys<'_, C>, max_chunk_size: usize) -> Self {
        Self {
            worker_id_for_test,
            batch: keys.new_run(),
            local_sorted_rows: Vec::new(),
            max_sorted_rows_limit: max_chunk_size * 30,
            total_memory_usage: 0,
            sorted_rows: None,
        }
    }

    /// Go `injectFailPointForParallelSortWorker`.
    fn inject_fail_point_for_parallel_sort_worker(&self, trigger_factor: i32) {
        inject_parallel_sort_random_fail(trigger_factor);
        tidb_failpoint::inject(FP_SLOW_SOME_WORKERS, |value| {
            if value.bool() && self.worker_id_for_test % 2 == 0 && tidb_go_rand::int31n(10000) < 10 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });
    }

    /// Go `multiWayMergeLocalSortedRows`.
    fn multi_way_merge_local_sorted_rows<C: Columns>(
        &mut self,
        keys: &SortKeys<'_, C>,
    ) -> Result<SortPartition, ExecError> {
        let runs = std::mem::take(&mut self.local_sorted_rows);
        keys.merge(runs, |loop_count| {
            let mut result = Ok(());
            tidb_failpoint::inject(FP_PARALLEL_SORT_RANDOM_FAIL, |value| {
                if value.bool() && tidb_go_rand::int31n(10000) < 2 {
                    result = Err(ExecError::internal("failpoint error"));
                }
            });
            result?;
            if loop_count % 100 == 0 {
                keys.memory.check().map_err(ExecError::from)?;
            }
            Ok(())
        })
    }

    /// Go `sortBatchRows`.
    fn sort_batch_rows<C: Columns>(&mut self, keys: &SortKeys<'_, C>) -> Result<(), ExecError> {
        let mut rows = std::mem::replace(&mut self.batch, keys.new_run());
        let mut checkpoint = SIGNAL_CHECKPOINT_FOR_PARALLEL_SORT;
        // Go's `SignalCheckpointForSort` failpoint adds 1024 per comparison,
        // so the killer is polled every ~20 comparisons.
        tidb_failpoint::inject(
            "github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort",
            |value| {
                if value.bool() {
                    checkpoint = SIGNAL_CHECKPOINT_FOR_PARALLEL_SORT / 1025 + 1;
                }
            },
        );
        rows.sort_with_checkpoint(keys.by_items, keys.compare_funcs, keys.ctx, keys.memory, checkpoint)?;
        self.local_sorted_rows.push(rows);
        Ok(())
    }

    /// Go `sortLocalRows`.
    pub(crate) fn sort_local_rows<C: Columns>(
        &mut self,
        keys: &SortKeys<'_, C>,
    ) -> Result<SortPartition, ExecError> {
        // Handle remaining batch rows whose row number is not over the
        // `maxSortedRowsLimit`.
        if self.batch.num_rows() > 0 {
            self.sort_batch_rows(keys)?;
        }
        self.multi_way_merge_local_sorted_rows(keys)
    }

    /// Go `fetchChunksAndSortImpl` for one received chunk.
    fn process_chunk<C: Columns>(
        &mut self,
        chunk: Chunk,
        memory_usage: i64,
        keys: &SortKeys<'_, C>,
    ) -> Result<(), ExecError> {
        // Memory usage of the chunk has been consumed at the chunk fetcher.
        self.total_memory_usage += memory_usage;
        self.batch.add(chunk);
        if self.batch.num_rows() >= self.max_sorted_rows_limit {
            self.sort_batch_rows(keys)?;
        }
        self.inject_fail_point_for_parallel_sort_worker(3);
        Ok(())
    }
}

/// Go `fetcherAndWorkerSyncer`: chunks handed to the workers and not yet
/// processed.
#[derive(Default)]
struct WaitGroup {
    count: Mutex<usize>,
    zero: Condvar,
}

impl WaitGroup {
    fn add(&self) {
        *lock(&self.count) += 1;
    }

    fn done(&self) {
        let mut count = lock(&self.count);
        *count -= 1;
        if *count == 0 {
            self.zero.notify_all();
        }
    }

    fn wait(&self) {
        let mut count = lock(&self.count);
        while *count > 0 {
            count = self.zero.wait(count).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SpillStatus {
    NotSpilled,
    NeedSpill,
    InSpilling,
}

/// Go `parallelSortSpillHelper`'s shared state: the spill status under its
/// condition variable, and the figures the action saw.
pub(crate) struct ParallelSortSpillHelper {
    status: Mutex<SpillStatus>,
    cond: Condvar,
    /// Go `sortExec.memTracker`, which `hasEnoughDataToSpill` reads.
    sort_tracker: Arc<Tracker>,
    bytes_consumed: AtomicI64,
    bytes_limit: AtomicI64,
}

impl ParallelSortSpillHelper {
    pub(crate) fn new(sort_tracker: Arc<Tracker>) -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(SpillStatus::NotSpilled),
            cond: Condvar::new(),
            sort_tracker,
            bytes_consumed: AtomicI64::new(0),
            bytes_limit: AtomicI64::new(0),
        })
    }

    /// Go `isSpillNeeded`.
    pub(crate) fn is_spill_needed(&self) -> bool {
        *lock(&self.status) == SpillStatus::NeedSpill
    }

    /// Go `setInSpilling`.
    fn set_in_spilling(&self) {
        *lock(&self.status) = SpillStatus::InSpilling;
    }

    /// Go `setNotSpilled` followed by the `cond.Broadcast` that wakes the
    /// actions waiting for the spill.
    fn set_not_spilled_and_broadcast(&self) {
        *lock(&self.status) = SpillStatus::NotSpilled;
        self.cond.notify_all();
    }
}

/// Go `hasEnoughDataToSpill`.
fn has_enough_data_to_spill(sort_tracker: &Tracker, passed_in_tracker: &Tracker) -> bool {
    sort_tracker.bytes_consumed() >= passed_in_tracker.get_bytes_limit() / 10
}

/// Go `parallelSortSpillAction`.
pub(crate) struct ParallelSortSpillAction {
    base: BaseOomAction,
    helper: Arc<ParallelSortSpillHelper>,
}

impl ParallelSortSpillAction {
    pub(crate) fn new(helper: Arc<ParallelSortSpillHelper>) -> ArcAction {
        Arc::new(Self {
            base: BaseOomAction::default(),
            helper,
        })
    }

    /// Go `actionImpl`: true when it set the spill flag.
    fn action_impl(&self, t: &Tracker) -> bool {
        let mut status = lock(&self.helper.status);
        while *status == SpillStatus::InSpilling {
            status = self.helper.cond.wait(status).unwrap_or_else(PoisonError::into_inner);
        }
        if t.check_exceed()
            && *status == SpillStatus::NotSpilled
            && has_enough_data_to_spill(&self.helper.sort_tracker, t)
        {
            *status = SpillStatus::NeedSpill;
            self.helper.bytes_consumed.store(t.bytes_consumed(), SeqCst);
            self.helper.bytes_limit.store(t.get_bytes_limit(), SeqCst);
            return true;
        }
        false
    }
}

impl ActionOnExceed for ParallelSortSpillAction {
    fn action(&self, t: &Arc<Tracker>) {
        if self.action_impl(t) {
            return;
        }
        if t.check_exceed() && !has_enough_data_to_spill(&self.helper.sort_tracker, t) {
            self.base.trigger_fall_back_action(t);
        }
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
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

/// The sort's state the parallel fetch reads and fills.
pub(crate) struct ParallelSortContext<'a> {
    pub helper: &'a ParallelSortSpillHelper,
    /// Go `SortExec.memTracker`.
    pub tracker: &'a Arc<Tracker>,
    /// Go `SortExec.diskTracker`.
    pub disk_tracker: &'a Arc<tidb_disk::Tracker>,
    pub concurrency: usize,
    pub max_chunk_size: usize,
    pub spill_chunk_size: usize,
}

/// What the parallel fetch leaves for the result generator.
pub(crate) struct ParallelSortResult {
    /// Go `spillHelper.sortedRowsInDisk`; empty when nothing spilled.
    pub sorted_rows_in_disk: Vec<SortPartition>,
    /// Go `Parallel.sortedRowsIters`: each worker's merged run, when nothing
    /// spilled.
    pub sorted_rows: Vec<SortPartition>,
    /// What the fetcher charged for the rows still in memory (the workers'
    /// `totalMemoryUsage`), released when they spill.
    pub memory_usage: i64,
}

/// Go `fetchChunksParallel` through the workers' exit: starts the workers,
/// fetches the child's chunks into them, spills when the action asks, and
/// returns once every worker has produced its run.
pub(crate) fn fetch_chunks_parallel<C>(
    keys: &SortKeys<'_, C>,
    child: &mut dyn Executor,
    sort: &ParallelSortContext<'_>,
) -> Result<ParallelSortResult, ExecError>
where
    C: Columns + Sync,
{
    let workers: Vec<Mutex<ParallelSortWorker>> = (0..sort.concurrency.max(1))
        .map(|id| Mutex::new(ParallelSortWorker::new(id, keys, sort.max_chunk_size)))
        .collect();
    let (sender, receiver) = crossbeam_channel::bounded::<(Chunk, i64)>(workers.len());
    let syncer = WaitGroup::default();
    let first_error: Mutex<Option<ExecError>> = Mutex::new(None);
    let mut sorted_rows_in_disk = Vec::new();

    std::thread::scope(|scope| {
        for worker in &workers {
            let receiver = receiver.clone();
            let (syncer, first_error) = (&syncer, &first_error);
            scope.spawn(move || run_worker(worker, &receiver, syncer, first_error, keys));
        }
        drop(receiver);

        let fetched = recover_executor_panic(|| {
            fetch_chunks_from_child(keys, child, sort, &workers, &sender, &syncer, &first_error, &mut sorted_rows_in_disk)
        });
        if let Err(error) = fetched {
            lock(&first_error).get_or_insert(error);
        }
        // Go's deferred tail: wait for the workers, spill what they hold if
        // a spill happened, then close the channel.
        syncer.wait();
        if lock(&first_error).is_none() && !sorted_rows_in_disk.is_empty() {
            if let Err(error) = spill(keys, sort, &workers, &mut sorted_rows_in_disk) {
                lock(&first_error).get_or_insert(error);
            }
        }
        drop(sender);
    });

    if let Some(error) = first_error.into_inner().unwrap_or_else(PoisonError::into_inner) {
        for run in &mut sorted_rows_in_disk {
            run.close();
        }
        return Err(error);
    }
    let mut memory_usage = 0;
    let mut sorted_rows = Vec::with_capacity(workers.len());
    for worker in workers {
        let worker = worker.into_inner().unwrap_or_else(PoisonError::into_inner);
        memory_usage += worker.total_memory_usage;
        sorted_rows.extend(worker.sorted_rows);
    }
    Ok(ParallelSortResult {
        sorted_rows_in_disk,
        sorted_rows,
        memory_usage,
    })
}

/// Go `parallelSortWorker.run`: processes chunks until the channel closes,
/// then merges its sorted batches into its run. A worker that failed keeps
/// draining the channel so the fetcher's wait for in-flight chunks returns.
fn run_worker<C: Columns>(
    worker: &Mutex<ParallelSortWorker>,
    receiver: &Receiver<(Chunk, i64)>,
    syncer: &WaitGroup,
    first_error: &Mutex<Option<ExecError>>,
    keys: &SortKeys<'_, C>,
) {
    let mut failed = false;
    while let Ok((chunk, memory_usage)) = receiver.recv() {
        if !failed {
            if let Err(error) = recover_worker_panic(|| lock(worker).process_chunk(chunk, memory_usage, keys)) {
                lock(first_error).get_or_insert(error);
                failed = true;
            }
        }
        syncer.done();
    }
    if failed {
        return;
    }
    let result = recover_worker_panic(|| {
        let mut worker = lock(worker);
        worker.inject_fail_point_for_parallel_sort_worker(100);
        // Put local sorted rows into the run the result generator reads.
        let sorted_rows = worker.sort_local_rows(keys)?;
        worker.sorted_rows = Some(sorted_rows);
        Ok(())
    });
    if let Err(error) = result {
        lock(first_error).get_or_insert(error);
    }
}

/// Go `fetchChunksFromChild`.
#[allow(clippy::too_many_arguments)]
fn fetch_chunks_from_child<C: Columns + Sync>(
    keys: &SortKeys<'_, C>,
    child: &mut dyn Executor,
    sort: &ParallelSortContext<'_>,
    workers: &[Mutex<ParallelSortWorker>],
    sender: &Sender<(Chunk, i64)>,
    syncer: &WaitGroup,
    first_error: &Mutex<Option<ExecError>>,
    sorted_rows_in_disk: &mut Vec<SortPartition>,
) -> Result<(), ExecError> {
    loop {
        // Go's `Next` returns the first worker error, and `Close` then stops
        // the fetcher.
        if lock(first_error).is_some() {
            return Ok(());
        }
        let mut chunk = child.new_chunk();
        child.next(&mut chunk)?;
        let row_count = chunk.num_rows();
        if row_count == 0 {
            return Ok(());
        }
        let memory_usage =
            chunk.memory_usage() + tidb_chunk::row::ROW_SIZE * i64::try_from(row_count).unwrap_or(i64::MAX);
        sort.tracker.consume(memory_usage);
        syncer.add();
        if sender.send((chunk, memory_usage)).is_err() {
            syncer.done();
            return Ok(());
        }
        // Go `checkSpillAndExecute`.
        if sort.helper.is_spill_needed() {
            // Wait for the stop of all workers.
            syncer.wait();
            spill(keys, sort, workers, sorted_rows_in_disk)?;
        }
        inject_parallel_sort_random_fail(3);
        // The statement's kill signal (the 8175 cancellation a fallback
        // action raises) stops the fetch here, as in the serial path.
        keys.memory.check()?;
    }
}

/// Go `parallelSortSpillHelper.spill`: every worker merges what it holds,
/// the worker runs are merged, and the result is written to one spill file.
fn spill<C: Columns + Sync>(
    keys: &SortKeys<'_, C>,
    sort: &ParallelSortContext<'_>,
    workers: &[Mutex<ParallelSortWorker>],
    sorted_rows_in_disk: &mut Vec<SortPartition>,
) -> Result<(), ExecError> {
    struct SpillDone<'a>(&'a ParallelSortSpillHelper);
    impl Drop for SpillDone<'_> {
        fn drop(&mut self) {
            // Spill is done, broadcast to wake up all waiting actions.
            self.0.set_not_spilled_and_broadcast();
        }
    }
    sort.helper.set_in_spilling();
    let _done = SpillDone(sort.helper);

    recover_executor_panic(|| {
        let results: Vec<Result<SortPartition, ExecError>> = std::thread::scope(|scope| {
            let handles: Vec<_> = workers
                .iter()
                .map(|worker| {
                    scope.spawn(move || {
                        recover_worker_panic(|| {
                            inject_error_for_issue_59655(10)?;
                            let sorted_rows = lock(worker).sort_local_rows(keys)?;
                            inject_parallel_sort_random_fail(200);
                            Ok(sorted_rows)
                        })
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .unwrap_or_else(|_| Err(ExecError::internal("parallel sort worker panicked")))
                })
                .collect()
        });
        let mut runs = Vec::with_capacity(results.len());
        for result in results {
            runs.push(result?);
        }
        let merged = keys.merge(runs, |_| Ok(()))?;
        spill_impl(sort, merged, sorted_rows_in_disk, || release_memory(sort.tracker, workers))
    })
}

/// Go `releaseMemory`.
fn release_memory(tracker: &Arc<Tracker>, workers: &[Mutex<ParallelSortWorker>]) {
    let mut total_released_memory = 0;
    for worker in workers {
        let mut worker = lock(worker);
        total_released_memory += worker.total_memory_usage;
        worker.total_memory_usage = 0;
    }
    tracker.consume(-total_released_memory);
}

/// Go `spillImpl`: writes the merged rows not yet returned to one spill file
/// and, when it holds rows, keeps it and releases their memory.
pub(crate) fn spill_impl(
    sort: &ParallelSortContext<'_>,
    mut merged: SortPartition,
    sorted_rows_in_disk: &mut Vec<SortPartition>,
    release_memory: impl FnOnce(),
) -> Result<(), ExecError> {
    tracing::info!(
        consumed = sort.helper.bytes_consumed.load(SeqCst),
        quota = sort.helper.bytes_limit.load(SeqCst),
        "memory exceeds quota, spill to disk now."
    );
    inject_parallel_sort_random_fail(200);
    inject_error_for_issue_59655(500)?;
    if merged.num_unread_rows() == 0 {
        return Ok(());
    }
    merged.set_small_spill_chunk_size_for_test(sort.spill_chunk_size);
    merged.attach_disk_tracker(sort.disk_tracker);
    merged.spill_unread_rows_to_disk(|| {
        inject_panic_for_issue_63216();
        Ok(())
    })?;
    inject_parallel_sort_random_fail(200);
    sorted_rows_in_disk.push(merged);
    release_memory();
    Ok(())
}

/// Go `parallel_sort_test.go` and `parallel_sort_spill_test.go`.
#[cfg(test)]
mod tests {
    use std::path::Path;

    use tidb_datatype::FieldTypeCode;
    use tidb_executor_internal_exec::ExecutorMeta;
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;
    use tidb_expr::schema::Schema;
    use tidb_exprctx::NoColumns;
    use tidb_vardef::OomAction;

    use super::*;
    use crate::test_temp_storage::{scratch_dir, storage};
    use crate::SortExec;

    const HARD_LIMIT_1: i64 = 100_000;
    const HARD_LIMIT_2: i64 = HARD_LIMIT_1 * 10;
    /// Go `sortexec.ResultChannelCapacity`.
    const RESULT_CHANNEL_CAPACITY: usize = 10;
    /// Go `SetSmallSpillChunkSizeForTest`.
    const SMALL_SPILL_CHUNK_SIZE: usize = 16;

    fn schema() -> Schema {
        Schema::new(
            (0..2)
                .map(|index| {
                    let mut column = Column::new(index + 1, FieldType::new(FieldTypeCode::LongLong));
                    column.index = index;
                    column
                })
                .collect(),
        )
    }

    /// Go `testutil.SortCase` with `OrderByIdx: []int{0, 1}`: two BIGINT
    /// columns, the generated rows of `testutil.BuildMockDataSource`.
    struct SortCase {
        rows: Vec<[i64; 2]>,
    }

    impl SortCase {
        fn new(rows: usize, ndvs: [i64; 2]) -> Self {
            let mut rand = tidb_go_rand::Rand::new(rows as i64);
            let mut value = |ndv: i64| if ndv == 0 { rand.int63() } else { rand.intn(ndv) };
            Self {
                rows: (0..rows).map(|_| [value(ndvs[0]), value(ndvs[1])]).collect(),
            }
        }

        fn by_items() -> Vec<ByItems> {
            schema()
                .columns
                .iter()
                .map(|column| ByItems {
                    expr: Expression::Column(column.clone()),
                    desc: false,
                })
                .collect()
        }
    }

    /// Go `testutil.MockDataSource`: the rows in chunks of `MaxChunkSize`
    /// (32), replayed on every `Open`.
    struct MockDataSource {
        meta: ExecutorMeta,
        rows: Arc<Vec<[i64; 2]>>,
        emitted: usize,
    }

    impl Executor for MockDataSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.emitted = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.emitted + self.meta.max_chunk_size()).min(self.rows.len());
            for row in &self.rows[self.emitted..end] {
                req.append_int64(0, row[0]);
                req.append_int64(1, row[1]);
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

    /// Go `buildSortExec` with `InitChunkSize = MaxChunkSize = 32`.
    fn build_sort_exec(case: &SortCase, memory: &StatementMemory, concurrency: usize) -> SortExec<NoColumns> {
        let source = MockDataSource {
            meta: ExecutorMeta::new(schema(), 1, 32, 32),
            rows: Arc::new(case.rows.clone()),
            emitted: 0,
        };
        let mut exec = SortExec::new(
            ExecutorMeta::new(schema(), 0, 32, 32),
            SortCase::by_items(),
            Box::new(source),
            NoColumns,
            memory.clone(),
        )
        .with_concurrency(concurrency);
        exec.set_spill_chunk_size_for_test(SMALL_SPILL_CHUNK_SIZE);
        exec
    }

    /// Go `executeSortExecutor` (parallel), with `trigger` run after the
    /// eleventh `Next` as `executeSortExecutorAndManullyTriggerSpill` does.
    fn execute_sort_executor(exec: &mut SortExec<NoColumns>, trigger: Option<&dyn Fn()>) -> Vec<[i64; 2]> {
        exec.open().unwrap();
        let mut result = Vec::new();
        let mut chunk = exec.new_chunk();
        for i in 0.. {
            exec.next(&mut chunk).unwrap();
            if i == 10 {
                if let Some(trigger) = trigger {
                    trigger();
                }
            }
            if chunk.num_rows() == 0 {
                break;
            }
            for row in 0..chunk.num_rows() {
                let row = chunk.get_row(row);
                result.push([row.get_int64(0), row.get_int64(1)]);
            }
        }
        result
    }

    /// Go `checkCorrectness`: the result is the input in key order (both
    /// columns are keys, so the order is exact).
    fn check_correctness(case: &SortCase, result: &[[i64; 2]]) {
        let mut expected = case.rows.clone();
        expected.sort_unstable();
        assert_eq!(result.len(), expected.len());
        assert!(result == expected.as_slice(), "the parallel sort returned rows out of order");
    }

    /// Go `executeInFailpoint`: drains until an error or the end; the test
    /// passes when nothing hangs.
    fn execute_in_failpoint(exec: &mut SortExec<NoColumns>, hard_limit: i64, tracker: Option<&Arc<Tracker>>) {
        exec.open().unwrap();
        let mut chunk = exec.new_chunk();
        for i in 0.. {
            if exec.next(&mut chunk).is_err() || chunk.num_rows() == 0 {
                break;
            }
            if i == 10 && hard_limit > 0 {
                // Trigger the spill.
                let tracker = tracker.expect("a tracker with the hard limit");
                tracker.consume(hard_limit);
                tracker.consume(-hard_limit);
            }
        }
        exec.close().unwrap();
    }

    fn spill_files_in(dir: &Path) -> usize {
        std::fs::read_dir(dir).map_or(0, |entries| entries.filter_map(Result::ok).count())
    }

    /// Go `parallelSortTest`.
    fn parallel_sort_test(exec: &mut SortExec<NoColumns>, case: &SortCase, memory: &StatementMemory) {
        let result = execute_sort_executor(exec, None);
        exec.close().unwrap();
        check_correctness(case, &result);
        assert_eq!(memory.bytes_consumed(), 0, "close releases every byte the sort charged");
    }

    #[test]
    fn test_parallel_sort() {
        let _slow = tidb_failpoint::enable_for_test(FP_SLOW_SOME_WORKERS, "return(true)");
        let _signal = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort",
            "return(true)",
        );
        // We have two columns and ensure that nvd*nvd is less than the rows.
        let case = SortCase::new(30000, [100, 100]);
        let memory = StatementMemory::new(-1, OomAction::Log, 0);
        let mut exec = build_sort_exec(&case, &memory, 5);
        for _ in 0..10 {
            parallel_sort_test(&mut build_sort_exec(&case, &memory, 5), &case, &memory);
            parallel_sort_test(&mut exec, &case, &memory);
        }
    }

    #[test]
    fn test_failpoint() {
        let _random = tidb_failpoint::enable_for_test(FP_PARALLEL_SORT_RANDOM_FAIL, "return(true)");
        let _slow = tidb_failpoint::enable_for_test(FP_SLOW_SOME_WORKERS, "return(true)");
        let _signal = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort",
            "return(true)",
        );
        let case = SortCase::new(65536, [0, 0]);
        let memory = StatementMemory::new(-1, OomAction::Log, 0);
        let concurrency = RESULT_CHANNEL_CAPACITY * 2;
        let mut exec = build_sort_exec(&case, &memory, concurrency);
        for _ in 0..30 {
            execute_in_failpoint(&mut build_sort_exec(&case, &memory, concurrency), 0, None);
            execute_in_failpoint(&mut exec, 0, None);
        }
    }

    /// Go `oneSpillCase`.
    fn one_spill_case(exec: &mut SortExec<NoColumns>, case: &SortCase) {
        let result = execute_sort_executor(exec, None);
        assert!(exec.is_spill_triggered_in_parallel_sort_for_test());
        assert_eq!(exec.get_spilled_row_num_in_parallel_sort_for_test(), case.rows.len() as i64);
        exec.close().unwrap();
        check_correctness(case, &result);
    }

    /// Go `inMemoryThenSpill`.
    fn in_memory_then_spill(exec: &mut SortExec<NoColumns>, case: &SortCase, memory: &StatementMemory) {
        let stmt_tracker = Arc::clone(memory.stmt_tracker());
        let trigger = move || {
            stmt_tracker.consume(HARD_LIMIT_2);
            stmt_tracker.consume(-HARD_LIMIT_2);
        };
        let result = execute_sort_executor(exec, Some(&trigger));
        assert!(exec.is_spill_triggered_in_parallel_sort_for_test());
        assert!(exec.get_spilled_row_num_in_parallel_sort_for_test() < case.rows.len() as i64);
        exec.close().unwrap();
        check_correctness(case, &result);
    }

    fn spill_memory(dir: &Path, hard_limit: i64) -> StatementMemory {
        StatementMemory::new(hard_limit, OomAction::Log, 0).with_spill_storage(storage(dir))
    }

    #[test]
    fn test_parallel_sort_spill_disk() {
        let dir = scratch_dir("TestParallelSortSpillDisk");
        let _slow = tidb_failpoint::enable_for_test(FP_SLOW_SOME_WORKERS, "return(true)");
        let _signal = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort",
            "return(true)",
        );
        let case = SortCase::new(10000, [0, 0]);

        let memory = spill_memory(&dir, HARD_LIMIT_1);
        let mut exec = build_sort_exec(&case, &memory, 5);
        for _ in 0..10 {
            one_spill_case(&mut build_sort_exec(&case, &memory, 5), &case);
            one_spill_case(&mut exec, &case);
        }

        let memory = spill_memory(&dir, HARD_LIMIT_2);
        let mut exec = build_sort_exec(&case, &memory, 5);
        for _ in 0..10 {
            in_memory_then_spill(&mut build_sort_exec(&case, &memory, 5), &case, &memory);
            in_memory_then_spill(&mut exec, &case, &memory);
        }
        assert_eq!(spill_files_in(&dir), 0, "util.CheckNoLeakFiles");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_parallel_sort_spill_disk_failpoint() {
        let dir = scratch_dir("TestParallelSortSpillDiskFailpoint");
        let _slow = tidb_failpoint::enable_for_test(FP_SLOW_SOME_WORKERS, "return(true)");
        let _signal = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort",
            "return(true)",
        );
        let _random = tidb_failpoint::enable_for_test(FP_PARALLEL_SORT_RANDOM_FAIL, "return(true)");
        let _disk = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError",
            "return(true)",
        );
        let case = SortCase::new(10000, [0, 0]);

        let memory = spill_memory(&dir, HARD_LIMIT_1);
        let mut exec = build_sort_exec(&case, &memory, 5);
        for _ in 0..20 {
            execute_in_failpoint(&mut build_sort_exec(&case, &memory, 5), 0, None);
            execute_in_failpoint(&mut exec, 0, None);
        }

        let memory = spill_memory(&dir, HARD_LIMIT_2);
        let mut exec = build_sort_exec(&case, &memory, 5);
        for _ in 0..20 {
            execute_in_failpoint(
                &mut build_sort_exec(&case, &memory, 5),
                HARD_LIMIT_2,
                Some(memory.session_tracker()),
            );
            execute_in_failpoint(&mut exec, HARD_LIMIT_2, Some(memory.session_tracker()));
        }
        assert_eq!(spill_files_in(&dir), 0, "util.CheckNoLeakFiles");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_issue_59655() {
        let dir = scratch_dir("TestIssue59655");
        let _issue = tidb_failpoint::enable_for_test(FP_ISSUE_59655, "return(true)");
        let case = SortCase::new(10000, [0, 0]);
        let memory = spill_memory(&dir, HARD_LIMIT_1);
        let concurrency = RESULT_CHANNEL_CAPACITY * 2;
        let mut exec = build_sort_exec(&case, &memory, concurrency);
        for _ in 0..20 {
            execute_in_failpoint(&mut build_sort_exec(&case, &memory, concurrency), 0, None);
            execute_in_failpoint(&mut exec, 0, None);
        }
        assert_eq!(spill_files_in(&dir), 0, "util.CheckNoLeakFiles");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_issue_63216() {
        let dir = scratch_dir("TestIssue63216");
        let _issue = tidb_failpoint::enable_for_test(FP_ISSUE_63216, "return(true)");
        let case = SortCase::new(10000, [0, 0]);
        let memory = spill_memory(&dir, HARD_LIMIT_1);
        execute_in_failpoint(&mut build_sort_exec(&case, &memory, 5), 0, None);
        assert_eq!(spill_files_in(&dir), 0, "util.CheckNoLeakFiles");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
