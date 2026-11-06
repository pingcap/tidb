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

//! `pkg/executor/join/index_lookup_hash_join.go`: Go's
//! `IndexNestedLoopHashJoin`.
//!
//! One outer worker builds tasks as `IndexLookUpJoin`'s does; each of N inner
//! workers builds a hash table over a task's outer rows, reads the inner rows
//! the task's lookup contents reach and joins them with its own `Joiner`,
//! sending filled chunks to the main thread. Unordered, the inner rows probe
//! the outer hash table (`TryToMatchOuters`) and results arrive in any order;
//! with `KeepOuterOrder`, each outer row's matched inner rows are collected
//! first and joined in outer order through the task's own result channel.
//!
//! Go builds the outer hash table on its own goroutine while the inner rows
//! are fetched; here the worker builds it before the fetch.

use super::index_lookup_join::{handle_lookup, panic_message, IndexLookUpJoinParts};
use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Mutex;
use std::thread::JoinHandle;

use tidb_chunk::iterator::{Iterator4RowPtr, LendingIterator};

use crate::join_stats::IndexLookUpJoinRuntimeStats;
use crate::joiner::{JoinType, Joiner, NAAJType, OuterRowStatusFlag};

/// Go `numResChkHold`: join chunks one ordered worker holds.
const NUM_RES_CHK_HOLD: usize = 4;

/// Go `maxRowsPerFetch`: the inner rows an incremental lookup reads at once.
const MAX_ROWS_PER_FETCH: usize = 4096;

/// Go `indexHashJoinResult`: a filled join chunk and the worker's resource
/// channel it goes back to, or an error.
type HashJoinResult = Result<(Chunk, SyncSender<Chunk>), ExecError>;

/// Go `indexHashJoinTask`, as the outer worker hands it on.
struct IndexHashJoinTask {
    outer: OuterBatch,
    _outer_charge: TrackerCharge,
    /// Go `task.resultCh`, with `KeepOuterOrder` only.
    result_ch: Option<SyncSender<HashJoinResult>>,
}

/// The hash keys of one side: Go `HashCols` and `HashTypes`.
#[derive(Clone)]
struct HashKeys {
    cols: Vec<usize>,
    types: Vec<FieldType>,
}

/// Go `IndexNestedLoopHashJoin`.
pub struct IndexNestedLoopHashJoin<C: Columns + Clone + Send + Sync + 'static> {
    meta: ExecutorMeta,
    outer_child: Option<Box<dyn Executor>>,
    outer_worker: Option<JoinHandle<Box<dyn Executor>>>,
    inner_workers: Vec<JoinHandle<()>>,
    source: Arc<Mutex<IndexLookupSource>>,
    shared: Arc<IndexTaskShared<C>>,
    outer_filter: Vec<Expression>,
    outer_not_null: Vec<usize>,
    /// Go `Joiners`, one per inner worker.
    joiner: Box<dyn Joiner>,
    keep_outer_order: bool,
    outer_hash: HashKeys,
    inner_hash: HashKeys,
    null_eq: Vec<bool>,
    /// Go `resultCh` (unordered).
    result_ch: Option<Receiver<HashJoinResult>>,
    /// Go `taskCh` (ordered): each task's result channel, in outer order.
    task_ch: Option<Receiver<Receiver<HashJoinResult>>>,
    /// Go `curTask`.
    cur_task: Option<Receiver<HashJoinResult>>,
    finished: Arc<AtomicBool>,
    concurrency: usize,
    max_batch_size: usize,
    worker_ranges_bytes: i64,
    runtime_stats: Option<(Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>, i64)>,
    stats: Option<Arc<IndexLookUpJoinRuntimeStats>>,
    prepared: bool,
}

impl<C: Columns + Clone + Send + Sync + 'static> IndexNestedLoopHashJoin<C> {
    /// Go `executorBuilder.buildIndexNestedLoopHashJoin`'s executor, from the
    /// same parts as [`IndexLookUpJoin::new`].
    pub fn new(build: IndexLookUpJoinBuild<C>, keep_outer_order: bool) -> Result<Self, ExecError> {
        let parts = IndexLookUpJoinParts::new(build, false)?;
        let outer_is_left = parts.shared.outer_is_left;
        let side = |key: &EquiKey, outer: bool| {
            if outer == outer_is_left {
                key.left
            } else {
                key.right
            }
        };
        let mut outer_hash = HashKeys { cols: Vec::new(), types: Vec::new() };
        let mut inner_hash = HashKeys { cols: Vec::new(), types: Vec::new() };
        for key in &parts.shared.keys {
            let (outer_col, inner_col) = (side(key, true), side(key, false));
            let inner_type = parts.shared.inner_types[inner_col].clone();
            // Go `buildIndexLookUpJoin`: "Use the probe table's collation".
            let mut outer_type = parts.shared.outer_types[outer_col].clone();
            if outer_type.eval_type() == tidb_datatype::EvalType::String {
                outer_type.set_collation(inner_type.collation());
            }
            outer_hash.cols.push(outer_col);
            outer_hash.types.push(outer_type);
            inner_hash.cols.push(inner_col);
            inner_hash.types.push(inner_type);
        }
        let null_eq = parts.shared.keys.iter().map(|key| key.null_safe).collect();
        Ok(Self {
            meta: parts.meta,
            outer_child: Some(parts.outer),
            outer_worker: None,
            inner_workers: Vec::new(),
            source: Arc::new(Mutex::new(parts.source)),
            shared: parts.shared,
            outer_filter: parts.outer_filter,
            outer_not_null: parts.outer_not_null,
            joiner: parts.joiner,
            keep_outer_order,
            outer_hash,
            inner_hash,
            null_eq,
            result_ch: None,
            task_ch: None,
            cur_task: None,
            finished: Arc::new(AtomicBool::new(false)),
            concurrency: parts.concurrency,
            max_batch_size: parts.max_batch_size,
            worker_ranges_bytes: parts.worker_ranges_bytes,
            runtime_stats: parts.runtime_stats,
            stats: parts.stats,
            prepared: false,
        })
    }

    /// Go `supportIncrementalLookUp`.
    fn support_incremental_look_up(&self) -> bool {
        !self.keep_outer_order
            && matches!(
                self.joiner.join_type(),
                JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::AntiSemiJoin
            )
    }

    /// Go `startWorkers`.
    fn start_workers(&mut self, init_batch_size: usize) -> Result<(), ExecError> {
        let concurrency = self.concurrency;
        let (inner_tx, inner_rx) = crossbeam_channel::bounded::<IndexHashJoinTask>(concurrency);
        let (result_tx, result_rx) = sync_channel::<HashJoinResult>(concurrency);
        let (task_tx, task_rx) = sync_channel::<Receiver<HashJoinResult>>(concurrency);
        if self.keep_outer_order {
            self.task_ch = Some(task_rx);
        } else {
            self.result_ch = Some(result_rx);
        }
        let mut child = self
            .outer_child
            .take()
            .ok_or_else(|| ExecError::internal("index hash join has no outer child"))?;
        let outer = HashJoinOuterWorker {
            shared: Arc::clone(&self.shared),
            outer_filter: self.outer_filter.clone(),
            outer_not_null: self.outer_not_null.clone(),
            batch_size: init_batch_size.min(self.max_batch_size),
            max_batch_size: self.max_batch_size,
            keep_outer_order: self.keep_outer_order,
            finished: Arc::clone(&self.finished),
        };
        let unordered_errors = (!self.keep_outer_order).then(|| result_tx.clone());
        let span = tidb_tracing::current_span();
        let acc = tidb_executor_internal_exec::ruv2::current_next_io_acc();
        self.outer_worker = Some(
            std::thread::Builder::new()
                .name("IndexHashJoinOuterWorker".to_owned())
                .spawn(move || {
                    let _span = tidb_tracing::with_thread_span(span);
                    let _acc = tidb_executor_internal_exec::ruv2::with_next_io_acc(acc);
                    outer.run(child.as_mut(), &inner_tx, &task_tx, unordered_errors.as_ref());
                    child
                })
                .map_err(|error| ExecError::internal(format!("index hash join outer worker: {error}")))?,
        );
        let max_fetch_rows = self.support_incremental_look_up().then_some(MAX_ROWS_PER_FETCH);
        let held = if self.keep_outer_order { NUM_RES_CHK_HOLD } else { 1 };
        for _ in 0..concurrency {
            self.shared
                .memory
                .stmt_tracker()
                .consume(self.worker_ranges_bytes);
            // Go `joinChkResourceCh[i]`.
            let (resource_tx, resource_rx) = sync_channel(held);
            for _ in 0..held {
                // Go `exec.NewFirstChunk(e)`.
                let _ = resource_tx.send(self.meta.new_first_chunk());
            }
            let worker = HashJoinInnerWorker {
                shared: Arc::clone(&self.shared),
                source: Arc::clone(&self.source),
                finished: Arc::clone(&self.finished),
                joiner: self.joiner.clone_joiner(),
                outer_hash: self.outer_hash.clone(),
                inner_hash: self.inner_hash.clone(),
                null_eq: self.null_eq.clone(),
                max_fetch_rows,
                resource_tx,
                resource_rx,
                result_ch: (!self.keep_outer_order).then(|| result_tx.clone()),
            };
            let inner_rx = inner_rx.clone();
            let span = tidb_tracing::current_span();
            let acc = tidb_executor_internal_exec::ruv2::current_next_io_acc();
            self.inner_workers.push(
                std::thread::Builder::new()
                    .name("IndexHashJoinInnerWorker".to_owned())
                    .spawn(move || {
                        let _span = tidb_tracing::with_thread_span(span);
                        let _acc = tidb_executor_internal_exec::ruv2::with_next_io_acc(acc);
                        worker.run(&inner_rx);
                    })
                    .map_err(|error| ExecError::internal(format!("index hash join inner worker: {error}")))?,
            );
        }
        Ok(())
    }

    /// Go `getResultFromChannel` + `handleResult`.
    fn handle_result(req: &mut Chunk, result: Option<HashJoinResult>) -> Result<(), ExecError> {
        if let Some(result) = result {
            let (mut chk, src) = result?;
            req.swap_columns(&mut chk);
            let _ = src.send(chk);
        }
        Ok(())
    }
}

/// Go `indexHashJoinOuterWorker`.
struct HashJoinOuterWorker<C: Columns> {
    shared: Arc<IndexTaskShared<C>>,
    outer_filter: Vec<Expression>,
    outer_not_null: Vec<usize>,
    batch_size: usize,
    max_batch_size: usize,
    keep_outer_order: bool,
    finished: Arc<AtomicBool>,
}

impl<C: Columns> HashJoinOuterWorker<C> {
    /// Go `indexHashJoinOuterWorker.run`: a build error or a recovered panic
    /// reaches the main thread through the result path it reads.
    fn run(
        mut self,
        child: &mut dyn Executor,
        inner_ch: &crossbeam_channel::Sender<IndexHashJoinTask>,
        task_ch: &SyncSender<Receiver<HashJoinResult>>,
        result_ch: Option<&SyncSender<HashJoinResult>>,
    ) {
        let outcome = catch_unwind(AssertUnwindSafe(|| self.run_tasks(child, inner_ch, task_ch)));
        let error = match outcome {
            Ok(Ok(())) => return,
            Ok(Err(error)) => error,
            Err(panic) => {
                self.finished.store(true, Ordering::SeqCst);
                ExecError::internal(format!("IndexHashJoinOuterWorker panicked: {}", panic_message(&panic)))
            }
        };
        match result_ch {
            Some(result_ch) => {
                let _ = result_ch.send(Err(error));
            }
            None => {
                let (tx, rx) = sync_channel(1);
                let _ = tx.send(Err(error));
                let _ = task_ch.send(rx);
            }
        }
    }

    fn run_tasks(
        &mut self,
        child: &mut dyn Executor,
        inner_ch: &crossbeam_channel::Sender<IndexHashJoinTask>,
        task_ch: &SyncSender<Receiver<HashJoinResult>>,
    ) -> Result<(), ExecError> {
        loop {
            if self.finished.load(Ordering::SeqCst) {
                return Ok(());
            }
            // Go's hash join never publishes its parent's required rows.
            let outer = build_outer_task(
                child,
                &self.shared.outer_types,
                &self.outer_filter,
                &self.outer_not_null,
                &self.shared.ctx,
                self.shared.vectorized,
                &mut self.batch_size,
                self.max_batch_size,
                0,
            )?;
            if outer.rows.len() == 0 {
                return Ok(());
            }
            let charge = TrackerCharge::new(&self.shared.tracker, outer.bytes);
            self.shared.memory.check().map_err(ExecError::from)?;
            let (result_tx, result_rx) = if self.keep_outer_order {
                let (tx, rx) = sync_channel(NUM_RES_CHK_HOLD);
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };
            let task = IndexHashJoinTask {
                outer,
                _outer_charge: charge,
                result_ch: result_tx,
            };
            if inner_ch.send(task).is_err() {
                return Ok(());
            }
            if let Some(result_rx) = result_rx {
                if task_ch.send(result_rx).is_err() {
                    return Ok(());
                }
            }
        }
    }
}

/// Go `indexHashJoinInnerWorker`.
struct HashJoinInnerWorker<C: Columns> {
    shared: Arc<IndexTaskShared<C>>,
    source: Arc<Mutex<IndexLookupSource>>,
    finished: Arc<AtomicBool>,
    joiner: Box<dyn Joiner>,
    outer_hash: HashKeys,
    inner_hash: HashKeys,
    null_eq: Vec<bool>,
    /// Go `maxFetchSize`.
    max_fetch_rows: Option<usize>,
    resource_tx: SyncSender<Chunk>,
    resource_rx: Receiver<Chunk>,
    /// Go `iw.resultCh`, unordered only.
    result_ch: Option<SyncSender<HashJoinResult>>,
}

/// Go `task.lookupMap`: the outer rows by the hash of their join keys.
type OuterLookupMap = FastBytesMap<Vec<RowPtr>>;

impl<C: Columns> HashJoinInnerWorker<C> {
    /// Go `indexHashJoinInnerWorker.run`.
    fn run(mut self, task_ch: &crossbeam_channel::Receiver<IndexHashJoinTask>) {
        let Ok(mut join_chk) = self.resource_rx.recv() else {
            return;
        };
        for task in task_ch.iter() {
            if self.finished.load(Ordering::SeqCst) {
                return;
            }
            let task_result = task.result_ch.clone();
            let outcome = catch_unwind(AssertUnwindSafe(|| self.handle_task(task, join_chk)))
                .unwrap_or_else(|panic| {
                    self.finished.store(true, Ordering::SeqCst);
                    Err(ExecError::internal(format!(
                        "IndexHashJoinInnerWorker panicked: {}",
                        panic_message(&panic)
                    )))
                });
            match outcome {
                Ok(chk) => join_chk = chk,
                Err(error) => {
                    let sink = task_result.as_ref().or(self.result_ch.as_ref());
                    if let Some(sink) = sink {
                        let _ = sink.send(Err(error));
                    }
                    if task_result.is_none() {
                        return;
                    }
                    let Ok(chk) = self.resource_rx.recv() else {
                        return;
                    };
                    join_chk = chk;
                }
            }
        }
        // Go: an unordered worker's last partial chunk goes to the result
        // channel when its task channel closes.
        if let Some(result_ch) = &self.result_ch {
            if join_chk.num_rows() > 0 {
                let _ = result_ch.send(Ok((join_chk, self.resource_tx.clone())));
            }
        }
    }

    /// Go `indexHashJoinInnerWorker.handleTask`: returns the join chunk the
    /// worker holds afterwards.
    fn handle_task(&mut self, mut task: IndexHashJoinTask, join_chk: Chunk) -> Result<Chunk, ExecError> {
        let lookup_map = self.build_hash_table_for_outer_result(&task.outer)?;
        let ordered = task.result_ch.is_some();
        let source = Arc::clone(&self.source);
        let shared = Arc::clone(&self.shared);
        let max_fetch_rows = self.max_fetch_rows;
        let mut join_chk = Some(join_chk);
        // Go `task.outerRowStatus`, kept across the task's inner windows.
        let mut status = FastBytesMap::default();
        let outer = &mut task.outer;
        handle_lookup(&shared, &source, outer, max_fetch_rows, |outer, inner, done| {
            if ordered {
                return Ok(());
            }
            let chk = join_chk.take().expect("the worker holds a join chunk");
            join_chk = Some(self.do_join_unordered(outer, &lookup_map, inner, done, chk, &mut status)?);
            Ok(())
        })
        .and_then(|inner| {
            let chk = join_chk.take().expect("the worker holds a join chunk");
            if !ordered {
                return Ok(chk);
            }
            let result_ch = task.result_ch.take().expect("an ordered task's result channel");
            self.do_join_in_order(&task.outer, &lookup_map, &inner, chk, &result_ch)
        })
    }

    fn hash_of(keys: &HashKeys, row: Row<'_>) -> Result<Vec<u8>, ExecError> {
        let datums: Vec<Datum> = keys
            .cols
            .iter()
            .zip(&keys.types)
            .map(|(col, field_type)| row.get_datum(*col, field_type))
            .collect();
        let indices: Vec<usize> = (0..datums.len()).collect();
        tidb_codec::hash_row(&datums, &keys.types, &indices)
            .map_err(|error| ExecError::internal(format!("index hash join key hash: {error:?}")))
    }

    fn has_null_in_join_key(&self, keys: &HashKeys, row: Row<'_>) -> bool {
        keys.cols
            .iter()
            .zip(&self.null_eq)
            .any(|(col, null_eq)| row.is_null(*col) && !null_eq)
    }

    /// Go `buildHashTableForOuterResult`: every outer row passing the outer
    /// filter and carrying no NULL in a `=` key.
    fn build_hash_table_for_outer_result(&self, outer: &OuterBatch) -> Result<OuterLookupMap, ExecError> {
        let started = std::time::Instant::now();
        let mut lookup_map = OuterLookupMap::default();
        for index in 0..outer.len() {
            if !outer.outer_matched(index) {
                continue;
            }
            let row = outer.row(index);
            if self.has_null_in_join_key(&self.outer_hash, row) {
                continue;
            }
            lookup_map
                .entry(Self::hash_of(&self.outer_hash, row)?)
                .or_default()
                .push(outer.ptrs[index]);
        }
        if let Some(stats) = &self.shared.stats {
            stats.record_build(started.elapsed());
        }
        Ok(lookup_map)
    }

    /// Go `getMatchedOuterRows`: the outer rows whose keys equal the inner
    /// row's; a semi join skips the ones already matched.
    fn get_matched_outer_rows(
        &self,
        inner_row: Row<'_>,
        outer: &OuterBatch,
        lookup_map: &OuterLookupMap,
        status: &FastBytesMap<OuterRowStatusFlag>,
    ) -> Result<Vec<RowPtr>, ExecError> {
        if self.has_null_in_join_key(&self.inner_hash, inner_row) {
            return Ok(Vec::new());
        }
        let Some(candidates) = lookup_map.get(&Self::hash_of(&self.inner_hash, inner_row)?) else {
            return Ok(Vec::new());
        };
        let is_semi_join = matches!(
            self.joiner.join_type(),
            JoinType::SemiJoin | JoinType::AntiSemiJoin | JoinType::LeftOuterSemiJoin | JoinType::AntiLeftOuterSemiJoin
        );
        let inner_datums = key_datums(&self.inner_hash, inner_row);
        let indices: Vec<usize> = (0..inner_datums.len()).collect();
        let mut matched = Vec::new();
        for ptr in candidates {
            let outer_datums = key_datums(&self.outer_hash, outer.rows.get_row(*ptr));
            let equal = tidb_codec::equal_chunk_row(
                &inner_datums,
                &self.inner_hash.types,
                &indices,
                &outer_datums,
                &self.outer_hash.types,
                &indices,
            )
            .map_err(|error| ExecError::internal(format!("index hash join key compare: {error:?}")))?;
            if !equal || (is_semi_join && status.get(&ptr_key(*ptr)) == Some(&OuterRowStatusFlag::Matched)) {
                continue;
            }
            matched.push(*ptr);
        }
        Ok(matched)
    }

    /// Hands a full join chunk to the main thread and takes a fresh one.
    fn send(&self, chk: Chunk, result_ch: &SyncSender<HashJoinResult>) -> Result<Chunk, ExecError> {
        result_ch
            .send(Ok((chk, self.resource_tx.clone())))
            .map_err(|_| ExecError::internal("the index hash join stopped"))?;
        self.resource_rx
            .recv()
            .map_err(|_| ExecError::internal("indexHashJoinInnerWorker.doJoinUnordered failed"))
    }

    /// Go `doJoinUnordered` over one window of inner rows; once the inner
    /// side is done, the outer rows nothing matched reach `OnMissMatch`.
    fn do_join_unordered(
        &mut self,
        outer: &OuterBatch,
        lookup_map: &OuterLookupMap,
        inner: &PreparedIndexInner,
        done: bool,
        mut chk: Chunk,
        status: &mut FastBytesMap<OuterRowStatusFlag>,
    ) -> Result<Chunk, ExecError> {
        let started = std::time::Instant::now();
        let result_ch = self.result_ch.clone().expect("an unordered worker's result channel");
        let mut row_status = Vec::new();
        for chk_idx in 0..inner.rows.num_chunks() {
            for row_idx in 0..inner.rows.num_rows_of_chunk(chk_idx) {
                let inner_row = inner.rows.get_row(RowPtr::new(chk_idx as u32, row_idx as u32));
                let matched = self.get_matched_outer_rows(inner_row, outer, lookup_map, status)?;
                if matched.is_empty() {
                    continue;
                }
                let mut cursor = 0;
                let mut iter = LendingIterator::RowPtr(Iterator4RowPtr::new(&outer.rows, matched.clone()));
                iter.begin();
                while iter.current().is_some() {
                    self.joiner.try_to_match_outers(&mut iter, inner_row, &mut chk, &mut row_status)?;
                    for flag in &row_status {
                        let key = ptr_key(matched[cursor]);
                        let current = status.get(&key).copied().unwrap_or(OuterRowStatusFlag::Unmatched);
                        if *flag == OuterRowStatusFlag::Matched || current == OuterRowStatusFlag::Unmatched {
                            status.insert(key, *flag);
                        }
                        cursor += 1;
                    }
                    if chk.is_full() {
                        chk = self.send(chk, &result_ch)?;
                    }
                }
            }
        }
        if done {
            for index in 0..outer.len() {
                let flag = status
                    .get(&ptr_key(outer.ptrs[index]))
                    .copied()
                    .unwrap_or(OuterRowStatusFlag::Unmatched);
                if flag == OuterRowStatusFlag::Matched {
                    continue;
                }
                self.joiner
                    .on_miss_match(flag == OuterRowStatusFlag::HasNull, outer.row(index), &mut chk);
                if chk.is_full() {
                    chk = self.send(chk, &result_ch)?;
                }
            }
        }
        if let Some(stats) = &self.shared.stats {
            stats.record_join(started.elapsed());
        }
        Ok(chk)
    }

    /// Go `doJoinInOrder`: each outer row's matched inner rows, joined in
    /// outer order through the task's result channel, which closes after.
    fn do_join_in_order(
        &mut self,
        outer: &OuterBatch,
        lookup_map: &OuterLookupMap,
        inner: &PreparedIndexInner,
        mut chk: Chunk,
        result_ch: &SyncSender<HashJoinResult>,
    ) -> Result<Chunk, ExecError> {
        let started = std::time::Instant::now();
        let none = FastBytesMap::default();
        let mut matched_inner: FastBytesMap<Vec<RowPtr>> = FastBytesMap::default();
        for chk_idx in 0..inner.rows.num_chunks() {
            for row_idx in 0..inner.rows.num_rows_of_chunk(chk_idx) {
                let inner_ptr = RowPtr::new(chk_idx as u32, row_idx as u32);
                let inner_row = inner.rows.get_row(inner_ptr);
                for outer_ptr in self.get_matched_outer_rows(inner_row, outer, lookup_map, &none)? {
                    matched_inner.entry(ptr_key(outer_ptr)).or_default().push(inner_ptr);
                }
            }
        }
        for index in 0..outer.len() {
            let outer_row = outer.row(index);
            let rows = matched_inner.remove(&ptr_key(outer.ptrs[index])).unwrap_or_default();
            let (mut has_matched, mut has_null) = (false, false);
            let mut iter = LendingIterator::RowPtr(Iterator4RowPtr::new(&inner.rows, rows));
            iter.begin();
            while iter.current().is_some() {
                let (matched, is_null) =
                    self.joiner.try_to_match_inners(outer_row, &mut iter, &mut chk, NAAJType::Unknown)?;
                has_matched |= matched;
                has_null |= is_null;
                if chk.is_full() {
                    chk = self.send(chk, result_ch)?;
                }
            }
            if !has_matched {
                self.joiner.on_miss_match(has_null, outer_row, &mut chk);
            }
        }
        if let Some(stats) = &self.shared.stats {
            stats.record_join(started.elapsed());
        }
        // Go: a non-empty last chunk is a result, an empty one goes back.
        if chk.num_rows() > 0 {
            chk = self.send(chk, result_ch)?;
        }
        Ok(chk)
    }
}

fn key_datums(keys: &HashKeys, row: Row<'_>) -> Vec<Datum> {
    keys.cols
        .iter()
        .zip(&keys.types)
        .map(|(col, field_type)| row.get_datum(*col, field_type))
        .collect()
}

fn ptr_key(ptr: RowPtr) -> Vec<u8> {
    let mut key = Vec::with_capacity(8);
    key.extend_from_slice(&ptr.chk_idx.to_le_bytes());
    key.extend_from_slice(&ptr.row_idx.to_le_bytes());
    key
}

impl<C: Columns + Clone + Send + Sync + 'static> Executor for IndexNestedLoopHashJoin<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.outer_child
            .as_mut()
            .ok_or_else(|| ExecError::internal("index hash join has no outer child"))?
            .open()?;
        self.source.lock().unwrap().open()?;
        self.finished.store(false, Ordering::SeqCst);
        self.prepared = false;
        Ok(())
    }

    /// Go `IndexNestedLoopHashJoin.Next`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if !self.prepared {
            self.start_workers(req.required_rows())?;
            self.prepared = true;
        }
        req.reset();
        if !self.keep_outer_order {
            let result = self.result_ch.as_ref().and_then(|ch| ch.recv().ok());
            return Self::handle_result(req, result);
        }
        // Go `runInOrder`.
        loop {
            if self.cur_task.is_none() {
                match self.task_ch.as_ref().and_then(|ch| ch.recv().ok()) {
                    Some(task) => self.cur_task = Some(task),
                    None => return Ok(()),
                }
            }
            match self.cur_task.as_ref().expect("a current task").recv() {
                Ok(result) => return Self::handle_result(req, Some(result)),
                Err(_) => self.cur_task = None,
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        if let (Some((source, plan_id)), Some(stats)) = (&self.runtime_stats, &self.stats) {
            source.register_stats(*plan_id, Box::new(stats.snapshot()));
        }
        self.finished.store(true, Ordering::SeqCst);
        self.result_ch = None;
        self.task_ch = None;
        self.cur_task = None;
        if let Some(worker) = self.outer_worker.take() {
            match worker.join() {
                Ok(child) => self.outer_child = Some(child),
                Err(_) => return Err(ExecError::internal("the index hash join outer worker panicked")),
            }
        }
        for worker in self.inner_workers.drain(..) {
            let _ = worker.join();
        }
        self.prepared = false;
        let closed = self.source.lock().unwrap().close();
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
