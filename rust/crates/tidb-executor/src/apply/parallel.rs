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

//! Go `pkg/executor/parallel_apply.go`. Persistent inner workers compete for
//! outer rows; ordered workers send per-row results through a paced reorder
//! worker. Only outerList, innerList and the shared ApplyCache charge memory.

use super::native::{ApplyRuntimeSink, ApplyRuntimeSnapshot, InnerRows};
use crate::{ExecError, Executor, ExecutorMeta, StatementMemory, StmtContext};
use crossbeam_channel::{bounded, select_biased, Receiver, Sender, TryRecvError};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, MutexGuard,
};
use std::thread::JoinHandle;
use tidb_chunk::{
    chunk::Chunk,
    iterator::{ChunkIterator, Iterator4List, LendingIterator, ListIteratorPosition},
    list::{List, RowPtr},
};
use tidb_datatype::FieldType;
use tidb_executor_internal_applycache::ApplyCache;
use tidb_executor_join::joiner::{Joiner, NAAJType};
use tidb_expr::{column::CorrelatedColumn, expression::Expression, schema::Schema};
use tidb_memory::Tracker;

/// Go `outerRow`: the row remains owned by outerList until Close.
#[derive(Clone, Copy)]
struct OuterRow {
    row: RowPtr,
    selected: bool,
    seq: u64,
}
/// Go `result`: None is EOF; errors retain their executor identity.
type ApplyResult = Result<Option<Chunk>, ExecError>;
/// Go `orderedResult`.
struct OrderedResult {
    seq: u64,
    chks: Result<Vec<Chunk>, ExecError>,
}

/// Go `ParallelNestedLoopApplyExec`'s per-worker slices, grouped by worker ID.
/// These are inner executors, not serial NestedLoopApplyExec instances.
pub(crate) struct ApplyWorker {
    inner_exec: Box<dyn Executor>,
    inner_filter: Vec<Expression>,
    cor_cols: Vec<CorrelatedColumn>,
    joiner: Box<dyn Joiner>,
    inner_list: Option<Arc<InnerRows>>,
    inner_chunk: Chunk,
    inner_selected: Vec<bool>,
    inner_iter: Option<ListIteratorPosition>,
    outer_row: Option<OuterRow>,
    has_match: bool,
    has_null: bool,
}
impl ApplyWorker {
    /// Go `executorBuilder.buildApply`: one cloned inner and Joiner per worker.
    pub(crate) fn new(
        inner_exec: Box<dyn Executor>,
        inner_filter: Vec<Expression>,
        cor_cols: Vec<CorrelatedColumn>,
        joiner: Box<dyn Joiner>,
    ) -> Self {
        Self {
            inner_exec,
            inner_filter,
            cor_cols,
            joiner,
            inner_list: None,
            inner_chunk: Chunk::default(),
            inner_selected: Vec::new(),
            inner_iter: None,
            outer_row: None,
            has_match: false,
            has_null: false,
        }
    }

    /// Go `ParallelNestedLoopApplyExec.fetchAllInners`.
    fn fetch_all_inners(&mut self, e: &Workers) -> Result<(), ExecError> {
        let mut key = Vec::new();
        {
            let list = lock(&e.outer_list);
            let row = list.get_row(self.outer_row.expect("selected outer row").row);
            for col in &self.cor_cols {
                let ty = col
                    .column
                    .ret_type
                    .as_ref()
                    .expect("correlated column type");
                let value = row.get_datum(col.column.index as usize, ty);
                col.bind(value.clone());
                if e.cache.is_some() {
                    tidb_codec::Encoder::new(tidb_datatype::new_collation_enabled())
                        .append_key_in_timezone(
                            &e.context.session_zone(),
                            &mut key,
                            std::slice::from_ref(&value),
                        )
                        .map_err(|error| {
                            ExecError::internal(format!("cannot encode Apply cache key: {error}"))
                        })?;
                }
            }
        }
        if let Some(cache) = &e.cache {
            e.cache_access_counter.fetch_add(1, Ordering::Relaxed);
            tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/parallelApplyGetCachePanic");
            if let Some(rows) = lock(cache).get(&key) {
                self.inner_list = Some(rows);
                e.cache_hit_counter.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }
        // Go defers Close even when Open fails or panics.
        let result = tidb_driver_error::panic_recovery::recover_executor_panic(|| {
            self.inner_exec.open()?;
            if !e.delay("github.com/pingcap/tidb/pkg/executor/parallelApplySlowInner")? {
                return Ok(());
            }
            if e.cache.is_some() {
                // Go NewList here has its own, unattached tracker. ApplyCache.Set
                // charges its consumed bytes; it does not inherit the Open list.
                self.inner_list = Some(Arc::new(InnerRows {
                    list: List::new(
                        self.inner_exec.ret_field_types(),
                        e.meta.init_cap(),
                        e.meta.max_chunk_size(),
                    ),
                }));
            } else {
                Arc::get_mut(self.inner_list.as_mut().expect("Open inner list"))
                    .expect("uncached list is private")
                    .list
                    .reset();
            }
            loop {
                if e.exiting() {
                    return Ok(());
                }
                e.memory.check().map_err(ExecError::from)?;
                self.inner_exec.next(&mut self.inner_chunk)?;
                if self.inner_chunk.num_rows() == 0 {
                    break;
                }
                self.inner_selected = tidb_expr::evaluator::vectorized_filter(
                    &e.context,
                    e.context.enable_vectorized_expression(),
                    &self.inner_filter,
                    &self.inner_chunk,
                    std::mem::take(&mut self.inner_selected),
                )?;
                let rows = Arc::get_mut(self.inner_list.as_mut().unwrap())
                    .expect("new inner list is private");
                for i in 0..self.inner_chunk.num_rows() {
                    let row = self.inner_chunk.get_row(i);
                    if self.inner_selected[row.idx()] {
                        rows.list.append_row(row);
                    }
                }
                e.memory.check().map_err(ExecError::from)?;
            }
            Ok(())
        });
        let _ =
            tidb_driver_error::panic_recovery::recover_executor_panic(|| self.inner_exec.close());
        result?;
        if let Some(cache) = &e.cache {
            tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/parallelApplySetCachePanic");
            let rows = self.inner_list.as_ref().unwrap();
            let cache = lock(cache);
            cache.set_shared(key, rows.clone(), rows.list.mem_tracker().bytes_consumed());
            e.cache_tracker.replace_bytes_used(cache.memory_consumed());
        }
        e.memory.check().map_err(ExecError::from)
    }

    /// Go `ParallelNestedLoopApplyExec.fetchNextOuterRow`.
    fn fetch_next_outer_row(&mut self, e: &Workers, req: &mut Chunk) -> Option<OuterRow> {
        loop {
            let row = e.receive(&e.outer_row_ch)?;
            if row.selected {
                return Some(row);
            }
            if e.outer {
                self.joiner
                    .on_miss_match(false, lock(&e.outer_list).get_row(row.row), req);
                if req.is_full() {
                    return None;
                }
            }
        }
    }

    /// Go `Joiner.TryToMatchInners`, retaining innerIter across result chunks.
    fn try_to_match_inners(&mut self, e: &Workers, req: &mut Chunk) -> Result<bool, ExecError> {
        let rows = self.inner_list.as_ref().expect("fetched inner list");
        let mut iter = match self.inner_iter {
            Some(position) => LendingIterator::List(Iterator4List::resume(&rows.list, position)),
            None => {
                let mut iter = LendingIterator::list(&rows.list);
                iter.begin();
                iter
            }
        };
        if iter.current().is_some() {
            let list = lock(&e.outer_list);
            let (matched, null) = self.joiner.try_to_match_inners(
                list.get_row(self.outer_row.unwrap().row),
                &mut iter,
                req,
                NAAJType::Unknown,
            )?;
            self.has_match |= matched;
            self.has_null |= null;
        }
        let exhausted = iter.current().is_none();
        let LendingIterator::List(iter) = iter else {
            unreachable!()
        };
        self.inner_iter = Some(iter.position());
        Ok(exhausted)
    }

    /// Go `ParallelNestedLoopApplyExec.fillInnerChunk`.
    fn fill_inner_chunk(&mut self, e: &Workers, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        loop {
            let exhausted = match (self.inner_iter, self.inner_list.as_ref()) {
                (Some(position), Some(rows)) => Iterator4List::resume(&rows.list, position)
                    .current()
                    .is_none(),
                _ => true,
            };
            if exhausted {
                if let Some(row) = self.outer_row {
                    if !self.has_match {
                        self.joiner.on_miss_match(
                            self.has_null,
                            lock(&e.outer_list).get_row(row.row),
                            req,
                        );
                    }
                }
                self.outer_row = self.fetch_next_outer_row(e, req);
                if req.is_full() || self.outer_row.is_none() {
                    return Ok(());
                }
                self.has_match = false;
                self.has_null = false;
                self.fetch_all_inners(e)?;
                self.inner_iter = None;
            }
            self.try_to_match_inners(e, req)?;
            if req.is_full() || e.exiting() {
                return Ok(());
            }
        }
    }

    /// Go `ParallelNestedLoopApplyExec.processOneOuterRow`.
    fn process_one_outer_row(
        &mut self,
        e: &Workers,
        row: OuterRow,
    ) -> Result<Vec<Chunk>, ExecError> {
        if !row.selected {
            if !e.outer {
                return Ok(Vec::new());
            }
            let mut chk = Chunk::new(e.meta.ret_field_types(), 1, 1);
            self.joiner
                .on_miss_match(false, lock(&e.outer_list).get_row(row.row), &mut chk);
            return Ok(vec![chk]);
        }
        let mut chk = e.meta.new_first_chunk();
        self.outer_row = Some(row);
        self.has_match = false;
        self.has_null = false;
        self.fetch_all_inners(e)?;
        self.inner_iter = None;
        let mut chks = Vec::new();
        loop {
            let exhausted = self.try_to_match_inners(e, &mut chk)?;
            if chk.is_full() {
                chks.push(chk);
                chk = e.meta.new_first_chunk();
            }
            if exhausted || e.exiting() {
                break;
            }
        }
        if !self.has_match {
            self.joiner.on_miss_match(
                self.has_null,
                lock(&e.outer_list).get_row(row.row),
                &mut chk,
            );
        }
        if chk.num_rows() > 0 {
            chks.push(chk);
        }
        Ok(chks)
    }
}

/// Go `ParallelNestedLoopApplyExec`'s shared worker state and channels.
struct Workers {
    meta: ExecutorMeta,
    context: StmtContext,
    memory: StatementMemory,
    outer: bool,
    outer_list: Mutex<List>,
    free_chk_ch: (Sender<Chunk>, Receiver<Chunk>),
    result_chk_ch: (Sender<ApplyResult>, Receiver<ApplyResult>),
    outer_row_ch: Receiver<OuterRow>,
    outer_pace_ch: Option<(Sender<()>, Receiver<()>)>,
    exit: Receiver<()>,
    cache: Option<Mutex<ApplyCache<InnerRows>>>,
    cache_tracker: Arc<Tracker>,
    cache_access_counter: AtomicU64,
    cache_hit_counter: AtomicU64,
}
/// Go `sync.Mutex.Lock` / `Unlock`; poisoned worker locks remain closable.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
impl Workers {
    /// Go `ParallelNestedLoopApplyExec` worker exit selects.
    fn exiting(&self) -> bool {
        !matches!(self.exit.try_recv(), Err(TryRecvError::Empty))
    }
    /// Go `ParallelNestedLoopApplyExec` channel receives selected against `exit`.
    fn receive<T>(&self, ch: &Receiver<T>) -> Option<T> {
        select_biased! { recv(self.exit) -> _ => None, recv(ch) -> value => value.ok() }
    }
    /// Go `ParallelNestedLoopApplyExec` channel sends selected against `exit`.
    fn send<T>(&self, ch: &Sender<T>, value: T) -> bool {
        select_biased! { recv(self.exit) -> _ => false, send(ch, value) -> result => result.is_ok() }
    }
    /// Go `ParallelNestedLoopApplyExec.putResult`: true means exit.
    fn put_result(&self, result: ApplyResult) -> bool {
        !self.send(&self.result_chk_ch.0, result)
    }
    /// Go `parallelApplySlowInner` / `parallelApplyOrderedSleep` cancellable delay.
    fn delay(&self, name: &str) -> Result<bool, ExecError> {
        if let Some(ms) = tidb_failpoint::eval(name).and_then(|value| value.as_int()) {
            select_biased! {
                recv(self.exit) -> _ => return Ok(false),
                recv(crossbeam_channel::after(std::time::Duration::from_millis(ms.max(0) as u64))) -> _ => {}
            }
        }
        self.memory.check().map_err(ExecError::from)?;
        Ok(!self.exiting())
    }
    /// Go `ParallelNestedLoopApplyExec.handleWorkerPanic`.
    fn handle_worker_panic(&self, work: impl FnOnce() -> Result<(), ExecError>) {
        let result = tidb_driver_error::panic_recovery::recover_worker_panic(work);
        if let Err(error) = result {
            if !self.exiting() {
                self.put_result(Err(error));
            }
        }
    }
    /// Go `ParallelNestedLoopApplyExec.outerWorker`.
    fn outer_worker(
        &self,
        outer: &Mutex<Box<dyn Executor>>,
        filter: &[Expression],
        rows: Sender<OuterRow>,
    ) -> Result<(), ExecError> {
        let mut selected = Vec::new();
        let mut seq = 0;
        loop {
            tidb_failpoint::eval(
                "github.com/pingcap/tidb/pkg/executor/parallelApplyOuterWorkerPanic",
            );
            if self.exiting() {
                return Ok(());
            }
            let mut chk = lock(outer).new_chunk();
            lock(outer).next(&mut chk)?;
            if chk.num_rows() == 0 {
                return Ok(());
            }
            let mut list = lock(&self.outer_list);
            let chk_idx = list.num_chunks();
            list.add(chk);
            let chk = list.get_chunk(chk_idx);
            selected = tidb_expr::evaluator::vectorized_filter(
                &self.context,
                self.context.enable_vectorized_expression(),
                filter,
                chk,
                selected,
            )?;
            let outgoing: Vec<_> = (0..chk.num_rows())
                .map(|i| OuterRow {
                    row: RowPtr::new(chk_idx as u32, i as u32),
                    selected: selected[chk.get_row(i).idx()],
                    seq: seq + i as u64,
                })
                .collect();
            seq += outgoing.len() as u64;
            drop(list);
            self.memory.check().map_err(ExecError::from)?;
            for row in outgoing {
                if let Some(pace) = &self.outer_pace_ch {
                    if !self.send(&pace.0, ()) {
                        return Ok(());
                    }
                }
                if !self.send(&rows, row) {
                    return Ok(());
                }
            }
        }
    }
    /// Go `ParallelNestedLoopApplyExec.innerWorker`.
    fn inner_worker(&self, worker: &mut ApplyWorker) -> Result<(), ExecError> {
        while let Some(mut chk) = self.receive(&self.free_chk_ch.1) {
            tidb_failpoint::eval(
                "github.com/pingcap/tidb/pkg/executor/parallelApplyInnerWorkerPanic",
            );
            worker.fill_inner_chunk(self, &mut chk)?;
            if chk.num_rows() == 0 || self.put_result(Ok(Some(chk))) {
                return Ok(());
            }
        }
        Ok(())
    }
    /// Go `ParallelNestedLoopApplyExec.innerWorkerOrdered`.
    fn inner_worker_ordered(
        &self,
        worker: &mut ApplyWorker,
        ordered: Sender<OrderedResult>,
    ) -> Result<(), ExecError> {
        while let Some(row) = self.receive(&self.outer_row_ch) {
            tidb_failpoint::eval(
                "github.com/pingcap/tidb/pkg/executor/parallelApplyInnerWorkerOrderedPanic",
            );
            if !self.delay("github.com/pingcap/tidb/pkg/executor/parallelApplyOrderedSleep")? {
                return Ok(());
            }
            let chks = worker.process_one_outer_row(self, row);
            let failed = chks.is_err();
            if !self.send(&ordered, OrderedResult { seq: row.seq, chks }) || failed {
                return Ok(());
            }
        }
        Ok(())
    }
    /// Go `reorderWorker.flushOutput`.
    fn flush_output(&self, output: Chunk) -> Option<Chunk> {
        if self.put_result(Ok(Some(output))) {
            return None;
        }
        let mut output = self.receive(&self.free_chk_ch.1)?;
        output.reset();
        Some(output)
    }
    /// Go `ParallelNestedLoopApplyExec.reorderWorker`.
    fn reorder_worker(&self, ordered: Receiver<OrderedResult>) -> Result<(), ExecError> {
        let Some(mut output) = self.receive(&self.free_chk_ch.1) else {
            return Ok(());
        };
        let mut pending = BTreeMap::new();
        let mut next_seq = 0;
        while let Some(result) = self.receive(&ordered) {
            pending.insert(result.seq, result.chks?);
            loop {
                while let Some(chks) = pending.remove(&next_seq) {
                    next_seq += 1;
                    self.outer_pace_ch
                        .as_ref()
                        .unwrap()
                        .1
                        .recv()
                        .expect("dispatched row owns pace token");
                    for chk in chks {
                        for i in 0..chk.num_rows() {
                            output.append_row(chk.get_row(i));
                            if output.is_full() {
                                let Some(next) = self.flush_output(output) else {
                                    return Ok(());
                                };
                                output = next;
                            }
                        }
                    }
                }
                if output.num_rows() == 0 {
                    break;
                }
                match ordered.try_recv() {
                    Ok(result) => {
                        pending.insert(result.seq, result.chks?);
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        self.put_result(Ok(Some(output)));
                        self.put_result(Ok(None));
                        return Ok(());
                    }
                    Err(TryRecvError::Empty) => {}
                }
                let Some(next) = self.flush_output(output) else {
                    return Ok(());
                };
                output = next;
                break;
            }
        }
        if !self.exiting() {
            if output.num_rows() > 0 {
                self.put_result(Ok(Some(output)));
            }
            self.put_result(Ok(None));
        }
        Ok(())
    }
    /// Go `ParallelNestedLoopApplyExec.notifyWorker` (ordered mode's bridge
    /// uses the same join, with orderedResultCh closed by its last sender).
    fn notify_worker(&self, handles: &Mutex<Vec<JoinHandle<()>>>, ordered: bool) {
        for handle in lock(handles).drain(..) {
            let _ = handle.join();
        }
        if !ordered {
            self.put_result(Ok(None));
        }
    }
}

/// Go `ParallelNestedLoopApplyExec`.
pub(crate) struct ParallelNestedLoopApplyExec {
    meta: ExecutorMeta,
    outer_exec: Arc<Mutex<Box<dyn Executor>>>,
    outer_filter: Vec<Expression>,
    workers: Vec<Arc<Mutex<ApplyWorker>>>,
    outer: bool,
    keep_order: bool,
    cache_enabled: bool,
    context: StmtContext,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
    shared: Option<Arc<Workers>>,
    outer_row_tx: Option<Sender<OuterRow>>,
    exit: Option<Sender<()>>,
    worker_wg: Arc<Mutex<Vec<JoinHandle<()>>>>,
    notify_wg: Vec<JoinHandle<()>>,
    started: bool,
    drained: bool,
    opened: bool,
    runtime_sink: Option<ApplyRuntimeSink>,
}
impl ParallelNestedLoopApplyExec {
    /// Go `executorBuilder.buildApply`.
    pub(crate) fn new(
        meta: ExecutorMeta,
        outer_exec: Box<dyn Executor>,
        outer_filter: Vec<Expression>,
        workers: Vec<ApplyWorker>,
        outer: bool,
        keep_order: bool,
        cache_enabled: bool,
        context: StmtContext,
        runtime_sink: Option<ApplyRuntimeSink>,
    ) -> Self {
        let memory = context.statement_memory();
        let tracker = memory.operator_tracker(meta.id());
        Self {
            meta,
            outer_exec: Arc::new(Mutex::new(outer_exec)),
            outer_filter,
            workers: workers
                .into_iter()
                .map(|worker| Arc::new(Mutex::new(worker)))
                .collect(),
            outer,
            keep_order,
            cache_enabled,
            context,
            memory,
            tracker,
            shared: None,
            outer_row_tx: None,
            exit: None,
            worker_wg: Arc::new(Mutex::new(Vec::new())),
            notify_wg: Vec::new(),
            started: false,
            drained: false,
            opened: false,
            runtime_sink,
        }
    }
    /// Go `ParallelNestedLoopApplyExec.Next`'s first-call worker launch.
    fn start_workers(&mut self) -> Result<(), ExecError> {
        self.started = true;
        let e = self.shared.as_ref().unwrap().clone();
        let outer = self.outer_exec.clone();
        let filter = self.outer_filter.clone();
        let rows = self.outer_row_tx.take().unwrap();
        lock(&self.worker_wg).push(spawn_worker(move || {
            e.handle_worker_panic(|| e.outer_worker(&outer, &filter, rows))
        })?);
        let (ordered_tx, ordered_rx) = bounded(self.workers.len() * 2);
        for worker in &self.workers {
            let e = self.shared.as_ref().unwrap().clone();
            let worker = worker.clone();
            let ordered = self.keep_order.then(|| ordered_tx.clone());
            lock(&self.worker_wg).push(spawn_worker(move || {
                e.handle_worker_panic(|| match ordered {
                    Some(ordered) => e.inner_worker_ordered(&mut lock(&worker), ordered),
                    None => e.inner_worker(&mut lock(&worker)),
                })
            })?);
        }
        if self.keep_order {
            let e = self.shared.as_ref().unwrap().clone();
            self.notify_wg.push(spawn_worker(move || {
                e.handle_worker_panic(|| e.reorder_worker(ordered_rx))
            })?);
        }
        let e = self.shared.as_ref().unwrap().clone();
        let handles = self.worker_wg.clone();
        let ordered = self.keep_order;
        self.notify_wg.push(spawn_worker(move || {
            e.handle_worker_panic(|| {
                e.notify_worker(&handles, ordered);
                drop(ordered_tx);
                Ok(())
            })
        })?);
        Ok(())
    }
    /// Go `ParallelNestedLoopApplyExec.Close`: cancel before joining, and
    /// never close the outer executor while a worker can still use it.
    fn retire_workers(&mut self) {
        self.exit.take();
        self.outer_row_tx.take();
        self.memory.cancel_coprocessor_workers();
        for handle in self.notify_wg.drain(..) {
            let _ = handle.join();
        }
        for handle in lock(&self.worker_wg).drain(..) {
            let _ = handle.join();
        }
        self.started = false;
    }
}
/// Go `ParallelNestedLoopApplyExec.Next`: launch workers with the parent's RU context.
fn spawn_worker(work: impl FnOnce() + Send + 'static) -> Result<JoinHandle<()>, ExecError> {
    std::thread::Builder::new()
        .name("parallel-apply".to_owned())
        .spawn(tidb_executor_internal_exec::ruv2::inherit_goroutine(work))
        .map_err(|error| {
            ExecError::internal(format!("cannot start parallel Apply worker: {error}"))
        })
}
impl Executor for ParallelNestedLoopApplyExec {
    /// Go `ParallelNestedLoopApplyExec.Open`.
    fn open(&mut self) -> Result<(), ExecError> {
        if self.opened {
            self.close()?;
        }
        self.memory.renew_coprocessor_worker_scope();
        lock(&self.outer_exec).open()?;
        self.opened = true;
        self.drained = false;
        let n = self.workers.len();
        let list = List::new(
            lock(&self.outer_exec).ret_field_types(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        list.mem_tracker()
            .set_label(tidb_memory::LABEL_FOR_OUTER_LIST);
        list.mem_tracker().attach_to(&self.tracker);
        for worker in &self.workers {
            let mut worker = lock(worker);
            worker.inner_chunk = worker.inner_exec.new_chunk();
            let list = List::new(
                worker.inner_exec.ret_field_types(),
                self.meta.init_cap(),
                self.meta.max_chunk_size(),
            );
            list.mem_tracker()
                .set_label(tidb_memory::LABEL_FOR_INNER_LIST);
            list.mem_tracker().attach_to(&self.tracker);
            worker.inner_list = Some(Arc::new(InnerRows { list }));
            worker.inner_iter = None;
            worker.outer_row = None;
            worker.inner_selected.clear();
            worker.has_match = false;
            worker.has_null = false;
        }
        let (outer_tx, outer_rx) = bounded(0);
        let (exit_tx, exit_rx) = bounded(0);
        self.outer_row_tx = Some(outer_tx);
        self.exit = Some(exit_tx);
        let cache_tracker = Tracker::new(tidb_memory::LABEL_FOR_APPLY_CACHE, -1);
        if self.cache_enabled {
            cache_tracker.attach_to(&self.tracker);
        }
        let e = Arc::new(Workers {
            meta: self.meta.clone(),
            context: self.context.clone(),
            memory: self.memory.clone(),
            outer: self.outer,
            outer_list: Mutex::new(list),
            free_chk_ch: bounded(n),
            result_chk_ch: bounded(n + 1),
            outer_row_ch: outer_rx,
            outer_pace_ch: self.keep_order.then(|| bounded(n * 4)),
            exit: exit_rx,
            cache: self
                .cache_enabled
                .then(|| Mutex::new(ApplyCache::new(self.context.apply_cache_capacity()))),
            cache_tracker,
            cache_access_counter: AtomicU64::new(0),
            cache_hit_counter: AtomicU64::new(0),
        });
        for _ in 0..n {
            e.free_chk_ch.0.send(self.meta.new_first_chunk()).unwrap();
        }
        self.shared = Some(e);
        self.memory.check().map_err(ExecError::from)
    }
    /// Go `ParallelNestedLoopApplyExec.Next`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.drained {
            req.reset();
            return Ok(());
        }
        let result = (|| {
            if !self.started {
                self.start_workers()?;
            }
            let e = self.shared.as_ref().unwrap();
            loop {
                self.memory.check().map_err(ExecError::from)?;
                select_biased! {
                    recv(e.result_chk_ch.1) -> result => {
                        match result.map_err(|_| ExecError::internal("parallel Apply result channel closed"))?? {
                            Some(mut chk) => { req.swap_columns(&mut chk); e.send(&e.free_chk_ch.0, chk); }
                            None => { req.reset(); self.drained = true; }
                        }
                        return Ok(());
                    },
                    default(std::time::Duration::from_millis(20)) => {}
                }
            }
        })();
        if result.is_err() {
            self.drained = true;
            self.retire_workers();
        }
        result
    }
    /// Go `ParallelNestedLoopApplyExec.Close`.
    fn close(&mut self) -> Result<(), ExecError> {
        self.retire_workers();
        if let Some(e) = self.shared.take() {
            if let Some(sink) = &self.runtime_sink {
                *lock(sink) = Some(ApplyRuntimeSnapshot {
                    enabled: self.cache_enabled,
                    concurrency: self.workers.len(),
                    accesses: e.cache_access_counter.load(Ordering::Relaxed),
                    hits: e.cache_hit_counter.load(Ordering::Relaxed),
                });
            }
            for worker in &self.workers {
                let mut worker = lock(worker);
                worker.inner_list = None;
                worker.inner_chunk = Chunk::default();
                worker.inner_iter = None;
                worker.outer_row = None;
            }
            let mut list = lock(&e.outer_list);
            list.clear();
            list.mem_tracker().detach();
            e.cache_tracker.detach();
        }
        if self.opened {
            self.opened = false;
            lock(&self.outer_exec).close()
        } else {
            Ok(())
        }
    }
    /// Go `BaseExecutor.Schema`.
    fn schema(&self) -> &Schema {
        self.meta.schema()
    }
    /// Go `BaseExecutor.RetFieldTypes`.
    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }
    /// Go `BaseExecutor.InitCap`.
    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }
    /// Go `BaseExecutor.MaxChunkSize`.
    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }
    /// Go `BaseExecutor.NewChunk`.
    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}
impl Drop for ParallelNestedLoopApplyExec {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
#[cfg(test)]
#[path = "parallel_tests.rs"]
mod tests;
