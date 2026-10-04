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

//! Parallel Apply coordinates independently rebound native serial workers.
//! The serial worker retains Joiner, NULL/mismatch and inner-list semantics.
//! Each lane owns one outer row and at most one queued output chunk; pacing
//! is bounded by concurrency. Ordered consumption keeps the outer order;
//! unordered consumption takes any ready lane. Close cancels blocked queues,
//! joins all admitted work, then retires shared rows/cache and the outer child.

use super::native::{ApplyRuntimeSink, ApplyRuntimeSnapshot, InnerRows, NestedLoopApplyExec};
use crate::{
    apply_cache::ApplyCache, worker_pool::LanePool, ExecError, Executor, ExecutorMeta,
    StatementMemory, StmtContext,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::{expression::Expression, schema::Schema};
use tidb_util::memory::Tracker;

/// One row owned by a lane, populated only before its task starts.
pub(crate) type WorkerFeed = Arc<Mutex<(Option<Chunk>, bool)>>;
/// Source adapter; it changes no tuple or filter policy.
pub(crate) struct WorkerOuter {
    meta: ExecutorMeta,
    feed: WorkerFeed,
}
impl WorkerOuter {
    pub(crate) fn new(meta: ExecutorMeta, feed: WorkerFeed) -> Self {
        Self { meta, feed }
    }
}
impl Executor for WorkerOuter {
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if let Some(mut row) = self
            .feed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
            .take()
        {
            std::mem::swap(req, &mut row);
        }
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        self.feed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0 = None;
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
    fn agg_tree_input_empty(&self) -> bool {
        self.feed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1
    }
}

/// Polls lane retirement before issuing another inner request.
pub(crate) struct CancellableInner {
    inner: Box<dyn Executor>,
    stop: Arc<AtomicBool>,
}
impl CancellableInner {
    pub(crate) fn new(inner: Box<dyn Executor>, stop: Arc<AtomicBool>) -> Self {
        Self { inner, stop }
    }
}
impl Executor for CancellableInner {
    fn open(&mut self) -> Result<(), ExecError> {
        self.inner.open()
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.stop.load(Ordering::Acquire) {
            req.reset();
            return Ok(());
        }
        self.inner.next(req)
    }
    fn close(&mut self) -> Result<(), ExecError> {
        self.inner.close()
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

enum Event {
    Rows(Chunk),
    Done(ApplyRuntimeSnapshot),
    Error(ExecError),
}
struct Queue {
    slot: Mutex<Option<Event>>,
    available: Condvar,
    wake: Arc<(Mutex<()>, Condvar)>,
    stop: Arc<AtomicBool>,
    tracker: Arc<Tracker>,
    memory: StatementMemory,
}
impl Queue {
    fn send(&self, event: Event) -> Result<bool, ExecError> {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while slot.is_some() && !self.stop.load(Ordering::Acquire) {
            slot = self
                .available
                .wait(slot)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if self.stop.load(Ordering::Acquire) {
            return Ok(false);
        }
        if let Event::Rows(chunk) = &event {
            self.tracker.replace_bytes_used(chunk.memory_usage());
            if let Err(error) = self.memory.check() {
                self.tracker.replace_bytes_used(0);
                return Err(error);
            }
        }
        *slot = Some(event);
        drop(slot);
        let _guard = self
            .wake
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.wake.1.notify_all();
        Ok(true)
    }
    fn take(&self) -> Option<Event> {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let event = slot.take();
        if event.is_some() {
            self.tracker.replace_bytes_used(0);
            self.available.notify_all();
        }
        event
    }
    fn retire(&self) {
        let _guard = self
            .slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.available.notify_all();
    }
}

pub(crate) struct ApplyWorker {
    pub executor: Arc<Mutex<NestedLoopApplyExec>>,
    pub feed: WorkerFeed,
}

/// Go ParallelNestedLoopApplyExec, composed from the common native row worker.
pub(crate) struct ParallelNestedLoopApplyExec {
    meta: ExecutorMeta,
    outer: Box<dyn Executor>,
    outer_filter: Vec<Expression>,
    workers: Vec<ApplyWorker>,
    keep_order: bool,
    cache_enabled: bool,
    cache: Option<Arc<ApplyCache<InnerRows>>>,
    context: StmtContext,
    stop: Arc<AtomicBool>,
    pool: Option<LanePool>,
    queues: Vec<Arc<Queue>>,
    wake: Arc<(Mutex<()>, Condvar)>,
    active: Vec<bool>,
    remaining: usize,
    outer_chunk: Chunk,
    selected: Vec<bool>,
    outer_cursor: usize,
    done: bool,
    opened: bool,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
    rows_tracker: Arc<Tracker>,
    snapshot: ApplyRuntimeSnapshot,
    runtime_sink: Option<ApplyRuntimeSink>,
}
impl ParallelNestedLoopApplyExec {
    pub(crate) fn new(
        meta: ExecutorMeta,
        outer: Box<dyn Executor>,
        outer_filter: Vec<Expression>,
        workers: Vec<ApplyWorker>,
        keep_order: bool,
        cache_enabled: bool,
        context: StmtContext,
        stop: Arc<AtomicBool>,
        runtime_sink: Option<ApplyRuntimeSink>,
    ) -> Self {
        let memory = context.statement_memory();
        let tracker = memory.operator_tracker(meta.id());
        let rows_tracker = memory.operator_tracker(meta.id());
        let outer_chunk = outer.new_chunk();
        Self {
            meta,
            outer,
            outer_filter,
            workers,
            keep_order,
            cache_enabled,
            cache: None,
            context,
            stop,
            pool: None,
            queues: Vec::new(),
            wake: Arc::new((Mutex::new(()), Condvar::new())),
            active: Vec::new(),
            remaining: 0,
            outer_chunk,
            selected: Vec::new(),
            outer_cursor: 0,
            done: false,
            opened: false,
            memory,
            tracker,
            rows_tracker,
            snapshot: Default::default(),
            runtime_sink,
        }
    }
    fn account(&self) -> Result<(), ExecError> {
        self.tracker.replace_bytes_used(
            self.outer_chunk.memory_usage()
                + self
                    .cache
                    .as_ref()
                    .map_or(0, |cache| cache.key_memory_consumed()),
        );
        self.memory.check()
    }
    fn start_round(&mut self) -> Result<(), ExecError> {
        self.pool.as_ref().expect("opened Apply pool").wait();
        self.active.fill(false);
        for id in 0..self.workers.len() {
            if self.outer_cursor == self.outer_chunk.num_rows() {
                self.outer.next(&mut self.outer_chunk)?;
                self.outer_cursor = 0;
                if self.outer_chunk.num_rows() == 0 {
                    self.done = true;
                    break;
                }
                self.selected = tidb_expr::evaluator::vectorized_filter(
                    &self.context,
                    self.context.enable_vectorized_expression(),
                    &self.outer_filter,
                    &self.outer_chunk,
                    std::mem::take(&mut self.selected),
                )?;
                self.account()?;
            }
            let row = self.outer_chunk.get_row(self.outer_cursor);
            let selected = self.selected[row.idx()];
            self.outer_cursor += 1;
            let mut input = self.outer.new_chunk();
            input.append_row(row);
            let empty_agg = self.outer_chunk.num_rows() == 1 && self.outer.agg_tree_input_empty();
            *self.workers[id]
                .feed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = (Some(input), empty_agg);
            let executor = self.workers[id].executor.clone();
            let queue = self.queues[id].clone();
            let stop = self.stop.clone();
            let cache = self.cache.clone();
            let rows_tracker = self.rows_tracker.clone();
            self.active[id] = true;
            self.remaining += 1;
            self.pool
                .as_ref()
                .unwrap()
                .submit(move || {
                    let mut executor = executor
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let result = crate::sort_util::recover_worker_panic(
                        || -> Result<ApplyRuntimeSnapshot, ExecError> {
                            executor.open()?;
                            executor.prepare_worker_row(selected, cache, rows_tracker);
                            loop {
                                if stop.load(Ordering::Acquire) {
                                    break;
                                }
                                let mut output = executor.new_chunk();
                                executor.next(&mut output)?;
                                if output.num_rows() == 0 {
                                    break;
                                }
                                if !queue.send(Event::Rows(output))? {
                                    break;
                                }
                            }
                            Ok(executor.cache_snapshot())
                        },
                    );
                    let closed = crate::sort_util::recover_worker_panic(|| executor.close());
                    let result = result.and_then(|snapshot| closed.map(|()| snapshot));
                    let event = match result {
                        Ok(snapshot) => Event::Done(snapshot),
                        Err(error) => Event::Error(error),
                    };
                    let _ = queue.send(event);
                })
                .map_err(|()| ExecError::internal("parallel Apply worker pool closed"))?;
        }
        Ok(())
    }
    fn receive(&mut self) -> Result<Option<Chunk>, ExecError> {
        let mut guard = self
            .wake
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            for id in 0..self.active.len() {
                if !self.active[id] {
                    continue;
                }
                if let Some(event) = self.queues[id].take() {
                    match event {
                        Event::Rows(chunk) => return Ok(Some(chunk)),
                        Event::Error(error) => return Err(error),
                        Event::Done(snapshot) => {
                            self.active[id] = false;
                            self.remaining -= 1;
                            self.snapshot.accesses += snapshot.accesses;
                            self.snapshot.hits += snapshot.hits;
                        }
                    }
                } else if self.keep_order {
                    break;
                }
            }
            if self.remaining == 0 {
                return Ok(None);
            }
            self.memory.check()?;
            guard = self
                .wake
                .1
                .wait_timeout(guard, std::time::Duration::from_millis(50))
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }
    fn retire_workers(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.memory.cancel_coprocessor_workers();
        for queue in &self.queues {
            queue.retire();
        }
        self.wake.1.notify_all();
        self.pool.take(); // LanePool Drop joins before worker/context ownership retires.
        for (id, active) in self.active.iter().enumerate() {
            if *active {
                let snapshot = self.workers[id]
                    .executor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .cache_snapshot();
                self.snapshot.accesses += snapshot.accesses;
                self.snapshot.hits += snapshot.hits;
            }
        }
        self.queues.clear();
        self.active.clear();
        self.remaining = 0;
        self.cache = None;
    }
}
impl Executor for ParallelNestedLoopApplyExec {
    fn open(&mut self) -> Result<(), ExecError> {
        if self.opened {
            self.close()?;
        }
        self.outer.open()?;
        self.opened = true;
        self.stop.store(false, Ordering::Release);
        self.memory.renew_coprocessor_worker_scope();
        self.outer_chunk = self.outer.new_chunk();
        self.outer_cursor = 0;
        self.done = false;
        self.snapshot = ApplyRuntimeSnapshot {
            enabled: self.cache_enabled,
            concurrency: self.workers.len(),
            ..Default::default()
        };
        self.cache = self
            .cache_enabled
            .then(|| Arc::new(ApplyCache::new(self.context.apply_cache_capacity())));
        self.pool = Some(LanePool::new("parallel-apply", self.workers.len()));
        self.active = vec![false; self.workers.len()];
        self.queues = (0..self.workers.len())
            .map(|_| {
                Arc::new(Queue {
                    slot: Mutex::new(None),
                    available: Condvar::new(),
                    wake: self.wake.clone(),
                    stop: self.stop.clone(),
                    tracker: self.memory.operator_tracker(self.meta.id()),
                    memory: self.memory.clone(),
                })
            })
            .collect();
        self.account()
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let result = (|| loop {
            if self.remaining == 0 {
                if self.done {
                    return Ok(());
                }
                self.start_round()?;
                if self.remaining == 0 {
                    return Ok(());
                }
            }
            if let Some(mut chunk) = self.receive()? {
                std::mem::swap(req, &mut chunk);
                self.account()?;
                return Ok(());
            }
        })();
        if result.is_err() {
            self.done = true;
            self.retire_workers();
        }
        result
    }
    fn close(&mut self) -> Result<(), ExecError> {
        self.retire_workers();
        self.outer_chunk = Chunk::default();
        self.selected.clear();
        self.tracker.replace_bytes_used(0);
        if let Some(sink) = &self.runtime_sink {
            *sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(self.snapshot);
        }
        if self.opened {
            self.opened = false;
            self.outer.close()
        } else {
            Ok(())
        }
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
impl Drop for ParallelNestedLoopApplyExec {
    fn drop(&mut self) {
        let _ = self.close();
        self.rows_tracker.detach();
        self.tracker.detach();
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        self.slot
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.tracker.replace_bytes_used(0);
        self.tracker.detach();
    }
}
#[cfg(test)]
#[path = "parallel_tests.rs"]
mod tests;
