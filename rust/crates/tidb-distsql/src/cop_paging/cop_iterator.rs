// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go copIterator ownership: one statement-scoped lite reader, otherwise
//! independently progressing raw responses. Row decoding belongs to the caller.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Wake, Waker};

use tidb_txnkv::rpc::{execution_runtime, CompletionError, CompletionNotifier, UnaryCallContext};

use crate::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};

pub(crate) trait CopTaskSource: QueryResponse {
    fn try_next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError>;
    fn task_count(&self) -> usize;
    fn has_remaining_work(&self) -> bool;
    fn call(&self) -> UnaryCallContext;
    fn use_task_worker(&mut self);
    fn set_waker(&mut self, waker: Waker);
}

pub(crate) trait CopWorkerSource: CopTaskSource + Sized {
    fn keep_order(&self) -> bool;
    fn concurrency(&self) -> usize;
    fn into_tasks(self) -> Vec<Self>;
    fn task(&self) -> &crate::RegionTaskEnvelope;
    fn use_row_hints(&self) -> bool;
}

pub(crate) type ConcurrentStart<R> = fn(R, bool) -> Box<dyn QueryResponse + Send>;

enum Driver<R> {
    Inline(R),
    Concurrent(Box<dyn QueryResponse + Send>),
    Closed,
}

/// Raw coprocessor iterator. A single-task read may borrow its statement's
/// lite slot; continuations and other readers run independently of consumption.
pub struct CopIterator<R: QueryResponse> {
    driver: Driver<R>,
    start: Option<ConcurrentStart<R>>,
    lite: Option<Arc<AtomicBool>>,
    limiter_wait: crate::LimiterWaitStats,
}

impl<R: QueryResponse> CopIterator<R> {
    pub(crate) fn new(
        mut source: R,
        start: Option<ConcurrentStart<R>>,
        token: Option<Arc<AtomicBool>>,
    ) -> Self
    where
        R: CopTaskSource,
    {
        if start.is_some() {
            source.use_task_worker();
        }
        // Explicitly injected workerless sources exercise the same task state
        // machine without transferring their thread-local fixture ownership.
        let lite = if start.is_some() && source.task_count() == 1 {
            token.filter(|token| {
                token
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            })
        } else {
            None
        };
        let driver = match start {
            Some(start) if lite.is_none() => Driver::Concurrent(start(source, false)),
            _ => Driver::Inline(source),
        };
        Self {
            driver,
            start,
            lite,
            limiter_wait: crate::LimiterWaitStats::default(),
        }
    }
}

impl<R: QueryResponse> CopIterator<R> {
    fn release_lite(&mut self) {
        if let Some(token) = self.lite.take() {
            token.store(false, Ordering::Release);
        }
    }

    fn close_inner(&mut self) {
        self.release_lite();
        match std::mem::replace(&mut self.driver, Driver::Closed) {
            Driver::Inline(mut source) => {
                source.close();
                self.limiter_wait.merge(source.limiter_wait_stats());
            }
            Driver::Concurrent(mut source) => {
                source.close();
                self.limiter_wait.merge(source.limiter_wait_stats());
            }
            Driver::Closed => {}
        }
    }
}

impl<R: CopTaskSource> QueryResponse for CopIterator<R> {
    fn limiter_wait_stats(&self) -> crate::LimiterWaitStats {
        let mut stats = self.limiter_wait;
        match &self.driver {
            Driver::Inline(source) => stats.merge(source.limiter_wait_stats()),
            Driver::Concurrent(source) => stats.merge(source.limiter_wait_stats()),
            Driver::Closed => {}
        }
        stats
    }

    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        let result = match &mut self.driver {
            Driver::Inline(source) => recover(|| source.next()),
            Driver::Concurrent(source) => return source.next(),
            Driver::Closed => return Ok(None),
        };
        // Go releases TryCopLiteWorker after the first response, including
        // errors. A retained iterator must never release a later owner's slot.
        self.release_lite();
        if matches!(&result, Ok(Some(_))) {
            if let (Some(start), Driver::Inline(source)) = (self.start, &self.driver) {
                if source.has_remaining_work() {
                    let Driver::Inline(source) =
                        std::mem::replace(&mut self.driver, Driver::Closed)
                    else {
                        unreachable!()
                    };
                    self.driver = Driver::Concurrent(start(source, true));
                }
            }
        } else if !matches!(&result, Err(QueryResponseError::Pending)) {
            self.close_inner();
        }
        result
    }

    fn close(&mut self) {
        self.close_inner();
    }
}

impl<R: QueryResponse> Drop for CopIterator<R> {
    fn drop(&mut self) {
        self.close_inner();
    }
}

type Row = Result<QueryResultSubset, QueryResponseError>;

enum Event {
    Row(Row),
    Finished,
}

struct Task<R> {
    source: Option<R>,
    rows: VecDeque<Row>,
    finished: bool,
    response_capacity: usize,
    lane: usize,
}

struct State<R> {
    tasks: Vec<Task<R>>,
    ready: VecDeque<(usize, Event)>,
    next_task: usize,
    waiting: [Option<usize>; 2],
    current: usize,
    retired: usize,
    live_workers: usize,
    limiter_wait: crate::LimiterWaitStats,
}

/// Go's task sender, per-task ordered channels and unordered rendezvous.
/// Workers own transport state; this lock never covers RPCs or recovery.
struct WorkerGroup<R> {
    state: Mutex<State<R>>,
    ready: CompletionNotifier,
    available: tokio::sync::Notify,
    spaces: Vec<tokio::sync::Notify>,
    worker_wakes: Vec<Arc<tokio::sync::Notify>>,
    joined: Condvar,
    call: UnaryCallContext,
    ordered: bool,
    window: usize,
    closed: AtomicBool,
}

struct TaskWake(Arc<tokio::sync::Notify>);

impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_one();
    }
}

struct WorkerExit<R>(Arc<WorkerGroup<R>>);

impl<R> Drop for WorkerExit<R> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        state.live_workers -= 1;
        drop(state);
        self.0.joined.notify_all();
    }
}

impl<R: CopTaskSource + Send + 'static> WorkerGroup<R> {
    async fn next_task(&self, lane: usize) -> Option<(usize, R)> {
        loop {
            // notify_waiters does not retain a permit: subscribe before
            // observing the send window, just like a channel receive.
            let available = self.available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            let (task, exhausted, admitted) = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if self.closed.load(Ordering::Acquire) {
                    return None;
                }
                let previous_next = state.next_task;
                // Go's single task sender visits tasks in order and sends to
                // a one-slot normal or small-task channel. A full target
                // channel blocks that sender, not just its worker lane.
                while state.next_task < state.tasks.len()
                    && state.next_task < state.retired + self.window
                {
                    let index = state.next_task;
                    let target = state.tasks[index].lane;
                    if state.waiting[target].is_some() {
                        break;
                    }
                    state.waiting[target] = Some(index);
                    state.next_task += 1;
                }
                let task = state.waiting[lane]
                    .take()
                    .map(|index| (index, state.tasks[index].source.take().unwrap()));
                (
                    task,
                    state.next_task == state.tasks.len(),
                    state.next_task != previous_next,
                )
            };
            // Admission may have filled only the other lane. Wake its
            // receivers even when this worker did not dequeue a task.
            if admitted || task.is_some() {
                self.available.notify_waiters();
            }
            if task.is_some() {
                return task;
            }
            if exhausted {
                return None;
            }
            available.await;
        }
    }

    async fn send_row(&self, index: usize, row: Row) -> bool {
        if !self.ordered {
            return self.send_unordered(index, Event::Row(row)).await;
        }
        let mut row = Some(row);
        loop {
            let space = self.spaces[index].notified();
            let published = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if self.closed.load(Ordering::Acquire) {
                    return false;
                }
                if state.tasks[index].rows.len() < state.tasks[index].response_capacity {
                    state.tasks[index].rows.push_back(row.take().unwrap());
                    Some(index == state.current)
                } else {
                    None
                }
            };
            if let Some(wake_reader) = published {
                if wake_reader {
                    self.ready.notify(0);
                }
                return true;
            }
            // The task builder owns Go's paging/non-paging channel size.
            // This worker holds one additional response while sending.
            space.await;
        }
    }

    async fn send_unordered(&self, index: usize, event: Event) -> bool {
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if self.closed.load(Ordering::Acquire) {
                return false;
            }
            state.ready.push_back((index, event));
        }
        self.ready.notify(0);
        // Exactly one consumer acknowledgment permits this worker's next
        // send. Notify retains an early acknowledgment; no second copy of
        // channel state or polling of the iterator mutex is needed.
        self.spaces[index].notified().await;
        !self.closed.load(Ordering::Acquire)
    }

    async fn finish_task(&self, index: usize) {
        if !self.ordered {
            self.send_unordered(index, Event::Finished).await;
            return;
        }
        let wake_reader = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.tasks[index].finished = true;
            index == state.current
        };
        if wake_reader {
            self.ready.notify(0);
        }
    }

    fn receive(&self) -> Option<Result<Option<QueryResultSubset>, QueryResponseError>> {
        loop {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if self.closed.load(Ordering::Acquire) {
                return Some(Ok(None));
            }
            if self.ordered {
                let index = state.current;
                if index == state.tasks.len() {
                    return Some(Ok(None));
                }
                if let Some(row) = state.tasks[index].rows.pop_front() {
                    drop(state);
                    self.spaces[index].notify_one();
                    return Some(row.map(Some));
                }
                if !state.tasks[index].finished {
                    return None;
                }
                state.current += 1;
                state.retired += 1;
                drop(state);
                self.available.notify_waiters();
                continue;
            }
            let Some((index, event)) = state.ready.pop_front() else {
                return (state.retired == state.tasks.len()).then_some(Ok(None));
            };
            match event {
                Event::Row(row) => {
                    drop(state);
                    self.spaces[index].notify_one();
                    return Some(row.map(Some));
                }
                Event::Finished => {
                    state.retired += 1;
                    drop(state);
                    self.spaces[index].notify_one();
                    self.available.notify_waiters();
                }
            }
        }
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.call.cancellation().cancel();
        self.available.notify_waiters();
        for wake in &self.worker_wakes {
            wake.notify_one();
        }
        for space in &self.spaces {
            space.notify_one();
        }
        let pending = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.ready.clear();
            state
                .tasks
                .iter_mut()
                .filter_map(|task| {
                    task.rows.clear();
                    task.source.take()
                })
                .collect::<Vec<_>>()
        };
        drop(pending);
        blocking(|| {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            while state.live_workers != 0 {
                state = self.joined.wait(state).unwrap_or_else(|p| p.into_inner());
            }
        });
        self.ready.notify(0);
    }
}

async fn run_worker<R: CopTaskSource + Send + 'static>(
    exit: WorkerExit<R>,
    wake: Arc<tokio::sync::Notify>,
    lane: usize,
) {
    let group = &exit.0;
    while let Some((index, mut source)) = group.next_task(lane).await {
        source.set_waker(Waker::from(Arc::new(TaskWake(Arc::clone(&wake)))));
        loop {
            let completed = wake.notified();
            if group.closed.load(Ordering::Acquire) {
                break;
            }
            match recover(|| source.try_next()) {
                Err(QueryResponseError::Pending) => completed.await,
                Ok(Some(row)) => {
                    if !group.send_row(index, Ok(row)).await {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    group.send_row(index, Err(error)).await;
                    break;
                }
            }
        }
        // Drop this task's pending handles before publishing its finish.
        // It does not cancel the shared query or another worker's task.
        group
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .limiter_wait
            .merge(source.limiter_wait_stats());
        drop(source);
        if group.closed.load(Ordering::Acquire) {
            return;
        }
        group.finish_task(index).await;
    }
}

struct ConcurrentResponse<R: CopTaskSource + Send + 'static> {
    group: Arc<WorkerGroup<R>>,
}

impl<R: CopTaskSource + Send + 'static> QueryResponse for ConcurrentResponse<R> {
    fn limiter_wait_stats(&self) -> crate::LimiterWaitStats {
        self.group
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .limiter_wait
    }

    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        let result = (|| loop {
            while self
                .group
                .ready
                .try_take()
                .map_err(completion_error)?
                .is_some()
            {}
            if let Some(result) = self.group.receive() {
                return result;
            }
            self.group
                .ready
                .wait(&self.group.call)
                .map_err(completion_error)?;
        })();
        if !matches!(&result, Ok(Some(_))) {
            self.group.close();
        }
        result
    }
    fn close(&mut self) {
        self.group.close();
    }
}

impl<R: CopTaskSource + Send + 'static> Drop for ConcurrentResponse<R> {
    fn drop(&mut self) {
        self.group.close();
    }
}

pub(crate) fn start_concurrent<R: CopWorkerSource + Send + 'static>(
    source: R,
    lite_fallback: bool,
) -> Box<dyn QueryResponse + Send> {
    let call = source.call();
    let limiter_wait = source.limiter_wait_stats();
    let ordered = source.keep_order();
    let requested_concurrency = source.concurrency();
    let use_row_hints = source.use_row_hints() && !lite_fallback;
    let tasks = source.into_tasks();
    let runtime =
        execution_runtime().expect("runtime was initialized before installing the worker factory");
    let small_count = if use_row_hints {
        tasks
            .iter()
            .filter(|source| source.task().is_small())
            .count()
    } else {
        0
    };
    let mut small_workers =
        crate::RegionTaskEnvelope::small_concurrency(small_count, runtime.metrics().num_workers());
    if ordered {
        small_workers = small_workers.min(20);
    }
    // Go liteWorker.runWorkerConcurrently starts one normal worker even
    // when region rebuilding has split its original task into many tasks.
    let normal_workers = if lite_fallback {
        1
    } else {
        requested_concurrency.min(tasks.len() - small_count).max(1)
    };
    let workers = normal_workers + small_workers;
    let group = Arc::new(WorkerGroup {
        spaces: (0..tasks.len())
            .map(|_| tokio::sync::Notify::new())
            .collect(),
        state: Mutex::new(State {
            tasks: tasks
                .into_iter()
                .map(|source| {
                    let task = source.task();
                    Task {
                        response_capacity: task.response_channel_capacity,
                        lane: usize::from(small_workers > 0 && task.is_small()),
                        source: Some(source),
                        rows: VecDeque::new(),
                        finished: false,
                    }
                })
                .collect(),
            ready: VecDeque::new(),
            next_task: 0,
            waiting: [None; 2],
            current: 0,
            retired: 0,
            live_workers: workers,
            limiter_wait,
        }),
        ready: CompletionNotifier::new(),
        available: tokio::sync::Notify::new(),
        worker_wakes: (0..workers)
            .map(|_| Arc::new(tokio::sync::Notify::new()))
            .collect(),
        joined: Condvar::new(),
        call,
        ordered,
        window: if ordered { 2 * workers } else { workers },
        closed: AtomicBool::new(false),
    });
    for (index, wake) in group.worker_wakes.iter().enumerate() {
        drop(runtime.spawn(run_worker(
            WorkerExit(Arc::clone(&group)),
            Arc::clone(wake),
            usize::from(index >= normal_workers),
        )));
    }
    Box::new(ConcurrentResponse { group })
}

/// Only synchronous RPC/recovery work hands its scheduler worker back. The
/// ordinary async completion path neither parks a thread nor creates a job.
pub(super) fn blocking<T>(work: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

pub(super) fn completion_error(error: CompletionError) -> QueryResponseError {
    use super::DirectUnaryTransportError;
    use tidb_txnkv::rpc::DirectUnaryClientError;

    match error {
        CompletionError::Cancelled => QueryResponseError::Cancelled,
        CompletionError::DeadlineExceeded => {
            QueryResponseError::Source(DirectUnaryTransportError::DeadlineExceeded.to_string())
        }
        other => QueryResponseError::Source(
            DirectUnaryTransportError::Client(DirectUnaryClientError::Runtime(other.to_string()))
                .to_string(),
        ),
    }
}

fn recover(
    next: impl FnOnce() -> Result<Option<QueryResultSubset>, QueryResponseError>,
) -> Result<Option<QueryResultSubset>, QueryResponseError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(next)).unwrap_or_else(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload");
        Err(QueryResponseError::Source(format!(
            "coprocessor worker panicked: {message}"
        )))
    })
}
