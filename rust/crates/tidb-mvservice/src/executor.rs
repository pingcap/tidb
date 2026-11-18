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

//! Go master `pkg/mvservice/executor.go`: bounded-concurrency task
//! execution with a timeout and optional backpressure. Go's goroutines are
//! threads here; its wait groups are counters under a condition variable.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use crossbeam_channel::{bounded, Receiver, Sender};
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;

use crate::backpressure::{TaskBackpressureController, DEFAULT_TASK_BACKPRESSURE_DELAY};
use crate::time_proxy::{mvs_after, mvs_now, MvsTimer};

/// One task body: Go `func() error`.
pub type TaskFn = Box<dyn FnOnce() -> Result<(), String> + Send + 'static>;

const TASK_EXECUTOR_STATE_INIT: i32 = 0;
const TASK_EXECUTOR_STATE_RUNNING: i32 = 1;
const TASK_EXECUTOR_STATE_CLOSED: i32 = 2;

/// Go `TaskExecutor.metrics.counters`.
#[derive(Default)]
pub(crate) struct ExecutorCounters {
    pub(crate) submitted_count: AtomicI64,
    pub(crate) finished_count: AtomicI64,
    pub(crate) failed_count: AtomicI64,
    pub(crate) timeout_count: AtomicI64,
    pub(crate) rejected_count: AtomicI64,
    pub(crate) backpressure_count: AtomicI64,
}

/// Go `TaskExecutor.metrics.gauges`.
#[derive(Default)]
pub(crate) struct ExecutorGauges {
    pub(crate) running_count: AtomicI64,
    pub(crate) waiting_count: AtomicI64,
    pub(crate) timed_out_running_count: AtomicI64,
    pub(crate) backpressure_blocked: AtomicI64,
}

/// Go `TaskExecutor.metrics`.
#[derive(Default)]
pub(crate) struct ExecutorMetrics {
    pub(crate) counters: ExecutorCounters,
    pub(crate) gauges: ExecutorGauges,
}

struct TaskRequest {
    name: String,
    task: TaskFn,
}

/// Go's `sync.WaitGroup`.
#[derive(Default)]
struct WaitGroup {
    count: Mutex<i64>,
    zero: Condvar,
}

impl WaitGroup {
    fn add(&self, delta: i64) {
        let mut count = self.count.lock().unwrap();
        *count += delta;
        if *count <= 0 {
            self.zero.notify_all();
        }
    }

    fn done(&self) {
        self.add(-1);
    }

    fn wait(&self) {
        let mut count = self.count.lock().unwrap();
        while *count > 0 {
            count = self.zero.wait(count).unwrap();
        }
    }
}

/// Go `TaskExecutor`: executes MV tasks with bounded concurrency, a
/// timeout and optional backpressure.
pub struct TaskExecutor {
    pub(crate) max_concurrency: AtomicI64,
    timeout_nanos: AtomicI64,
    lifecycle_state: AtomicI32,
    backpressure: RwLock<Option<Arc<dyn TaskBackpressureController>>>,
    close_tx: Mutex<Option<Sender<()>>>,
    close_rx: Receiver<()>,
    pub(crate) metrics: ExecutorMetrics,
    /// Go `queue.mu` / `queue.cond` / `queue.tasks`.
    queue: Mutex<VecDeque<TaskRequest>>,
    queue_cond: Condvar,
    /// Go `workers.count` / `workers.wg`.
    workers_count: AtomicI64,
    workers_wg: WaitGroup,
    tasks_wg: Arc<WaitGroup>,
}

impl TaskExecutor {
    /// Go `NewTaskExecutor`.
    #[must_use]
    pub fn new(max_concurrency: i64, timeout: i64) -> Arc<Self> {
        let max_concurrency = if max_concurrency <= 0 { 1 } else { max_concurrency };
        let (close_tx, close_rx) = bounded(0);
        Arc::new(Self {
            max_concurrency: AtomicI64::new(max_concurrency),
            timeout_nanos: AtomicI64::new(timeout),
            lifecycle_state: AtomicI32::new(TASK_EXECUTOR_STATE_INIT),
            backpressure: RwLock::new(None),
            close_tx: Mutex::new(Some(close_tx)),
            close_rx,
            metrics: ExecutorMetrics::default(),
            queue: Mutex::new(VecDeque::new()),
            queue_cond: Condvar::new(),
            workers_count: AtomicI64::new(0),
            workers_wg: WaitGroup::default(),
            tasks_wg: Arc::new(WaitGroup::default()),
        })
    }

    /// Go `Run`: starts workers per the current max concurrency; true only
    /// when this call started them.
    pub fn run(self: &Arc<Self>) -> bool {
        if self
            .lifecycle_state
            .compare_exchange(TASK_EXECUTOR_STATE_INIT, TASK_EXECUTOR_STATE_RUNNING, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        let workers = self.max_concurrency.load(Ordering::SeqCst).max(1);
        self.start_workers(workers);
        true
    }

    /// Go `setMaxConcurrency`.
    pub(crate) fn set_max_concurrency(self: &Arc<Self>, max_concurrency: i64) {
        if max_concurrency <= 0 || self.lifecycle_state.load(Ordering::SeqCst) == TASK_EXECUTOR_STATE_CLOSED {
            return;
        }
        let prev = self.max_concurrency.load(Ordering::SeqCst);
        if max_concurrency == prev {
            return;
        }
        self.max_concurrency.store(max_concurrency, Ordering::SeqCst);
        if max_concurrency > prev {
            self.start_workers(max_concurrency - prev);
            return;
        }
        let _guard = self.queue.lock().unwrap();
        self.queue_cond.notify_all();
    }

    /// Go `setTimeout`.
    pub(crate) fn set_timeout(&self, timeout: i64) {
        if self.lifecycle_state.load(Ordering::SeqCst) == TASK_EXECUTOR_STATE_CLOSED {
            return;
        }
        self.timeout_nanos.store(timeout, Ordering::SeqCst);
    }

    /// Go `SetBackpressureController`: `None` disables backpressure.
    pub fn set_backpressure_controller(&self, controller: Option<Arc<dyn TaskBackpressureController>>) {
        *self.backpressure.write().unwrap() = controller;
    }

    /// Go `Submit`: enqueues one named task while the executor accepts work.
    pub fn submit(&self, name: impl Into<String>, task: TaskFn) -> bool {
        let mut queue = self.queue.lock().unwrap();
        if self.lifecycle_state.load(Ordering::SeqCst) == TASK_EXECUTOR_STATE_CLOSED {
            self.metrics.counters.rejected_count.fetch_add(1, Ordering::SeqCst);
            return false;
        }
        self.metrics.counters.submitted_count.fetch_add(1, Ordering::SeqCst);
        self.metrics.gauges.waiting_count.fetch_add(1, Ordering::SeqCst);
        self.tasks_wg.add(1);
        queue.push_back(TaskRequest { name: name.into(), task });
        self.queue_cond.notify_one();
        true
    }

    /// Go `Close`: true when this call closed the executor and waited for
    /// every worker and task.
    pub fn close(&self) -> bool {
        if self.lifecycle_state.swap(TASK_EXECUTOR_STATE_CLOSED, Ordering::SeqCst) == TASK_EXECUTOR_STATE_CLOSED {
            return false;
        }
        // Go `close(e.closeCh)`.
        self.close_tx.lock().unwrap().take();
        let pending = {
            let mut queue = self.queue.lock().unwrap();
            let pending = queue.len() as i64;
            queue.clear();
            self.queue_cond.notify_all();
            pending
        };
        if pending > 0 {
            self.metrics.gauges.waiting_count.fetch_sub(pending, Ordering::SeqCst);
            self.tasks_wg.add(-pending);
        }
        self.tasks_wg.wait();
        self.workers_wg.wait();
        true
    }

    fn start_workers(self: &Arc<Self>, n: i64) {
        for _ in 0..n {
            self.workers_count.fetch_add(1, Ordering::SeqCst);
            self.workers_wg.add(1);
            let executor = Arc::clone(self);
            std::thread::Builder::new()
                .name("mvservice-task".to_owned())
                .spawn(move || executor.worker_loop())
                .expect("spawn an MV service task worker");
        }
    }

    /// Go `workerLoop`.
    fn worker_loop(self: Arc<Self>) {
        loop {
            let Some(req) = self.next_task() else {
                break;
            };
            self.run_task(req.name, req.task);
        }
        self.workers_wg.done();
    }

    fn closed(&self) -> bool {
        self.lifecycle_state.load(Ordering::SeqCst) == TASK_EXECUTOR_STATE_CLOSED
    }

    /// Go `nextTask`: waits for work, honors worker down-scaling, and
    /// applies backpressure.
    fn next_task(&self) -> Option<TaskRequest> {
        let mut in_backpressure_wait = false;
        loop {
            {
                let mut queue = self.queue.lock().unwrap();
                while !self.closed() && queue.is_empty() {
                    if self.try_exit_worker_with_lock() {
                        return None;
                    }
                    queue = self.queue_cond.wait(queue).unwrap();
                }
                if self.should_exit_worker_with_lock(&queue) {
                    return None;
                }
            }

            let (blocked, delay) = self.should_backpressure();
            if blocked {
                if !in_backpressure_wait {
                    self.metrics.counters.backpressure_count.fetch_add(1, Ordering::SeqCst);
                    in_backpressure_wait = true;
                }
                self.metrics.gauges.backpressure_blocked.fetch_add(1, Ordering::SeqCst);
                crossbeam_channel::select! {
                    recv(self.close_rx) -> _ => {}
                    recv(mvs_after(delay)) -> _ => {}
                }
                self.metrics.gauges.backpressure_blocked.fetch_sub(1, Ordering::SeqCst);
                continue;
            }
            in_backpressure_wait = false;

            let mut queue = self.queue.lock().unwrap();
            if self.should_exit_worker_with_lock(&queue) {
                return None;
            }
            if let Some(req) = queue.pop_front() {
                drop(queue);
                self.metrics.gauges.waiting_count.fetch_sub(1, Ordering::SeqCst);
                return Some(req);
            }
        }
    }

    /// Go `shouldExitWorkerWithLock`.
    fn should_exit_worker_with_lock(&self, queue: &VecDeque<TaskRequest>) -> bool {
        if self.closed() && queue.is_empty() {
            self.workers_count.fetch_sub(1, Ordering::SeqCst);
            return true;
        }
        self.try_exit_worker_with_lock()
    }

    /// Go `shouldBackpressure`.
    fn should_backpressure(&self) -> (bool, i64) {
        let Some(controller) = self.backpressure.read().unwrap().clone() else {
            return (false, 0);
        };
        let (blocked, delay) = controller.should_backpressure();
        if !blocked {
            return (false, 0);
        }
        (true, if delay <= 0 { DEFAULT_TASK_BACKPRESSURE_DELAY } else { delay })
    }

    /// Go `tryExitWorkerWithLock`: one worker exits while more run than the
    /// configured maximum.
    fn try_exit_worker_with_lock(&self) -> bool {
        loop {
            let cur = self.workers_count.load(Ordering::SeqCst);
            let max_workers = self.max_concurrency.load(Ordering::SeqCst);
            if cur <= max_workers {
                return false;
            }
            if self.workers_count.compare_exchange(cur, cur - 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                return true;
            }
        }
    }

    /// Go `runTask`: one task with timeout handling and metrics.
    fn run_task(self: &Arc<Self>, name: String, task: TaskFn) {
        self.metrics.gauges.running_count.fetch_add(1, Ordering::SeqCst);
        let task_start = mvs_now();
        let timeout = self.timeout_nanos.load(Ordering::SeqCst);
        if timeout <= 0 {
            let result = safe_execute(&name, task);
            self.metrics.gauges.running_count.fetch_sub(1, Ordering::SeqCst);
            self.finish_task(&result);
            return;
        }

        // Include the entire task execution in the timeout window.
        let timer = MvsTimer::new(timeout);
        let (done_tx, done_rx) = bounded(1);
        let task_name = name.clone();
        std::thread::Builder::new()
            .name("mvservice-task-run".to_owned())
            .spawn(move || {
                let _ = done_tx.send(safe_execute(&task_name, task));
            })
            .expect("spawn an MV service task");

        crossbeam_channel::select! {
            recv(done_rx) -> result => {
                timer.stop();
                self.metrics.gauges.running_count.fetch_sub(1, Ordering::SeqCst);
                self.finish_task(&result.unwrap_or_else(|_| Err("task result lost".to_owned())));
            }
            recv(timer.c()) -> _ => {
                self.metrics.counters.timeout_count.fetch_add(1, Ordering::SeqCst);
                self.metrics.gauges.running_count.fetch_sub(1, Ordering::SeqCst);
                self.metrics.gauges.timed_out_running_count.fetch_add(1, Ordering::SeqCst);
                bg_logger().warn(
                    "task timed out, continue in background",
                    &[
                        Field::new("task", Value::Str(name)),
                        Field::new("exec_start", Value::Str(task_start.format_date_time())),
                        Field::new("timeout", Value::Duration(timeout)),
                        Field::new("running_count", Value::I64(self.metrics.gauges.running_count.load(Ordering::SeqCst))),
                        Field::new("waiting_count", Value::I64(self.metrics.gauges.waiting_count.load(Ordering::SeqCst))),
                        Field::new(
                            "timed_out_running_count",
                            Value::I64(self.metrics.gauges.timed_out_running_count.load(Ordering::SeqCst)),
                        ),
                    ],
                );
                let executor = Arc::clone(self);
                std::thread::Builder::new()
                    .name("mvservice-task-timeout".to_owned())
                    .spawn(move || {
                        let result = done_rx.recv().unwrap_or_else(|_| Err("task result lost".to_owned()));
                        executor.metrics.gauges.timed_out_running_count.fetch_sub(1, Ordering::SeqCst);
                        executor.finish_task(&result);
                    })
                    .expect("spawn an MV service task waiter");
            }
        }
    }

    /// Go `finishTask`: marks one task done and reports its result.
    fn finish_task(&self, result: &Result<(), String>) {
        self.tasks_wg.done();
        self.metrics.counters.finished_count.fetch_add(1, Ordering::SeqCst);
        if result.is_err() {
            self.metrics.counters.failed_count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Go `safeExecute`: runs the task, a panic becoming an error.
fn safe_execute(task_name: &str, task: TaskFn) -> Result<(), String> {
    match catch_unwind(AssertUnwindSafe(task)) {
        Ok(result) => result,
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_owned());
            bg_logger().error(
                "task panicked in executor",
                &[Field::new("task", Value::Str(task_name.to_owned())), Field::new("panic", Value::Str(message.clone()))],
            );
            Err(format!("task panicked, task={task_name}, panic={message}"))
        }
    }
}
