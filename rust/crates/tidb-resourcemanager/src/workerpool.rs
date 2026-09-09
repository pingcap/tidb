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

//! Reusable generic workers from `pkg/resourcemanager/pool/workerpool`.

use std::any::TypeId;
use std::error::Error;
use std::fmt::Display;
use std::fmt::{self};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
#[cfg(feature = "failpoints")]
use std::sync::OnceLock;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime};

use crate::poolmanager::Channel;
use crate::util::Component;

/// Shared cancellable pipeline context.
pub struct Context<E> {
    cancellation: Cancellation,
    parent: Option<crossbeam_channel::Receiver<()>>,
    first_error: Mutex<Option<E>>,
}

impl<E> Context<E>
where
    E: Clone + Display,
{
    /// Creates a context derived from an optional workerpool parent context.
    pub fn new(parent: Option<&Arc<Self>>) -> Arc<Self> {
        Arc::new(Self {
            cancellation: Cancellation::new(),
            parent: parent.map(|parent| parent.cancelled()),
            first_error: Mutex::new(None),
        })
    }

    /// Stores the first error, logs every error, and cancels the pipeline.
    pub fn on_error(&self, error: E) {
        let mut first = self
            .first_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if first.is_none() {
            *first = Some(error.clone());
        }
        drop(first);
        tracing::error!(error = %error, "worker pool encountered error");
        self.cancel();
    }

    /// Returns the first business-logic error.
    pub fn operator_error(&self) -> Option<E> {
        self.first_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Cancels the context.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Returns a channel that becomes ready when this context is cancelled.
    pub fn cancelled(&self) -> crossbeam_channel::Receiver<()> {
        self.cancellation.receiver()
    }

    fn parent_cancelled(&self) -> crossbeam_channel::Receiver<()> {
        self.parent.clone().unwrap_or_else(crossbeam_channel::never)
    }
}

/// A task whose panic can be converted into a business error.
pub trait TaskMayPanic<E>: Send + 'static {
    /// Returns recovery metric label, function information, and optional error.
    fn recover_args(&self) -> (String, String, Option<E>);
}

/// One reusable worker instance.
pub trait Worker<T, R, E>: Send + 'static {
    /// Handles one task and optionally sends results.
    fn handle_task(&mut self, task: T, send: &mut dyn FnMut(R)) -> Result<(), E>;
    /// Closes worker-owned resources.
    fn close(&mut self) -> Result<(), E>;
}

/// Capacity tuning contract used without importing generic pool types.
pub trait Tuner {
    /// Tunes worker count and optionally waits for removed workers.
    fn tune(&self, workers: i32, wait: bool);
}

/// Placeholder result type for a pool without a result channel.
#[derive(Default)]
pub struct NoResult;

/// Configuration option applied during worker-pool construction.
pub trait PoolOption<T, R, E> {
    /// Applies this option to a new pool.
    fn apply(&self, pool: &WorkerPool<T, R, E>);
}

/// Default error reported when a task panics without supplying one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PanicError(String);

impl fmt::Display for PanicError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for PanicError {}

type WorkerFactory<T, R, E> = dyn Fn() -> Option<Box<dyn Worker<T, R, E>>> + Send + Sync + 'static;

struct Runtime<E> {
    pipeline: Arc<Context<E>>,
    local: Cancellation,
}

impl<E> Clone for Runtime<E> {
    fn clone(&self) -> Self {
        Self {
            pipeline: Arc::clone(&self.pipeline),
            local: self.local.clone(),
        }
    }
}

struct ActiveWorkers {
    count: AtomicI32,
    mutex: Mutex<()>,
    done: Condvar,
}

struct TuneConfig {
    wait: Arc<TuneWait>,
}

struct TuneWait {
    count: AtomicI32,
    mutex: Mutex<()>,
    done: Condvar,
}

impl TuneWait {
    fn new() -> Self {
        Self {
            count: AtomicI32::new(0),
            mutex: Mutex::new(()),
            done: Condvar::new(),
        }
    }

    fn add(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    fn finish(&self) {
        let _guard = self
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.count.fetch_sub(1, Ordering::SeqCst);
        self.done.notify_all();
    }

    fn wait(&self) {
        let mut guard = self
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.count.load(Ordering::SeqCst) != 0 {
            guard = self
                .done
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

/// A reusable generic worker pool.
pub struct WorkerPool<T, R, E> {
    name: String,
    num_workers: Mutex<i32>,
    origin_workers: i32,
    running_tasks: Arc<AtomicI32>,
    task_channel: Mutex<Option<Channel<T>>>,
    result_channel: Mutex<Option<Channel<R>>>,
    quit_channel: Channel<TuneConfig>,
    active: Arc<ActiveWorkers>,
    create_worker: Arc<WorkerFactory<T, R, E>>,
    last_tune_ts: Mutex<SystemTime>,
    started: AtomicBool,
    runtime: Mutex<Option<Runtime<E>>>,
}

impl<T, R, E> WorkerPool<T, R, E>
where
    T: TaskMayPanic<E>,
    R: Send + 'static,
    E: Error + Clone + From<PanicError> + Send + Sync + 'static,
{
    /// Creates a worker pool. Nonpositive worker counts become one.
    pub fn new(
        name: String,
        _component: Component,
        workers: isize,
        create_worker: impl Fn() -> Option<Box<dyn Worker<T, R, E>>> + Send + Sync + 'static,
        options: Vec<Box<dyn PoolOption<T, R, E>>>,
    ) -> Arc<Self> {
        let workers = workers.max(1);
        new_worker_pool_failpoint(workers);
        let workers = workers as i32;
        let pool = Arc::new(Self {
            name,
            num_workers: Mutex::new(workers),
            origin_workers: workers,
            running_tasks: Arc::new(AtomicI32::new(0)),
            task_channel: Mutex::new(None),
            result_channel: Mutex::new(None),
            quit_channel: Channel::bounded(0),
            active: Arc::new(ActiveWorkers {
                count: AtomicI32::new(0),
                mutex: Mutex::new(()),
                done: Condvar::new(),
            }),
            create_worker: Arc::new(create_worker),
            last_tune_ts: Mutex::new(go_zero_time()),
            started: AtomicBool::new(false),
            runtime: Mutex::new(None),
        });
        for option in options {
            option.apply(&pool);
        }
        pool
    }

    /// Sets the task channel before start.
    pub fn set_task_receiver(&self, receiver: Channel<T>) {
        *self
            .task_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(receiver);
    }

    /// Sets the result channel before start.
    pub fn set_result_sender(&self, sender: Channel<R>) {
        *self
            .result_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sender);
    }

    /// Starts the configured number of reusable workers.
    pub fn start(&self, context: Arc<Context<E>>) {
        let mut task = self
            .task_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if task.is_none() {
            *task = Some(Channel::bounded(0));
        }
        drop(task);
        let mut result = self
            .result_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if result.is_none() && TypeId::of::<R>() != TypeId::of::<NoResult>() {
            *result = Some(Channel::bounded(0));
        }
        drop(result);
        *self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Runtime {
            pipeline: context,
            local: Cancellation::new(),
        });
        let workers = self
            .num_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for _ in 0..*workers {
            self.run_worker();
        }
        self.started.store(true, Ordering::SeqCst);
    }

    /// Adds one task, unless the pool or pipeline context is cancelled.
    pub fn add_task(&self, task: T) {
        let runtime = self.current_runtime();
        let channel = self.current_task_channel();
        let sender = channel
            .sender()
            .unwrap_or_else(|| panic!("send on closed channel"));
        crossbeam_channel::select! {
            recv(runtime.local.receiver()) -> _ => {},
            recv(runtime.pipeline.cancelled()) -> _ => {},
            recv(runtime.pipeline.parent_cancelled()) -> _ => {},
            send(sender, task) -> result => {
                result.unwrap_or_else(|_| panic!("send on closed channel"));
            },
        }
    }

    /// Returns the optional result channel.
    pub fn result_channel(&self) -> Option<Channel<R>> {
        self.result_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Tunes worker count. Nonpositive counts become one.
    pub fn tune(&self, workers: i32, wait: bool) {
        let workers = workers.max(1);
        *self
            .last_tune_ts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = SystemTime::now();
        let mut current = self
            .num_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tracing::info!(from = *current, to = workers, "tune worker pool");
        if !self.started.load(Ordering::SeqCst) {
            *current = workers;
            return;
        }
        let difference = workers.wrapping_sub(*current);
        if difference > 0 {
            for _ in 0..difference {
                self.run_worker();
            }
        } else if difference < 0 {
            let tune_wait = Arc::new(TuneWait::new());
            let runtime = self.current_runtime();
            let quit_sender = self
                .quit_channel
                .sender()
                .unwrap_or_else(|| panic!("send on closed channel"));
            for _ in 0..difference.wrapping_neg() {
                tune_wait.add();
                let config = TuneConfig {
                    wait: Arc::clone(&tune_wait),
                };
                let mut config = Some(config);
                crossbeam_channel::select! {
                    recv(runtime.local.receiver()) -> _ => {
                        tune_wait.finish();
                        break;
                    },
                    recv(runtime.pipeline.cancelled()) -> _ => {
                        tracing::info!(from = *current, to = workers, "context done when tuning worker pool");
                        tune_wait.finish();
                        break;
                    },
                    recv(runtime.pipeline.parent_cancelled()) -> _ => {
                        tracing::info!(from = *current, to = workers, "context done when tuning worker pool");
                        tune_wait.finish();
                        break;
                    },
                    send(quit_sender, config.take().unwrap()) -> result => {
                        result.unwrap_or_else(|_| panic!("send on closed channel"));
                    },
                }
            }
            if wait {
                tune_wait.wait();
            }
        }
        *current = workers;
    }

    /// Returns the last tuning time.
    pub fn last_tuner_ts(&self) -> SystemTime {
        *self
            .last_tune_ts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns configured worker count.
    pub fn cap(&self) -> i32 {
        *self
            .num_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns workers currently handling tasks.
    pub fn running(&self) -> i32 {
        self.running_tasks.load(Ordering::SeqCst)
    }

    /// Returns the pool name.
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Cancels and closes the pool, then waits for all workers.
    pub fn close_and_wait(&self) {
        if let Some(runtime) = self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            runtime.local.cancel();
        }
        self.quit_channel.close();
        self.release();
    }

    /// Waits for workers, cancels tuners, and closes the result channel.
    pub fn release(&self) {
        let mut guard = self
            .active
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.active.count.load(Ordering::SeqCst) != 0 {
            guard = self
                .active
                .done
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(guard);
        if let Some(runtime) = self
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            runtime.local.cancel();
        }
        if let Some(result) = self
            .result_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            result.close();
        }
    }

    /// Returns initial worker count.
    pub fn origin_concurrency(&self) -> i32 {
        self.origin_workers
    }

    fn run_worker(&self) {
        let Some(mut worker) = (self.create_worker)() else {
            return;
        };
        // Go re-reads `p.taskChan`/`p.resChan` on every select, so a
        // SetTaskReceiver/SetResultSender after Start would re-route live
        // workers; here the channels are cloned once at spawn. Re-wiring a
        // running pool is invalid usage upstream (the callers set both
        // before Start), and this makes that contract explicit.
        let task_channel = self.current_task_channel();
        let result_channel = self
            .result_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let quit_receiver = self.quit_channel.receiver();
        let runtime = self.current_runtime();
        let active = Arc::clone(&self.active);
        let running = Arc::clone(&self.running_tasks);
        active.count.fetch_add(1, Ordering::SeqCst);
        std::thread::spawn(move || {
            let _active = ActiveGuard(active);
            let task_receiver = task_channel.receiver();
            let local_cancelled = runtime.local.receiver();
            let pipeline_cancelled = runtime.pipeline.cancelled();
            let parent_cancelled = runtime.pipeline.parent_cancelled();
            let mut tune_wait = None;
            loop {
                crossbeam_channel::select! {
                    recv(task_receiver) -> task => {
                        let Ok(task) = task else { break };
                        running.fetch_add(1, Ordering::SeqCst);
                        let _running = RunningGuard(&running);
                        let (label, function, panic_error) = task.recover_args();
                        let handled = catch_unwind(AssertUnwindSafe(|| {
                            let mut send = |result| {
                                let Some(channel) = &result_channel else { return };
                                let sender = channel
                                    .sender()
                                    .unwrap_or_else(|| panic!("send on closed channel"));
                                crossbeam_channel::select! {
                                    recv(local_cancelled) -> _ => {},
                                    recv(pipeline_cancelled) -> _ => {},
                                    recv(parent_cancelled) -> _ => {},
                                    send(sender, result) -> sent => {
                                        sent.unwrap_or_else(|_| panic!("send on closed channel"));
                                    },
                                }
                            };
                            worker.handle_task(task, &mut send)
                        }));
                        match handled {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => runtime.pipeline.on_error(error),
                            Err(_) => {
                                let error = panic_error.unwrap_or_else(|| {
                                    E::from(PanicError(format!(
                                        "task panic: {label}, func info: {function}"
                                    )))
                                });
                                runtime.pipeline.on_error(error);
                            },
                        }
                    },
                    recv(quit_receiver) -> config => {
                        if let Ok(config) = config {
                            tune_wait = Some(config.wait);
                        }
                        break;
                    },
                    recv(local_cancelled) -> _ => break,
                    recv(pipeline_cancelled) -> _ => break,
                    recv(parent_cancelled) -> _ => break,
                }
            }
            let close_result = worker.close();
            if let Some(tune_wait) = tune_wait {
                tune_wait.finish();
            }
            if let Err(error) = close_result {
                runtime.pipeline.on_error(error);
            }
        });
    }

    fn current_runtime(&self) -> Runtime<E> {
        self.runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("worker pool must be started")
    }

    fn current_task_channel(&self) -> Channel<T> {
        self.task_channel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("worker pool task channel must be initialized")
    }
}

impl<T, R, E> Tuner for WorkerPool<T, R, E>
where
    T: TaskMayPanic<E>,
    R: Send + 'static,
    E: Error + Clone + From<PanicError> + Send + Sync + 'static,
{
    fn tune(&self, workers: i32, wait: bool) {
        WorkerPool::tune(self, workers, wait);
    }
}

struct ActiveGuard(Arc<ActiveWorkers>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let _guard = self
            .0
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.0.count.fetch_sub(1, Ordering::SeqCst);
        self.0.done.notify_all();
    }
}

struct RunningGuard<'a>(&'a AtomicI32);

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct Cancellation {
    inner: Arc<CancellationInner>,
}

struct CancellationInner {
    sender: Mutex<Option<crossbeam_channel::Sender<()>>>,
    receiver: crossbeam_channel::Receiver<()>,
}

impl Cancellation {
    fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(0);
        Self {
            inner: Arc::new(CancellationInner {
                sender: Mutex::new(Some(sender)),
                receiver,
            }),
        }
    }

    fn cancel(&self) {
        self.inner
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    fn receiver(&self) -> crossbeam_channel::Receiver<()> {
        self.inner.receiver.clone()
    }
}

fn go_zero_time() -> SystemTime {
    SystemTime::UNIX_EPOCH
        .checked_sub(Duration::from_secs(62_135_596_800))
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

fn new_worker_pool_failpoint(workers: isize) {
    #[cfg(feature = "failpoints")]
    fail::fail_point!("NewWorkerPool", |_| {
        let hook = new_worker_pool_failpoint_hook()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook(workers);
        }
    });
    #[cfg(not(feature = "failpoints"))]
    let _ = workers;
}

#[cfg(feature = "failpoints")]
type NewWorkerPoolFailpointHook = Arc<dyn Fn(isize) + Send + Sync>;

#[cfg(feature = "failpoints")]
fn new_worker_pool_failpoint_hook() -> &'static Mutex<Option<NewWorkerPoolFailpointHook>> {
    static HOOK: OnceLock<Mutex<Option<NewWorkerPoolFailpointHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

/// Installs the native callback used by Go `failpoint.InjectCall("NewWorkerPool", numWorkers)`.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub fn set_new_worker_pool_failpoint_hook(hook: Option<NewWorkerPoolFailpointHook>) {
    *new_worker_pool_failpoint_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = hook;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use std::sync::atomic::AtomicI64;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestError(String);

    impl std::fmt::Display for TestError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(&self.0)
        }
    }

    impl Error for TestError {}

    impl From<PanicError> for TestError {
        fn from(error: PanicError) -> Self {
            Self(error.to_string())
        }
    }

    #[derive(Clone, Copy)]
    struct Int64Task(i64);

    impl TaskMayPanic<TestError> for Int64Task {
        fn recover_args(&self) -> (String, String, Option<TestError>) {
            (String::new(), String::new(), Option::None)
        }
    }

    struct TaskWait {
        remaining: Mutex<i32>,
        done: Condvar,
    }

    impl TaskWait {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                remaining: Mutex::new(0),
                done: Condvar::new(),
            })
        }

        fn add(&self, count: i32) {
            *self
                .remaining
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += count;
        }

        fn finish(&self) {
            let mut remaining = self
                .remaining
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *remaining -= 1;
            self.done.notify_all();
        }

        fn wait(&self) {
            let mut remaining = self
                .remaining
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while *remaining != 0 {
                remaining = self
                    .done
                    .wait(remaining)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }

    struct MyWorker {
        count: Arc<AtomicI64>,
        wait: Arc<TaskWait>,
    }

    impl Worker<Int64Task, (), TestError> for MyWorker {
        fn handle_task(
            &mut self,
            task: Int64Task,
            _send: &mut dyn FnMut(()),
        ) -> Result<(), TestError> {
            self.count.fetch_add(task.0, Ordering::SeqCst);
            self.wait.finish();
            Ok(())
        }

        fn close(&mut self) -> Result<(), TestError> {
            Ok(())
        }
    }

    fn my_pool(
        workers: isize,
        count: Arc<AtomicI64>,
        wait: Arc<TaskWait>,
    ) -> Arc<WorkerPool<Int64Task, (), TestError>> {
        WorkerPool::new(
            "test".to_owned(),
            Component::Unknown,
            workers,
            move || {
                Some(Box::new(MyWorker {
                    count: Arc::clone(&count),
                    wait: Arc::clone(&wait),
                }))
            },
            vec![],
        )
    }

    #[test]
    fn test_worker_pool() {
        let count = Arc::new(AtomicI64::new(0));
        let wait = TaskWait::new();
        let pool = my_pool(3, Arc::clone(&count), Arc::clone(&wait));
        pool.start(Context::new(Option::None));
        let results = pool.result_channel().unwrap();
        let result_receiver = results.receiver();
        let result_consumer = std::thread::spawn(move || while result_receiver.recv().is_ok() {});

        for expected in [45_i64, 90, 135] {
            wait.add(10);
            for value in 0..10 {
                pool.add_task(Int64Task(value));
            }
            wait.wait();
            assert_eq!(expected, count.load(Ordering::SeqCst));
            if expected == 45 {
                assert_eq!(3, pool.cap());
                pool.tune(5, false);
            } else if expected == 90 {
                assert_eq!(5, pool.cap());
                pool.tune(2, false);
            } else {
                assert_eq!(2, pool.cap());
            }
        }
        pool.close_and_wait();
        result_consumer.join().unwrap();
    }

    #[test]
    fn test_tune_pool_size() {
        tune_pool_size_random();
        tune_pool_size_before_start();
        tune_pool_size_context_done_when_reduce_and_wait();
    }

    fn tune_pool_size_random() {
        let count = Arc::new(AtomicI64::new(0));
        let wait = TaskWait::new();
        let pool = my_pool(3, count, wait);
        pool.start(Context::new(Option::None));
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let mut random = rand::rngs::StdRng::seed_from_u64(seed);
        for _ in 0..100 {
            let wait = random.gen_bool(0.5);
            let larger = pool.cap() + random.gen_range(0..10) + 2;
            pool.tune(larger, wait);
            assert_eq!(larger, pool.cap(), "seed: {seed}");
            let smaller = pool.cap() / 2;
            pool.tune(smaller, wait);
            assert_eq!(smaller, pool.cap(), "seed: {seed}");
        }
        pool.close_and_wait();
    }

    fn tune_pool_size_before_start() {
        let pool = my_pool(10, Arc::new(AtomicI64::new(0)), TaskWait::new());
        pool.tune(5, true);
        pool.start(Context::new(Option::None));
        pool.close_and_wait();
        assert_eq!(5, pool.cap());
    }

    fn tune_pool_size_context_done_when_reduce_and_wait() {
        let pool = my_pool(10, Arc::new(AtomicI64::new(0)), TaskWait::new());
        let context = Context::new(Option::None);
        pool.start(Arc::clone(&context));
        context.cancel();
        pool.tune(5, true);
        pool.release();
    }

    struct DummyWorker;

    impl<R: Default + Send + 'static> Worker<Int64Task, R, TestError> for DummyWorker {
        fn handle_task(
            &mut self,
            _task: Int64Task,
            send: &mut dyn FnMut(R),
        ) -> Result<(), TestError> {
            send(R::default());
            Ok(())
        }

        fn close(&mut self) -> Result<(), TestError> {
            Ok(())
        }
    }

    #[test]
    fn test_worker_pool_none_result() {
        let pool: Arc<WorkerPool<Int64Task, NoResult, TestError>> = WorkerPool::new(
            "test".to_owned(),
            Component::Unknown,
            3,
            || Some(Box::new(DummyWorker)),
            vec![],
        );
        pool.start(Context::new(Option::None));
        assert!(pool.result_channel().is_none());
        pool.close_and_wait();

        let pool: Arc<WorkerPool<Int64Task, i64, TestError>> = WorkerPool::new(
            "test".to_owned(),
            Component::Unknown,
            3,
            || Some(Box::new(DummyWorker)),
            vec![],
        );
        pool.start(Context::new(Option::None));
        assert!(pool.result_channel().is_some());
        pool.close_and_wait();

        let pool: Arc<WorkerPool<Int64Task, (), TestError>> = WorkerPool::new(
            "test".to_owned(),
            Component::Unknown,
            3,
            || Some(Box::new(DummyWorker)),
            vec![],
        );
        pool.start(Context::new(Option::None));
        assert!(pool.result_channel().is_some());
        pool.close_and_wait();
    }

    struct SendingWorker;

    impl Worker<Int64Task, i64, TestError> for SendingWorker {
        fn handle_task(
            &mut self,
            _task: Int64Task,
            send: &mut dyn FnMut(i64),
        ) -> Result<(), TestError> {
            send(0);
            Ok(())
        }

        fn close(&mut self) -> Result<(), TestError> {
            Ok(())
        }
    }

    #[test]
    fn test_worker_pool_custom_channel() {
        let pool: Arc<WorkerPool<Int64Task, i64, TestError>> = WorkerPool::new(
            "test".to_owned(),
            Component::Unknown,
            3,
            || Some(Box::new(SendingWorker)),
            vec![],
        );
        let tasks = Channel::bounded(0);
        pool.set_task_receiver(tasks.clone());
        let results = Channel::bounded(0);
        pool.set_result_sender(results.clone());
        let result_receiver = results.receiver();
        let count = Arc::new(AtomicI32::new(0));
        let consumer_count = Arc::clone(&count);
        let consumer = std::thread::spawn(move || {
            while result_receiver.recv().is_ok() {
                consumer_count.fetch_add(1, Ordering::SeqCst);
            }
        });
        pool.start(Context::new(Option::None));
        for value in 0..5 {
            tasks.send(Int64Task(value));
        }
        tasks.close();
        pool.release();
        consumer.join().unwrap();
        assert_eq!(5, count.load(Ordering::SeqCst));
    }

    #[test]
    fn test_worker_pool_cancel_context() {
        let context = Context::new(Option::None);
        let pool: Arc<WorkerPool<Int64Task, i64, TestError>> = WorkerPool::new(
            "test".to_owned(),
            Component::Unknown,
            3,
            || Some(Box::new(SendingWorker)),
            vec![],
        );
        pool.start(Arc::clone(&context));
        pool.add_task(Int64Task(1));
        context.cancel();
        pool.release();
        assert_eq!(0, pool.running());
    }
}
