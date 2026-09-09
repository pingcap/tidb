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

//! Non-reusing goroutine-pool equivalent from `pkg/resourcemanager/pool/spool`.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::{Duration, SystemTime};

use prometheus::{Gauge, GaugeVec, Opts};

use crate::pool::{BasePool, PoolError};
use crate::poolmanager::{ExitChannel, Meta, Task, TaskChannel, TaskManager};
use crate::util::{Component, GoroutinePool};

const WAIT_INTERVAL: Duration = Duration::from_millis(5);

/// Options applied when creating a pool.
pub struct Options {
    /// Whether submissions wait for capacity.
    pub blocking: bool,
}

/// A pool construction option.
pub type PoolOption = Box<dyn FnOnce(&mut Options)>;

/// Returns the source default options.
pub fn default_option() -> Options {
    Options { blocking: true }
}

/// Sets whether submissions wait for capacity.
pub fn with_blocking(blocking: bool) -> PoolOption {
    Box::new(move |options| options.blocking = blocking)
}

/// Errors returned while constructing a pool.
#[derive(Debug)]
pub enum NewPoolError {
    /// Invalid pool parameters.
    Pool(PoolError),
    /// Resource-manager registration failed.
    Register(&'static str),
}

impl fmt::Display for NewPoolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pool(error) => error.fmt(formatter),
            Self::Register(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for NewPoolError {}

struct WorkerState {
    running: AtomicI32,
    mutex: Mutex<()>,
    done: Condvar,
}

/// A pool where every accepted function runs on a newly spawned thread.
pub struct Pool {
    options: Options,
    origin_capacity: i32,
    capacity: RwLock<i32>,
    workers: Arc<WorkerState>,
    waiting: AtomicI32,
    stopped: AtomicBool,
    pending_mutex: Mutex<()>,
    pending_done: Condvar,
    concurrency_metric: Gauge,
    task_manager: TaskManager,
    base: BasePool,
}

impl Pool {
    /// Creates and registers a pool.
    pub fn new(
        name: String,
        size: i32,
        component: Component,
        options: Vec<PoolOption>,
    ) -> Result<Arc<Self>, NewPoolError> {
        let mut loaded = default_option();
        for option in options {
            option(&mut loaded);
        }
        let metric = pool_concurrency_metric()
            .with_label_values(&[&name])
            .clone();
        if size == 0 {
            return Err(NewPoolError::Pool(PoolError::ParamsInvalid));
        }
        let mut base = BasePool::new();
        base.set_name(name.clone());
        let pool = Arc::new(Self {
            options: loaded,
            origin_capacity: size,
            capacity: RwLock::new(size),
            workers: Arc::new(WorkerState {
                running: AtomicI32::new(0),
                mutex: Mutex::new(()),
                done: Condvar::new(),
            }),
            waiting: AtomicI32::new(0),
            stopped: AtomicBool::new(false),
            pending_mutex: Mutex::new(()),
            pending_done: Condvar::new(),
            concurrency_metric: metric,
            task_manager: TaskManager::new(size),
            base,
        });
        pool.concurrency_metric.set(size as f64);
        let registered: Arc<dyn GoroutinePool> = pool.clone();
        crate::instance_resource_manager()
            .register(registered, name, component)
            .map_err(NewPoolError::Register)?;
        Ok(pool)
    }

    /// Changes pool capacity.
    pub fn tune(&self, size: i32) {
        if size == 0 {
            return;
        }
        let mut capacity = self
            .capacity
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.base.set_last_tune_ts(SystemTime::now());
        let old = *capacity;
        *capacity = size;
        self.concurrency_metric.set(size as f64);
        if old == size {
            return;
        }
        if old < size && self.workers.running.load(Ordering::SeqCst) < size {
            let (_, task) = self.task_manager.overclock();
            if let Some(task) = task {
                self.workers.running.fetch_add(1, Ordering::SeqCst);
                self.spawn(Box::new(move || run_task(task)));
            }
            return;
        }
        if self.workers.running.load(Ordering::SeqCst) > size {
            self.task_manager.downclock();
        }
    }

    /// Runs one function in a newly spawned worker.
    pub fn run(&self, function: Task) -> Result<(), PoolError> {
        let _pending = PendingSubmission::new(self);
        if self.stopped.load(Ordering::SeqCst) {
            return Err(PoolError::Closed);
        }
        if !self.check_and_add_running(1).1 {
            return Err(PoolError::Overload);
        }
        self.spawn(function);
        Ok(())
    }

    /// Runs functions from one channel with the admitted concurrency.
    pub fn run_with_concurrency(
        &self,
        functions: TaskChannel,
        concurrency: u32,
    ) -> Result<(), PoolError> {
        let _pending = PendingSubmission::new(self);
        if self.stopped.load(Ordering::SeqCst) {
            return Err(PoolError::Closed);
        }
        let requested_concurrency = concurrency as i32;
        let (admitted_concurrency, run) = self.check_and_add_running(concurrency);
        if !run {
            return Err(PoolError::Overload);
        }
        let task = Arc::new(Meta::new(
            self.base.generate_task_id(),
            Some(ExitChannel::bounded(1)),
            Some(functions),
            requested_concurrency,
        ));
        self.task_manager.register_task(Arc::clone(&task));
        for _ in 0..admitted_concurrency {
            let task = Arc::clone(&task);
            self.spawn(Box::new(move || run_task(task)));
        }
        Ok(())
    }

    /// Returns current capacity.
    pub fn cap(&self) -> i32 {
        *self
            .capacity
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns the number of running workers.
    pub fn running(&self) -> i32 {
        let _capacity = self
            .capacity
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.workers.running.load(Ordering::SeqCst)
    }

    /// Stops submissions, waits for submitters and workers, and unregisters.
    pub fn release_and_wait(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let mut pending = self
            .pending_mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.waiting.load(Ordering::SeqCst) > 0 {
            pending = self
                .pending_done
                .wait(pending)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(pending);
        let mut workers = self
            .workers
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.workers.running.load(Ordering::SeqCst) > 0 {
            workers = self
                .workers
                .done
                .wait(workers)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        crate::instance_resource_manager().unregister(&self.base.name());
    }

    fn check_and_add_running(&self, concurrency: u32) -> (i32, bool) {
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                return (0, false);
            }
            let capacity = self
                .capacity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let available = *capacity - self.workers.running.load(Ordering::SeqCst);
            if available > 0 {
                let admitted = available.min(concurrency as i32);
                self.workers.running.fetch_add(admitted, Ordering::SeqCst);
                return (admitted, true);
            }
            if !self.options.blocking {
                return (0, false);
            }
            drop(capacity);
            std::thread::sleep(WAIT_INTERVAL);
        }
    }

    fn spawn(&self, function: Task) {
        let workers = Arc::clone(&self.workers);
        std::thread::spawn(move || {
            if let Err(panic) = catch_unwind(AssertUnwindSafe(function)) {
                tracing::error!(?panic, "recover panic");
            }
            let _done = workers
                .mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            workers.running.fetch_sub(1, Ordering::SeqCst);
            workers.done.notify_all();
        });
    }
}

impl GoroutinePool for Pool {
    fn release_and_wait(&self) {
        Pool::release_and_wait(self);
    }

    fn tune(&self, size: i32) {
        Pool::tune(self, size);
    }

    fn last_tuner_ts(&self) -> SystemTime {
        self.base.last_tuner_ts()
    }

    fn cap(&self) -> i32 {
        Pool::cap(self)
    }

    fn running(&self) -> i32 {
        Pool::running(self)
    }

    fn name(&self) -> String {
        self.base.name()
    }

    fn origin_concurrency(&self) -> i32 {
        self.origin_capacity
    }
}

struct PendingSubmission<'a> {
    pool: &'a Pool,
}

impl<'a> PendingSubmission<'a> {
    fn new(pool: &'a Pool) -> Self {
        pool.waiting.fetch_add(1, Ordering::SeqCst);
        Self { pool }
    }
}

impl Drop for PendingSubmission<'_> {
    fn drop(&mut self) {
        let _pending = self
            .pool
            .pending_mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.pool.waiting.fetch_sub(1, Ordering::SeqCst);
        self.pool.pending_done.notify_one();
    }
}

fn run_task(task: Arc<Meta>) {
    let task_channel = task.task_channel();
    let exit_channel = task.exit_channel();
    task.increment_task();
    let _running = TaskRunning(&task);
    let task_receiver = task_channel.as_ref().map(TaskChannel::receiver);
    let exit_receiver = exit_channel.as_ref().map(ExitChannel::receiver);
    loop {
        crossbeam_channel::select! {
            recv(task_receiver.as_ref().unwrap_or(&crossbeam_channel::never())) -> function => {
                match function {
                    Ok(function) => function(),
                    Err(_) => return,
                }
            },
            recv(exit_receiver.as_ref().unwrap_or(&crossbeam_channel::never())) -> _ => return,
        }
    }
}

struct TaskRunning<'a>(&'a Meta);

impl Drop for TaskRunning<'_> {
    fn drop(&mut self) {
        self.0.decrement_task();
    }
}

fn pool_concurrency_metric() -> &'static GaugeVec {
    static METRIC: OnceLock<GaugeVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let metric = GaugeVec::new(
            Opts::new("pool_concurrency", "How many concurrency in the pool")
                .namespace("tidb")
                .subsystem("rm"),
            &["type"],
        )
        .expect("resource-manager pool metric definition must be valid");
        prometheus::default_registry()
            .register(Box::new(metric.clone()))
            .expect("resource-manager pool metric must register once");
        metric
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn wait_until(mut condition: impl FnMut() -> bool, timeout: Duration, interval: Duration) {
        let deadline = Instant::now() + timeout;
        while !condition() {
            assert!(
                Instant::now() < deadline,
                "condition was not met before timeout"
            );
            std::thread::sleep(interval);
        }
    }

    #[test]
    fn test_release_when_running_pool() {
        let pool = Pool::new(
            "TestReleaseWhenRunningPool".to_owned(),
            1,
            Component::Unknown,
            vec![],
        )
        .unwrap();
        let mut submitters = Vec::new();
        for _ in 0..2 {
            let pool = Arc::clone(&pool);
            submitters.push(std::thread::spawn(move || {
                for _ in 0..30 {
                    let _ = pool.run(Box::new(|| {
                        std::thread::sleep(Duration::from_micros(100));
                    }));
                }
            }));
        }
        std::thread::sleep(Duration::from_micros(100));
        pool.release_and_wait();
        for submitter in submitters {
            submitter.join().unwrap();
        }
    }

    #[test]
    fn test_pool_tune_scale_up_and_down() {
        let (release, wait) = crossbeam_channel::bounded::<()>(0);
        let pool = Pool::new(
            "TestPoolTuneScaleUp".to_owned(),
            2,
            Component::Unknown,
            vec![with_blocking(true)],
        )
        .unwrap();
        for _ in 0..2 {
            let wait = wait.clone();
            pool.run(Box::new(move || {
                let _ = wait.recv();
            }))
            .unwrap();
        }
        assert_eq!(2, pool.running());

        pool.tune(3);
        let wait_one = wait.clone();
        pool.run(Box::new(move || {
            let _ = wait_one.recv();
        }))
        .unwrap();
        assert_eq!(3, pool.running());

        let mut submitters = Vec::new();
        for _ in 0..5 {
            let pool = Arc::clone(&pool);
            let wait = wait.clone();
            submitters.push(std::thread::spawn(move || {
                let _ = pool.run(Box::new(move || {
                    let _ = wait.recv();
                }));
            }));
        }
        pool.tune(8);
        for submitter in submitters {
            submitter.join().unwrap();
        }
        wait_until(
            || pool.running() == 8,
            Duration::from_secs(1),
            Duration::from_millis(200),
        );

        pool.tune(2);
        for _ in 0..6 {
            release.send(()).unwrap();
        }
        wait_until(
            || pool.running() == 2,
            Duration::from_secs(1),
            Duration::from_millis(200),
        );
        for _ in 0..2 {
            release.send(()).unwrap();
        }
        wait_until(
            || pool.running() == 0,
            Duration::from_secs(1),
            Duration::from_millis(200),
        );

        let count = Arc::new(AtomicI32::new(0));
        let functions = TaskChannel::bounded(10);
        pool.run_with_concurrency(functions.clone(), 2).unwrap();
        assert_eq!(2, pool.running());
        for _ in 0..10 {
            let count = Arc::clone(&count);
            functions.send(Box::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
            }));
        }
        wait_until(
            || count.load(Ordering::SeqCst) == 10,
            Duration::from_secs(1),
            Duration::from_millis(200),
        );
        assert_eq!(2, pool.running());
        functions.close();
        wait_until(
            || pool.running() == 0,
            Duration::from_secs(1),
            Duration::from_millis(200),
        );
        pool.release_and_wait();
    }

    #[test]
    fn test_run_overload() {
        let stop = Arc::new(AtomicBool::new(false));
        let pool = Pool::new(
            "TestMaxBlockingSubmit".to_owned(),
            10,
            Component::Unknown,
            vec![with_blocking(false)],
        )
        .unwrap();
        for _ in 0..10 {
            let stop = Arc::clone(&stop);
            pool.run(Box::new(move || {
                while !stop.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
            }))
            .unwrap();
        }
        assert_eq!(
            PoolError::Overload,
            pool.run(Box::new(|| demo_function())).unwrap_err()
        );
        stop.store(true, Ordering::SeqCst);
        pool.release_and_wait();
    }

    #[test]
    fn test_run_with_not_enough() {
        let functions = TaskChannel::bounded(10);
        let pool = Pool::new(
            "TestRunWithNotEnough".to_owned(),
            10,
            Component::Unknown,
            vec![with_blocking(false)],
        )
        .unwrap();
        pool.run_with_concurrency(functions.clone(), 110).unwrap();
        assert_eq!(10, pool.running());
        assert_eq!(
            PoolError::Overload,
            pool.run_with_concurrency(functions.clone(), 1).unwrap_err()
        );
        assert_eq!(PoolError::Overload, pool.run(Box::new(|| {})).unwrap_err());
        functions.close();
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(0, pool.running());
        pool.release_and_wait();
    }

    #[test]
    fn test_run_with_not_enough2() {
        let functions = TaskChannel::bounded(10);
        let count = Arc::new(AtomicI32::new(0));
        let pool = Pool::new(
            "TestRunWithNotEnough2".to_owned(),
            1,
            Component::Unknown,
            vec![with_blocking(false)],
        )
        .unwrap();
        pool.run_with_concurrency(functions.clone(), 2).unwrap();
        assert_eq!(1, pool.running());
        assert!(pool.run_with_concurrency(functions.clone(), 1).is_err());
        assert!(pool.run(Box::new(|| {})).is_err());
        for _ in 0..100 {
            let count = Arc::clone(&count);
            functions.send(Box::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
            }));
        }
        functions.close();
        std::thread::sleep(Duration::from_micros(100));
        assert_eq!(0, pool.running());
        assert_eq!(100, count.load(Ordering::SeqCst));
        pool.release_and_wait();
    }

    #[test]
    fn test_with_task_manager() {
        let pool = Pool::new(
            "TestWithTaskManager".to_owned(),
            1,
            Component::Unknown,
            vec![with_blocking(false)],
        )
        .unwrap();
        let functions = TaskChannel::bounded(10);
        pool.run_with_concurrency(functions.clone(), 2).unwrap();
        let (ready, ready_receiver) = crossbeam_channel::bounded(1);
        functions.send(Box::new(move || {
            let _ = ready.send(());
        }));
        wait_until(
            || ready_receiver.try_recv().is_ok(),
            Duration::from_secs(1),
            Duration::from_millis(10),
        );
        assert_eq!(1, pool.running());

        pool.tune(2);
        std::thread::sleep(Duration::from_micros(100));
        assert_eq!(2, pool.running());
        pool.tune(3);
        wait_until(
            || pool.running() == 3,
            Duration::from_secs(1),
            Duration::from_millis(200),
        );
        pool.tune(2);
        wait_until(
            || pool.running() == 2,
            Duration::from_secs(1),
            Duration::from_millis(200),
        );
        pool.tune(1);
        wait_until(
            || pool.running() == 1,
            Duration::from_secs(1),
            Duration::from_millis(200),
        );
        functions.close();
        pool.release_and_wait();
    }

    fn demo_function() {
        recurse(2);
    }

    fn recurse(depth: usize) {
        if depth == 0 {
            return;
        }
        let stack = [0_u8; 100];
        let _ = stack[3];
        recurse(depth - 1);
    }
}
