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

//! Go `pkg/statistics/handle/initstats`.

use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use tidb_log::{Field, Value};
use tidb_util::logutil::{bg_logger, sample_logger_factory, SampledLogger, LOG_FIELD_CATEGORY};

/// An atomic `f64` with Go `atomic.Float64` load/store behavior.
pub struct AtomicF64(AtomicU64);

impl AtomicF64 {
    /// Creates an atomic floating-point value.
    #[must_use]
    pub const fn new(value: f64) -> Self {
        Self(AtomicU64::new(value.to_bits()))
    }

    /// Go `Load`.
    #[must_use]
    pub fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::SeqCst))
    }

    /// Go `Store`.
    pub fn store(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::SeqCst);
    }
}

/// Go `InitStatsPercentage`.
pub static INIT_STATS_PERCENTAGE: AtomicF64 = AtomicF64::new(0.0);

static SAMPLE_LOGGER: LazyLock<SampledLogger> = LazyLock::new(|| {
    sample_logger_factory(
        Duration::from_secs(60),
        1,
        vec![Field::new(
            LOG_FIELD_CATEGORY,
            Value::Str("stats".to_owned()),
        )],
    )()
});

/// Go `GetConcurrency`.
#[must_use]
pub fn get_concurrency() -> isize {
    let config = tidb_config::config_tree::config::get_global_config();
    let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
    let parallelism = isize::try_from(parallelism).unwrap_or(isize::MAX);
    let concurrency = if config.performance.force_init_stats {
        parallelism.saturating_sub(2)
    } else {
        parallelism / 2
    };
    concurrency.clamp(2, 16)
}

/// Go `Task`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Task {
    /// Lower table-ID range bound.
    pub start_tid: i64,
    /// Upper table-ID range bound.
    pub end_tid: i64,
}

/// Error returned by a range-worker task.
pub type TaskError = Box<dyn Error + Send + Sync + 'static>;

/// Result returned by a range-worker task.
pub type TaskResult = Result<(), TaskError>;

type ProcessTask = dyn Fn(Task) -> TaskResult + Send + Sync + 'static;

struct WorkerInner {
    progress_logger: SampledLogger,
    task_name: String,
    receiver: Receiver<Task>,
    process_task: Arc<ProcessTask>,
    task_count: u64,
    complete_task_count: AtomicU64,
    total_percentage: f64,
    total_percentage_step: f64,
}

/// Go `RangeWorker`.
pub struct RangeWorker {
    inner: Arc<WorkerInner>,
    sender: Mutex<Option<Sender<Task>>>,
    concurrency: isize,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl RangeWorker {
    /// Go `NewRangeWorker`.
    #[must_use]
    pub fn new(
        task_name: impl Into<String>,
        process_task: impl Fn(Task) -> TaskResult + Send + Sync + 'static,
        concurrency: isize,
        total_task_count: u64,
        total_percentage_step: f64,
    ) -> Self {
        let (sender, receiver) = bounded(1);
        Self {
            inner: Arc::new(WorkerInner {
                progress_logger: SAMPLE_LOGGER.clone(),
                task_name: task_name.into(),
                receiver,
                process_task: Arc::new(process_task),
                task_count: total_task_count,
                complete_task_count: AtomicU64::new(0),
                total_percentage: INIT_STATS_PERCENTAGE.load(),
                total_percentage_step,
            }),
            sender: Mutex::new(Some(sender)),
            concurrency,
            workers: Mutex::new(Vec::new()),
        }
    }

    /// Go `LoadStats`.
    pub fn load_stats(&self) {
        let mut workers = self.workers.lock().expect("worker list lock poisoned");
        for _ in 0..self.concurrency {
            let inner = Arc::clone(&self.inner);
            workers.push(std::thread::spawn(move || load_stats(inner)));
        }
    }

    /// Go `SendTask`.
    pub fn send_task(&self, task: Task) {
        self.sender
            .lock()
            .expect("task sender lock poisoned")
            .as_ref()
            .expect("send on closed init-stats task channel")
            .send(task)
            .expect("send on closed init-stats task channel");
    }

    /// Go `Wait`.
    pub fn wait(&self) {
        self.sender
            .lock()
            .expect("task sender lock poisoned")
            .take()
            .expect("close of closed init-stats task channel");
        let workers = std::mem::take(&mut *self.workers.lock().expect("worker list lock poisoned"));
        for worker in workers {
            if let Err(payload) = worker.join() {
                std::panic::resume_unwind(payload);
            }
        }
    }
}

fn load_stats(inner: Arc<WorkerInner>) {
    loop {
        let task = inner.receiver.recv();
        let Ok(task) = task else { return };
        if let Err(error) = (inner.process_task)(task) {
            bg_logger().error(
                "load stats failed",
                &[Field::new(
                    "error",
                    Value::Error {
                        basic: error.to_string(),
                        verbose: None,
                    },
                )],
            );
        }
        let complete = inner.complete_task_count.fetch_add(1, Ordering::SeqCst) + 1;
        let percentage = complete as f64 / inner.task_count as f64 * inner.total_percentage_step
            + inner.total_percentage;
        INIT_STATS_PERCENTAGE.store(percentage);
        inner.progress_logger.info(
            &format!(
                "load {} [{}/{}]",
                inner.task_name, complete, inner.task_count
            ),
            &[],
        );
    }
}
