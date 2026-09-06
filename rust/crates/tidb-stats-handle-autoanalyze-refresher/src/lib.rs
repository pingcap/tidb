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

//! Go `pkg/statistics/handle/autoanalyze/refresher`.

use chrono::Utc;
use std::collections::HashSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use tidb_stats_handle_autoanalyze_priorityqueue::{
    AnalysisPriorityQueue, AutoAnalysisTimeWindow, PartitionPruneMode, PriorityQueueSnapshot,
    QueueError, RunningAnalysisJob,
};

/// The session/global values read by Go `AnalyzeHighestPriorityTables` on
/// each invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct RefreshParameters {
    /// Raw `tidb_auto_analyze_ratio` value.
    pub auto_analyze_ratio: String,
    /// Current partition prune mode.
    pub prune_mode: PartitionPruneMode,
    /// Raw `tidb_auto_analyze_start_time` value.
    pub start_time: String,
    /// Raw `tidb_auto_analyze_end_time` value.
    pub end_time: String,
    /// Live process-wide auto-analyze concurrency.
    pub max_concurrency: usize,
}

/// One job accepted by Go's refresher worker.
pub trait WorkerJob: Send + 'static {
    /// Go `AnalysisJob.GetTableID`.
    fn table_id(&self) -> i64;

    /// Go `AnalysisJob.Analyze`.
    fn analyze(self: Box<Self>);
}

impl WorkerJob for RunningAnalysisJob {
    fn table_id(&self) -> i64 {
        self.table_id()
    }

    fn analyze(self: Box<Self>) {
        let _ = (*self).analyze();
    }
}

#[derive(Debug)]
struct WorkerState {
    running_jobs: HashSet<i64>,
    max_concurrency: usize,
}

/// Go `worker`: concurrency admission plus running-table ownership.
pub struct Worker {
    state: Arc<(Mutex<WorkerState>, Condvar)>,
}

impl Worker {
    /// Go `NewWorker`.
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            state: Arc::new((
                Mutex::new(WorkerState {
                    running_jobs: HashSet::new(),
                    max_concurrency,
                }),
                Condvar::new(),
            )),
        }
    }

    /// Go `UpdateConcurrency`.
    pub fn update_concurrency(&self, new_concurrency: usize) {
        self.lock_state().max_concurrency = new_concurrency;
    }

    /// Go `SubmitJob`. The capacity decision and running-set insertion are
    /// one critical section; panics are recovered and always release the ID.
    pub fn submit_job<J: WorkerJob>(&self, job: J) -> bool {
        let table_id = job.table_id();
        {
            let mut state = self.lock_state();
            if state.running_jobs.len() >= state.max_concurrency {
                return false;
            }
            state.running_jobs.insert(table_id);
        }
        let state = Arc::clone(&self.state);
        std::thread::spawn(move || {
            let _ = catch_unwind(AssertUnwindSafe(|| Box::new(job).analyze()));
            let (lock, stopped) = &*state;
            let mut state = lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.running_jobs.remove(&table_id);
            stopped.notify_all();
        });
        true
    }

    /// Go `GetRunningJobs`, returning a defensive copy.
    pub fn running_jobs(&self) -> HashSet<i64> {
        self.lock_state().running_jobs.clone()
    }

    /// Go `GetMaxConcurrency`.
    pub fn max_concurrency(&self) -> usize {
        self.lock_state().max_concurrency
    }

    /// Go `Stop` / `WaitAutoAnalyzeFinishedForTest`.
    pub fn stop(&self) {
        let (lock, stopped) = &*self.state;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.running_jobs.is_empty() {
            state = stopped
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, WorkerState> {
        self.state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Go `Refresher`. Calls are serialized by the owning statistics handle.
pub struct Refresher {
    jobs: Arc<AnalysisPriorityQueue>,
    worker: Worker,
    auto_analysis_time_window: AutoAnalysisTimeWindow,
    last_seen_prune_mode: Option<PartitionPruneMode>,
    last_seen_auto_analyze_ratio: Option<f64>,
}

impl Refresher {
    /// Go `NewRefresher`; DDL registration belongs to the server's notifier
    /// adapter, which owns the same queue instance.
    pub fn new(jobs: Arc<AnalysisPriorityQueue>, max_concurrency: usize) -> Self {
        Self {
            jobs,
            worker: Worker::new(max_concurrency),
            auto_analysis_time_window: AutoAnalysisTimeWindow::default(),
            last_seen_prune_mode: None,
            last_seen_auto_analyze_ratio: None,
        }
    }

    /// Go `AnalyzeHighestPriorityTables`.
    pub fn analyze_highest_priority_tables(&mut self, parameters: &RefreshParameters) -> bool {
        let ratio = tidb_stats_handle_autoanalyze_exec::parse_auto_analyze_ratio(
            &parameters.auto_analyze_ratio,
        );
        if !self.jobs.is_initialized() {
            if self.jobs.initialize().is_err() {
                return false;
            }
            self.last_seen_auto_analyze_ratio = Some(ratio);
            self.last_seen_prune_mode = Some(parameters.prune_mode);
        } else if self.last_seen_auto_analyze_ratio != Some(ratio)
            || self.last_seen_prune_mode != Some(parameters.prune_mode)
        {
            // Go publishes the latest observed settings before rebuilding;
            // a failed rebuild is retried only after another setting change.
            self.last_seen_auto_analyze_ratio = Some(ratio);
            self.last_seen_prune_mode = Some(parameters.prune_mode);
            if self.jobs.rebuild().is_err() {
                return false;
            }
        }

        let Ok((start, end)) = tidb_stats_handle_autoanalyze_exec::parse_auto_analysis_window(
            &parameters.start_time,
            &parameters.end_time,
        ) else {
            return false;
        };
        self.auto_analysis_time_window = AutoAnalysisTimeWindow::new(start, end);
        if !self
            .auto_analysis_time_window
            .is_within_time_window(Utc::now())
        {
            return false;
        }

        self.worker.update_concurrency(parameters.max_concurrency);
        let max_concurrency = self.worker.max_concurrency();
        let running_jobs = self.worker.running_jobs();
        let remaining = max_concurrency.saturating_sub(running_jobs.len());
        if remaining == 0 {
            return false;
        }

        let mut submitted = 0_usize;
        while submitted < remaining {
            let mut job = match self.jobs.pop() {
                Ok(job) => job,
                Err(QueueError::HeapIsEmpty) => break,
                Err(_) => return false,
            };
            if running_jobs.contains(&job.table_id()) {
                continue;
            }
            if !job.validate_and_prepare().valid {
                continue;
            }
            if self.worker.submit_job(job) {
                submitted += 1;
            }
        }
        submitted > 0
    }

    /// Go `GetPriorityQueueSnapshot`.
    pub fn snapshot(&self) -> Result<PriorityQueueSnapshot, QueueError> {
        self.jobs.snapshot()
    }

    /// Go `GetRunningJobs`.
    pub fn running_jobs(&self) -> HashSet<i64> {
        self.worker.running_jobs()
    }

    /// Go `ProcessDMLChangesForTest`'s production operation.
    pub fn process_dml_changes(&self) {
        if self.jobs.is_initialized() {
            let _ = self.jobs.process_dml_changes();
        }
    }

    /// Go `RequeueMustRetryJobsForTest`'s production operation.
    pub fn requeue_must_retry_jobs(&self) {
        let _ = self.jobs.requeue_must_retry_jobs();
    }

    /// Go `IsQueueInitializedForTest`.
    pub fn is_queue_initialized(&self) -> bool {
        self.jobs.is_initialized()
    }

    /// Go `Len`.
    pub fn len(&self) -> usize {
        self.jobs.len().expect("initialized priority queue")
    }

    /// Go `WaitAutoAnalyzeFinishedForTest`.
    pub fn wait_auto_analyze_finished(&self) {
        self.worker.stop();
    }

    /// Go `Close`: wait for jobs before closing the queue.
    pub fn close(&self) {
        self.worker.stop();
        self.jobs.close();
    }

    /// Go `ClosePriorityQueue`.
    pub fn close_priority_queue(&self) {
        self.jobs.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;
    use tidb_stats_handle_autoanalyze_priorityqueue::{
        AnalysisJobContext, PriorityQueueSource, QueueInventory, TableLookup,
    };

    struct EmptySource {
        inventory_reads: AtomicUsize,
        version: AtomicU64,
    }

    impl AnalysisJobContext for EmptySource {
        fn lookup_table(&self, _table_id: i64) -> TableLookup {
            TableLookup::TableMissing
        }

        fn last_failed_analysis_duration(
            &self,
            _schema: &str,
            _table: &str,
            _partitions: &[String],
        ) -> Result<i64, String> {
            unreachable!("empty inventory has no jobs")
        }

        fn average_analysis_duration(
            &self,
            _schema: &str,
            _table: &str,
            _partitions: &[String],
        ) -> Result<i64, String> {
            unreachable!("empty inventory has no jobs")
        }

        fn auto_analyze(
            &self,
            _stats_version: i32,
            _need_version_rewrite_warning: bool,
            _sql: &str,
            _arguments: &[String],
        ) -> bool {
            unreachable!("empty inventory has no jobs")
        }

        fn auto_analyze_partition_batch_size(&self) -> usize {
            8192
        }
    }

    impl PriorityQueueSource for EmptySource {
        fn next_check_version_with_offset(&self) -> u64 {
            self.version.fetch_add(1, Ordering::SeqCst)
        }

        fn queue_inventory(&self) -> Result<QueueInventory, String> {
            self.inventory_reads.fetch_add(1, Ordering::SeqCst);
            Ok(QueueInventory {
                tables: Vec::new(),
                locked_table_ids: HashSet::new(),
                prune_mode: PartitionPruneMode::Dynamic,
                auto_analyze_ratio: 0.5,
                requested_version: 2,
                current_ts: 0,
                auto_analyze_min_count: 0,
            })
        }
    }

    fn parameters() -> RefreshParameters {
        RefreshParameters {
            auto_analyze_ratio: "0.5".to_owned(),
            prune_mode: PartitionPruneMode::Dynamic,
            start_time: "00:00 +0000".to_owned(),
            end_time: "23:59 +0000".to_owned(),
            max_concurrency: 3,
        }
    }

    #[deny(unused_must_use)]
    #[test]
    fn go_refresher_query_returns_can_be_ignored() {
        Worker::new(1);
        let worker = Worker::new(1);
        worker.running_jobs();
        worker.max_concurrency();

        let source = Arc::new(EmptySource {
            inventory_reads: AtomicUsize::new(0),
            version: AtomicU64::new(1),
        });
        let queue = AnalysisPriorityQueue::new(source);
        queue.initialize().expect("empty queue initializes");
        Refresher::new(Arc::clone(&queue), 1);
        let refresher = Refresher::new(queue, 1);
        refresher.running_jobs();
        refresher.is_queue_initialized();
        refresher.len();
        refresher.close();
    }

    struct TestJob {
        table_id: i64,
        started: Option<mpsc::Sender<i64>>,
        release: Option<mpsc::Receiver<()>>,
        panic: bool,
    }

    impl WorkerJob for TestJob {
        fn table_id(&self) -> i64 {
            self.table_id
        }

        fn analyze(self: Box<Self>) {
            if let Some(started) = self.started {
                let _ = started.send(self.table_id);
            }
            if let Some(release) = self.release {
                let _ = release.recv();
            }
            assert!(!self.panic, "simulated panic");
        }
    }

    #[test]
    fn source_worker_new_update_submit_capacity_and_running_copy() {
        let worker = Worker::new(2);
        assert_eq!(worker.max_concurrency(), 2);
        worker.update_concurrency(2);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_one_tx, release_one_rx) = mpsc::channel();
        let (release_two_tx, release_two_rx) = mpsc::channel();
        assert!(worker.submit_job(TestJob {
            table_id: 1,
            started: Some(started_tx.clone()),
            release: Some(release_one_rx),
            panic: false,
        }));
        assert!(worker.submit_job(TestJob {
            table_id: 2,
            started: Some(started_tx),
            release: Some(release_two_rx),
            panic: false,
        }));
        let mut started = [started_rx.recv().unwrap(), started_rx.recv().unwrap()];
        started.sort_unstable();
        assert_eq!(started, [1, 2]);
        assert_eq!(worker.running_jobs(), HashSet::from([1, 2]));
        assert!(!worker.submit_job(TestJob {
            table_id: 3,
            started: None,
            release: None,
            panic: false,
        }));
        release_one_tx.send(()).unwrap();
        release_two_tx.send(()).unwrap();
        worker.stop();
        assert!(worker.running_jobs().is_empty());
        worker.update_concurrency(10);
        assert_eq!(worker.max_concurrency(), 10);
    }

    #[test]
    fn source_worker_recovers_single_and_multiple_panics() {
        let worker = Worker::new(2);
        assert!(worker.submit_job(TestJob {
            table_id: 1,
            started: None,
            release: None,
            panic: true,
        }));
        assert!(worker.submit_job(TestJob {
            table_id: 2,
            started: None,
            release: None,
            panic: true,
        }));
        worker.stop();
        assert!(worker.running_jobs().is_empty());
    }

    #[test]
    fn source_worker_stop_waits_for_running_job() {
        let worker = Arc::new(Worker::new(1));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        assert!(worker.submit_job(TestJob {
            table_id: 1,
            started: Some(started_tx),
            release: Some(release_rx),
            panic: false,
        }));
        started_rx.recv().unwrap();
        let stopping = Arc::clone(&worker);
        let (stopped_tx, stopped_rx) = mpsc::channel();
        std::thread::spawn(move || {
            stopping.stop();
            stopped_tx.send(()).unwrap();
        });
        assert!(stopped_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release_tx.send(()).unwrap();
        stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn source_refresher_initializes_before_window_and_rebuilds_on_settings_change() {
        let source = Arc::new(EmptySource {
            inventory_reads: AtomicUsize::new(0),
            version: AtomicU64::new(1),
        });
        let queue = AnalysisPriorityQueue::new(source.clone());
        let mut refresher = Refresher::new(queue, 1);
        let mut outside = parameters();
        let now = Utc::now();
        let minute = (now.time() + chrono::Duration::minutes(2)).format("%H:%M");
        outside.start_time = format!("{minute} +0000");
        outside.end_time = outside.start_time.clone();
        assert!(!refresher.analyze_highest_priority_tables(&outside));
        assert!(refresher.is_queue_initialized());
        assert_eq!(source.inventory_reads.load(Ordering::SeqCst), 1);

        let mut changed = parameters();
        changed.auto_analyze_ratio = "0.2".to_owned();
        changed.prune_mode = PartitionPruneMode::Static;
        assert!(!refresher.analyze_highest_priority_tables(&changed));
        assert_eq!(source.inventory_reads.load(Ordering::SeqCst), 2);
        assert_eq!(refresher.len(), 0);
        refresher.close();
    }

    #[test]
    fn source_refresher_rejects_invalid_window_after_initializing() {
        let source = Arc::new(EmptySource {
            inventory_reads: AtomicUsize::new(0),
            version: AtomicU64::new(1),
        });
        let queue = AnalysisPriorityQueue::new(source);
        let mut refresher = Refresher::new(queue, 1);
        let mut invalid = parameters();
        invalid.start_time = "invalid".to_owned();
        assert!(!refresher.analyze_highest_priority_tables(&invalid));
        assert!(refresher.is_queue_initialized());
        refresher.close();
    }
}
