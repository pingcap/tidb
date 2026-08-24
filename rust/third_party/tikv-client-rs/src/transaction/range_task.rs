// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Native range-task scheduling derived from client-go `txnkv/rangetask`.

//! A task is a contiguous group of region intersections. The producer keeps
//! discovery ordered, while up to `concurrency` handlers run at once. The
//! first handler error cancels the shared token and prevents queued work from
//! starting, matching the source worker pool's stop-on-error contract.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use log::info;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::task::JoinSet;

use crate::async_util::Cancellation;
use crate::pd::PdClient;
use crate::retry::RetryBackoffer;
use crate::transaction::Client;
use crate::BoundRange;
use crate::Result;

pub(crate) const DEFAULT_REGIONS_PER_TASK: usize = 128;
const LOCATE_REGION_MAX_BACKOFF_MS: u64 = 20_000;
const DEFAULT_STAT_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// The completed and failed region counts returned by one source task.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TaskStat {
    pub completed_regions: usize,
    pub failed_regions: usize,
}

/// Handles one contiguous, bounded range task. A handler must check the
/// cancellation token between physical requests, as client-go's worker does.
#[async_trait]
pub trait RangeTaskHandler: Clone + Send + Sync + 'static {
    async fn handle(
        &self,
        cancellation: Cancellation,
        range: (Vec<u8>, Vec<u8>),
    ) -> (TaskStat, Result<()>);
}

/// Source-compatible scheduler for operations performed across a key range.
pub struct Runner<PdC: PdClient, H: RangeTaskHandler> {
    name: String,
    identifier: String,
    pd_client: Arc<PdC>,
    handler: H,
    concurrency: usize,
    regions_per_task: usize,
    stat_log_interval: std::time::Duration,
    completed_regions: Arc<AtomicUsize>,
    failed_regions: Arc<AtomicUsize>,
}

impl<PdC: PdClient, H: RangeTaskHandler> Runner<PdC, H> {
    pub fn new(
        name: impl Into<String>,
        pd_client: Arc<PdC>,
        concurrency: usize,
        handler: H,
    ) -> Self {
        let name = name.into();
        Self::new_with_id(name.clone(), name, pd_client, concurrency, handler)
    }

    /// Source `NewRangeTaskRunnerWithID`: metrics are keyed by `name`, while
    /// human-readable progress logging can distinguish individual runners.
    pub fn new_with_id(
        name: impl Into<String>,
        identifier: impl Into<String>,
        pd_client: Arc<PdC>,
        concurrency: usize,
        handler: H,
    ) -> Self {
        let name = name.into();
        let identifier = identifier.into();
        let identifier = if identifier.is_empty() {
            name.clone()
        } else {
            identifier
        };
        assert!(concurrency > 0, "range task concurrency must be at least 1");
        Self {
            name,
            identifier,
            pd_client,
            handler,
            concurrency,
            regions_per_task: DEFAULT_REGIONS_PER_TASK,
            stat_log_interval: DEFAULT_STAT_LOG_INTERVAL,
            completed_regions: Arc::new(AtomicUsize::new(0)),
            failed_regions: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn set_regions_per_task(&mut self, regions_per_task: usize) {
        assert!(
            regions_per_task > 0,
            "range task regions_per_task must be at least 1"
        );
        self.regions_per_task = regions_per_task;
    }

    /// Changes the source periodic progress-log cadence. As in Go,
    /// non-positive intervals are rejected by ticker construction at run time.
    pub fn set_stat_log_interval(&mut self, interval: std::time::Duration) {
        self.stat_log_interval = interval;
    }

    pub fn completed_regions(&self) -> usize {
        self.completed_regions.load(Ordering::Acquire)
    }

    pub fn failed_regions(&self) -> usize {
        self.failed_regions.load(Ordering::Acquire)
    }

    pub async fn run_on_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<()> {
        crate::stats::reset_range_task_completed(&self.name);
        let _metric_reset = RangeTaskMetricReset {
            name: self.name.clone(),
        };
        self.completed_regions.store(0, Ordering::Release);
        if !end_key.is_empty() && start_key >= end_key {
            info!(
                "range task ignored empty range; name={}, start_key={}, end_key={}",
                self.identifier,
                crate::redact::key(&start_key),
                crate::redact::key(&end_key)
            );
            return Ok(());
        }
        info!(
            "range task started; name={}, start_key={}, end_key={}, concurrency={}",
            self.identifier,
            crate::redact::key(&start_key),
            crate::redact::key(&end_key),
            self.concurrency
        );

        let cancellation = Cancellation::default();
        // client-go has `concurrency` workers and a channel with the same
        // capacity. A worker removes its task before running its handler, so
        // discovery can run ahead of handlers by both active and queued work.
        let (sender, receiver) = mpsc::channel(self.concurrency);
        let receiver = Arc::new(AsyncMutex::new(receiver));
        // Wakes every idle worker and the blocked producer after a handler
        // error. The handler still receives `Cancellation`, which is its
        // source-compatible control token.
        let (stop, _) = watch::channel(false);
        let mut workers = JoinSet::new();
        for worker_index in 0..self.concurrency {
            let worker = Self::run_worker(
                self.name.clone(),
                self.handler.clone(),
                receiver.clone(),
                cancellation.clone(),
                stop.clone(),
                stop.subscribe(),
                self.completed_regions.clone(),
                self.failed_regions.clone(),
            );
            workers.spawn(async move { (worker_index, worker.await) });
        }

        let started_at = Instant::now();
        let mut progress_ticker = tokio::time::interval(self.stat_log_interval);
        // Tokio's first interval tick is immediate, unlike Go's `NewTicker`.
        progress_ticker.tick().await;
        let mut stop_producer = stop.subscribe();
        let mut next_key = start_key;
        let producer_result = loop {
            let load_key = next_key.clone().into();
            let mut backoffer = new_locate_region_backoffer(cancellation.clone());
            let loaded_regions = tokio::select! {
                loaded = self.pd_client.batch_load_regions_from_key(&load_key, self.regions_per_task, &mut backoffer) => loaded,
                _ = progress_ticker.tick() => {
                    info!(
                        "range task in progress; name={}, elapsed_ms={}, completed_regions={}",
                        self.identifier,
                        started_at.elapsed().as_millis(),
                        self.completed_regions()
                    );
                    continue;
                }
            };
            let regions = match loaded_regions {
                Ok(regions) => regions,
                Err(error) => break Err(error),
            };
            let mut task_end: Vec<u8> = regions
                .last()
                .expect("batch-loaded region list cannot be empty")
                .end_key()
                .into();
            let is_last = task_end.is_empty() || (!end_key.is_empty() && task_end >= end_key);
            if is_last {
                task_end = end_key.clone();
            }
            let task = (next_key.clone(), task_end.clone());
            let push_started = Instant::now();
            let sent = tokio::select! {
                result = sender.send(task) => Some(result),
                _ = stop_producer.changed() => None,
            };
            crate::stats::observe_range_task_push_duration(&self.name, push_started.elapsed());
            let Some(sent) = sent else {
                break Ok(());
            };
            if sent.is_err() {
                break Ok(());
            }
            if is_last {
                break Ok(());
            }
            next_key = task_end;
        };
        drop(sender);

        let mut worker_errors = std::iter::repeat_with(|| None)
            .take(self.concurrency)
            .collect::<Vec<_>>();
        while let Some(joined) = workers.join_next().await {
            match joined {
                Ok((worker_index, Ok(()))) => worker_errors[worker_index] = None,
                Ok((worker_index, Err(error))) => worker_errors[worker_index] = Some(error),
                Err(error) => return Err(error.into()),
            }
        }
        let worker_error = worker_errors.into_iter().flatten().next();
        match producer_result {
            Err(error) => {
                info!(
                    "range task failed loading regions; name={}, elapsed_ms={}, completed_regions={}, failed_regions={}, error={}",
                    self.identifier,
                    started_at.elapsed().as_millis(),
                    self.completed_regions(),
                    self.failed_regions(),
                    error
                );
                Err(error)
            }
            Ok(()) => match worker_error {
                Some(error) => {
                    info!(
                        "range task failed; name={}, elapsed_ms={}, completed_regions={}, failed_regions={}, error={}",
                        self.identifier,
                        started_at.elapsed().as_millis(),
                        self.completed_regions(),
                        self.failed_regions(),
                        error
                    );
                    Err(error)
                }
                None => {
                    info!(
                        "range task finished; name={}, elapsed_ms={}, completed_regions={}",
                        self.identifier,
                        started_at.elapsed().as_millis(),
                        self.completed_regions()
                    );
                    Ok(())
                }
            },
        }
    }

    async fn run_worker(
        name: String,
        handler: H,
        receiver: Arc<AsyncMutex<mpsc::Receiver<(Vec<u8>, Vec<u8>)>>>,
        cancellation: Cancellation,
        stop: watch::Sender<bool>,
        mut stop_receiver: watch::Receiver<bool>,
        completed_regions: Arc<AtomicUsize>,
        failed_regions: Arc<AtomicUsize>,
    ) -> Result<()> {
        loop {
            if *stop_receiver.borrow() {
                return Ok(());
            }
            let task = {
                let mut receiver = receiver.lock().await;
                tokio::select! {
                    task = receiver.recv() => task,
                    _ = stop_receiver.changed() => return Ok(()),
                }
            };
            let Some(task) = task else {
                return Ok(());
            };
            if *stop_receiver.borrow() {
                return Ok(());
            }
            let (stat, result) = handler.handle(cancellation.clone(), task.clone()).await;
            completed_regions.fetch_add(stat.completed_regions, Ordering::AcqRel);
            failed_regions.fetch_add(stat.failed_regions, Ordering::AcqRel);
            crate::stats::add_range_task_stats(&name, stat.completed_regions, stat.failed_regions);
            if let Err(error) = result {
                info!(
                    "range task worker cancelling after error; name={}, start_key={}, end_key={}, error={}",
                    name,
                    crate::redact::key(&task.0),
                    crate::redact::key(&task.1),
                    error
                );
                cancellation.cancel();
                stop.send_replace(true);
                return Err(error);
            }
        }
    }
}

struct RangeTaskMetricReset {
    name: String,
}

impl Drop for RangeTaskMetricReset {
    fn drop(&mut self) {
        crate::stats::reset_range_task_completed(&self.name);
    }
}

/// Creates the fresh 20-second cumulative backoffer used by every source
/// region-cache batch load.
pub fn new_locate_region_backoffer(cancellation: Cancellation) -> RetryBackoffer {
    RetryBackoffer::new(cancellation, LOCATE_REGION_MAX_BACKOFF_MS)
}

/// A reusable destructive range operation with source-compatible progress
/// accounting. `completed_regions` remains observable when execution fails.
pub struct DeleteRangeTask {
    client: Client,
    range: BoundRange,
    notify_only: bool,
    concurrency: usize,
    completed_regions: usize,
}

impl DeleteRangeTask {
    /// Creates a task that immediately removes every MVCC version in `range`.
    pub fn new(client: &Client, range: impl Into<BoundRange>, concurrency: usize) -> Self {
        Self {
            client: client.clone(),
            range: range.into(),
            notify_only: false,
            concurrency,
            completed_regions: 0,
        }
    }

    /// Creates a task that only replicates the range-deletion notification.
    pub fn new_notify(client: &Client, range: impl Into<BoundRange>, concurrency: usize) -> Self {
        Self {
            notify_only: true,
            ..Self::new(client, range, concurrency)
        }
    }

    /// Executes the task and updates the completed-region count even when a
    /// later region fails.
    pub async fn execute(&mut self) -> Result<()> {
        let (completed_regions, result) = self
            .client
            .run_delete_range_task_with_progress(
                self.range.clone(),
                self.concurrency,
                self.notify_only,
            )
            .await;
        self.completed_regions = completed_regions;
        result
    }

    /// Returns the number of regions successfully processed by the last run.
    pub fn completed_regions(&self) -> usize {
        self.completed_regions
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::mock::MockPdClient;
    use crate::proto::metapb;
    use crate::region::RegionWithLeader;
    use crate::Error;

    #[derive(Clone, Default)]
    struct RecordingHandler {
        ranges: Arc<Mutex<Vec<(Vec<u8>, Vec<u8>)>>>,
        fail_first: bool,
    }

    #[derive(Clone, Default)]
    struct ConcurrentHandler {
        active: Arc<AtomicUsize>,
        peak_active: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl RangeTaskHandler for ConcurrentHandler {
        async fn handle(
            &self,
            _cancellation: Cancellation,
            _range: (Vec<u8>, Vec<u8>),
        ) -> (TaskStat, Result<()>) {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak_active.fetch_max(active, Ordering::AcqRel);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.active.fetch_sub(1, Ordering::AcqRel);
            (
                TaskStat {
                    completed_regions: 1,
                    ..Default::default()
                },
                Ok(()),
            )
        }
    }

    #[async_trait]
    impl RangeTaskHandler for RecordingHandler {
        async fn handle(
            &self,
            cancellation: Cancellation,
            range: (Vec<u8>, Vec<u8>),
        ) -> (TaskStat, Result<()>) {
            assert!(!cancellation.is_cancelled());
            let mut ranges = self.ranges.lock().unwrap();
            let first = ranges.is_empty();
            ranges.push(range);
            drop(ranges);
            if self.fail_first && first {
                (
                    TaskStat {
                        failed_regions: 1,
                        ..Default::default()
                    },
                    Err(Error::StringError("source task failure".to_owned())),
                )
            } else {
                (
                    TaskStat {
                        completed_regions: 1,
                        ..Default::default()
                    },
                    Ok(()),
                )
            }
        }
    }

    fn key_range(start: &str, end: &str) -> (Vec<u8>, Vec<u8>) {
        (start.as_bytes().to_vec(), end.as_bytes().to_vec())
    }

    fn alphabet_regions() -> Vec<RegionWithLeader> {
        let mut boundaries = vec![Vec::new()];
        boundaries.extend((b'a'..=b'z').map(|key| vec![key]));
        boundaries.push(Vec::new());
        boundaries
            .windows(2)
            .enumerate()
            .map(|(index, bounds)| {
                let leader = metapb::Peer {
                    id: index as u64 + 1,
                    store_id: index as u64 + 101,
                    ..Default::default()
                };
                RegionWithLeader::new(
                    metapb::Region {
                        id: index as u64 + 1,
                        start_key: bounds[0].clone(),
                        end_key: bounds[1].clone(),
                        peers: vec![leader.clone()],
                        ..Default::default()
                    },
                    Some(leader),
                )
            })
            .collect()
    }

    fn source_range_table() -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<(Vec<u8>, Vec<u8>)>>) {
        let mut all_ranges = vec![key_range("", "a")];
        all_ranges.extend((b'a'..b'z').map(|key| {
            (
                vec![key],
                vec![key.checked_add(1).expect("alphabet boundary")],
            )
        }));
        all_ranges.push(key_range("z", ""));

        let inputs = vec![
            key_range("", ""),
            key_range("", "b"),
            key_range("b", ""),
            key_range("b", "x"),
            key_range("a", "d"),
            key_range("a\0", "d\0"),
            (vec![b'a', 0xff, 0xff, 0xff], vec![b'c', 0xff, 0xff, 0xff]),
            key_range("a1", "a2"),
            key_range("a", "a"),
            key_range("a3", "a3"),
        ];
        let expected = vec![
            all_ranges.clone(),
            all_ranges[..2].to_vec(),
            all_ranges[2..].to_vec(),
            all_ranges[2..24].to_vec(),
            vec![
                key_range("a", "b"),
                key_range("b", "c"),
                key_range("c", "d"),
            ],
            vec![
                key_range("a\0", "b"),
                key_range("b", "c"),
                key_range("c", "d"),
                key_range("d", "d\0"),
            ],
            vec![
                (vec![b'a', 0xff, 0xff, 0xff], vec![b'b']),
                key_range("b", "c"),
                (vec![b'c'], vec![b'c', 0xff, 0xff, 0xff]),
            ],
            vec![key_range("a1", "a2")],
            vec![],
            vec![],
        ];
        (inputs, expected)
    }

    fn batch_ranges(ranges: &[(Vec<u8>, Vec<u8>)], batch_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        ranges
            .chunks(batch_size)
            .map(|chunk| (chunk[0].0.clone(), chunk.last().unwrap().1.clone()))
            .collect()
    }

    #[tokio::test]
    async fn source_runner_groups_region_intersections_and_counts_results() {
        let handler = RecordingHandler::default();
        let ranges = handler.ranges.clone();
        let mut runner = Runner::new(
            "range-task-grouping",
            Arc::new(MockPdClient::default()),
            1,
            handler,
        );
        runner.set_regions_per_task(1);
        runner.run_on_range(vec![], vec![250, 250]).await.unwrap();

        assert_eq!(
            *ranges.lock().unwrap(),
            vec![(vec![], vec![10]), (vec![10], vec![250, 250])]
        );
        assert_eq!(runner.completed_regions(), 2);
        assert_eq!(runner.failed_regions(), 0);
        assert_eq!(
            crate::stats::range_task_push_duration_samples("range-task-grouping"),
            2
        );
    }

    #[test]
    fn source_runner_keeps_a_distinct_log_identifier_and_interval() {
        let mut runner = Runner::new_with_id(
            "range-task-metric-name",
            "range-task-log-id",
            Arc::new(MockPdClient::default()),
            1,
            RecordingHandler::default(),
        );
        runner.set_stat_log_interval(Duration::from_secs(7));
        assert_eq!(runner.name, "range-task-metric-name");
        assert_eq!(runner.identifier, "range-task-log-id");
        assert_eq!(runner.stat_log_interval, Duration::from_secs(7));

        let fallback = Runner::new_with_id(
            String::from("range-task-fallback"),
            "",
            Arc::new(MockPdClient::default()),
            1,
            RecordingHandler::default(),
        );
        assert_eq!(fallback.name, "range-task-fallback");
        assert_eq!(fallback.identifier, "range-task-fallback");
    }

    #[tokio::test]
    async fn source_runner_stops_after_the_first_task_error() {
        let handler = RecordingHandler {
            fail_first: true,
            ..Default::default()
        };
        let ranges = handler.ranges.clone();
        let mut runner = Runner::new(
            "range-task-stop",
            Arc::new(MockPdClient::default()),
            1,
            handler,
        );
        runner.set_regions_per_task(1);
        let error = runner
            .run_on_range(vec![], vec![250, 250])
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "source task failure");
        assert_eq!(*ranges.lock().unwrap(), vec![(vec![], vec![10])]);
        assert_eq!(runner.completed_regions(), 0);
        assert_eq!(runner.failed_regions(), 1);
        assert_eq!(
            crate::stats::range_task_stat("range-task-stop", "failed-regions"),
            1.0
        );
    }

    #[tokio::test]
    async fn source_runner_resets_completed_but_retains_failed_between_runs() {
        let handler = RecordingHandler {
            fail_first: true,
            ..Default::default()
        };
        let mut runner = Runner::new(
            "range-task-reuse",
            Arc::new(MockPdClient::default()),
            1,
            handler,
        );
        runner.set_regions_per_task(1);

        runner
            .run_on_range(vec![], vec![250, 250])
            .await
            .unwrap_err();
        assert_eq!(runner.completed_regions(), 0);
        assert_eq!(runner.failed_regions(), 1);

        runner.run_on_range(vec![], vec![250, 250]).await.unwrap();
        assert_eq!(runner.completed_regions(), 2);
        assert_eq!(runner.failed_regions(), 1);
    }

    #[test]
    fn source_locate_backoffer_uses_the_package_budget() {
        let backoffer = new_locate_region_backoffer(Cancellation::default());
        assert_eq!(
            backoffer.max_sleep_ms(),
            LOCATE_REGION_MAX_BACKOFF_MS * (backoffer.variables().backoff_weight.max(1) as u64)
        );
    }

    #[tokio::test]
    async fn source_runner_bounds_concurrent_task_handlers() {
        let handler = ConcurrentHandler::default();
        let peak_active = handler.peak_active.clone();
        let mut runner = Runner::new(
            "range-task-concurrency",
            Arc::new(MockPdClient::default()),
            2,
            handler,
        );
        runner.set_regions_per_task(1);
        runner.run_on_range(vec![], vec![250, 250]).await.unwrap();

        assert_eq!(peak_active.load(Ordering::Acquire), 2);
        assert_eq!(runner.completed_regions(), 2);
        assert_eq!(
            crate::stats::range_task_stat("range-task-concurrency", "completed-regions"),
            0.0
        );
    }

    #[tokio::test]
    async fn original_integration_range_and_batch_matrix() {
        let (inputs, expected) = source_range_table();
        for concurrency in 1..5 {
            let handler = RecordingHandler::default();
            let ranges = handler.ranges.clone();
            let mut runner = Runner::new(
                format!("range-task-table-{concurrency}"),
                Arc::new(MockPdClient::with_regions(alphabet_regions())),
                concurrency,
                handler,
            );
            for regions_per_task in 1..=5 {
                runner.set_regions_per_task(regions_per_task);
                for (input, expected) in inputs.iter().zip(&expected) {
                    runner
                        .run_on_range(input.0.clone(), input.1.clone())
                        .await
                        .unwrap();
                    let mut obtained = std::mem::take(&mut *ranges.lock().unwrap());
                    obtained.sort_by(|left, right| left.0.cmp(&right.0));
                    let expected = batch_ranges(expected, regions_per_task);
                    assert_eq!(obtained, expected);
                    assert_eq!(runner.completed_regions(), expected.len());
                    assert_eq!(runner.failed_regions(), 0);
                }
            }
        }
    }

    #[tokio::test]
    async fn original_integration_error_matrix() {
        let (inputs, expected) = source_range_table();
        for concurrency in 1..5 {
            for (input, subranges) in inputs.iter().zip(&expected) {
                for failed_range in subranges {
                    let handler = ErrorAtHandler {
                        failed_start: failed_range.0.clone(),
                    };
                    let mut runner = Runner::new(
                        format!("range-task-error-{concurrency}"),
                        Arc::new(MockPdClient::with_regions(alphabet_regions())),
                        concurrency,
                        handler,
                    );
                    runner.set_regions_per_task(1);
                    runner
                        .run_on_range(input.0.clone(), input.1.clone())
                        .await
                        .unwrap_err();
                    assert!(runner.completed_regions() < subranges.len());
                    assert_eq!(runner.failed_regions(), 1);
                }
            }
        }
    }

    #[derive(Clone)]
    struct ErrorAtHandler {
        failed_start: Vec<u8>,
    }

    #[async_trait]
    impl RangeTaskHandler for ErrorAtHandler {
        async fn handle(
            &self,
            _cancellation: Cancellation,
            range: (Vec<u8>, Vec<u8>),
        ) -> (TaskStat, Result<()>) {
            if range.0 == self.failed_start {
                (
                    TaskStat {
                        failed_regions: 1,
                        ..Default::default()
                    },
                    Err(Error::StringError("test error".to_owned())),
                )
            } else {
                (
                    TaskStat {
                        completed_regions: 1,
                        ..Default::default()
                    },
                    Ok(()),
                )
            }
        }
    }
}
