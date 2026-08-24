// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Native range-task scheduling derived from client-go `txnkv/rangetask`.

//! A task is a contiguous group of region intersections. The producer keeps
//! discovery ordered, while up to `concurrency` handlers run at once. The
//! first handler error cancels the shared token and drops pending futures,
//! matching the source worker pool's stop-on-error contract.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::async_util::Cancellation;
use crate::pd::PdClient;
use crate::store::region_stream_for_range;
use crate::Result;

pub(crate) const DEFAULT_REGIONS_PER_TASK: usize = 128;

/// The completed and failed region counts returned by one source task.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TaskStat {
    pub(crate) completed_regions: usize,
    pub(crate) failed_regions: usize,
}

/// Handles one contiguous, bounded range task. A handler must check the
/// cancellation token between physical requests, as client-go's worker does.
#[async_trait]
pub(crate) trait RangeTaskHandler: Clone + Send + Sync + 'static {
    async fn handle(
        &self,
        cancellation: Cancellation,
        range: (Vec<u8>, Vec<u8>),
    ) -> (TaskStat, Result<()>);
}

/// Source-compatible scheduler for operations performed across a key range.
pub(crate) struct Runner<PdC: PdClient, H: RangeTaskHandler> {
    name: &'static str,
    pd_client: Arc<PdC>,
    handler: H,
    concurrency: usize,
    regions_per_task: usize,
    completed_regions: AtomicUsize,
    failed_regions: AtomicUsize,
}

impl<PdC: PdClient, H: RangeTaskHandler> Runner<PdC, H> {
    pub(crate) fn new(
        name: &'static str,
        pd_client: Arc<PdC>,
        concurrency: usize,
        handler: H,
    ) -> Self {
        assert!(concurrency > 0, "range task concurrency must be at least 1");
        Self {
            name,
            pd_client,
            handler,
            concurrency,
            regions_per_task: DEFAULT_REGIONS_PER_TASK,
            completed_regions: AtomicUsize::new(0),
            failed_regions: AtomicUsize::new(0),
        }
    }

    pub(crate) fn set_regions_per_task(&mut self, regions_per_task: usize) {
        assert!(
            regions_per_task > 0,
            "range task regions_per_task must be at least 1"
        );
        self.regions_per_task = regions_per_task;
    }

    pub(crate) fn completed_regions(&self) -> usize {
        self.completed_regions.load(Ordering::Acquire)
    }

    pub(crate) fn failed_regions(&self) -> usize {
        self.failed_regions.load(Ordering::Acquire)
    }

    pub(crate) async fn run_on_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<()> {
        crate::stats::reset_range_task_completed(self.name);
        let _metric_reset = RangeTaskMetricReset { name: self.name };
        self.completed_regions.store(0, Ordering::Release);
        self.failed_regions.store(0, Ordering::Release);
        if !end_key.is_empty() && start_key >= end_key {
            return Ok(());
        }

        let cancellation = Cancellation::default();
        let task_cancellation = cancellation.clone();
        let handler = self.handler.clone();
        let tasks = region_stream_for_range((start_key, end_key), self.pd_client.clone())
            .map(|result| result.map(|(range, _)| range))
            .chunks(self.regions_per_task)
            .map(move |ranges| {
                let handler = handler.clone();
                let cancellation = task_cancellation.clone();
                async move {
                    let ranges = ranges.into_iter().collect::<Result<Vec<_>>>()?;
                    let first = ranges.first().expect("region task chunk cannot be empty");
                    let last = ranges.last().expect("region task chunk cannot be empty");
                    Ok::<_, crate::Error>(
                        handler
                            .handle(cancellation, (first.0.clone(), last.1.clone()))
                            .await,
                    )
                }
            })
            .buffer_unordered(self.concurrency);
        futures::pin_mut!(tasks);

        while let Some(outcome) = tasks.next().await {
            let (stat, result) = outcome?;
            self.completed_regions
                .fetch_add(stat.completed_regions, Ordering::AcqRel);
            self.failed_regions
                .fetch_add(stat.failed_regions, Ordering::AcqRel);
            crate::stats::add_range_task_stats(
                self.name,
                stat.completed_regions,
                stat.failed_regions,
            );
            if let Err(error) = result {
                cancellation.cancel();
                return Err(error);
            }
        }
        Ok(())
    }
}

struct RangeTaskMetricReset {
    name: &'static str,
}

impl Drop for RangeTaskMetricReset {
    fn drop(&mut self) {
        crate::stats::reset_range_task_completed(self.name);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::mock::MockPdClient;
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
}
