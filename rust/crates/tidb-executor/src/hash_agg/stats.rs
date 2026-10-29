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

//! Go `pkg/executor/aggregate/agg_util.go`'s `HashAggRuntimeStats` and
//! `AggWorkerStat`: what a parallel hash aggregation registers on its plan
//! when it closes (`partial_worker:{...}, final_worker:{...}`).

use std::any::Any;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_model::go_duration::format_go_duration;

use tidb_util::execdetails::{RuntimeStats, TP_HASH_AGG_RUNTIME_STAT};

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// Go `AggWorkerStat` of one running worker.
#[derive(Debug, Default)]
pub struct AggWorkerStat {
    task_num: AtomicI64,
    wait_time: AtomicI64,
    exec_time: AtomicI64,
    worker_time: AtomicI64,
}

impl AggWorkerStat {
    /// Go `TaskNum++`.
    pub fn add_task(&self) {
        self.task_num.fetch_add(1, Ordering::Relaxed);
    }

    /// Go `updateWaitTime`.
    pub fn add_wait(&self, since: Instant) {
        self.wait_time
            .fetch_add(nanos(since.elapsed()), Ordering::Relaxed);
    }

    /// Go `updateExecTime`.
    pub fn add_exec(&self, since: Instant) {
        self.exec_time
            .fetch_add(nanos(since.elapsed()), Ordering::Relaxed);
    }

    /// Go `updateWorkerTime`.
    pub fn add_worker_time(&self, since: Instant) {
        self.worker_time
            .fetch_add(nanos(since.elapsed()), Ordering::Relaxed);
    }

    fn snapshot(&self) -> AggWorkerStatSnapshot {
        AggWorkerStatSnapshot {
            task_num: self.task_num.load(Ordering::Relaxed),
            wait_time: self.wait_time.load(Ordering::Relaxed),
            exec_time: self.exec_time.load(Ordering::Relaxed),
            worker_time: self.worker_time.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AggWorkerStatSnapshot {
    task_num: i64,
    wait_time: i64,
    exec_time: i64,
    worker_time: i64,
}

/// Go `HashAggRuntimeStats` while the aggregation runs.
#[derive(Debug, Default)]
pub struct HashAggRuntimeStats {
    partial_concurrency: usize,
    partial_wall_time: AtomicI64,
    final_concurrency: usize,
    final_wall_time: AtomicI64,
    partial_stats: Mutex<Vec<Arc<AggWorkerStat>>>,
    final_stats: Mutex<Vec<Arc<AggWorkerStat>>>,
}

impl HashAggRuntimeStats {
    /// Go `initRuntimeStats`.
    #[must_use]
    pub fn new(partial_concurrency: usize, final_concurrency: usize) -> Self {
        Self {
            partial_concurrency,
            final_concurrency,
            ..Self::default()
        }
    }

    /// Go `e.stats.PartialStats = append(..., w.stats)` for a new worker.
    #[must_use]
    pub fn partial_worker(&self) -> Arc<AggWorkerStat> {
        let stat = Arc::new(AggWorkerStat::default());
        self.partial_stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::clone(&stat));
        stat
    }

    /// Go `e.stats.FinalStats = append(..., w.stats)` for a new worker.
    #[must_use]
    pub fn final_worker(&self) -> Arc<AggWorkerStat> {
        let stat = Arc::new(AggWorkerStat::default());
        self.final_stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::clone(&stat));
        stat
    }

    /// Go `atomic.AddInt64(partialWallTimePtr, time.Since(partialStart))`.
    pub fn add_partial_wall_time(&self, since: Instant) {
        self.partial_wall_time
            .fetch_add(nanos(since.elapsed()), Ordering::Relaxed);
    }

    /// Go `atomic.AddInt64(finalWallTimePtr, time.Since(finalStart))`.
    pub fn add_final_wall_time(&self, since: Instant) {
        self.final_wall_time
            .fetch_add(nanos(since.elapsed()), Ordering::Relaxed);
    }

    /// The stats as Close registers them.
    #[must_use]
    pub fn snapshot(&self) -> HashAggRuntimeStatsSnapshot {
        let collect = |stats: &Mutex<Vec<Arc<AggWorkerStat>>>| {
            stats
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|stat| stat.snapshot())
                .collect()
        };
        HashAggRuntimeStatsSnapshot {
            partial_concurrency: self.partial_concurrency,
            partial_wall_time: self.partial_wall_time.load(Ordering::Relaxed),
            final_concurrency: self.final_concurrency,
            final_wall_time: self.final_wall_time.load(Ordering::Relaxed),
            partial_stats: collect(&self.partial_stats),
            final_stats: collect(&self.final_stats),
        }
    }
}

/// Go `HashAggRuntimeStats` as registered.
#[derive(Clone, Debug, Default)]
pub struct HashAggRuntimeStatsSnapshot {
    partial_concurrency: usize,
    partial_wall_time: i64,
    final_concurrency: usize,
    final_wall_time: i64,
    partial_stats: Vec<AggWorkerStatSnapshot>,
    final_stats: Vec<AggWorkerStatSnapshot>,
}

/// Go `HashAggRuntimeStats.workerString` (Go `time.Duration` `%s`).
fn worker_string(
    buf: &mut String,
    prefix: &str,
    concurrency: usize,
    wall_time: i64,
    worker_stats: &[AggWorkerStatSnapshot],
) {
    let (mut total_time, mut total_wait, mut total_exec, mut total_task_num) = (0, 0, 0, 0);
    for worker in worker_stats {
        total_time += worker.worker_time;
        total_wait += worker.wait_time;
        total_exec += worker.exec_time;
        total_task_num += worker.task_num;
    }
    buf.push_str(prefix);
    buf.push_str(&format!(
        "_worker:{{wall_time:{}, concurrency:{concurrency}, task_num:{total_task_num}, \
         tot_wait:{}, tot_exec:{}, tot_time:{}",
        format_go_duration(wall_time),
        format_go_duration(total_wait),
        format_go_duration(total_exec),
        format_go_duration(total_time),
    ));
    let n = worker_stats.len();
    if n > 0 {
        let mut sorted = worker_stats.to_vec();
        sorted.sort_by_key(|worker| worker.worker_time);
        buf.push_str(&format!(
            ", max:{}, p95:{}",
            format_go_duration(sorted[n - 1].worker_time),
            format_go_duration(sorted[n * 19 / 20].worker_time),
        ));
    }
    buf.push('}');
}

impl RuntimeStats for HashAggRuntimeStatsSnapshot {
    /// Go `HashAggRuntimeStats.String`.
    fn string(&self) -> String {
        let mut buf = String::with_capacity(64);
        worker_string(
            &mut buf,
            "partial",
            self.partial_concurrency,
            self.partial_wall_time,
            &self.partial_stats,
        );
        buf.push_str(", ");
        worker_string(
            &mut buf,
            "final",
            self.final_concurrency,
            self.final_wall_time,
            &self.final_stats,
        );
        buf
    }

    /// Go `HashAggRuntimeStats.Merge`.
    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<HashAggRuntimeStatsSnapshot>() else {
            return;
        };
        self.partial_wall_time += tmp.partial_wall_time;
        self.final_wall_time += tmp.final_wall_time;
        self.partial_stats.extend_from_slice(&tmp.partial_stats);
        self.final_stats.extend_from_slice(&tmp.final_stats);
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_HASH_AGG_RUNTIME_STAT
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's `partial_worker:{wall_time:433.25µs, concurrency:5, task_num:1,
    /// tot_wait:408µs, tot_exec:11.375µs, tot_time:2.123916ms,
    /// max:425.958µs, p95:425.958µs}, final_worker:{...}`: Go's duration
    /// spelling, the maximum and the 95th percentile worker time.
    #[test]
    fn string_matches_gos_keys() {
        let snapshot = HashAggRuntimeStatsSnapshot {
            partial_concurrency: 5,
            partial_wall_time: 433_250,
            final_concurrency: 5,
            final_wall_time: 0,
            partial_stats: vec![
                AggWorkerStatSnapshot {
                    task_num: 1,
                    wait_time: 408_000,
                    exec_time: 11_375,
                    worker_time: 425_958,
                },
                AggWorkerStatSnapshot {
                    worker_time: 1_697_958,
                    ..AggWorkerStatSnapshot::default()
                },
            ],
            final_stats: Vec::new(),
        };
        assert_eq!(
            snapshot.string(),
            "partial_worker:{wall_time:433.25µs, concurrency:5, task_num:1, tot_wait:408µs, \
             tot_exec:11.375µs, tot_time:2.123916ms, max:1.697958ms, p95:1.697958ms}, \
             final_worker:{wall_time:0s, concurrency:5, task_num:0, tot_wait:0s, tot_exec:0s, \
             tot_time:0s}"
        );
    }
}
