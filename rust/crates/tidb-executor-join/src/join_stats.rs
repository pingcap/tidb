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

//! Go `pkg/executor/join/index_lookup_join.go`'s
//! `indexLookUpJoinRuntimeStats` and `innerWorkerRuntimeStats`: what an index
//! join registers on its plan when it closes (`inner:{total, concurrency,
//! task, construct, fetch, build}, probe:..`).

use std::any::Any;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use tidb_execdetails::{format_duration, RuntimeStats, TP_INDEX_LOOK_UP_JOIN_RUNTIME_STATS};

/// Go `innerWorkerRuntimeStats`, updated by the inner workers.
#[derive(Debug, Default)]
pub struct InnerWorkerRuntimeStats {
    total_time: AtomicI64,
    task: AtomicI64,
    construct: AtomicI64,
    fetch: AtomicI64,
    build: AtomicI64,
    join: AtomicI64,
}

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// Go `indexLookUpJoinRuntimeStats` while the join runs: workers update it
/// through shared references, [`Self::snapshot`] is what Close registers.
#[derive(Debug, Default)]
pub struct IndexLookUpJoinRuntimeStats {
    concurrency: usize,
    probe: AtomicI64,
    inner_worker: InnerWorkerRuntimeStats,
}

impl IndexLookUpJoinRuntimeStats {
    /// Go `e.stats = &indexLookUpJoinRuntimeStats{}; e.stats.concurrency = n`.
    #[must_use]
    pub fn new(concurrency: usize) -> Self {
        Self {
            concurrency,
            ..Self::default()
        }
    }

    /// Go `constructLookupContent`'s deferred `task++` and `construct`.
    pub fn record_construct(&self, elapsed: Duration) {
        self.inner_worker.task.fetch_add(1, Ordering::Relaxed);
        self.inner_worker
            .construct
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
        self.inner_worker
            .total_time
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    /// Go `fetchInnerResults`' deferred `fetch`.
    pub fn record_fetch(&self, elapsed: Duration) {
        self.inner_worker
            .fetch
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
        self.inner_worker
            .total_time
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    /// Go `buildLookUpMap`'s deferred `build`.
    pub fn record_build(&self, elapsed: Duration) {
        self.inner_worker
            .build
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
        self.inner_worker
            .total_time
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    /// Go `IndexNestedLoopHashJoin`'s `join`, the inner workers' join time.
    pub fn record_join(&self, elapsed: Duration) {
        self.inner_worker
            .join
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    /// Go `IndexLookUpJoin.Next`'s `probe`.
    pub fn record_probe(&self, elapsed: Duration) {
        self.probe.fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    /// The stats as Close registers them.
    #[must_use]
    pub fn snapshot(&self) -> IndexLookUpJoinRuntimeStatsSnapshot {
        IndexLookUpJoinRuntimeStatsSnapshot {
            concurrency: self.concurrency,
            probe: self.probe.load(Ordering::Relaxed),
            total_time: self.inner_worker.total_time.load(Ordering::Relaxed),
            task: self.inner_worker.task.load(Ordering::Relaxed),
            construct: self.inner_worker.construct.load(Ordering::Relaxed),
            fetch: self.inner_worker.fetch.load(Ordering::Relaxed),
            build: self.inner_worker.build.load(Ordering::Relaxed),
            join: self.inner_worker.join.load(Ordering::Relaxed),
            adaptive_limit_snapshot: None,
        }
    }
}

/// Go `indexLookUpJoinRuntimeStats` as registered.
#[derive(Clone, Debug, Default)]
pub struct IndexLookUpJoinRuntimeStatsSnapshot {
    concurrency: usize,
    probe: i64,
    total_time: i64,
    task: i64,
    construct: i64,
    fetch: i64,
    build: i64,
    join: i64,
    /// Go `adaptiveLimitSnapshot`: the close-time diagnostic snapshot
    /// rendered by EXPLAIN ANALYZE. Runtime-stat merges retain one copy
    /// instead of adding it.
    pub(crate) adaptive_limit_snapshot:
        Option<tidb_executor_internal_exec::adaptive_limit_controller::AdaptiveLimitSnapshot>,
}

fn fmt(nanos: i64) -> String {
    format_duration(Duration::from_nanos(u64::try_from(nanos).unwrap_or(0)))
}

impl RuntimeStats for IndexLookUpJoinRuntimeStatsSnapshot {
    /// Go `indexLookUpJoinRuntimeStats.String`.
    fn string(&self) -> String {
        let mut buf = String::with_capacity(16);
        if self.total_time > 0 {
            buf.push_str("inner:{total:");
            buf.push_str(&fmt(self.total_time));
            buf.push_str(", concurrency:");
            if self.concurrency > 0 {
                buf.push_str(&self.concurrency.to_string());
            } else {
                buf.push_str("OFF");
            }
            buf.push_str(", task:");
            buf.push_str(&self.task.to_string());
            buf.push_str(", construct:");
            buf.push_str(&fmt(self.construct));
            buf.push_str(", fetch:");
            buf.push_str(&fmt(self.fetch));
            buf.push_str(", build:");
            buf.push_str(&fmt(self.build));
            if self.join > 0 {
                buf.push_str(", join:");
                buf.push_str(&fmt(self.join));
            }
            buf.push('}');
        }
        if self.probe > 0 {
            if !buf.is_empty() {
                buf.push_str(", ");
            }
            buf.push_str("probe:");
            buf.push_str(&fmt(self.probe));
        }
        if let Some(snapshot) = &self.adaptive_limit_snapshot {
            if !buf.is_empty() {
                buf.push_str(", ");
            }
            buf.push_str("adaptive:{outer:");
            buf.push_str(&format!("{}/{}", snapshot.outer_fetched, snapshot.outer_consumed));
            if snapshot.has_lookup_stage {
                buf.push_str(&format!(", lookup:{}/{}", snapshot.lookup_handles, snapshot.lookup_rows));
            }
            buf.push_str(", outstanding:");
            buf.push_str(&snapshot.outer_outstanding_at_stop.to_string());
            if snapshot.has_lookup_stage {
                buf.push('/');
                buf.push_str(&snapshot.lookup_outstanding_at_stop.to_string());
                buf.push_str(", blocked:outer=");
                buf.push_str(&format_duration(snapshot.outer_admission_blocked));
                buf.push_str(",lookup=");
                buf.push_str(&format_duration(snapshot.lookup_admission_blocked));
            } else {
                buf.push_str(", blocked:");
                buf.push_str(&format_duration(snapshot.outer_admission_blocked));
            }
            buf.push('}');
        }
        buf
    }

    /// Go `indexLookUpJoinRuntimeStats.Merge`.
    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other
            .as_any()
            .downcast_ref::<IndexLookUpJoinRuntimeStatsSnapshot>()
        else {
            return;
        };
        self.probe += tmp.probe;
        self.total_time += tmp.total_time;
        self.task += tmp.task;
        self.construct += tmp.construct;
        self.fetch += tmp.fetch;
        self.build += tmp.build;
        self.join += tmp.join;
        // The snapshot describes one executor lifecycle, so retain the first
        // copy instead of accumulating the same statement-local counters.
        if self.adaptive_limit_snapshot.is_none() {
            self.adaptive_limit_snapshot = tmp.adaptive_limit_snapshot;
        }
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_INDEX_LOOK_UP_JOIN_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's `inner:{total:1.14ms, concurrency:5, task:1, construct:15.5µs,
    /// fetch:1.12ms, build:5.63µs}, probe:18.1µs`.
    #[test]
    fn string_matches_gos_keys() {
        let stats = IndexLookUpJoinRuntimeStats::new(5);
        stats.record_construct(Duration::from_nanos(15_500));
        stats.record_fetch(Duration::from_micros(1_120));
        stats.record_build(Duration::from_nanos(5_630));
        stats.record_probe(Duration::from_nanos(18_100));
        assert_eq!(
            stats.snapshot().string(),
            "inner:{total:1.14ms, concurrency:5, task:1, construct:15.5µs, fetch:1.12ms, \
             build:5.63µs}, probe:18.1µs"
        );
        assert_eq!(IndexLookUpJoinRuntimeStats::new(0).snapshot().string(), "");
    }

    /// Go `TestIndexJoinRuntimeStats`' adaptive LIMIT half (0b505ecc58b):
    /// the snapshot renders after the probe, survives a clone and a merge as
    /// one copy, and a probe-only stats has no leading separator.
    #[test]
    fn test_index_join_runtime_stats_adaptive_snapshot() {
        let stats = IndexLookUpJoinRuntimeStats::new(5);
        stats.record_probe(Duration::from_secs(2));
        let mut stats = stats.snapshot();
        assert_eq!(stats.string(), "probe:2s");
        stats.adaptive_limit_snapshot = Some(tidb_executor_internal_exec::adaptive_limit_controller::AdaptiveLimitSnapshot {
            has_lookup_stage: true,
            output_rows: 1000,
            outer_fetched: 1413,
            outer_consumed: 1000,
            outer_outstanding_at_stop: 413,
            lookup_handles: 1256,
            lookup_rows: 1000,
            lookup_outstanding_at_stop: 256,
            stopped: true,
            ..Default::default()
        });
        let adaptive = "adaptive:{outer:1413/1000, lookup:1256/1000, outstanding:413/256, blocked:outer=0s,lookup=0s}";
        let cloned = stats.clone_box();
        assert!(cloned.string().contains(adaptive), "{}", cloned.string());
        let mut merged = IndexLookUpJoinRuntimeStatsSnapshot::default();
        merged.merge(cloned.as_ref());
        assert!(merged.string().contains(adaptive), "{}", merged.string());
        merged.merge(&stats);
        assert!(merged.string().contains("outer:1413/1000"), "{}", merged.string());
        let mut outer_only = IndexLookUpJoinRuntimeStatsSnapshot::default();
        outer_only.adaptive_limit_snapshot = Some(tidb_executor_internal_exec::adaptive_limit_controller::AdaptiveLimitSnapshot {
            outer_fetched: 3,
            outer_consumed: 3,
            ..Default::default()
        });
        assert_eq!(outer_only.string(), "adaptive:{outer:3/3, outstanding:0, blocked:0s}");
    }
}
