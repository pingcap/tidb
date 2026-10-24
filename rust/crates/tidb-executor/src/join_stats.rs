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

use crate::execdetails::{format_duration, RuntimeStats, TP_INDEX_LOOK_UP_JOIN_RUNTIME_STATS};

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
            buf.push_str(", probe:");
            buf.push_str(&fmt(self.probe));
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
}
