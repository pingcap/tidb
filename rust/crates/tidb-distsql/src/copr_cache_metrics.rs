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

//! Native counters for `pkg/store/copr/metrics/metrics.go`'s `evict`, `hit`,
//! and `miss` label values. The server metrics exporter can sample this
//! process-global source without coupling the DistSQL crate to Prometheus.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

static EVICT: AtomicU64 = AtomicU64::new(0);
static HIT: AtomicU64 = AtomicU64::new(0);
static MISS: AtomicU64 = AtomicU64::new(0);

/// One monotonic snapshot of the source cache counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CoprCacheMetricSnapshot {
    /// Entries removed by capacity eviction.
    pub evict: u64,
    /// TiKV match-version cache hits restored locally.
    pub hit: u64,
    /// Every non-hit TiKV response, admitted or not.
    pub miss: u64,
}

/// Reads all three monotonic counters.
#[must_use]
pub fn copr_cache_metric_snapshot() -> CoprCacheMetricSnapshot {
    CoprCacheMetricSnapshot {
        evict: EVICT.load(Relaxed),
        hit: HIT.load(Relaxed),
        miss: MISS.load(Relaxed),
    }
}

pub(crate) fn record_evict() {
    EVICT.fetch_add(1, Relaxed);
}

pub(crate) fn record_hit() {
    HIT.fetch_add(1, Relaxed);
}

pub(crate) fn record_miss() {
    MISS.fetch_add(1, Relaxed);
}
