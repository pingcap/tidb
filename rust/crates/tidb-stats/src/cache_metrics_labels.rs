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

//! Statistics-cache metric labels from
//! `pkg/statistics/handle/cache/metrics/metrics.go`.
//!
//! The Go initializer binds six cache counters and two gauges to these exact
//! label values in order. This leaf owns only the deterministic label contract;
//! Prometheus registration/handles, metric updates, cache behavior, and runtime
//! configuration remain external.

/// Counter labels bound by `InitMetricsVars`, in source order.
pub const STATS_CACHE_COUNTER_LABELS: [&str; 6] =
    ["miss", "hit", "update", "del", "evict", "reject"];

/// Gauge labels bound by `InitMetricsVars`, in source order.
pub const STATS_CACHE_GAUGE_LABELS: [&str; 2] = ["track", "capacity"];

/// Returns the source-ordered counter labels.
#[must_use]
pub const fn stats_cache_counter_labels() -> &'static [&'static str; 6] {
    &STATS_CACHE_COUNTER_LABELS
}

/// Returns the source-ordered gauge labels.
#[must_use]
pub const fn stats_cache_gauge_labels() -> &'static [&'static str; 2] {
    &STATS_CACHE_GAUGE_LABELS
}
