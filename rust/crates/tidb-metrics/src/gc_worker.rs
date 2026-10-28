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

//! Go `pkg/metrics/gc_worker.go`: the GC worker's families, under the
//! `tikvclient` subsystem.

use std::sync::LazyLock;

use prometheus::{Counter, CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts};

// GC-worker families from Go `pkg/metrics/gc_worker.go`, subsystem
// `tikvclient` like the client-go set.

/// Go `GCHistogram` (`pkg/metrics/gc_worker.go:44-51`): GC duration by
/// stage, 1s ~ 6days exponential buckets.
pub static GC_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let metric = HistogramVec::new(
        HistogramOpts::new("gc_seconds", "Bucketed histogram of gc duration.")
            .namespace("tidb")
            .subsystem("tikvclient")
            .buckets(prometheus::exponential_buckets(1.0, 2.0, 20).expect("20 positive buckets")),
        &["stage"],
    )
    .expect("valid gc seconds histogram");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc seconds histogram is registered once");
    metric
});

/// Go `GCActionRegionResultCounter` (`pkg/metrics/gc_worker.go:68-73`).
pub static GC_ACTION_REGION_RESULT: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
        Opts::new(
            "gc_action_result",
            "Counter of gc action result on region level.",
        )
        .namespace("tidb")
        .subsystem("tikvclient"),
        &["type"],
    )
    .expect("valid gc action result counter");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc action result counter is registered once");
    metric
});

/// Go `GCConfigGauge`.
pub static GC_CONFIG: LazyLock<GaugeVec> = LazyLock::new(|| {
    let metric = GaugeVec::new(
        Opts::new("gc_config", "GC related config.")
            .namespace("tidb")
            .subsystem("tikvclient"),
        &["type"],
    )
    .expect("valid gc config metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc config metric is registered once");
    metric
});

/// Go `GCFailureCounter`.
pub static GC_FAILURE: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
        Opts::new("gc_failure", "Counter of gc worker failures")
            .namespace("tidb")
            .subsystem("tikvclient"),
        &["type"],
    )
    .expect("valid gc failure metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc failure metric is registered once");
    metric
});

/// Go `GCRegionTooManyLocksCounter`.
pub static GC_REGION_TOO_MANY_LOCKS: LazyLock<Counter> = LazyLock::new(|| {
    let metric = Counter::new(
        "tidb_tikvclient_gc_region_too_many_locks",
        "Counter of skipping gc when there are too many locks in a region",
    )
    .expect("valid gc region too many locks metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc region too many locks metric is registered once");
    metric
});

/// Go `GCWorkerActionsCounter`.
pub static GC_WORKER_ACTIONS_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
        Opts::new("gc_worker_actions_total", "Counter of gc worker actions.")
            .namespace("tidb")
            .subsystem("tikvclient"),
        &["type"],
    )
    .expect("valid gc worker actions metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc worker actions metric is registered once");
    metric
});
