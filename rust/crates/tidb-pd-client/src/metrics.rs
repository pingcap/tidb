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

//! The `pd_client_*` dashboard surface.
//!
//! Go's PD client declares these families in `client@.../metrics/metrics.go`
//! and registers them on the process default registry, so the TiDB status
//! server serves them from the shared `/metrics` handler. This module
//! transcreates the families the TiDB dashboards read: the per-command
//! success/failure duration histograms (including Go's pre-materialized
//! `wait`/`tso`/... label values from `initLabelValues`), the request
//! handling histogram, the TSO RTT estimate, the forwarded-status gauge,
//! and the circuit-breaker counter. Names, help strings, label schemas,
//! and the 0.0005s×2¹³ buckets are copied verbatim from the Apache-2.0
//! licensed tikv/pd source tree.

use std::sync::LazyLock;

use prometheus::{exponential_buckets, CounterVec, GaugeVec, HistogramOpts, HistogramVec, Opts};

fn register<C>(collector: prometheus::Result<C>, what: &'static str) -> C
where
    C: prometheus::core::Collector + Clone + 'static,
{
    let metric = collector.unwrap_or_else(|error| panic!("{what} is constructible: {error}"));
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|error| panic!("{what} is registered once: {error}"));
    metric
}

/// Go `cmdDuration`
/// (`pd/client/metrics/metrics.go`: `pd_client_cmd_handle_cmds_duration_seconds`).
pub static CMD_HANDLE_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(
        HistogramVec::new(
            HistogramOpts::new(
                "handle_cmds_duration_seconds",
                "Bucketed histogram of processing time (s) of handled success cmds.",
            )
            .namespace("pd_client")
            .subsystem("cmd")
            .buckets(exponential_buckets(0.0005, 2.0, 13).expect("13 positive buckets")),
            &["type"],
        ),
        "pd cmd handle duration histogram",
    )
});

/// Go `cmdFailedDuration`
/// (`pd_client_cmd_handle_failed_cmds_duration_seconds`).
pub static CMD_HANDLE_FAILED_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(
        HistogramVec::new(
            HistogramOpts::new(
                "handle_failed_cmds_duration_seconds",
                "Bucketed histogram of processing time (s) of failed handled cmds.",
            )
            .namespace("pd_client")
            .subsystem("cmd")
            .buckets(exponential_buckets(0.0005, 2.0, 13).expect("13 positive buckets")),
            &["type"],
        ),
        "pd cmd handle failed duration histogram",
    )
});

/// Go `requestDuration`
/// (`pd_client_request_handle_requests_duration_seconds`).
pub static REQUEST_HANDLE_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(
        HistogramVec::new(
            HistogramOpts::new(
                "handle_requests_duration_seconds",
                "Bucketed histogram of processing time (s) of handled requests.",
            )
            .namespace("pd_client")
            .subsystem("request")
            .buckets(exponential_buckets(0.0005, 2.0, 13).expect("13 positive buckets")),
            &["type"],
        ),
        "pd request handle duration histogram",
    )
});

/// Go `EstimateTSOLatencyGauge`
/// (`pd_client_request_estimate_tso_latency`).
pub static ESTIMATE_TSO_LATENCY: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(
        GaugeVec::new(
            Opts::new(
                "estimate_tso_latency",
                "Estimated latency of an RTT of getting TSO",
            )
            .namespace("pd_client")
            .subsystem("request"),
            &["stream"],
        ),
        "pd estimate tso latency gauge",
    )
});

/// Go `RequestForwarded`
/// (`pd_client_request_forwarded_status`).
pub static REQUEST_FORWARDED_STATUS: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(
        GaugeVec::new(
            Opts::new(
                "forwarded_status",
                "The status to indicate if the request is forwarded",
            )
            .namespace("pd_client")
            .subsystem("request"),
            &["host", "delegate"],
        ),
        "pd forwarded status gauge",
    )
});

/// Go `CircuitBreakerCounters`
/// (`pd_client_request_circuit_breaker_count`).
pub static CIRCUIT_BREAKER_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register(
        CounterVec::new(
            Opts::new("circuit_breaker_count", "Circuit breaker counters")
                .namespace("pd_client")
                .subsystem("request"),
            &["name", "event"],
        ),
        "pd circuit breaker counter",
    )
});

/// The Go `initLabelValues` label values this client can produce,
/// materialized at startup so the exposition matches Go's.
const CMD_LABEL_VALUES: &[&str] = &[
    "wait",
    "tso",
    "get_region",
    "get_prev_region",
    "get_region_byid",
    "scan_regions",
    "batch_scan_regions",
    "get_store",
    "get_all_stores",
    "get_member_info",
    "get_gc_state",
];

/// Go `initLabelValues`: binds every label value once so the dashboard
/// families resolve against a fresh node.
pub fn init_dashboard_series() {
    LazyLock::force(&CMD_HANDLE_DURATION);
    LazyLock::force(&CMD_HANDLE_FAILED_DURATION);
    LazyLock::force(&REQUEST_HANDLE_DURATION);
    LazyLock::force(&ESTIMATE_TSO_LATENCY);
    LazyLock::force(&REQUEST_FORWARDED_STATUS);
    LazyLock::force(&CIRCUIT_BREAKER_COUNTER);
    for value in CMD_LABEL_VALUES {
        let _ = CMD_HANDLE_DURATION.with_label_values(&[value]);
        let _ = CMD_HANDLE_FAILED_DURATION.with_label_values(&[value]);
        let _ = REQUEST_HANDLE_DURATION.with_label_values(&[value]);
    }
    let _ = ESTIMATE_TSO_LATENCY.with_label_values(&["default"]);
}

/// The Go `type` label value for one client command
/// (`initLabelValues`' `CmdDuration*` bindings).
#[must_use]
pub fn cmd_type_label(operation: crate::error::PdOperation) -> &'static str {
    match operation {
        crate::error::PdOperation::GetMembers => "get_member_info",
        crate::error::PdOperation::GetRegion => "get_region",
        crate::error::PdOperation::GetPrevRegion => "get_prev_region",
        crate::error::PdOperation::GetRegionById => "get_region_byid",
        crate::error::PdOperation::ScanRegions => "scan_regions",
        crate::error::PdOperation::BatchScanRegions => "batch_scan_regions",
        crate::error::PdOperation::GetStore => "get_store",
        crate::error::PdOperation::GetAllStores => "get_all_stores",
        crate::error::PdOperation::Tso => "tso",
        crate::error::PdOperation::GetGcState => "get_gc_state",
    }
}

/// Go `cmdHandleDuration`/`requestDuration`: records one command's wall
/// time, on the success histogram or the failure histogram exactly as the
/// PD client's per-command defer does.
pub fn observe_cmd(operation: crate::error::PdOperation, seconds: f64, succeeded: bool) {
    let label = cmd_type_label(operation);
    let duration = if succeeded {
        CMD_HANDLE_DURATION.with_label_values(&[label])
    } else {
        CMD_HANDLE_FAILED_DURATION.with_label_values(&[label])
    };
    duration.observe(seconds);
    REQUEST_HANDLE_DURATION
        .with_label_values(&[label])
        .observe(seconds);
}

/// Go `CmdDurationTSOWait`: the end-to-end wait a caller experiences from
/// requesting a timestamp to receiving one.
pub fn observe_tso_wait(seconds: f64) {
    CMD_HANDLE_DURATION
        .with_label_values(&["wait"])
        .observe(seconds);
}
