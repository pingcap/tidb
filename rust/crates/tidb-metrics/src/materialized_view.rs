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

//! Go master `pkg/metrics/materialized_view.go`: the materialized view
//! service family.
//!
//! Copyright note: metric names, help strings, and label schemas are
//! transcribed from the Apache-2.0-licensed pingcap/tidb source tree.

use prometheus::{CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts};
use std::sync::LazyLock;

fn register<C: prometheus::core::Collector + Clone + 'static>(collector: prometheus::Result<C>) -> C {
    let collector = collector.expect("valid metric definition");
    prometheus::default_registry().register(Box::new(collector.clone())).expect("metric registered once");
    collector
}

const MV_METRIC_COMPONENT_SERVICE: &str = "service";

/// Go `MVServiceTaskStatusGaugeVec`.
pub static MV_SERVICE_TASK_STATUS_GAUGE_VEC: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new("service_task_status", "Number of MV service and task executor tasks by component and status type.")
            .namespace("tidb")
            .subsystem("mv"),
        &["component", "type"],
    ))
});

/// Go `MVServiceOperationDurationHistogramVec`.
pub static MV_SERVICE_OPERATION_DURATION_HISTOGRAM_VEC: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new("service_operation_duration_seconds", "Bucketed histogram of MV service operation duration.")
            .namespace("tidb")
            .subsystem("mv")
            .buckets(prometheus::exponential_buckets(0.001, 2.0, 20).expect("valid buckets")),
        &["type", "result"],
    ))
});

/// Go `MVServiceRefreshScheduleDurationHistogram`.
pub static MV_SERVICE_REFRESH_SCHEDULE_DURATION_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    register(Histogram::with_opts(
        HistogramOpts::new(
            "service_refresh_schedule_duration_seconds",
            "Bucketed histogram of the interval between two successful MV service refreshes, excluding the current refresh duration.",
        )
        .namespace("tidb")
        .subsystem("mv")
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 25).expect("valid buckets")),
    ))
});

/// Go `MVServiceRunEventCounterVec`.
pub static MV_SERVICE_RUN_EVENT_COUNTER_VEC: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("service_run_event_total", "Counter of MV service scheduler and task executor events by component and event type.")
            .namespace("tidb")
            .subsystem("mv"),
        &["component", "type"],
    ))
});

fn service_gauge(status: &str) -> Gauge {
    MV_SERVICE_TASK_STATUS_GAUGE_VEC.with_label_values(&[MV_METRIC_COMPONENT_SERVICE, status])
}

/// Go `MVServiceMVRefreshTotalGauge`.
pub static MV_SERVICE_MV_REFRESH_TOTAL_GAUGE: LazyLock<Gauge> = LazyLock::new(|| service_gauge("mv_total"));
/// Go `MVServiceMVLogPurgeTotalGauge`.
pub static MV_SERVICE_MV_LOG_PURGE_TOTAL_GAUGE: LazyLock<Gauge> = LazyLock::new(|| service_gauge("mvlog_total"));
/// Go `MVServiceMVRefreshWarningGauge`.
pub static MV_SERVICE_MV_REFRESH_WARNING_GAUGE: LazyLock<Gauge> = LazyLock::new(|| service_gauge("mv_refresh_warning"));
/// Go `MVServiceMVRefreshOverdueGauge`.
pub static MV_SERVICE_MV_REFRESH_OVERDUE_GAUGE: LazyLock<Gauge> = LazyLock::new(|| service_gauge("mv_refresh_overdue"));
/// Go `MVServiceMVLogAccumulationGauge`.
pub static MV_SERVICE_MV_LOG_ACCUMULATION_GAUGE: LazyLock<Gauge> = LazyLock::new(|| service_gauge("mvlog_accumulation"));
/// Go `MVServicePanicGauge`.
pub static MV_SERVICE_PANIC_GAUGE: LazyLock<Gauge> = LazyLock::new(|| service_gauge("panic"));

/// Go `InitMVMetrics`: the family registers here.
pub fn init_dashboard_series() {
    LazyLock::force(&MV_SERVICE_TASK_STATUS_GAUGE_VEC);
    LazyLock::force(&MV_SERVICE_OPERATION_DURATION_HISTOGRAM_VEC);
    LazyLock::force(&MV_SERVICE_REFRESH_SCHEDULE_DURATION_HISTOGRAM);
    LazyLock::force(&MV_SERVICE_RUN_EVENT_COUNTER_VEC);
}
