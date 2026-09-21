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

//! Go `pkg/metrics/topsql.go`: the TopSQL families (ignore counter, agent
//! report histograms). The Rust node has no TopSQL subsystem yet; the
//! families register so the dashboard queries resolve, matching Go's
//! registered-but-quiescent state.
//!
//! Every definition mirrors its Go `pkg/metrics` declaration one for one
//! (name, help, labels). `init_dashboard_series` materializes the series
//! Go's subsystem startup writes, so the dashboards under
//! `pkg/metrics/grafana` resolve the same family set against the Rust node
//! as against Go master.
//!
//! Copyright note: metric names, help strings, and label schemas are
//! transcribed from the Apache-2.0-licensed pingcap/tidb source tree.

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, HistogramOpts, Opts};
use std::sync::LazyLock;

fn register<C: prometheus::core::Collector + Clone + 'static>(
    collector: prometheus::Result<C>,
) -> C {
    let collector = collector.expect("valid metric definition");
    prometheus::default_registry()
        .register(Box::new(collector.clone()))
        .expect("metric registered once");
    collector
}

/// Go `TopSQLIgnoredCounter` (`pkg/metrics`).
pub static TOPSQL_IGNORED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_topsql_ignored_total", "Counter of ignored top-sql metrics (register-sql, register-plan, collect-data and report-data), normally it should be 0."),
        &["type"],
    ))
});


/// Go `TopSQLReportDataHistogram` (`pkg/metrics/topsql.go`).
pub static TOPSQL_REPORT_DATA: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_topsql_report_data_total",
            "Bucket histogram of reporting records/sql/plan count to the top-sql agent.",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 20).expect("valid buckets")),
        &["type"],
    ))
});

/// Go `TopSQLReportDurationHistogram` (`pkg/metrics/topsql.go`).
pub static TOPSQL_REPORT_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_topsql_report_duration_seconds",
            "Bucket histogram of reporting time (s) to the top-sql agent",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 24).expect("valid buckets")),
        &["type", "result"],
    ))
});

/// The (fq name, help) of the report histogram families.
pub fn histogram_definitions() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "tidb_topsql_report_data_total",
            "Bucket histogram of reporting records/sql/plan count to the top-sql agent.",
        ),
        (
            "tidb_topsql_report_duration_seconds",
            "Bucket histogram of reporting time (s) to the top-sql agent",
        ),
    ]
}

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    let _ = TOPSQL_IGNORED_TOTAL.with_label_values(&["ignore_collect_channel_full"]);
    LazyLock::force(&TOPSQL_REPORT_DATA);
    LazyLock::force(&TOPSQL_REPORT_DURATION);
}
