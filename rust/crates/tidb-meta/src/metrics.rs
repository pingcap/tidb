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

//! Go `pkg/metrics/meta.go` (dashboard subset): the autoid client connection family.
//!
//! Every definition mirrors its Go `pkg/metrics` declaration one for one
//! (name, help, labels). `init_dashboard_series` materializes the series
//! Go's subsystem startup writes, so the dashboards under
//! `pkg/metrics/grafana` resolve the same family set against the Rust node
//! as against Go master.
//!
//! Copyright note: metric names, help strings, and label schemas are
//! transcribed from the Apache-2.0-licensed pingcap/tidb source tree.

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts};
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

/// Go `AutoIDClientConnResetCounter` (`pkg/metrics`).
pub static AUTOID_CLIENT_CONN_RESET: LazyLock<Counter> = LazyLock::new(|| {
    register(Counter::new(
        "tidb_meta_autoid_client_conn_reset_total",
        "Counter of resetting autoid client connection.",
    ))
});

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    LazyLock::force(&AUTOID_CLIENT_CONN_RESET);
}

pub static META_OPERATION_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_meta_operation_duration_seconds",
            "Bucketed histogram of processing time (s) of tidb meta data operations.",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &["type", "result"],
    ))
});

pub static AUTOID_OPERATION_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_autoid_operation_duration_seconds",
            "Bucketed histogram of processing time (s) of handled autoid.",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &["type", "result"],
    ))
});

/// The (fq name, help, kind) of every histogram family in this module,
/// for the exposition header shim that mirrors Go's registered-family output.
pub fn histogram_definitions() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "tidb_meta_operation_duration_seconds",
            "Bucketed histogram of processing time (s) of tidb meta data operations.",
        ),
        (
            "tidb_autoid_operation_duration_seconds",
            "Bucketed histogram of processing time (s) of handled autoid.",
        ),
    ]
}
