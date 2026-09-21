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

//! Go `pkg/metrics/distsql.go`: coprocessor cache and closest-read families.
//!
//! Every definition mirrors its Go `pkg/metrics` declaration one for one
//! (name, help, labels). `init_dashboard_series` materializes the series
//! Go's subsystem startup writes, so the dashboards under
//! `pkg/metrics/grafana` resolve the same family set against the Rust node
//! as against Go master.
//!
//! Copyright note: metric names, help strings, and label schemas are
//! transcribed from the Apache-2.0-licensed pingcap/tidb source tree.

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts, HistogramVec, HistogramOpts};
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

/// Go `DistSQLCoprCacheCounter` (`pkg/metrics`).
pub static COPR_CACHE: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_distsql_copr_cache", "coprocessor cache hit, evict and miss number"),
        &["type"],
    ))
});

/// Go `DistSQLCoprClosestReadCounter` (`pkg/metrics`).
pub static COPR_CLOSEST_READ: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_distsql_copr_closest_read", "counter of total copr read local read hit."),
        &["type"],
    ))
});

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    let _ = COPR_CACHE.with_label_values(&["evict"]);
    let _ = COPR_CLOSEST_READ.with_label_values(&["null"]);
}

pub static DISTSQL_QUERY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_distsql_handle_query_duration_seconds",
            "Bucketed histogram of processing time (s) of handled queries.",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &["type", "sql_type", "copr_type"],
    ))
});

pub static DISTSQL_SCAN_KEYS_NUM: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_distsql_scan_keys_num",
            "number of scanned keys for each query.",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &["type"],
    ))
});

pub static DISTSQL_PARTIAL_NUM: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_distsql_partial_num",
            "number of partial results for each query.",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &["type"],
    ))
});

pub static DISTSQL_SCAN_KEYS_PARTIAL_NUM: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_distsql_scan_keys_partial_num",
            "number of scanned keys for each partial result.",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &["type"],
    ))
});

pub static DISTSQL_COPR_RESP_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_distsql_copr_resp_size",
            "copr task response data size in bytes.",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 10).expect("valid buckets")),
        &["store"],
    ))
});

/// The (fq name, help, kind) of every histogram family in this module,
/// for the exposition header shim that mirrors Go's registered-family output.
pub fn histogram_definitions() -> Vec<(&'static str, &'static str)> {
    vec![
            ("tidb_distsql_handle_query_duration_seconds", "Bucketed histogram of processing time (s) of handled queries."),
            ("tidb_distsql_scan_keys_num", "number of scanned keys for each query."),
            ("tidb_distsql_partial_num", "number of partial results for each query."),
            ("tidb_distsql_scan_keys_partial_num", "number of scanned keys for each partial result."),
            ("tidb_distsql_copr_resp_size", "copr task response data size in bytes."),
    ]
}
