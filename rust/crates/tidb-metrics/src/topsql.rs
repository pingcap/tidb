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

//! Go `pkg/metrics/topsql.go`: the TopSQL reporter's families.

use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts};
use std::sync::LazyLock;

fn register<C: prometheus::core::Collector + Clone + 'static>(
    collector: prometheus::Result<C>,
) -> C {
    let collector = collector.expect("valid TopSQL metric definition");
    prometheus::default_registry()
        .register(Box::new(collector.clone()))
        .expect("TopSQL metric registered once");
    collector
}

pub static TOPSQL_IGNORED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_topsql_ignored_total",
            "Counter of ignored top-sql metrics (register-sql, register-plan, collect-data and report-data), normally it should be 0.",
        ),
        &["type"],
    ))
});

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
