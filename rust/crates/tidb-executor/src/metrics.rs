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

//! Go `pkg/metrics/executor.go`: the executor families the TiDB-Overview and TiDB-Query dashboards read.
//!
//! Every definition mirrors its Go `pkg/metrics` declaration one for one
//! (name, help, labels). `init_dashboard_series` materializes the series
//! Go's subsystem startup writes, so the dashboards under
//! `pkg/metrics/grafana` resolve the same family set against the Rust node
//! as against Go master.
//!
//! Copyright note: metric names, help strings, and label schemas are
//! transcribed from the Apache-2.0-licensed pingcap/tidb source tree.

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts};
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

/// Go `AffectedRowsCounter` (`pkg/metrics`).
pub static AFFECTED_ROWS: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_executor_affected_rows", "Counters of server affected rows."),
        &["sql_type"],
    ))
});

/// Go `ExecutorCounter` (`pkg/metrics`).
pub static EXPENSIVE_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_executor_expensive_total", "Counter of Expensive Executors."),
        &["type"],
    ))
});

/// Go `IndexLookUpCopTaskCount` (`pkg/metrics`).
pub static INDEX_LOOKUP_COP_TASK_COUNT: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_executor_index_lookup_cop_task_count", "Counter for index lookup cop tasks"),
        &["type"],
    ))
});

/// Go `IndexLookRowsCounter` (`pkg/metrics`).
pub static INDEX_LOOKUP_ROWS: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_executor_index_lookup_rows", "Counter of index lookup push-down rows."),
        &["type"],
    ))
});

/// Go `MppCoordinatorStats` (`pkg/metrics`).
pub static MPP_COORDINATOR_STATS: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new("tidb_executor_mpp_coordinator_stats", "Mpp Coordinator related stats"),
        &["type"],
    ))
});

/// Go `NetworkTransmissionStats` (`pkg/metrics`).
pub static NETWORK_TRANSMISSION: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_executor_network_transmission", "Counter of network transmission bytes."),
        &["type"],
    ))
});

/// Go `StmtNodeCounter` (`pkg/metrics`).
pub static STATEMENT_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_executor_statement_total", "Counter of StmtNode."),
        &["db", "resource_group", "type"],
    ))
});

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    let _ = AFFECTED_ROWS.with_label_values(&["Delete"]);
    let _ = EXPENSIVE_TOTAL.with_label_values(&["HashAggExec"]);
    let _ = INDEX_LOOKUP_COP_TASK_COUNT.with_label_values(&["index_scan_normal"]);
    let _ = INDEX_LOOKUP_ROWS.with_label_values(&["index_lookup_push_down_hit"]);
    let _ = MPP_COORDINATOR_STATS.with_label_values(&["active"]);
    let _ = NETWORK_TRANSMISSION.with_label_values(&["received_tiflash_cross_zone"]);
    let _ = STATEMENT_TOTAL.with_label_values(&["", "default", "AnalyzeTable"]);
}
