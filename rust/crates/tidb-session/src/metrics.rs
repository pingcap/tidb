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

//! Go `pkg/metrics/session.go` (dashboard subset): session transaction and resource-group families.
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

/// Go `ResourceGroupQueryTotalCounter` (`pkg/metrics`).
pub static RESOURCE_GROUP_QUERY_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_session_resource_group_query_total", "Counter of the total number of queries for the resource group"),
        &["name", "resource_group"],
    ))
});

/// Go `SessionRestrictedSQLCounter` (`pkg/metrics`).
pub static RESTRICTED_SQL_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    register(Counter::new("tidb_session_restricted_sql_total", "Counter of internal restricted sql."))
});

/// Go `FairLockingUsageCounter` (`pkg/metrics`).
pub static TRANSACTION_FAIR_LOCKING_USAGE: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_session_transaction_fair_locking_usage", "The counter of statements and transactions in which fair locking is used or takes effect"),
        &["type"],
    ))
});

/// Go `TxnStateEnteringCounter` (`pkg/metrics`).
pub static TXN_STATE_ENTERING_COUNT: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_session_txn_state_entering_count", "How many times transactions enter this state"),
        &["type"],
    ))
});

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    let _ = RESOURCE_GROUP_QUERY_TOTAL.with_label_values(&["default", "default"]);
    LazyLock::force(&RESTRICTED_SQL_TOTAL);
    let _ = TRANSACTION_FAIR_LOCKING_USAGE.with_label_values(&["stmt-effective"]);
    let _ = TXN_STATE_ENTERING_COUNT.with_label_values(&["acquiring_lock"]);
}
