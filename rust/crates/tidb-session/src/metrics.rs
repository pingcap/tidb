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

use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
};
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

/// Go `SessionRetryErrorCounter` (`pkg/metrics/session.go`).
pub static SESSION_RETRY_ERROR: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_session_retry_error_total",
            "Counter of session retry error.",
        ),
        &["sql_type", "type"],
    ))
});

/// Go `ResourceGroupQueryTotalCounter` (`pkg/metrics`).
pub static RESOURCE_GROUP_QUERY_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_session_resource_group_query_total",
            "Counter of the total number of queries for the resource group",
        ),
        &["name", "resource_group"],
    ))
});

/// Go `SessionRestrictedSQLCounter` (`pkg/metrics`).
pub static RESTRICTED_SQL_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    register(Counter::new(
        "tidb_session_restricted_sql_total",
        "Counter of internal restricted sql.",
    ))
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
        Opts::new(
            "tidb_session_txn_state_entering_count",
            "How many times transactions enter this state",
        ),
        &["type"],
    ))
});

/// Go `session.Parse`'s observation: the parse histogram is charged under
/// `LblInternal` for internal sessions and `LblGeneral` otherwise. The
/// session tier has no internal-session doors yet, so every current caller
/// passes `false`.
pub fn observe_parse_duration(seconds: f64, internal: bool) {
    let label = if internal { "internal" } else { "general" };
    SESSION_PARSE.with_label_values(&[label]).observe(seconds);
}

/// Go `session.ExecuteStmt`'s compile observation (`session.go:2624`).
pub fn observe_compile_duration(seconds: f64, internal: bool) {
    let label = if internal { "internal" } else { "general" };
    SESSION_COMPILE.with_label_values(&[label]).observe(seconds);
}

/// Go `ExecStmt.finishExecutor`'s run observation (`adapter.go:1723`).
pub fn observe_execute_duration(seconds: f64, internal: bool) {
    let label = if internal { "internal" } else { "general" };
    SESSION_EXECUTE.with_label_values(&[label]).observe(seconds);
}

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
///
/// Go pre-binds the transaction-state observers and counters for every
/// state at package init (`pkg/session/txninfo/txn_info.go`
/// `InitMetricsVars`, states idle / executing_sql / acquiring_lock /
/// committing / rolling_back), and the retry histogram for both scopes
/// (`pkg/session/metrics`). Those children therefore exist on Go's
/// `/metrics` export with zero counts, and the Rust node materializes the
/// same combinations here.
pub fn init_dashboard_series() {
    let _ = RESOURCE_GROUP_QUERY_TOTAL.with_label_values(&["default", "default"]);
    LazyLock::force(&RESTRICTED_SQL_TOTAL);
    let _ = TRANSACTION_FAIR_LOCKING_USAGE.with_label_values(&["stmt-effective"]);
    LazyLock::force(&STATEMENT_LOCK_KEYS_COUNT);
    LazyLock::force(&STATEMENT_PESSIMISTIC_RETRY_COUNT);
    // Go `pkg/session/txninfo` InitMetricsVars pre-binds every state
    // (idle / executing_sql / acquiring_lock / committing / rolling_back)
    // times two has_lock values, and the session retry histogram for both
    // scopes, so Go exports them at zero counts; mirror that here.
    for state in [
        "idle",
        "executing_sql",
        "acquiring_lock",
        "committing",
        "rolling_back",
    ] {
        let _ = TXN_STATE_ENTERING_COUNT.with_label_values(&[state]);
        let _ = TXN_STATE_SECONDS.with_label_values(&[state, "false"]);
        let _ = TXN_STATE_SECONDS.with_label_values(&[state, "true"]);
    }
    let _ = SESSION_RETRY.with_label_values(&["general"]);
    let _ = SESSION_RETRY.with_label_values(&["internal"]);
    LazyLock::force(&SESSION_PARSE);
    LazyLock::force(&SESSION_COMPILE);
    LazyLock::force(&SESSION_EXECUTE);
    LazyLock::force(&SESSION_RETRY_ERROR);
    LazyLock::force(&TRANSACTION_DURATION);
    LazyLock::force(&STATEMENT_PER_TRANSACTION);
    LazyLock::force(&STATEMENT_SHARED_LOCK_KEYS_COUNT);
}

pub static SESSION_PARSE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_parse_duration_seconds",
            "Bucketed histogram of processing time (s) in parse SQL.",
        )
        .buckets(prometheus::exponential_buckets(4e-05, 2.0, 28).expect("valid buckets")),
        &["sql_type"],
    ))
});

pub static SESSION_COMPILE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_compile_duration_seconds",
            "Bucketed histogram of processing time (s) in query optimize.",
        )
        .buckets(prometheus::exponential_buckets(4e-05, 2.0, 28).expect("valid buckets")),
        &["sql_type"],
    ))
});

pub static SESSION_EXECUTE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_execute_duration_seconds",
            "Bucketed histogram of processing time (s) in running executor.",
        )
        .buckets(prometheus::exponential_buckets(0.0001, 2.0, 30).expect("valid buckets")),
        &["sql_type"],
    ))
});

pub static SESSION_RETRY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_retry_num",
            "Bucketed histogram of session retry count.",
        )
        .buckets(prometheus::linear_buckets(0.0, 1.0, 21).expect("valid buckets")),
        &["scope"],
    ))
});

pub static STATEMENT_LOCK_KEYS_COUNT: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_statement_lock_keys_count",
            "Keys locking for a single statement",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 21).expect("valid buckets")),
        &[],
    ))
});

pub static STATEMENT_PESSIMISTIC_RETRY_COUNT: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_statement_pessimistic_retry_count",
            "Bucketed histogram of statement pessimistic retry count",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 16).expect("valid buckets")),
        &[],
    ))
});

pub static STATEMENT_SHARED_LOCK_KEYS_COUNT: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_statement_shared_lock_keys_count",
            "Keys locking for a single statement",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 21).expect("valid buckets")),
        &["type"],
    ))
});

pub static TRANSACTION_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_transaction_duration_seconds",
            "Bucketed histogram of a transaction execution duration, including retry.",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).expect("valid buckets")),
        &["txn_mode", "type", "scope"],
    ))
});

pub static STATEMENT_PER_TRANSACTION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_transaction_statement_num",
            "Bucketed histogram of statements count in each transaction.",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 16).expect("valid buckets")),
        &["txn_mode", "type", "scope"],
    ))
});

pub static TXN_STATE_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_session_txn_state_seconds",
            "Bucketed histogram of different states of a transaction.",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &["type", "has_lock"],
    ))
});

/// The (fq name, help, kind) of every histogram family in this module,
/// for the exposition header shim that mirrors Go's registered-family output.
pub fn histogram_definitions() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "tidb_session_parse_duration_seconds",
            "Bucketed histogram of processing time (s) in parse SQL.",
        ),
        (
            "tidb_session_compile_duration_seconds",
            "Bucketed histogram of processing time (s) in query optimize.",
        ),
        (
            "tidb_session_execute_duration_seconds",
            "Bucketed histogram of processing time (s) in running executor.",
        ),
        (
            "tidb_session_retry_num",
            "Bucketed histogram of session retry count.",
        ),
        (
            "tidb_session_statement_lock_keys_count",
            "Keys locking for a single statement",
        ),
        (
            "tidb_session_statement_pessimistic_retry_count",
            "Bucketed histogram of statement pessimistic retry count",
        ),
        (
            "tidb_session_statement_shared_lock_keys_count",
            "Keys locking for a single statement",
        ),
        (
            "tidb_session_transaction_duration_seconds",
            "Bucketed histogram of a transaction execution duration, including retry.",
        ),
        (
            "tidb_session_transaction_statement_num",
            "Bucketed histogram of statements count in each transaction.",
        ),
        (
            "tidb_session_txn_state_seconds",
            "Bucketed histogram of different states of a transaction.",
        ),
    ]
}
