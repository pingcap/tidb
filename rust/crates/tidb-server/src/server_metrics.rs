// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/metrics/server.go` (plus the server-layer families declared in
//! `pkg/metrics/metrics.go`): the `tidb_server_*`, `tidb_monitor_*`, and
//! `tidb_config_*` families the status server exports for the TiDB Grafana
//! dashboards.
//!
//! Every definition below is transcribed one for one from the Go source —
//! name, help string, labels, and buckets — with the Go symbol named in a
//! comment. Families whose bump sites live in subsystems this node has not
//! transcreated yet (TiFlash, PD HTTP APIs, IA remote reads)
//! are registered exactly as Go registers them at init, so the exported
//! family set matches even where the series stay at Go's init state.
//!
//! The two query families Go also declares here — `tidb_server_query_total`
//! and `tidb_server_handle_query_duration_seconds` — stay in
//! [`crate::query_metrics`], which transcribes `clientConn.addQueryMetrics`
//! together with the dispatch scope guard.


use prometheus::{
    Counter, CounterVec, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Opts,
};
use std::sync::LazyLock;

pub use tidb_metrics::server::*;

fn register<C: prometheus::core::Collector + Clone + 'static>(
    collector: prometheus::Result<C>,
) -> C {
    let collector = collector.expect("valid server metric definition");
    if let Err(error) = prometheus::default_registry().register(Box::new(collector.clone())) {
        // A duplicate metric family must not take the node down: the first
        // registration keeps serving the family, so the data Go would
        // export is still exported. Warn once at startup instead.
        let names: Vec<String> = collector
            .desc()
            .iter()
            .map(|desc| desc.fq_name.to_owned())
            .collect();
        eprintln!("[[server metric skipped as already registered: {error:?} for {names:?}]]");
    }
    collector
}

/// Go `pkg/server/metrics` disconnect bindings, as label values.
pub mod disconnect {
    pub const NORMAL: &str = super::labels::OK;
    pub const BY_CLIENT_WITH_ERROR: &str = super::labels::ERROR;
    pub const UNDETERMINED: &str = "undetermined";
}

/// Go `TimerEventCounter` (`pkg/metrics`).
pub static TIDB_SERVER_TIMER_EVENT_COUNT: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_server_timer_event_count", "Counter of timer event."),
        &["scope", "type"],
    ))
});

/// Go's unistore bootstrap materializes these series through subsystem
/// startup; mirror the same label combinations so the dashboards see the
/// same family surface.
pub(crate) fn init_dashboard_series() {
    // The planner's plan-cache families construct lazily without touching the
    // registry; register them here so the dashboard series below are served.
    {
        let registry = prometheus::default_registry();
        for collector in [
            Box::new(tidb_metrics::server::PLAN_CACHE_COUNTER.clone())
                as Box<dyn prometheus::core::Collector>,
            Box::new(tidb_metrics::server::PLAN_CACHE_MISS_COUNTER.clone()),
            Box::new(tidb_metrics::server::PLAN_CACHE_INSTANCE_MEMORY_USAGE.clone()),
            Box::new(tidb_metrics::server::PLAN_CACHE_INSTANCE_PLAN_NUM_COUNTER.clone()),
            Box::new(tidb_metrics::stats::PSEUDO_ESTIMATION.clone()),
        ] {
            
            let _ = registry.register(collector);
        }
    }
    LazyLock::force(&TIDB_SERVER_BINDING_CACHE_HIT_TOTAL);
    LazyLock::force(&TIDB_SERVER_BINDING_CACHE_MEM_LIMIT);
    LazyLock::force(&TIDB_SERVER_BINDING_CACHE_MEM_USAGE);
    LazyLock::force(&TIDB_SERVER_BINDING_CACHE_MISS_TOTAL);
    LazyLock::force(&TIDB_SERVER_BINDING_CACHE_NUM_BINDINGS);
    let _ = EVENT_TOTAL.with_label_values(&["server-start"]);
    let _ = MEMORY_USAGE.with_label_values(&["analyze", "inuse"]);
    let _ = PACKET_IO_BYTES.with_label_values(&["In"]);
    let _ = tidb_metrics::server::PLAN_CACHE_INSTANCE_MEMORY_USAGE
        .with_label_values(&[" instance-plan-cache"]);
    let _ = tidb_metrics::server::PLAN_CACHE_INSTANCE_PLAN_NUM_COUNTER
        .with_label_values(&[" instance-plan-cache"]);
    let _ = tidb_metrics::server::PLAN_CACHE_MISS_COUNTER.with_label_values(&["non-prepared"]);
    let _ = tidb_metrics::server::PLAN_CACHE_COUNTER.with_label_values(&["non-prepared"]);
    let _ = RC_CHECK_TS_CONFLICT_TOTAL.with_label_values(&["read_check"]);
    let _ = TIDB_SERVER_RUNAWAY_FLUSHER_ADD_TOTAL.with_label_values(&["quarantine-record"]);
    let _ = TIDB_SERVER_RUNAWAY_FLUSHER_TOTAL.with_label_values(&["quarantine-record", "error"]);
    let _ = TIDB_SERVER_RUNAWAY_SYNCER_CHECKPOINT.with_label_values(&["watch"]);
    let _ = TIDB_SERVER_RUNAWAY_SYNCER_TOTAL.with_label_values(&["error", "sync"]);
    let _ = SLOW_QUERY_TOTAL.with_label_values(&["general"]);
    let _ =
        TIDB_SERVER_TIMER_EVENT_COUNT.with_label_values(&["runtime.ttl", "full_refresh_timers"]);
    let _ = TIDB_SERVER_TTL_EVENT_COUNT.with_label_values(&["full_refresh_timers"]);
    LazyLock::force(&TIDB_SERVER_TTL_INSERT_ROWS);
    let _ = TIDB_SERVER_TTL_JOB_STATUS.with_label_values(&["cancelling"]);
    let _ = TIDB_SERVER_TTL_PHASE_TIME.with_label_values(&["begin_txn", "delete_worker"]);
    let _ = TIDB_SERVER_TTL_PROCESSED_EXPIRED_ROWS.with_label_values(&["error", "delete"]);
    let _ = TIDB_SERVER_TTL_TASK_STATUS.with_label_values(&["deleting"]);
    let _ = TIDB_SERVER_TTL_WATERMARK_DELAY.with_label_values(&["01 hour", "schedule"]);
}

/// Aggregates every histogram-family (fq name, help) definition across the
/// workspace's metric modules plus the vendored client-go registry. Go's
/// exposition emits HELP/TYPE for registered-but-childless histogram vecs;
/// rust-prometheus omits them, so the status server appends the missing
/// headers and Prometheus/Grafana see the same family surface as against Go
/// master.
pub(crate) fn family_catalog() -> Vec<(String, String)> {
    let mut catalog: Vec<(String, String)> = Vec::new();
    for (fq, help) in tidb_metrics::session::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    for (fq, help) in tidb_metrics::distsql::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    for (fq, help) in tidb_metrics::ddl::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    for (fq, help) in tidb_metrics::domain::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    for (fq, help) in tidb_metrics::executor::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    for (fq, help) in tidb_metrics::memory::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    for (fq, help) in tidb_metrics::meta::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    for (fq, help) in tidb_util::topsql_reporter::metrics::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    for (fq, help) in tidb_txnkv::client_go_metrics::histogram_definitions() {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    // Go `ConnIdleDurationHistogram` (server.go), childless until a
    // connection idles between two commands.
    catalog.push((
        "tidb_server_conn_idle_duration_seconds".to_owned(),
        "Bucketed histogram of connection idle time (s).".to_owned(),
    ));
    // These four slow-query histograms plus the two RPC-count histograms and
    // the three IA remote-read counter vecs (server.go) stay childless until
    // their paths run, exactly as in Go.
    for (fq, help) in [
        (
            "tidb_server_slow_query_process_duration_seconds",
            "Bucketed histogram of processing time (s) of of slow queries.",
        ),
        (
            "tidb_server_slow_query_cop_duration_seconds",
            "Bucketed histogram of all cop processing time (s) of of slow queries.",
        ),
        (
            "tidb_server_slow_query_wait_duration_seconds",
            "Bucketed histogram of all cop waiting time (s) of of slow queries.",
        ),
        (
            "tidb_server_slow_query_cop_mvcc_ratio",
            "Bucketed histogram of all cop total keys / processed keys in slow queries.",
        ),
        (
            "tidb_server_query_statement_rpc_count",
            "Bucketed histogram of execution rpc count of handled query statements.",
        ),
        (
            "tidb_server_query_statement_processed_keys",
            "Bucketed histogram of processed key count during the scan of handled query statements.",
        ),
        (
            "tidb_server_ia_remote_read_segment_count",
            "Counter of IA remote read segments observed by TiDB.",
        ),
        (
            "tidb_server_ia_remote_read_segment_size_bytes",
            "Counter of IA remote read segment bytes observed by TiDB.",
        ),
    ] {
        catalog.push((fq.to_owned(), help.to_owned()));
    }
    // Go `pkg/metrics/ttl.go`: the TTL query duration histogram.
    catalog.push((
        "tidb_server_ttl_query_duration".to_owned(),
        "Bucketed histogram of processing time (s) of handled TTL queries.".to_owned(),
    ));
    // Go `pkg/metrics/server.go`: PlanCacheProcessDuration lives in
    // tidb-planner here; its children materialize with the plan cache.
    catalog.push((
        "tidb_server_plan_cache_process_duration_seconds".to_owned(),
        "Bucketed histogram of processing time (s) of plan cache operations.".to_owned(),
    ));
    catalog
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_cache_and_dashboard_share_metric_ownership() {
        init_dashboard_series();
        let cache = tidb_bindinfo::binding_cache::BindingCache::from_storage_rows(Vec::new(), 4096);
        assert!(std::ptr::eq(
            &*TIDB_SERVER_BINDING_CACHE_MEM_LIMIT,
            &*tidb_metrics::bindinfo::BINDING_CACHE_MEM_LIMIT
        ));
        assert!(std::ptr::eq(
            &*TIDB_SERVER_BINDING_CACHE_MEM_USAGE,
            &*tidb_metrics::bindinfo::BINDING_CACHE_MEM_USAGE
        ));
        assert!(std::ptr::eq(
            &*TIDB_SERVER_BINDING_CACHE_NUM_BINDINGS,
            &*tidb_metrics::bindinfo::BINDING_CACHE_NUM_BINDINGS
        ));
        cache.close();
    }

    #[test]
    fn server_families_expose_go_names_and_labels() {
        PACKET_IO_BYTES.with_label_values(&["read"]).inc();
        TIME_JUMP_BACK_TOTAL.inc();
        CONFIG_STATUS.with_label_values(&["token-limit"]).set(1000);
        TLS_VERSION.with_label_values(&["TLSv1.3"]).inc();
        TLS_CIPHER.with_label_values(&["AES-256-GCM"]).inc();
        MEMORY_USAGE
            .with_label_values(&["server", "current"])
            .set(1);
        PANIC_TOTAL.with_label_values(&["session"]).inc();
        CRITICAL_ERROR_TOTAL.inc();
        PREPARED_STMTS.set(0);
        TOKENS.set(0);
        MEMORY_QUOTA_BYTES.set(0);
        INTERNAL_SESSIONS.set(0);
        ACTIVE_USERS.set(0);
        MAXPROCS.set(1);
        GOGC.set(100);
        READ_FROM_TABLECACHE_TOTAL.inc();
        SLOW_QUERY_TOTAL.with_label_values(&["general"]).inc();
        TIFLASH_QUERY_TOTAL.with_label_values(&["mpp", "ok"]).inc();
        TIFLASH_FAILED_STORE
            .with_label_values(&["127.0.0.1:3930"])
            .set(0);
        PD_API_REQUEST_TOTAL.with_label_values(&["api", "ok"]).inc();
        CPU_PROFILE_TOTAL.inc();
        RC_CHECK_TS_CONFLICT_TOTAL
            .with_label_values(&["read_check"])
            .inc();
        CONN_GAUGE.with_label_values(&["default"]).inc();
        DISCONNECTION_TOTAL
            .with_label_values(&[disconnect::NORMAL])
            .inc();
        EVENT_TOTAL.with_label_values(&["server-start"]).inc();
        let label = execute_error_to_label(Some(
            tidb_error::plannererrors::ERR_STMT_NOT_FOUND.identity(),
        ));
        EXECUTE_ERROR_TOTAL
            .with_label_values(&[&label, "", "default"])
            .inc();
        PREPARED_STMTS.set(3);
        let mut body = prometheus::TextEncoder::new()
            .encode_to_string(&prometheus::gather())
            .expect("registered metrics encode");
        for family in [
            "tidb_server_packet_io_bytes",
            "tidb_server_connections",
            "tidb_server_disconnection_total",
            "tidb_server_prepared_stmts",
            "tidb_server_execute_error_total",
            "tidb_server_event_total",
            "tidb_monitor_time_jump_back_total",
            "tidb_config_status",
            "tidb_server_panic_total",
            "tidb_server_memory_usage",
        ] {
            assert!(body.contains(family), "{family} missing from:\n{body}");
        }
        assert!(
            body.contains(r#"tidb_server_connections{resource_group="default"}"#),
            "connections must carry the resource_group label like Go:\n{body}"
        );
        body.clear();
    }
}

/// Go's process init: `RegisterMetrics` for the server families, then the
/// dashboard series Go's subsystem startup writes.
pub fn init() {
    tidb_metrics::server::register_metrics();
    init_dashboard_series();
}

#[cfg(test)]
mod init_tests {
    #[test]
    fn init_registers_every_family() {
        super::init();
        tidb_util::topsql_reporter::metrics::init_metrics_vars();
        let catalog = super::family_catalog();
        for (name, help) in tidb_util::topsql_reporter::metrics::histogram_definitions() {
            assert!(
                catalog.contains(&(name.to_owned(), help.to_owned())),
                "{name} missing from the server metric catalog"
            );
        }
        let body = prometheus::TextEncoder::new()
            .encode_to_string(&prometheus::gather())
            .expect("encode");
        // Plain counters and gauges export their zero series from
        // registration alone; Vec families gain series on first labeled
        // use, exactly like Go's client_golang.
        for family in [
            "tidb_server_critical_error_total",
            "tidb_monitor_time_jump_back_total",
            "tidb_server_maxprocs",
            "tidb_server_gogc",
            "tidb_server_tokens",
            "tidb_server_prepared_stmts",
            "tidb_server_memory_quota_bytes",
            "tidb_server_internal_sessions",
            "tidb_server_active_users",
            "tidb_topsql_report_data_total",
            "tidb_topsql_report_duration_seconds",
        ] {
            assert!(body.contains(family), "{family} missing");
        }
    }
}

#[cfg(test)]
mod register_tests {
    use super::*;

    #[test]
    fn duplicate_family_registration_warns_instead_of_panicking() {
        // Two collectors declaring the SAME family name: the first register
        // wins the registry slot, the second must be skipped with a warning
        // (never panic) while remaining usable for child-materialization.
        let first = register(CounterVec::new(
            Opts::new("tidb_server_register_probe", "register probe."),
            &["kind"],
        ));
        let second = register(CounterVec::new(
            Opts::new("tidb_server_register_probe", "register probe."),
            &["kind"],
        ));
        first.with_label_values(&["a"]).inc();
        // The skipped collector still materializes children for its own
        // gather snapshot, without corrupting the registered family.
        second.with_label_values(&["a"]).inc();
        first.with_label_values(&["a"]).inc();
        assert_eq!(first.with_label_values(&["a"]).get(), 2.0);
        assert_eq!(second.with_label_values(&["a"]).get(), 1.0);
    }
}
