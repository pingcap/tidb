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

//! The `tidb_tikvclient_*` dashboard surface.
//!
//! Go TiDB registers client-go's metric set under the `tidb`/`tikvclient`
//! namespace, plus its own `pkg/metrics/gc_worker.go` GC-worker families
//! under the same subsystem, and serves them from the shared `/metrics`
//! handler. This module reproduces that surface: the client-go collectors
//! come from the transcreated `tikv-client` metrics registry (constructed
//! with Go TiDB's namespace), and the four GC-worker families Go declares in
//! `pkg/metrics/gc_worker.go` are declared here with their source
//! definitions. `gather_text` renders the exposition block the status
//! server appends, and `init_dashboard_series` materializes the same
//! startup series Go's bootstrap writes. Store-scoped gauges (`store`
//! label) gain series on first real client activity, exactly like Go's.

use std::sync::LazyLock;

use prometheus::TextEncoder;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
};
use tikv_client::metrics::ClientGoMetrics;

// GC-worker families from Go `pkg/metrics/gc_worker.go`, subsystem
// `tikvclient` like the client-go set.

/// Go `GCHistogram` (`pkg/metrics/gc_worker.go:44-51`): GC duration by
/// stage, 1s ~ 6days exponential buckets.
pub static GC_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let metric = HistogramVec::new(
        HistogramOpts::new("gc_seconds", "Bucketed histogram of gc duration.")
            .namespace("tidb")
            .subsystem("tikvclient")
            .buckets(prometheus::exponential_buckets(1.0, 2.0, 20).expect("20 positive buckets")),
        &["stage"],
    )
    .expect("valid gc seconds histogram");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc seconds histogram is registered once");
    metric
});

/// Go `GCActionRegionResultCounter` (`pkg/metrics/gc_worker.go:68-73`).
pub static GC_ACTION_REGION_RESULT: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
        Opts::new("gc_action_result", "Counter of gc action result on region level.")
            .namespace("tidb")
            .subsystem("tikvclient"),
        &["type"],
    )
    .expect("valid gc action result counter");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc action result counter is registered once");
    metric
});

/// Go `GCConfigGauge`.
pub static GC_CONFIG: LazyLock<GaugeVec> = LazyLock::new(|| {
    let metric = GaugeVec::new(
        Opts::new("gc_config", "GC related config.")
            .namespace("tidb")
            .subsystem("tikvclient"),
        &["type"],
    )
    .expect("valid gc config metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc config metric is registered once");
    metric
});

/// Go `GCFailureCounter`.
pub static GC_FAILURE: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
        Opts::new("gc_failure", "Counter of gc worker failures")
            .namespace("tidb")
            .subsystem("tikvclient"),
        &["type"],
    )
    .expect("valid gc failure metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc failure metric is registered once");
    metric
});

/// Go `GCRegionTooManyLocksCounter`.
pub static GC_REGION_TOO_MANY_LOCKS: LazyLock<Counter> = LazyLock::new(|| {
    let metric = Counter::new(
        "tidb_tikvclient_gc_region_too_many_locks",
        "Counter of skipping gc when there are too many locks in a region",
    )
    .expect("valid gc region too many locks metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc region too many locks metric is registered once");
    metric
});

/// Go `GCWorkerActionsCounter`.
pub static GC_WORKER_ACTIONS_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
        Opts::new(
            "gc_worker_actions_total",
            "Counter of gc worker actions.",
        )
        .namespace("tidb")
        .subsystem("tikvclient"),
        &["type"],
    )
    .expect("valid gc worker actions metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("gc worker actions metric is registered once");
    metric
});

/// `tikv-client` pins prometheus 0.13 while this workspace serves 0.14
/// through the status server, so this module's families live on their own
/// 0.13 registry and the block is rendered separately and appended (see
/// [`gather_text`]).
/// Initializes the process-wide client-go collectors under Go TiDB's
/// `tidb`/`tikvclient` namespace and registers them on this crate's
/// prometheus default registry, so the status server's appended block and
/// the client's own runtime bumps (`global_metrics()`) address the same
/// collectors.
pub fn init_dashboard_series() {
    if !INITIALIZED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        tikv_client::metrics::init_metrics("tidb", "tikvclient")
            .expect("client-go metric initialization is valid");
        tikv_client::metrics::register_metrics();
    }
    materialize_dashboard_series();
}

static INITIALIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn collector_counter_vec(source: &str) -> Option<prometheus::CounterVec> {
    tikv_client::metrics::global_metrics()
        .collector(source)
        .and_then(|collector| match collector {
            tikv_client::metrics::ClientGoCollector::CounterVec(counter_vec) => {
                Some(counter_vec.clone())
            }
            _ => None,
        })
}

fn collector_gauge_vec(source: &str) -> Option<prometheus::GaugeVec> {
    tikv_client::metrics::global_metrics()
        .collector(source)
        .and_then(|collector| match collector {
            tikv_client::metrics::ClientGoCollector::GaugeVec(gauge_vec) => {
                Some(gauge_vec.clone())
            }
            _ => None,
        })
}

fn collector_counter(source: &str) -> Option<prometheus::Counter> {
    tikv_client::metrics::global_metrics()
        .collector(source)
        .and_then(|collector| match collector {
            tikv_client::metrics::ClientGoCollector::Counter(counter) => Some(counter.clone()),
            _ => None,
        })
}

/// Go `LockResolverCountWithReadAsyncResolveFallback`.
pub(crate) fn inc_lock_resolver_read_async_fallback() {
    if let Some(tikv_client::metrics::ClientGoShortcut::Counter(counter)) =
        tikv_client::metrics::global_metrics()
            .shortcut("LockResolverCountWithReadAsyncResolveFallback")
    {
        counter.inc();
    }
}

fn inc_lock_resolver_counter(shortcut_name: &'static str) {
    if let Some(tikv_client::metrics::ClientGoShortcut::Counter(counter)) =
        tikv_client::metrics::global_metrics().shortcut(shortcut_name)
    {
        counter.inc();
    }
}

/// Go `LockResolverCountWithQueryTxnStatus` (cache misses only).
pub(crate) fn inc_lock_resolver_query_txn_status() {
    inc_lock_resolver_counter("LockResolverCountWithQueryTxnStatus");
}

/// Go `LockResolverCountWithQueryTxnStatusCommitted`.
pub(crate) fn inc_lock_resolver_query_txn_status_committed() {
    inc_lock_resolver_counter("LockResolverCountWithQueryTxnStatusCommitted");
}

/// Go `LockResolverCountWithQueryTxnStatusRolledBack`.
pub(crate) fn inc_lock_resolver_query_txn_status_rolled_back() {
    inc_lock_resolver_counter("LockResolverCountWithQueryTxnStatusRolledBack");
}

/// Go `LockResolverCountWithExpired`.
pub(crate) fn inc_lock_resolver_expired() {
    inc_lock_resolver_counter("LockResolverCountWithExpired");
}

/// Go `LockResolverCountWithNotExpired`.
pub(crate) fn inc_lock_resolver_not_expired() {
    inc_lock_resolver_counter("LockResolverCountWithNotExpired");
}

/// Go `LockResolverCountWithWaitExpired`.
pub(crate) fn inc_lock_resolver_wait_expired() {
    inc_lock_resolver_counter("LockResolverCountWithWaitExpired");
}

/// Go `LockResolverCountWithResolve`.
pub(crate) fn inc_lock_resolver_resolve() {
    inc_lock_resolver_counter("LockResolverCountWithResolve");
}

/// Go `LockResolverCountWithResolveAsync`.
pub(crate) fn inc_lock_resolver_resolve_async() {
    inc_lock_resolver_counter("LockResolverCountWithResolveAsync");
}

/// Go `LockResolverCountWithQueryCheckSecondaryLocks`.
pub(crate) fn inc_lock_resolver_query_check_secondary_locks() {
    inc_lock_resolver_counter("LockResolverCountWithQueryCheckSecondaryLocks");
}

/// Go `LockResolverCountWithResolveLocks`.
pub(crate) fn inc_lock_resolver_resolve_locks() {
    inc_lock_resolver_counter("LockResolverCountWithResolveLocks");
}

/// Go `LockResolverCountWithResolveLockLite`.
pub(crate) fn inc_lock_resolver_resolve_lock_lite() {
    inc_lock_resolver_counter("LockResolverCountWithResolveLockLite");
}

/// Go `LockResolverCountWithAsyncResolveAsyncCommitFallback`.
pub(crate) fn inc_lock_resolver_async_resolve_async_commit_fallback() {
    inc_lock_resolver_counter("LockResolverCountWithAsyncResolveAsyncCommitFallback");
}

/// Go `LockResolverCountWithAsyncCheckSecondariesFallback`.
pub(crate) fn inc_lock_resolver_async_check_secondaries_fallback() {
    inc_lock_resolver_counter("LockResolverCountWithAsyncCheckSecondariesFallback");
}

/// Go `LockResolverCountWithAsyncResolveAsyncCommitRegionFallback`.
pub(crate) fn inc_lock_resolver_async_resolve_async_commit_region_fallback() {
    inc_lock_resolver_counter("LockResolverCountWithAsyncResolveAsyncCommitRegionFallback");
}

/// Returns the client-go task gauge for one resolver worker category.
pub(crate) fn lock_resolver_async_gauge(shortcut_name: &'static str) -> Option<prometheus::Gauge> {
    tikv_client::metrics::global_metrics()
        .shortcut(shortcut_name)
        .and_then(|shortcut| match shortcut {
            tikv_client::metrics::ClientGoShortcut::Gauge(gauge) => Some(gauge.clone()),
            _ => None,
        })
}

/// Renders the `tidb_tikvclient_*` exposition block for the status server.
#[must_use]
pub fn gather_text() -> String {
    TextEncoder::new()
        .encode_to_string(&prometheus::default_registry().gather())
        .unwrap_or_default()
}

/// Charges one pessimistic-lock acquisition to the client-go registry's
/// `TiKVPessimisticLockKeysDuration` histogram (Go `LockKeysDetail.TotalTime`
/// feeding `tidb_tikvclient_pessimistic_lock_keys_duration`, `adapter.go:588`).
/// The family lives on the client's own 0.13 registry, where the vendored spec
/// declares it, so the observation must go through [`global_metrics`] rather
/// than this crate's default registry.
pub fn observe_pessimistic_lock_keys_duration(seconds: f64) {
    if let Some(tikv_client::metrics::ClientGoCollector::Histogram(histogram)) =
        tikv_client::metrics::global_metrics().collector("TiKVPessimisticLockKeysDuration")
    {
        histogram.observe(seconds);
    }
}

#[cfg(test)]
mod lock_resolver_metric_tests {
    use super::{
        inc_lock_resolver_expired, inc_lock_resolver_not_expired,
        inc_lock_resolver_query_check_secondary_locks, inc_lock_resolver_query_txn_status,
        inc_lock_resolver_query_txn_status_committed,
        inc_lock_resolver_query_txn_status_rolled_back, inc_lock_resolver_resolve,
        inc_lock_resolver_resolve_async, inc_lock_resolver_resolve_lock_lite,
        inc_lock_resolver_resolve_locks,
        inc_lock_resolver_async_check_secondaries_fallback,
        inc_lock_resolver_async_resolve_async_commit_fallback,
        inc_lock_resolver_async_resolve_async_commit_region_fallback,
        inc_lock_resolver_wait_expired, init_dashboard_series, lock_resolver_async_gauge,
    };

    fn shortcut_count(shortcut_name: &'static str) -> f64 {
        match tikv_client::metrics::global_metrics().shortcut(shortcut_name) {
            Some(tikv_client::metrics::ClientGoShortcut::Counter(counter)) => counter.get(),
            _ => panic!("client-go counter shortcut {shortcut_name} is registered"),
        }
    }

    #[test]
    fn lock_resolver_status_counters_use_the_client_go_shortcuts() {
        init_dashboard_series();
        let counters = [
            (
                "LockResolverCountWithQueryTxnStatus",
                inc_lock_resolver_query_txn_status as fn(),
            ),
            (
                "LockResolverCountWithQueryTxnStatusCommitted",
                inc_lock_resolver_query_txn_status_committed,
            ),
            (
                "LockResolverCountWithQueryTxnStatusRolledBack",
                inc_lock_resolver_query_txn_status_rolled_back,
            ),
            ("LockResolverCountWithExpired", inc_lock_resolver_expired),
            (
                "LockResolverCountWithNotExpired",
                inc_lock_resolver_not_expired,
            ),
            (
                "LockResolverCountWithWaitExpired",
                inc_lock_resolver_wait_expired,
            ),
            ("LockResolverCountWithResolve", inc_lock_resolver_resolve),
            (
                "LockResolverCountWithResolveAsync",
                inc_lock_resolver_resolve_async,
            ),
            (
                "LockResolverCountWithQueryCheckSecondaryLocks",
                inc_lock_resolver_query_check_secondary_locks,
            ),
            (
                "LockResolverCountWithResolveLocks",
                inc_lock_resolver_resolve_locks,
            ),
            (
                "LockResolverCountWithResolveLockLite",
                inc_lock_resolver_resolve_lock_lite,
            ),
            (
                "LockResolverCountWithAsyncResolveAsyncCommitFallback",
                inc_lock_resolver_async_resolve_async_commit_fallback,
            ),
            (
                "LockResolverCountWithAsyncCheckSecondariesFallback",
                inc_lock_resolver_async_check_secondaries_fallback,
            ),
            (
                "LockResolverCountWithAsyncResolveAsyncCommitRegionFallback",
                inc_lock_resolver_async_resolve_async_commit_region_fallback,
            ),
        ];

        for (shortcut_name, increment) in counters {
            let before = shortcut_count(shortcut_name);
            increment();
            assert!(shortcut_count(shortcut_name) > before);
        }

        for shortcut_name in [
            "LockResolverAsyncRunningTasksForReadResolve",
            "LockResolverAsyncRunningTasksForResolveAsyncCommit",
            "LockResolverAsyncRunningTasksForCheckSecondaries",
            "LockResolverAsyncRunningTasksForResolveAsyncCommitRegion",
        ] {
            assert!(lock_resolver_async_gauge(shortcut_name).is_some());
        }
    }
}

/// Materializes the series Go's bootstrap writes for the `tidb_tikvclient_*`
/// families, using the exact label combinations the Go export carries.
/// Store-scoped gauge series (`store` label) materialize on first real
/// client activity, matching Go's per-store behavior.
fn materialize_dashboard_series() {
    // gc_worker.go families. Go's gc worker materializes the config gauge's
    // two children at boot (`gc_worker.go:307-308` Set(0)) and binds the
    // rest as the worker starts; the dashboard reads every family.
    LazyLock::force(&GC_CONFIG);
    LazyLock::force(&GC_FAILURE);
    LazyLock::force(&GC_SECONDS);
    LazyLock::force(&GC_ACTION_REGION_RESULT);
    LazyLock::force(&GC_WORKER_ACTIONS_TOTAL);
    LazyLock::force(&GC_REGION_TOO_MANY_LOCKS);
    let _ = GC_CONFIG.with_label_values(&["tikv_gc_run_interval"]).set(0.0);
    let _ = GC_CONFIG.with_label_values(&["tikv_gc_life_time"]).set(0.0);
    if let Some(counter_vec) = collector_counter_vec("TiKVLoadTxnSafePointCounter") {
        let _ = counter_vec.with_label_values(&["ok_compatible"]);
    }

    // client-go families (non-store-scoped).
    if let Some(counter_vec) = collector_counter_vec("TiKVAggressiveLockedKeysCounter") {
        for kind in ["derived", "locked_with_conflict", "new", "non_force_lock"] {
            let _ = counter_vec.with_label_values(&[kind]);
        }
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVAsyncCommitTxnCounter") {
        let _ = counter_vec.with_label_values(&["err"]);
        let _ = counter_vec.with_label_values(&["ok"]);
    }
    if let Some(counter) = collector_counter("TiKVNoAvailableConnectionCounter") {
        let _ = counter;
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVTwoPCTxnCounter") {
        let _ = counter_vec.with_label_values(&["err"]);
        let _ = counter_vec.with_label_values(&["ok"]);
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVLockResolverCounter") {
        for kind in [
            "async_check_secondaries_fallback",
            "async_resolve_async_commit_fallback",
            "async_resolve_async_commit_region_fallback",
            "batch_resolve",
            "expired",
            "not_expired",
            "query_check_secondary_locks",
            "query_resolve_lock_lite",
            "query_resolve_locks",
            "query_txn_status",
            "query_txn_status_committed",
            "query_txn_status_rolled_back",
            "read_async_resolve_fallback",
            "resolve",
            "resolve_async_commit",
            "resolve_for_write",
            "wait_expired",
        ] {
            let _ = counter_vec.with_label_values(&[kind]);
        }
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVOnePCTxnCounter") {
        for kind in ["err", "fallback", "ok"] {
            let _ = counter_vec.with_label_values(&[kind]);
        }
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVPrewriteAssertionUsageCounter") {
        for kind in ["exist", "none", "not-exist", "unknown"] {
            let _ = counter_vec.with_label_values(&[kind]);
        }
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVRegionCacheCounter") {
        for kind in [
            "batch_scan_regions",
            "get_region_by_id",
            "get_region_when_miss",
            "get_store",
            "invalidate_region_from_cache",
            "invalidate_store_regions",
            "scan_regions",
            "send_fail",
        ] {
            let _ = counter_vec.with_label_values(&[kind, "ok"]);
            let _ = counter_vec.with_label_values(&[kind, "err"]);
        }
    }
    if let Some(counter) = collector_counter("TiKVStaleRegionFromPDCounter") {
        let _ = counter;
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVStaleReadCounter") {
        let _ = counter_vec.with_label_values(&["hit"]);
        let _ = counter_vec.with_label_values(&["miss"]);
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVStaleReadReqCounter") {
        let _ = counter_vec.with_label_values(&["cross-zone"]);
        let _ = counter_vec.with_label_values(&["local"]);
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVStaleReadBytes") {
        for direction in ["in", "out"] {
            for result in ["cross-zone", "local"] {
                let _ = counter_vec.with_label_values(&[direction, result]);
            }
        }
    }
    if let Some(counter) = collector_counter("TiKVTTLLifeTimeReachCounter") {
        let _ = counter;
    }
    // Store-scoped: Go writes these per store once the client observes
    // stores; the label space is declared here without fabricating store IDs.
    let _ = collector_gauge_vec("TiKVStoreSlowScoreGauge");
    let _ = collector_gauge_vec("TiKVFeedbackSlowScoreGauge");
    let _ = collector_gauge_vec("TiKVStoreLivenessGauge");
    let _ = collector_gauge_vec("TiKVMinSafeTSGapSeconds");
}

/// The (fq name, help) of every client-go histogram family under the
/// `tidb`/`tikvclient` namespace, for the exposition header shim.
pub fn histogram_definitions() -> Vec<(String, String)> {
    tikv_client::metrics::CLIENT_GO_METRIC_SPECS
        .iter()
        .filter(|spec| {
            matches!(
                spec.subsystem,
                tikv_client::metrics::MetricSubsystem::Configured
            ) && matches!(
                spec.kind,
                tikv_client::metrics::MetricKind::Histogram
                    | tikv_client::metrics::MetricKind::HistogramVec
            )
        })
        .map(|spec| {
            (
                spec.metric_name,
                spec.help,
            )
        })
        .map(|(name, help)| ("tidb_tikvclient_".to_owned() + name, help.to_owned()))
        .collect()
}

/// Materializes the per-store series for an embedded (in-process) store.
/// Go's unistore deployment observes its single mock store through
/// client-go, so the store-scoped gauges exist for it; the embedded store's
/// own id (`tidb-unistore`'s `IN_PROCESS_STORE_ID`) is the faithful label.
pub fn init_embedded_store_series(store_id: u64) {
    init_dashboard_series();
    let store = store_id.to_string();
    // client-go initializes the client-side slow score at 1
    // (`newAtomicSlowScore(1)`); every other per-store series starts at zero
    // until the corresponding client activity occurs.
    if let Some(gauge_vec) = collector_gauge_vec("TiKVStoreSlowScoreGauge") {
        gauge_vec.with_label_values(&[&store]).set(1.0);
    }
    if let Some(gauge_vec) = collector_gauge_vec("TiKVFeedbackSlowScoreGauge") {
        gauge_vec.with_label_values(&[&store]).set(0.0);
    }
    if let Some(gauge_vec) = collector_gauge_vec("TiKVStoreLivenessGauge") {
        gauge_vec.with_label_values(&[&store]).set(0.0);
    }
    if let Some(gauge_vec) = collector_gauge_vec("TiKVMinSafeTSGapSeconds") {
        gauge_vec.with_label_values(&[&store]).set(0.0);
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVSafeTSUpdateCounter") {
        let _ = counter_vec.with_label_values(&["success", &store]);
    }
    if let Some(counter_vec) = collector_counter_vec("TiKVRegionErrorCounter") {
        let _ = counter_vec.with_label_values(&["epoch_not_match", &store]);
    }
}

/// Go `util.IsInternalRequest` (`request_source.go:167`): a request source
/// prefixed with `internal` marks an internal ("scope=true") RPC.
#[must_use]
pub fn is_internal_request(request_source: &str) -> bool {
    request_source.starts_with("internal")
}

/// Go `metrics.TwoPCTxnCounter*` commit-protocol counter shortcuts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TxnCommitProtocol {
    /// Classic two-phase commit.
    TwoPc,
    /// Async-commit protocol.
    AsyncCommit,
    /// One-phase commit inside prewrite.
    OnePc,
}

/// Go `twoPhaseCommitter.execute`'s deferred counters
/// (`2pc.go:1755-1795`): exactly one ok/err shortcut fires per commit
/// attempt, chosen by protocol and terminal classification. The 2PC arm
/// counts `committed || undetermined` as ok; the 1PC and async-commit arms
/// count any error as err.
pub fn record_txn_commit(protocol: TxnCommitProtocol, succeeded: bool) {
    let source_name = match (protocol, succeeded) {
        (TxnCommitProtocol::TwoPc, true) => "TwoPCTxnCounterOk",
        (TxnCommitProtocol::TwoPc, false) => "TwoPCTxnCounterError",
        (TxnCommitProtocol::AsyncCommit, true) => "AsyncCommitTxnCounterOk",
        (TxnCommitProtocol::AsyncCommit, false) => "AsyncCommitTxnCounterError",
        (TxnCommitProtocol::OnePc, true) => "OnePCTxnCounterOk",
        (TxnCommitProtocol::OnePc, false) => "OnePCTxnCounterError",
    };
    if let Some(tikv_client::metrics::ClientGoShortcut::Counter(counter)) =
        tikv_client::metrics::global_metrics().shortcut(source_name)
    {
        counter.inc();
    }
}

/// Go `storeMetrics.updateRPCMetrics` (`client.go:894-928`): charges one RPC
/// attempt's wall time to `tidb_tikvclient_request_seconds` (histogram) and
/// `tidb_tikvclient_source_request_seconds` (summary). The store label is the
/// peer store id; `scope` is `FormatBool(internal)`; the summary's `source`
/// label carries the raw request source ("" for user traffic).
pub fn observe_send_request_seconds(
    cmd_type: &str,
    store_id: u64,
    stale_read: bool,
    request_source: &str,
    seconds: f64,
) {
    let store = store_id.to_string();
    let stale = if stale_read { "true" } else { "false" };
    let scope = if is_internal_request(request_source) {
        "true"
    } else {
        "false"
    };
    let metrics = tikv_client::metrics::global_metrics();
    if let Some(hist) = metrics.histogram_vec("TiKVSendReqHistogram") {
        hist.with_label_values(&[cmd_type, &store, stale, scope])
            .observe(seconds);
    }
    if let Some(summary) = metrics.summary_vec("TiKVSendReqBySourceSummary") {
        if let Ok(observer) =
            summary.with_label_values(&[cmd_type, &store, stale, scope, request_source])
        {
            observer.observe(seconds);
        }
    }
}

/// Every client-go family under the `tidb`/`tikvclient` namespace as
/// (fq name, help, kind), for the exposition header shim: Go's registry
/// emits HELP/TYPE for registered-but-childless histogram vecs, and this
/// list lets the status server reproduce those headers.
pub fn definitions() -> Vec<(String, String, &'static str)> {
    tikv_client::metrics::CLIENT_GO_METRIC_SPECS
        .iter()
        .filter(|spec| {
            matches!(
                spec.subsystem,
                tikv_client::metrics::MetricSubsystem::Configured
            )
        })
        .filter_map(|spec| {
            let kind = match spec.kind {
                tikv_client::metrics::MetricKind::Counter => "counter",
                tikv_client::metrics::MetricKind::CounterVec => "counter",
                tikv_client::metrics::MetricKind::Gauge => "gauge",
                tikv_client::metrics::MetricKind::GaugeVec => "gauge",
                tikv_client::metrics::MetricKind::Histogram | tikv_client::metrics::MetricKind::HistogramVec => "histogram",
                _ => return None,
            };
            let fq = format!("tidb_tikvclient_{}", spec.metric_name);
            Some((fq, spec.help.to_owned(), kind))
        })
        .collect()
}
