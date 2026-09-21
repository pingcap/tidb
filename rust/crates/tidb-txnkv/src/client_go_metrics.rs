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
use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts, Registry};
use tikv_client::metrics::ClientGoMetrics;

// GC-worker families from Go `pkg/metrics/gc_worker.go`, subsystem
// `tikvclient` like the client-go set.

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

/// Materializes the series Go's bootstrap writes for the `tidb_tikvclient_*`
/// families, using the exact label combinations the Go export carries.
/// Store-scoped gauge series (`store` label) materialize on first real
/// client activity, matching Go's per-store behavior.
fn materialize_dashboard_series() {
    // gc_worker.go families.

    LazyLock::force(&GC_REGION_TOO_MANY_LOCKS);
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

