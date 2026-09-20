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

use std::collections::HashMap;
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
    REGISTRY
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
    REGISTRY
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
    REGISTRY
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
    REGISTRY
        .register(Box::new(metric.clone()))
        .expect("gc worker actions metric is registered once");
    metric
});

/// `tikv-client` pins prometheus 0.13 while this workspace serves 0.14
/// through the status server, so this module's families live on their own
/// 0.13 registry and the block is rendered separately and appended (see
/// [`gather_text`]).
static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

/// The client-go collectors under Go TiDB's `tidb`/`tikvclient` namespace.
static CLIENT_GO: LazyLock<ClientGoMetrics> = LazyLock::new(|| {
    let metrics = ClientGoMetrics::new("tidb", "tikvclient", HashMap::new())
        .expect("client-go metric definitions are valid");
    metrics
        .register_metrics(&REGISTRY)
        .expect("client-go metrics register on a fresh registry");
    metrics
});

fn collector_counter_vec(source: &str) -> Option<prometheus::CounterVec> {
    CLIENT_GO
        
        .collector(source)
        .and_then(|collector| match collector {
            tikv_client::metrics::ClientGoCollector::CounterVec(counter_vec) => {
                Some(counter_vec.clone())
            }
            _ => None,
        })
}

fn collector_gauge_vec(source: &str) -> Option<prometheus::GaugeVec> {
    CLIENT_GO
        
        .collector(source)
        .and_then(|collector| match collector {
            tikv_client::metrics::ClientGoCollector::GaugeVec(gauge_vec) => {
                Some(gauge_vec.clone())
            }
            _ => None,
        })
}

fn collector_counter(source: &str) -> Option<prometheus::Counter> {
    CLIENT_GO
        
        .collector(source)
        .and_then(|collector| match collector {
            tikv_client::metrics::ClientGoCollector::Counter(counter) => Some(counter.clone()),
            _ => None,
        })
}

/// Renders the `tidb_tikvclient_*` exposition block for the status server.
#[must_use]
pub fn gather_text() -> String {
    LazyLock::force(&CLIENT_GO);
    TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_default()
}

/// Materializes the series Go's bootstrap writes for the `tidb_tikvclient_*`
/// families, using the exact label combinations the Go export carries.
/// Store-scoped gauge series (`store` label) materialize on first real
/// client activity, matching Go's per-store behavior.
pub fn init_dashboard_series() {
    LazyLock::force(&CLIENT_GO);
    // gc_worker.go families.
    let _ = GC_CONFIG.with_label_values(&["tikv_gc_life_time"]);
    let _ = GC_CONFIG.with_label_values(&["tikv_gc_run_interval"]);
    let _ = GC_FAILURE.with_label_values(&["prepare"]);
    LazyLock::force(&GC_REGION_TOO_MANY_LOCKS);
    let _ = GC_WORKER_ACTIONS_TOTAL.with_label_values(&["check_leader"]);
    let _ = GC_WORKER_ACTIONS_TOTAL.with_label_values(&["register_leader"]);

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
    // LoadTxnSafePointCounter gains its per-type series on client activity.
    if let Some(counter_vec) = collector_counter_vec("TiKVLoadTxnSafePointCounter") {
        let _ = counter_vec.with_label_values(&["ok_compatible"]);
    }
}
