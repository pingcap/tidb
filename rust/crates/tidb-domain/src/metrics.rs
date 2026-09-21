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

//! Go `pkg/metrics/domain.go` (dashboard subset): schema cache and lease
//! families. The plan-replayer counters Go declares in stats.go live in
//! `tidb-stats-handle-metrics` here.
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

/// Go `InfoCacheCounters` (`pkg/metrics`).
pub static INFOCACHE_COUNTERS: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_domain_infocache_counters", "Counters of infoCache: get/hit."),
        &["action", "type"],
    ))
});

/// Go `InfoSchemaV2CacheCounter` (`pkg/metrics`).
pub static INFOSCHEMA_V2_CACHE_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_domain_infoschema_v2_cache", "infoschema cache v2 hit, evict and miss number"),
        &["type"],
    ))
});

/// Go `InfoSchemaV2CacheCount` (`pkg/metrics`).
pub static INFOSCHEMA_V2_CACHE_COUNT: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::with_opts(
        Opts::new("tidb_domain_infoschema_v2_cache_count", "infoschema cache v2 table count"),
    ))
});

/// Go `InfoSchemaV2CacheLimit` (`pkg/metrics`).
pub static INFOSCHEMA_V2_CACHE_LIMIT: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::with_opts(
        Opts::new("tidb_domain_infoschema_v2_cache_limit", "infoschema cache v2 limit"),
    ))
});

/// Go `InfoSchemaV2CacheSize` (`pkg/metrics`).
pub static INFOSCHEMA_V2_CACHE_SIZE: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::with_opts(
        Opts::new("tidb_domain_infoschema_v2_cache_size", "infoschema cache v2 size"),
    ))
});

/// Go `LeaseExpireTime` (`pkg/metrics`).
pub static LEASE_EXPIRE_TIME: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::with_opts(
        Opts::new("tidb_domain_lease_expire_time", "When the last time the lease is expired, it is in seconds"),
    ))
});

/// Go `LoadSchemaCounter` (`pkg/metrics`).
pub static LOAD_SCHEMA_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_domain_load_schema_total", "Counter of load schema"),
        &["type"],
    ))
});

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    let _ = INFOCACHE_COUNTERS.with_label_values(&["get", "latest"]);
    let _ = INFOSCHEMA_V2_CACHE_COUNTER.with_label_values(&["evict"]);
    LazyLock::force(&INFOSCHEMA_V2_CACHE_COUNT);
    LazyLock::force(&INFOSCHEMA_V2_CACHE_LIMIT);
    LazyLock::force(&INFOSCHEMA_V2_CACHE_SIZE);
    LazyLock::force(&LEASE_EXPIRE_TIME);
    let _ = LOAD_SCHEMA_COUNTER.with_label_values(&["reset"]);
}

pub static DOMAIN_LOAD_SCHEMA_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_domain_load_schema_duration_seconds",
            "Bucketed histogram of processing time (s) in load schema.",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 20).expect("valid buckets")),
        &["action"],
    ))
});

pub static INFOSCHEMA_TABLE_BY_NAME_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_infoschema_table_by_name_duration_nanoseconds",
            "infoschema v2 TableByName API duration",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 30).expect("valid buckets")),
        &["type"],
    ))
});

/// The (fq name, help, kind) of every histogram family in this module,
/// for the exposition header shim that mirrors Go's registered-family output.
pub fn histogram_definitions() -> Vec<(&'static str, &'static str)> {
    vec![
            ("tidb_domain_load_schema_duration_seconds", "Bucketed histogram of processing time (s) in load schema."),
            ("tidb_infoschema_table_by_name_duration_nanoseconds", "infoschema v2 TableByName API duration"),
    ]
}
