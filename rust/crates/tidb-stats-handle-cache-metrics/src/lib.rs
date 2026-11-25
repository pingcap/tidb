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

//! Go `pkg/statistics/handle/cache/metrics`.

use std::sync::{LazyLock, Mutex};

use prometheus::core::{Atomic, AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{exponential_buckets, Gauge, GaugeVec, Histogram, HistogramOpts, Opts};
use tidb_zeropool::ThreadShards;

/// Value of a statistics cache operation counter.
pub struct CacheOpValue {
    increments: ThreadShards<AtomicU64>,
    base: Mutex<u64>,
}

impl CacheOpValue {
    fn total(&self) -> u64 {
        self.increments
            .iter()
            .map(Atomic::get)
            .fold(0, u64::wrapping_add)
    }
}

impl Atomic for CacheOpValue {
    type T = u64;

    fn new(value: u64) -> Self {
        Self {
            increments: ThreadShards::new(|| AtomicU64::new(0)),
            base: Mutex::new(value),
        }
    }

    fn set(&self, value: u64) {
        let mut base = self
            .base
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Apply resets as an offset so they never clear a shard's increments.
        *base = value.wrapping_sub(self.total());
    }

    fn get(&self) -> u64 {
        let base = self
            .base
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        base.wrapping_add(self.total())
    }

    #[inline]
    fn inc_by(&self, delta: u64) {
        self.increments.local().inc_by(delta);
    }

    #[inline]
    fn dec_by(&self, delta: u64) {
        self.increments.local().dec_by(delta);
    }
}

/// Statistics cache operation counter exported as a single Prometheus series.
pub type CacheOpCounter = GenericCounter<CacheOpValue>;

#[derive(Clone)]
struct MetricsVars {
    miss_counter: CacheOpCounter,
    hit_counter: CacheOpCounter,
    update_counter: CacheOpCounter,
    del_counter: CacheOpCounter,
    evict_counter: CacheOpCounter,
    reject_counter: CacheOpCounter,
    cost_gauge: Gauge,
    capacity_gauge: Gauge,
}

static STATS_CACHE_COUNTER: LazyLock<GenericCounterVec<CacheOpValue>> = LazyLock::new(|| {
    let metric = GenericCounterVec::new(
        Opts::new("stats_cache_op", "Counter for statsCache operation")
            .namespace("tidb")
            .subsystem("statistics"),
        &["type"],
    )
    .expect("valid stats cache counter");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("stats cache counter is registered once");
    metric
});

static STATS_CACHE_GAUGE: LazyLock<GaugeVec> = LazyLock::new(|| {
    let metric = GaugeVec::new(
        Opts::new("stats_cache_val", "gauge of stats cache value")
            .namespace("tidb")
            .subsystem("statistics"),
        &["type"],
    )
    .expect("valid stats cache gauge");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("stats cache gauge is registered once");
    metric
});

fn bind_metrics_vars() -> MetricsVars {
    MetricsVars {
        miss_counter: STATS_CACHE_COUNTER.with_label_values(&["miss"]),
        hit_counter: STATS_CACHE_COUNTER.with_label_values(&["hit"]),
        update_counter: STATS_CACHE_COUNTER.with_label_values(&["update"]),
        del_counter: STATS_CACHE_COUNTER.with_label_values(&["del"]),
        evict_counter: STATS_CACHE_COUNTER.with_label_values(&["evict"]),
        reject_counter: STATS_CACHE_COUNTER.with_label_values(&["reject"]),
        cost_gauge: STATS_CACHE_GAUGE.with_label_values(&["track"]),
        capacity_gauge: STATS_CACHE_GAUGE.with_label_values(&["capacity"]),
    }
}

static METRICS_VARS: LazyLock<MetricsVars> = LazyLock::new(bind_metrics_vars);

/// Go `InitMetricsVars`.
pub fn init_metrics_vars() {
    // The collector's label children remain registered for its lifetime.
    LazyLock::force(&METRICS_VARS);
}

/// Records one cache hit or miss on the registered counter.
#[inline]
pub fn record_lookup(hit: bool) {
    let counter = if hit {
        &METRICS_VARS.hit_counter
    } else {
        &METRICS_VARS.miss_counter
    };
    counter.inc();
}

macro_rules! metric_accessor {
    ($name:ident, $field:ident, $ty:ty) => {
        #[doc = concat!("Clones Go `", stringify!($field), "`.")]
        #[must_use]
        pub fn $name() -> $ty {
            METRICS_VARS.$field.clone()
        }
    };
}

metric_accessor!(miss_counter, miss_counter, CacheOpCounter);
metric_accessor!(hit_counter, hit_counter, CacheOpCounter);
metric_accessor!(update_counter, update_counter, CacheOpCounter);
metric_accessor!(del_counter, del_counter, CacheOpCounter);
metric_accessor!(evict_counter, evict_counter, CacheOpCounter);
metric_accessor!(reject_counter, reject_counter, CacheOpCounter);
metric_accessor!(cost_gauge, cost_gauge, Gauge);
metric_accessor!(capacity_gauge, capacity_gauge, Gauge);

/// Go `pkg/metrics.StatsDeltaLoadHistogram`, observed by the cache package's
/// `Update` (`statscache.go:127`) on every exit path. Go places this family in
/// the central `pkg/metrics` crate; the port keeps it beside its only
/// consumer, with the exact Go name, help string, and buckets
/// (`prometheus.ExponentialBuckets(0.01, 2, 24)` — 10ms to ~23.5h).
static STATS_DELTA_LOAD_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    let metric = Histogram::with_opts(
        HistogramOpts::new(
            "stats_delta_load_duration_seconds",
            "Bucketed histogram of processing time for the background statistics loading job",
        )
        .namespace("tidb")
        .subsystem("statistics")
        .buckets(exponential_buckets(0.01, 2.0, 24).expect("24 positive buckets")),
    )
    .expect("valid stats delta load histogram");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("stats delta load histogram is registered once");
    metric
});

/// Clones Go `StatsDeltaLoadHistogram`.
#[must_use]
pub fn stats_delta_load_histogram() -> Histogram {
    STATS_DELTA_LOAD_HISTOGRAM.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_counter_sums_concurrent_increments_after_reset() {
        let counter = CacheOpCounter::new("cache_ops", "Cache operations").unwrap();
        let other = counter.clone();
        counter.inc_by(17);
        counter.reset();
        assert_eq!(other.get(), 0);

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..2048 {
                        counter.inc();
                    }
                });
            }
        });
        assert_eq!(other.get(), 16_384);
        let metric = prometheus::core::Metric::metric(&other);
        assert_eq!(metric.get_counter().get_value(), 16_384.0);
    }

    #[test]
    fn source_labels_bind_distinct_children() {
        init_metrics_vars();
        let before = [
            miss_counter().get(),
            hit_counter().get(),
            update_counter().get(),
            del_counter().get(),
            evict_counter().get(),
            reject_counter().get(),
        ];
        record_lookup(false);
        record_lookup(true);
        update_counter().inc();
        del_counter().inc();
        evict_counter().inc();
        reject_counter().inc();
        let after = [
            miss_counter().get(),
            hit_counter().get(),
            update_counter().get(),
            del_counter().get(),
            evict_counter().get(),
            reject_counter().get(),
        ];
        for (before, after) in before.into_iter().zip(after) {
            assert_eq!(after, before + 1);
        }

        cost_gauge().set(12.0);
        capacity_gauge().set(34.0);
        assert_eq!(cost_gauge().get(), 12.0);
        assert_eq!(capacity_gauge().get(), 34.0);
    }
}
