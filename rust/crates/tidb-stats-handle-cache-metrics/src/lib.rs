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

use std::sync::{LazyLock, RwLock};

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts};

#[derive(Clone)]
struct MetricsVars {
    miss_counter: Counter,
    hit_counter: Counter,
    update_counter: Counter,
    del_counter: Counter,
    evict_counter: Counter,
    reject_counter: Counter,
    cost_gauge: Gauge,
    capacity_gauge: Gauge,
}

static STATS_CACHE_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
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

static METRICS_VARS: LazyLock<RwLock<MetricsVars>> =
    LazyLock::new(|| RwLock::new(bind_metrics_vars()));

/// Go `InitMetricsVars`.
pub fn init_metrics_vars() {
    *METRICS_VARS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = bind_metrics_vars();
}

macro_rules! metric_accessor {
    ($name:ident, $field:ident, $ty:ty) => {
        #[doc = concat!("Clones Go `", stringify!($field), "`.")]
        #[must_use]
        pub fn $name() -> $ty {
            METRICS_VARS
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .$field
                .clone()
        }
    };
}

metric_accessor!(miss_counter, miss_counter, Counter);
metric_accessor!(hit_counter, hit_counter, Counter);
metric_accessor!(update_counter, update_counter, Counter);
metric_accessor!(del_counter, del_counter, Counter);
metric_accessor!(evict_counter, evict_counter, Counter);
metric_accessor!(reject_counter, reject_counter, Counter);
metric_accessor!(cost_gauge, cost_gauge, Gauge);
metric_accessor!(capacity_gauge, capacity_gauge, Gauge);

#[cfg(test)]
mod tests {
    use super::*;

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
        miss_counter().inc();
        hit_counter().inc();
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
            assert_eq!(after, before + 1.0);
        }

        cost_gauge().set(12.0);
        capacity_gauge().set(34.0);
        assert_eq!(cost_gauge().get(), 12.0);
        assert_eq!(capacity_gauge().get(), 34.0);
    }
}
