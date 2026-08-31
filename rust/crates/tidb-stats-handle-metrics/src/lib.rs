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

//! Go `pkg/statistics/handle/metrics`: statistics-health bucket identities
//! and the child metric handles bound to TiDB's shared metric families.

use std::sync::{LazyLock, RwLock};

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts};

/// Go `StatsHealthyBucket0To50`.
pub const STATS_HEALTHY_BUCKET_0_TO_50: usize = 0;
/// Go `StatsHealthyBucket50To55`.
pub const STATS_HEALTHY_BUCKET_50_TO_55: usize = 1;
/// Go `StatsHealthyBucket55To60`.
pub const STATS_HEALTHY_BUCKET_55_TO_60: usize = 2;
/// Go `StatsHealthyBucket60To70`.
pub const STATS_HEALTHY_BUCKET_60_TO_70: usize = 3;
/// Go `StatsHealthyBucket70To80`.
pub const STATS_HEALTHY_BUCKET_70_TO_80: usize = 4;
/// Go `StatsHealthyBucket80To100`.
pub const STATS_HEALTHY_BUCKET_80_TO_100: usize = 5;
/// Go `StatsHealthyBucket100To100`.
pub const STATS_HEALTHY_BUCKET_100_TO_100: usize = 6;
/// Go `StatsHealthyBucketTotal`.
pub const STATS_HEALTHY_BUCKET_TOTAL: usize = 7;
/// Go `StatsHealthyBucketUnneededAnalyze`.
pub const STATS_HEALTHY_BUCKET_UNNEEDED_ANALYZE: usize = 8;
/// Go `StatsHealthyBucketPseudo`.
pub const STATS_HEALTHY_BUCKET_PSEUDO: usize = 9;
/// Go `StatsHealthyBucketCount`.
pub const STATS_HEALTHY_BUCKET_COUNT: usize = 10;

/// Go `HealthyBucketConfig`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthyBucketConfig {
    /// Position in the health gauge slice.
    pub index: usize,
    /// Exclusive upper bound; zero denotes a special category.
    pub upper_bound: i64,
    /// Prometheus `type` label.
    pub label: &'static str,
}

/// Go `HealthyBucketConfigs`, including its compatibility-preserving total label.
pub const HEALTHY_BUCKET_CONFIGS: [HealthyBucketConfig; STATS_HEALTHY_BUCKET_COUNT] = [
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_0_TO_50,
        upper_bound: 50,
        label: "[0,50)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_50_TO_55,
        upper_bound: 55,
        label: "[50,55)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_55_TO_60,
        upper_bound: 60,
        label: "[55,60)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_60_TO_70,
        upper_bound: 70,
        label: "[60,70)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_70_TO_80,
        upper_bound: 80,
        label: "[70,80)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_80_TO_100,
        upper_bound: 100,
        label: "[80,100)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_100_TO_100,
        upper_bound: 101,
        label: "[100,100]",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_TOTAL,
        upper_bound: 0,
        label: "[0,100]",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_UNNEEDED_ANALYZE,
        upper_bound: 0,
        label: "unneeded analyze",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_PSEUDO,
        upper_bound: 0,
        label: "pseudo",
    },
];

#[derive(Clone)]
struct MetricsVars {
    stats_healthy_gauges: Vec<Gauge>,
    dump_historical_stats_success_counter: Counter,
    dump_historical_stats_failed_counter: Counter,
}

static STATS_HEALTHY_GAUGE: LazyLock<GaugeVec> = LazyLock::new(|| {
    let metric = GaugeVec::new(
        Opts::new("stats_healthy", "Gauge of stats healthy")
            .namespace("tidb")
            .subsystem("statistics"),
        &["type"],
    )
    .expect("valid stats healthy metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("stats healthy metric is registered once");
    metric
});

static HISTORICAL_STATS_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
        Opts::new(
            "historical_stats",
            "counter of the historical stats operation",
        )
        .namespace("tidb")
        .subsystem("statistics"),
        &["type", "result"],
    )
    .expect("valid historical statistics metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("historical statistics metric is registered once");
    metric
});

static PLAN_REPLAYER_TASK_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    let metric = CounterVec::new(
        Opts::new("task", "counter of plan replayer captured task")
            .namespace("tidb")
            .subsystem("plan_replayer"),
        &["type", "result"],
    )
    .expect("valid plan replayer task metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("plan replayer task metric is registered once");
    metric
});

static PLAN_REPLAYER_REGISTER_TASK_GAUGE: LazyLock<Gauge> = LazyLock::new(|| {
    let metric = Gauge::with_opts(
        Opts::new("register_task", "gauge of plan replayer registered task")
            .namespace("tidb")
            .subsystem("plan_replayer"),
    )
    .expect("valid plan replayer register-task metric");
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .expect("plan replayer register-task metric is registered once");
    metric
});

fn bind_metrics_vars() -> MetricsVars {
    assert_eq!(
        HEALTHY_BUCKET_CONFIGS.len(),
        STATS_HEALTHY_BUCKET_COUNT,
        "HealthyBucketConfigs length mismatch"
    );
    MetricsVars {
        stats_healthy_gauges: HEALTHY_BUCKET_CONFIGS
            .iter()
            .map(|config| STATS_HEALTHY_GAUGE.with_label_values(&[config.label]))
            .collect(),
        dump_historical_stats_success_counter: HISTORICAL_STATS_COUNTER
            .with_label_values(&["dump", "success"]),
        dump_historical_stats_failed_counter: HISTORICAL_STATS_COUNTER
            .with_label_values(&["dump", "fail"]),
    }
}

static METRICS_VARS: LazyLock<RwLock<MetricsVars>> =
    LazyLock::new(|| RwLock::new(bind_metrics_vars()));

/// Go `InitMetricsVars`: rebinds every child handle to the shared families.
pub fn init_metrics_vars() {
    *METRICS_VARS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = bind_metrics_vars();
}

/// Clones Go `StatsHealthyGauges` in bucket-index order.
#[must_use]
pub fn stats_healthy_gauges() -> Vec<Gauge> {
    METRICS_VARS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .stats_healthy_gauges
        .clone()
}

/// Clones Go `DumpHistoricalStatsSuccessCounter`.
#[must_use]
pub fn dump_historical_stats_success_counter() -> Counter {
    METRICS_VARS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .dump_historical_stats_success_counter
        .clone()
}

/// Clones Go `DumpHistoricalStatsFailedCounter`.
#[must_use]
pub fn dump_historical_stats_failed_counter() -> Counter {
    METRICS_VARS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .dump_historical_stats_failed_counter
        .clone()
}

/// Complete Go `pkg/domain/metrics`, kept as a distinct package-shaped module
/// while sharing the process-global collectors owned by Go `pkg/metrics`.
pub mod domain_metrics {
    use super::{
        Counter, Gauge, HISTORICAL_STATS_COUNTER, PLAN_REPLAYER_REGISTER_TASK_GAUGE,
        PLAN_REPLAYER_TASK_COUNTER,
    };
    use std::sync::{LazyLock, RwLock};

    #[derive(Clone)]
    struct MetricsVars {
        generate_historical_stats_success_counter: Counter,
        generate_historical_stats_failed_counter: Counter,
        plan_replayer_dump_task_success: Counter,
        plan_replayer_dump_task_failed: Counter,
        plan_replayer_capture_task_send_counter: Counter,
        plan_replayer_capture_task_discard_counter: Counter,
        plan_replayer_register_task_gauge: Gauge,
    }

    fn bind_metrics_vars() -> MetricsVars {
        MetricsVars {
            generate_historical_stats_success_counter: HISTORICAL_STATS_COUNTER
                .with_label_values(&["generate", "success"]),
            generate_historical_stats_failed_counter: HISTORICAL_STATS_COUNTER
                .with_label_values(&["generate", "fail"]),
            plan_replayer_dump_task_success: PLAN_REPLAYER_TASK_COUNTER
                .with_label_values(&["dump", "success"]),
            plan_replayer_dump_task_failed: PLAN_REPLAYER_TASK_COUNTER
                .with_label_values(&["dump", "fail"]),
            plan_replayer_capture_task_send_counter: PLAN_REPLAYER_TASK_COUNTER
                .with_label_values(&["capture", "send"]),
            plan_replayer_capture_task_discard_counter: PLAN_REPLAYER_TASK_COUNTER
                .with_label_values(&["capture", "discard"]),
            plan_replayer_register_task_gauge: PLAN_REPLAYER_REGISTER_TASK_GAUGE.clone(),
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

    macro_rules! counter_accessor {
        ($doc:literal, $name:ident, $field:ident) => {
            #[doc = $doc]
            #[must_use]
            pub fn $name() -> Counter {
                METRICS_VARS
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .$field
                    .clone()
            }
        };
    }

    counter_accessor!(
        "Go `GenerateHistoricalStatsSuccessCounter`.",
        generate_historical_stats_success_counter,
        generate_historical_stats_success_counter
    );
    counter_accessor!(
        "Go `GenerateHistoricalStatsFailedCounter`.",
        generate_historical_stats_failed_counter,
        generate_historical_stats_failed_counter
    );
    counter_accessor!(
        "Go `PlanReplayerDumpTaskSuccess`.",
        plan_replayer_dump_task_success,
        plan_replayer_dump_task_success
    );
    counter_accessor!(
        "Go `PlanReplayerDumpTaskFailed`.",
        plan_replayer_dump_task_failed,
        plan_replayer_dump_task_failed
    );
    counter_accessor!(
        "Go `PlanReplayerCaptureTaskSendCounter`.",
        plan_replayer_capture_task_send_counter,
        plan_replayer_capture_task_send_counter
    );
    counter_accessor!(
        "Go `PlanReplayerCaptureTaskDiscardCounter`.",
        plan_replayer_capture_task_discard_counter,
        plan_replayer_capture_task_discard_counter
    );

    /// Go `PlanReplayerRegisterTaskGauge`.
    #[must_use]
    pub fn plan_replayer_register_task_gauge() -> Gauge {
        METRICS_VARS
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .plan_replayer_register_task_gauge
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_bucket_configs_match_go_order_and_labels() {
        assert_eq!(HEALTHY_BUCKET_CONFIGS.len(), STATS_HEALTHY_BUCKET_COUNT);
        assert_eq!(
            HEALTHY_BUCKET_CONFIGS
                .iter()
                .map(|config| (config.index, config.upper_bound, config.label))
                .collect::<Vec<_>>(),
            vec![
                (0, 50, "[0,50)"),
                (1, 55, "[50,55)"),
                (2, 60, "[55,60)"),
                (3, 70, "[60,70)"),
                (4, 80, "[70,80)"),
                (5, 100, "[80,100)"),
                (6, 101, "[100,100]"),
                (7, 0, "[0,100]"),
                (8, 0, "unneeded analyze"),
                (9, 0, "pseudo"),
            ]
        );
    }

    #[test]
    fn source_init_binds_all_gauges_and_historical_counters() {
        init_metrics_vars();
        let gauges = stats_healthy_gauges();
        assert_eq!(gauges.len(), STATS_HEALTHY_BUCKET_COUNT);
        for (index, gauge) in gauges.iter().enumerate() {
            gauge.set(index as f64);
            assert_eq!(gauge.get(), index as f64);
        }

        let success = dump_historical_stats_success_counter();
        let failed = dump_historical_stats_failed_counter();
        let success_before = success.get();
        let failed_before = failed.get();
        success.inc();
        failed.inc();
        assert_eq!(success.get(), success_before + 1.0);
        assert_eq!(failed.get(), failed_before + 1.0);
    }

    #[test]
    fn domain_source_init_binds_all_seven_handles_separately() {
        domain_metrics::init_metrics_vars();
        let generated = domain_metrics::generate_historical_stats_success_counter();
        let generate_failed = domain_metrics::generate_historical_stats_failed_counter();
        let generated_before = generated.get();
        let generate_failed_before = generate_failed.get();
        generated.inc();
        generate_failed.inc();
        assert_eq!(generated.get(), generated_before + 1.0);
        assert_eq!(generate_failed.get(), generate_failed_before + 1.0);

        let dump_success = domain_metrics::plan_replayer_dump_task_success();
        let dump_failed = domain_metrics::plan_replayer_dump_task_failed();
        let capture_send = domain_metrics::plan_replayer_capture_task_send_counter();
        let capture_discard = domain_metrics::plan_replayer_capture_task_discard_counter();
        let registered = domain_metrics::plan_replayer_register_task_gauge();
        dump_success.inc();
        dump_failed.inc();
        capture_send.inc();
        capture_discard.inc();
        registered.set(7.0);
        assert_eq!(registered.get(), 7.0);
    }
}
