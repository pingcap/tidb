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

//! Go `pkg/planner/core/metrics`: the plan cache's prometheus surfaces.
//!
//! Go binds fifteen children from five `pkg/metrics` families in
//! `InitMetricsVars` and hands them out through typed accessors. Families,
//! labels (including Go's own leading spaces), help strings, and the
//! 1ms→1.5d exponential lookup buckets are copied verbatim.

use std::sync::LazyLock;

use prometheus::{
    exponential_buckets, Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts,
    HistogramVec, Opts,
};

const LBL_TYPE: &str = "type";
const LBL_PREPARED: &str = "prepared";
const LBL_NON_PREPARED: &str = "non-prepared";
const LBL_UNSUPPORTED: &str = "non-prepared-unsupported";
const LBL_SESSION: &str = " session-plan-cache";
const LBL_INSTANCE: &str = " instance-plan-cache";
const LBL_LAST_EVICT: &str = " instance-plan-cache-last-evict";
const LBL_SESSION_LOOKUP: &str = " session-plan-cache-lookup";
const LBL_INSTANCE_LOOKUP: &str = " instance-plan-cache-lookup";
const LBL_CLONE: &str = " instance-plan-cache-clone";

/// Go `metrics.PlanCacheCounter`.
pub static PLAN_CACHE_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    CounterVec::new(
        Opts::new("plan_cache_total", "Counter of query using plan cache.")
            .namespace("tidb")
            .subsystem("server"),
        &[LBL_TYPE],
    )
    .expect("valid plan cache counter")
});

/// Go `metrics.PlanCacheMissCounter`.
pub static PLAN_CACHE_MISS_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    CounterVec::new(
        Opts::new("plan_cache_miss_total", "Counter of plan cache miss.")
            .namespace("tidb")
            .subsystem("server"),
        &[LBL_TYPE],
    )
    .expect("valid plan cache miss counter")
});

/// Go `metrics.PlanCacheInstanceMemoryUsage`.
pub static PLAN_CACHE_INSTANCE_MEMORY_USAGE: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new(
            "plan_cache_instance_memory_usage",
            "Total plan cache memory usage of all sessions in a instance",
        )
        .namespace("tidb")
        .subsystem("server"),
        &[LBL_TYPE],
    )
    .expect("valid plan cache memory gauge")
});

/// Go `metrics.PlanCacheInstancePlanNumCounter`.
pub static PLAN_CACHE_INSTANCE_PLAN_NUM_COUNTER: LazyLock<GaugeVec> = LazyLock::new(|| {
    GaugeVec::new(
        Opts::new(
            "plan_cache_instance_plan_num_total",
            "Counter of plan of all prepared plan cache in a instance",
        )
        .namespace("tidb")
        .subsystem("server"),
        &[LBL_TYPE],
    )
    .expect("valid plan cache plan num gauge")
});

/// Go `metrics.PseudoEstimation` (the statistics family this package binds
/// its two pseudo-estimation children from).
pub static PSEUDO_ESTIMATION: LazyLock<CounterVec> = LazyLock::new(|| {
    CounterVec::new(
        Opts::new(
            "pseudo_estimation_total",
            "Counter of pseudo estimation caused by outdated stats.",
        )
        .namespace("tidb")
        .subsystem("statistics"),
        &[LBL_TYPE],
    )
    .expect("valid pseudo estimation counter")
});

/// Go `metrics.PlanCacheProcessDuration` (1ms ~ 1.5days exponential buckets).
pub static PLAN_CACHE_PROCESS_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "plan_cache_process_duration_seconds",
            "Bucketed histogram of processing time (s) of plan cache operations.",
        )
        .namespace("tidb")
        .subsystem("server")
        .buckets(exponential_buckets(0.001, 2.0, 28).expect("28 positive buckets")),
        &[LBL_TYPE],
    )
    .expect("valid plan cache duration histogram")
});

struct Children {
    pseudo_nodata: Counter,
    pseudo_outdate: Counter,
    prepared_hit: Counter,
    non_prepared_hit: Counter,
    prepared_miss: Counter,
    non_prepared_miss: Counter,
    non_prepared_unsupported: Counter,
    session_plan_num: Gauge,
    session_memory: Gauge,
    instance_plan_num: Gauge,
    instance_memory: Gauge,
    instance_evict: Gauge,
    session_lookup: Histogram,
    instance_lookup: Histogram,
    instance_clone: Histogram,
}

static CHILDREN: LazyLock<Children> = LazyLock::new(|| Children {
    pseudo_nodata: PSEUDO_ESTIMATION.with_label_values(&["nodata"]),
    pseudo_outdate: PSEUDO_ESTIMATION.with_label_values(&["outdate"]),
    prepared_hit: PLAN_CACHE_COUNTER.with_label_values(&[LBL_PREPARED]),
    non_prepared_hit: PLAN_CACHE_COUNTER.with_label_values(&[LBL_NON_PREPARED]),
    prepared_miss: PLAN_CACHE_MISS_COUNTER.with_label_values(&[LBL_PREPARED]),
    non_prepared_miss: PLAN_CACHE_MISS_COUNTER.with_label_values(&[LBL_NON_PREPARED]),
    non_prepared_unsupported: PLAN_CACHE_MISS_COUNTER.with_label_values(&[LBL_UNSUPPORTED]),
    session_plan_num: PLAN_CACHE_INSTANCE_PLAN_NUM_COUNTER.with_label_values(&[LBL_SESSION]),
    session_memory: PLAN_CACHE_INSTANCE_MEMORY_USAGE.with_label_values(&[LBL_SESSION]),
    instance_plan_num: PLAN_CACHE_INSTANCE_PLAN_NUM_COUNTER.with_label_values(&[LBL_INSTANCE]),
    instance_memory: PLAN_CACHE_INSTANCE_MEMORY_USAGE.with_label_values(&[LBL_INSTANCE]),
    instance_evict: PLAN_CACHE_INSTANCE_PLAN_NUM_COUNTER.with_label_values(&[LBL_LAST_EVICT]),
    session_lookup: PLAN_CACHE_PROCESS_DURATION.with_label_values(&[LBL_SESSION_LOOKUP]),
    instance_lookup: PLAN_CACHE_PROCESS_DURATION.with_label_values(&[LBL_INSTANCE_LOOKUP]),
    instance_clone: PLAN_CACHE_PROCESS_DURATION.with_label_values(&[LBL_CLONE]),
});

/// Go `PseudoEstimationNotAvailable`.
#[must_use]
pub fn pseudo_estimation_not_available() -> &'static Counter {
    &CHILDREN.pseudo_nodata
}

/// Go `PseudoEstimationOutdate`.
#[must_use]
pub fn pseudo_estimation_outdate() -> &'static Counter {
    &CHILDREN.pseudo_outdate
}

/// Go `GetPlanCacheHitCounter`.
#[must_use]
pub fn plan_cache_hit_counter(is_non_prepared: bool) -> &'static Counter {
    if is_non_prepared {
        &CHILDREN.non_prepared_hit
    } else {
        &CHILDREN.prepared_hit
    }
}

/// Go `GetPlanCacheMissCounter`.
#[must_use]
pub fn plan_cache_miss_counter(is_non_prepared: bool) -> &'static Counter {
    if is_non_prepared {
        &CHILDREN.non_prepared_miss
    } else {
        &CHILDREN.prepared_miss
    }
}

/// Go `GetNonPrepPlanCacheUnsupportedCounter`.
#[must_use]
pub fn non_prep_plan_cache_unsupported_counter() -> &'static Counter {
    &CHILDREN.non_prepared_unsupported
}

/// Go `GetPlanCacheInstanceNumCounter`.
#[must_use]
pub fn plan_cache_instance_num_counter(instance_plan_cache: bool) -> &'static Gauge {
    if instance_plan_cache {
        &CHILDREN.instance_plan_num
    } else {
        &CHILDREN.session_plan_num
    }
}

/// Go `GetPlanCacheInstanceMemoryUsage`.
#[must_use]
pub fn plan_cache_instance_memory_usage(instance_plan_cache: bool) -> &'static Gauge {
    if instance_plan_cache {
        &CHILDREN.instance_memory
    } else {
        &CHILDREN.session_memory
    }
}

/// Go `GetPlanCacheInstanceEvict`.
#[must_use]
pub fn plan_cache_instance_evict() -> &'static Gauge {
    &CHILDREN.instance_evict
}

/// Go `GetPlanCacheLookupDuration`.
#[must_use]
pub fn plan_cache_lookup_duration(instance_plan_cache: bool) -> &'static Histogram {
    if instance_plan_cache {
        &CHILDREN.instance_lookup
    } else {
        &CHILDREN.session_lookup
    }
}

/// Go `GetPlanCacheCloneDuration`.
#[must_use]
pub fn plan_cache_clone_duration() -> &'static Histogram {
    &CHILDREN.instance_clone
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `InitMetricsVars` binds distinct children per kind and the
    /// accessors route prepared vs non-prepared callers to different
    /// counters/gauges for the same family.
    #[test]
    fn accessors_route_prepared_and_non_prepared_to_distinct_children() {
        assert!(
            std::ptr::eq(plan_cache_hit_counter(false), plan_cache_hit_counter(false)),
            "the same child is handed out per kind"
        );
        assert!(!std::ptr::eq(
            plan_cache_hit_counter(false),
            plan_cache_hit_counter(true)
        ));
        assert!(!std::ptr::eq(
            plan_cache_miss_counter(false),
            plan_cache_miss_counter(true)
        ));
        assert!(!std::ptr::eq(
            plan_cache_instance_num_counter(false),
            plan_cache_instance_num_counter(true)
        ));
        assert!(!std::ptr::eq(
            plan_cache_instance_memory_usage(false),
            plan_cache_instance_memory_usage(true)
        ));
        assert!(!std::ptr::eq(
            plan_cache_lookup_duration(false),
            plan_cache_lookup_duration(true)
        ));
        // The unsupported counter is single and the clone/evict faces have
        // their own children.
        assert!(!std::ptr::eq(
            non_prep_plan_cache_unsupported_counter(),
            plan_cache_miss_counter(true)
        ));
        assert!(!std::ptr::eq(
            plan_cache_instance_evict(),
            plan_cache_instance_num_counter(true)
        ));
        assert!(!std::ptr::eq(
            pseudo_estimation_not_available(),
            pseudo_estimation_outdate()
        ));
    }
}
