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

//! Go `pkg/metrics/memory.go`: global memory arbitrator runtime families.
//!
//! Every definition mirrors its Go `pkg/metrics` declaration one for one
//! (name, help, labels). `init_dashboard_series` materializes the series
//! Go's subsystem startup writes, so the dashboards under
//! `pkg/metrics/grafana` resolve the same family set against the Rust node
//! as against Go master.
//!
//! Copyright note: metric names, help strings, and label schemas are
//! transcribed from the Apache-2.0-licensed pingcap/tidb source tree.

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts};
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

/// Go `ArbitratorEventCount` (`pkg/metrics`).
pub static ARBITRATOR_EVENT: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_memory_arbitrator_event",
            "Event count of the global memory arbitrator",
        ),
        &["type"],
    ))
});

/// Go `ArbitratorMagnifiRatio` (`pkg/metrics`).
pub static ARBITRATOR_MAGNIFI_RATIO: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::with_opts(Opts::new(
        "tidb_memory_arbitrator_magnifi_ratio",
        "Runtime profile (heapinuse vs. quota) of the global memory arbitrator",
    )))
});

/// Go `ArbitratorQuotaBytes` (`pkg/metrics`).
pub static ARBITRATOR_QUOTA_BYTES: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new(
            "tidb_memory_arbitrator_quota_bytes",
            "Quota info of the global memory arbitrator",
        ),
        &["type"],
    ))
});

/// Go `ArbitratorRootPool` (`pkg/metrics`).
pub static ARBITRATOR_ROOT_POOL: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new(
            "tidb_memory_arbitrator_root_pool",
            "Root pool info of the global memory arbitrator",
        ),
        &["type"],
    ))
});

/// Go `ArbitratorTaskExecCount` (`pkg/metrics`).
pub static ARBITRATOR_TASK_EXEC: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_memory_arbitrator_task_exec",
            "Task execution count of the global memory arbitrator",
        ),
        &["type"],
    ))
});

/// Go `ArbitratorWaitingTaskNum` (`pkg/metrics`).
pub static ARBITRATOR_WAITING_TASK: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new(
            "tidb_memory_arbitrator_waiting_task",
            "Waiting task num of the global memory arbitrator",
        ),
        &["type"],
    ))
});

/// Go `ArbitratorWorkMode` (`pkg/metrics`).
pub static ARBITRATOR_WORK_MODE: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new(
            "tidb_memory_arbitrator_work_mode",
            "Work mode of the global memory arbitrator",
        ),
        &["type"],
    ))
});

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    let _ = ARBITRATOR_EVENT.with_label_values(&["awaitfree-pool-force-shrink"]);
    LazyLock::force(&ARBITRATOR_MAGNIFI_RATIO);
    let _ = ARBITRATOR_QUOTA_BYTES.with_label_values(&["allocated"]);
    let _ = ARBITRATOR_ROOT_POOL.with_label_values(&["digest-cache"]);
    let _ = ARBITRATOR_TASK_EXEC.with_label_values(&["cancel-prio-high"]);
    let _ = ARBITRATOR_WAITING_TASK.with_label_values(&["priority-high"]);
    let _ = ARBITRATOR_WORK_MODE.with_label_values(&["disable"]);
}

pub static MEMORY_ARBITRATION_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_memory_arbitration_duration_seconds",
            "Bucketed histogram of mem quota arbitration time (s) in SQL execution",
        )
        .buckets(
            prometheus::exponential_buckets(5e-05, 3.77873541252838, 17).expect("valid buckets"),
        ),
        &["type"],
    ))
});

/// The (fq name, help, kind) of every histogram family in this module,
/// for the exposition header shim that mirrors Go's registered-family output.
pub fn histogram_definitions() -> Vec<(&'static str, &'static str)> {
    vec![(
        "tidb_memory_arbitration_duration_seconds",
        "Bucketed histogram of mem quota arbitration time (s) in SQL execution",
    )]
}
