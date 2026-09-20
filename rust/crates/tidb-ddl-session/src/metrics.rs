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

//! Go `pkg/metrics/ddl.go` (dashboard subset): DDL job and worker families.
//!
//! Every definition mirrors its Go `pkg/metrics` declaration one for one
//! (name, help, labels). `init_dashboard_series` materializes the series
//! Go's subsystem startup writes, so the dashboards under
//! `pkg/metrics/grafana` resolve the same family set against the Rust node
//! as against Go master.
//!
//! Copyright note: metric names, help strings, and label schemas are
//! transcribed from the Apache-2.0-licensed pingcap/tidb source tree.

use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts};
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

/// Go `DDLRunningJobCount` (`pkg/metrics`).
pub static DDL_RUNNING_JOB_COUNT: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new("tidb_ddl_running_job_count", "Running DDL jobs count"),
        &["type"],
    ))
});

/// Go `JobsGauge` (`pkg/metrics`).
pub static JOBS_GAUGE: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new("tidb_ddl_waiting_jobs", "Gauge of jobs."),
        &["type"],
    ))
});

/// Go `DDLCounter` (`pkg/metrics`).
pub static DDL_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_ddl_worker_operation_total", "Counter of creating ddl/worker and isowner."),
        &["type"],
    ))
});

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    let _ = DDL_RUNNING_JOB_COUNT.with_label_values(&["general"]);
    let _ = JOBS_GAUGE.with_label_values(&["alter resource group"]);
    let _ = DDL_COUNTER.with_label_values(&["create_ddl_instance"]);
}
