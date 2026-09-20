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

//! Go `pkg/dxf/framework/dxfmetric` and `pkg/metrics/globalsort.go`: distributed-task and global-sort families.
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

/// Go `UsedSlotsGauge` (`pkg/metrics`).
pub static USED_SLOTS: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new("tidb_disttask_used_slots", "Gauge of used slots on a executor node."),
        &["service_scope"],
    ))
});

/// Go `MergeSortReadBytes` (`pkg/metrics`).
pub static MERGE_SORT_READ_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    register(Counter::new("tidb_global_sort_merge_sort_read_bytes", "Counter of bytes read in merge sort."))
});

/// Go `MergeSortWriteBytes` (`pkg/metrics`).
pub static MERGE_SORT_WRITE_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    register(Counter::new("tidb_global_sort_merge_sort_write_bytes", "Counter of bytes written in merge sort."))
});

/// Go `GlobalSortUploadWorkerCount` (`pkg/metrics`).
pub static UPLOAD_WORKER_COUNT: LazyLock<Gauge> = LazyLock::new(|| {
    register(Gauge::with_opts(
        Opts::new("tidb_global_sort_upload_worker_cnt", "Gauge of active parallel upload worker count."),
    ))
});

/// Materializes the series Go's subsystem startup writes, mirroring the
/// exported label combinations exactly.
pub fn init_dashboard_series() {
    let _ = USED_SLOTS.with_label_values(&[""]);
    LazyLock::force(&MERGE_SORT_READ_BYTES);
    LazyLock::force(&MERGE_SORT_WRITE_BYTES);
    LazyLock::force(&UPLOAD_WORKER_COUNT);
}
