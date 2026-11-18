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

//! Go master `pkg/mvservice/metrics_reporter.go`: the service's runtime
//! metrics into Go's `tidb_mv_service_*` families. Go caches the labelled
//! series per (type, result); the prometheus vectors here hold that cache.

use std::sync::Mutex;

use tidb_metrics::materialized_view as m;

use crate::service::{
    snapshot_task_executor_metrics, MVMetricsReporter, MVService, TaskExecutorMetricsSnapshot,
    MV_TASK_DURATION_TYPE_MLOG_ANALYZE, MV_TASK_DURATION_TYPE_PURGE, MV_TASK_DURATION_TYPE_REFRESH,
};

const MV_METRIC_COMPONENT_SERVICE: &str = "service";

/// Go `taskExecutorReportCache`: the counters last flushed.
#[derive(Clone, Copy, Debug, Default)]
struct TaskExecutorReportCache {
    submitted_count: i64,
    finished_count: i64,
    failed_count: i64,
    timeout_count: i64,
    rejected_count: i64,
    backpressure_count: i64,
}

/// Go `serviceHelper`'s metrics half: the report caches of the three
/// executors.
#[derive(Default)]
pub struct MetricsReporter {
    report_cache: Mutex<[TaskExecutorReportCache; 3]>,
}

/// Go `reportCounterDelta`: only the positive delta since the last flush.
fn report_counter_delta(component: &str, event: &str, last: &mut i64, current: i64) {
    if current > *last {
        m::MV_SERVICE_RUN_EVENT_COUNTER_VEC.with_label_values(&[component, event]).inc_by((current - *last) as f64);
    }
    *last = current;
}

impl MetricsReporter {
    /// Go `reportTaskExecutorMetrics`.
    fn report_task_executor_metrics(component: &str, metrics: TaskExecutorMetricsSnapshot, cache: &mut TaskExecutorReportCache) {
        report_counter_delta(component, "submitted", &mut cache.submitted_count, metrics.submitted_count);
        report_counter_delta(component, "finished", &mut cache.finished_count, metrics.finished_count);
        report_counter_delta(component, "failed", &mut cache.failed_count, metrics.failed_count);
        report_counter_delta(component, "timeout", &mut cache.timeout_count, metrics.timeout_count);
        report_counter_delta(component, "rejected", &mut cache.rejected_count, metrics.rejected_count);
        report_counter_delta(component, "backpressure", &mut cache.backpressure_count, metrics.backpressure_count);
        let status = |name: &str, value: i64| {
            m::MV_SERVICE_TASK_STATUS_GAUGE_VEC.with_label_values(&[component, name]).set(value as f64);
        };
        status("running", metrics.running_count);
        status("waiting", metrics.waiting_count);
        status("timed_out_running", metrics.timed_out_running_count);
        status("backpressure_blocked", metrics.backpressure_blocked);
    }
}

impl MVMetricsReporter for MetricsReporter {
    /// Go `reportMetrics`.
    fn report_metrics(&self, service: &MVService) {
        use std::sync::atomic::Ordering::SeqCst;
        {
            let mut cache = self.report_cache.lock().unwrap();
            Self::report_task_executor_metrics(
                MV_TASK_DURATION_TYPE_REFRESH,
                snapshot_task_executor_metrics(&service.refresh_executor),
                &mut cache[0],
            );
            Self::report_task_executor_metrics(
                MV_TASK_DURATION_TYPE_PURGE,
                snapshot_task_executor_metrics(&service.purge_executor),
                &mut cache[1],
            );
            Self::report_task_executor_metrics(
                MV_TASK_DURATION_TYPE_MLOG_ANALYZE,
                snapshot_task_executor_metrics(&service.mlog_analyze_executor),
                &mut cache[2],
            );
        }
        m::MV_SERVICE_MV_REFRESH_TOTAL_GAUGE.set(service.metrics.mv_count.load(SeqCst) as f64);
        m::MV_SERVICE_MV_LOG_PURGE_TOTAL_GAUGE.set(service.metrics.mv_log_count.load(SeqCst) as f64);
        let (warning, overdue) = service.refresh_alert_metric_counts();
        m::MV_SERVICE_MV_REFRESH_WARNING_GAUGE.set(warning as f64);
        m::MV_SERVICE_MV_REFRESH_OVERDUE_GAUGE.set(overdue as f64);
        m::MV_SERVICE_MV_LOG_ACCUMULATION_GAUGE.set(service.metrics.mv_log_accumulation_count.load(SeqCst) as f64);
    }

    /// Go `observeTaskDuration`.
    fn observe_task_duration(&self, task_type: &str, result: &str, duration: i64) {
        if duration < 0 {
            return;
        }
        m::MV_SERVICE_OPERATION_DURATION_HISTOGRAM_VEC.with_label_values(&[task_type, result]).observe(duration as f64 / 1e9);
    }

    /// Go `observeRunEvent`.
    fn observe_run_event(&self, event_type: &str) {
        if event_type.is_empty() {
            return;
        }
        m::MV_SERVICE_RUN_EVENT_COUNTER_VEC.with_label_values(&[MV_METRIC_COMPONENT_SERVICE, event_type]).inc();
    }
}
