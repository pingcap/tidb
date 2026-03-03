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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	mvMetricRunEventTaskExecSubmitted = "exec_submitted"
	mvMetricRunEventTaskExecFinished  = "exec_finished"
	mvMetricRunEventTaskExecFailed    = "exec_failed"
	mvMetricRunEventTaskExecTimeout   = "exec_timeout"
	mvMetricRunEventTaskExecRejected  = "exec_rejected"

	mvMetricTaskStatusTaskExecRunning         = "exec_running"
	mvMetricTaskStatusTaskExecWaiting         = "exec_waiting"
	mvMetricTaskStatusTaskExecTimedOutRunning = "exec_timed_out_running"

	mvMetricTaskStatusMVTotal       = "mv_total"
	mvMetricTaskStatusMVLogTotal    = "mvlog_total"
	mvMetricTaskStatusMVRefreshRun  = "mv_refresh_running"
	mvMetricTaskStatusMVLogPurgeRun = "mvlog_purge_running"
)

// Metrics for materialized view service.
var (
	MVServiceTaskStatusGaugeVec *prometheus.GaugeVec

	MVServiceOperationDurationHistogramVec *prometheus.HistogramVec

	MVServiceRunEventCounterVec *prometheus.CounterVec

	MVTaskExecutorSubmittedCounter prometheus.Counter
	MVTaskExecutorFinishedCounter  prometheus.Counter
	MVTaskExecutorFailedCounter    prometheus.Counter
	MVTaskExecutorTimeoutCounter   prometheus.Counter
	MVTaskExecutorRejectedCounter  prometheus.Counter

	MVTaskExecutorRunningTaskGauge         prometheus.Gauge
	MVTaskExecutorWaitingTaskGauge         prometheus.Gauge
	MVTaskExecutorTimedOutRunningTaskGauge prometheus.Gauge

	MVServiceMVRefreshTotalGauge    prometheus.Gauge
	MVServiceMVLogPurgeTotalGauge   prometheus.Gauge
	MVServiceMVRefreshRunningGauge  prometheus.Gauge
	MVServiceMVLogPurgeRunningGauge prometheus.Gauge
)

// InitMVMetrics initializes metrics for materialized view service.
func InitMVMetrics() {
	MVServiceTaskStatusGaugeVec = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_task_status",
			Help:      "Number of MV service and task executor tasks by status and role.",
		}, []string{LblType})

	MVServiceOperationDurationHistogramVec = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_operation_duration_seconds",
			Help:      "Bucketed histogram of MV service operation duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	MVServiceRunEventCounterVec = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_run_event_total",
			Help:      "Counter of MV service scheduler and task executor events.",
		}, []string{LblType})

	MVTaskExecutorSubmittedCounter = MVServiceRunEventCounterVec.WithLabelValues(mvMetricRunEventTaskExecSubmitted)
	MVTaskExecutorFinishedCounter = MVServiceRunEventCounterVec.WithLabelValues(mvMetricRunEventTaskExecFinished)
	MVTaskExecutorFailedCounter = MVServiceRunEventCounterVec.WithLabelValues(mvMetricRunEventTaskExecFailed)
	MVTaskExecutorTimeoutCounter = MVServiceRunEventCounterVec.WithLabelValues(mvMetricRunEventTaskExecTimeout)
	MVTaskExecutorRejectedCounter = MVServiceRunEventCounterVec.WithLabelValues(mvMetricRunEventTaskExecRejected)
	MVTaskExecutorRunningTaskGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricTaskStatusTaskExecRunning)
	MVTaskExecutorWaitingTaskGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricTaskStatusTaskExecWaiting)
	MVTaskExecutorTimedOutRunningTaskGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricTaskStatusTaskExecTimedOutRunning)

	MVServiceMVRefreshTotalGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricTaskStatusMVTotal)
	MVServiceMVLogPurgeTotalGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricTaskStatusMVLogTotal)
	MVServiceMVRefreshRunningGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricTaskStatusMVRefreshRun)
	MVServiceMVLogPurgeRunningGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricTaskStatusMVLogPurgeRun)
}
