// Copyright 2017 PingCAP, Inc.
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

// Metrics for materialized view service.
var (
	MVServiceTaskStatusGaugeVec *prometheus.GaugeVec

	MVServiceMetaFetchDurationHistogramVec *prometheus.HistogramVec

	MVServiceRunEventCounterVec *prometheus.CounterVec

	MVTaskExecutorSubmittedCounter prometheus.Counter
	MVTaskExecutorCompletedCounter prometheus.Counter
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
			Help:      "Number of MV service and task executor tasks by status.",
		}, []string{LblType})

	MVServiceMetaFetchDurationHistogramVec = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_meta_fetch_duration_seconds",
			Help:      "Bucketed histogram of MV service metadata fetch duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	MVServiceRunEventCounterVec = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_run_event_total",
			Help:      "Counter of MV service and task executor events.",
		}, []string{LblType})

	MVTaskExecutorSubmittedCounter = MVServiceRunEventCounterVec.WithLabelValues("task_executor_submitted")
	MVTaskExecutorCompletedCounter = MVServiceRunEventCounterVec.WithLabelValues("task_executor_completed")
	MVTaskExecutorFailedCounter = MVServiceRunEventCounterVec.WithLabelValues("task_executor_failed")
	MVTaskExecutorTimeoutCounter = MVServiceRunEventCounterVec.WithLabelValues("task_executor_timeout")
	MVTaskExecutorRejectedCounter = MVServiceRunEventCounterVec.WithLabelValues("task_executor_rejected")
	MVTaskExecutorRunningTaskGauge = MVServiceTaskStatusGaugeVec.WithLabelValues("running")
	MVTaskExecutorWaitingTaskGauge = MVServiceTaskStatusGaugeVec.WithLabelValues("waiting")
	MVTaskExecutorTimedOutRunningTaskGauge = MVServiceTaskStatusGaugeVec.WithLabelValues("timed_out_running")

	MVServiceMVRefreshTotalGauge = MVServiceTaskStatusGaugeVec.WithLabelValues("mv_total")
	MVServiceMVLogPurgeTotalGauge = MVServiceTaskStatusGaugeVec.WithLabelValues("mvlog_total")
	MVServiceMVRefreshRunningGauge = MVServiceTaskStatusGaugeVec.WithLabelValues("mv_refresh_running")
	MVServiceMVLogPurgeRunningGauge = MVServiceTaskStatusGaugeVec.WithLabelValues("mvlog_purge_running")
}
