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
	MVTaskExecutorEventCounter *prometheus.CounterVec

	MVTaskExecutorTaskStatus *prometheus.GaugeVec

	MVServiceTaskStatus *prometheus.GaugeVec

	MVTaskExecutorSubmittedCounter prometheus.Counter
	MVTaskExecutorCompletedCounter prometheus.Counter
	MVTaskExecutorFailedCounter    prometheus.Counter
	MVTaskExecutorTimeoutCounter   prometheus.Counter
	MVTaskExecutorRejectedCounter  prometheus.Counter

	MVTaskExecutorRunningTaskGauge prometheus.Gauge
	MVTaskExecutorWaitingTaskGauge prometheus.Gauge

	MVServiceMVRefreshPendingGauge  prometheus.Gauge
	MVServiceMVLogPurgePendingGauge prometheus.Gauge
	MVServiceMVRefreshRunningGauge  prometheus.Gauge
	MVServiceMVLogPurgeRunningGauge prometheus.Gauge
)

// InitMVMetrics initializes metrics for materialized view service.
func InitMVMetrics() {
	MVTaskExecutorEventCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "task_executor_event_total",
			Help:      "Counter of MV task executor events.",
		}, []string{LblType})

	MVTaskExecutorTaskStatus = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "task_executor_task_status",
			Help:      "Number of MV tasks in task executor by status.",
		}, []string{LblType})

	MVServiceTaskStatus = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_task_status",
			Help:      "Number of MV service tasks by status.",
		}, []string{LblType})

	MVTaskExecutorSubmittedCounter = MVTaskExecutorEventCounter.WithLabelValues("submitted")
	MVTaskExecutorCompletedCounter = MVTaskExecutorEventCounter.WithLabelValues("completed")
	MVTaskExecutorFailedCounter = MVTaskExecutorEventCounter.WithLabelValues("failed")
	MVTaskExecutorTimeoutCounter = MVTaskExecutorEventCounter.WithLabelValues("timeout")
	MVTaskExecutorRejectedCounter = MVTaskExecutorEventCounter.WithLabelValues("rejected")
	MVTaskExecutorRunningTaskGauge = MVTaskExecutorTaskStatus.WithLabelValues("running")
	MVTaskExecutorWaitingTaskGauge = MVTaskExecutorTaskStatus.WithLabelValues("waiting")

	MVServiceMVRefreshPendingGauge = MVServiceTaskStatus.WithLabelValues("mv_refresh_pending")
	MVServiceMVLogPurgePendingGauge = MVServiceTaskStatus.WithLabelValues("mvlog_purge_pending")
	MVServiceMVRefreshRunningGauge = MVServiceTaskStatus.WithLabelValues("mv_refresh_running")
	MVServiceMVLogPurgeRunningGauge = MVServiceTaskStatus.WithLabelValues("mvlog_purge_running")
}
