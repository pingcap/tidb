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
	mvMetricLabelComponent = "component"

	mvMetricComponentService = "service"

	mvMetricTaskStatusMVTotal           = "mv_total"
	mvMetricTaskStatusMVLogTotal        = "mvlog_total"
	mvMetricTaskStatusMVRefreshWarning  = "mv_refresh_warning"
	mvMetricTaskStatusMVRefreshOverdue  = "mv_refresh_overdue"
	mvMetricTaskStatusMVLogAccumulation = "mvlog_accumulation"
	mvMetricTaskStatusMVServicePanic    = "panic"
)

// Metrics for materialized view service.
var (
	MVServiceTaskStatusGaugeVec *prometheus.GaugeVec

	MVServiceOperationDurationHistogramVec    *prometheus.HistogramVec
	MVServiceRefreshScheduleDurationHistogram prometheus.Histogram

	MVServiceRunEventCounterVec *prometheus.CounterVec

	MVServiceMVRefreshTotalGauge    prometheus.Gauge
	MVServiceMVLogPurgeTotalGauge   prometheus.Gauge
	MVServiceMVRefreshWarningGauge  prometheus.Gauge
	MVServiceMVRefreshOverdueGauge  prometheus.Gauge
	MVServiceMVLogAccumulationGauge prometheus.Gauge
	MVServicePanicGauge             prometheus.Gauge
)

// InitMVMetrics initializes metrics for materialized view service.
func InitMVMetrics() {
	MVServiceTaskStatusGaugeVec = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_task_status",
			Help:      "Number of MV service and task executor tasks by component and status type.",
		}, []string{mvMetricLabelComponent, LblType})

	MVServiceOperationDurationHistogramVec = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_operation_duration_seconds",
			Help:      "Bucketed histogram of MV service operation duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	MVServiceRefreshScheduleDurationHistogram = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_refresh_schedule_duration_seconds",
			Help:      "Bucketed histogram of the interval between two successful MV service refreshes, excluding the current refresh duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 25), // 1s ~ 194d
		})

	MVServiceRunEventCounterVec = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "mv",
			Name:      "service_run_event_total",
			Help:      "Counter of MV service scheduler and task executor events by component and event type.",
		}, []string{mvMetricLabelComponent, LblType})

	MVServiceMVRefreshTotalGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricComponentService, mvMetricTaskStatusMVTotal)
	MVServiceMVLogPurgeTotalGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricComponentService, mvMetricTaskStatusMVLogTotal)
	MVServiceMVRefreshWarningGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricComponentService, mvMetricTaskStatusMVRefreshWarning)
	MVServiceMVRefreshOverdueGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricComponentService, mvMetricTaskStatusMVRefreshOverdue)
	MVServiceMVLogAccumulationGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricComponentService, mvMetricTaskStatusMVLogAccumulation)
	MVServicePanicGauge = MVServiceTaskStatusGaugeVec.WithLabelValues(mvMetricComponentService, mvMetricTaskStatusMVServicePanic)
}
