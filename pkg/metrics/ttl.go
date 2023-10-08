// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import "github.com/prometheus/client_golang/prometheus"

// TTL metrics
var (
	TTLQueryDuration *prometheus.HistogramVec

	TTLProcessedExpiredRowsCounter *prometheus.CounterVec

	TTLJobStatus *prometheus.GaugeVec

	TTLTaskStatus *prometheus.GaugeVec

	TTLPhaseTime *prometheus.CounterVec

	TTLInsertRowsCount prometheus.Counter

	TTLWatermarkDelay *prometheus.GaugeVec

	TTLEventCounter *prometheus.CounterVec

	TTLSyncTimerCounter prometheus.Counter

	TTLFullRefreshTimersCounter prometheus.Counter
)

// InitTTLMetrics initializes ttl metrics.
func InitTTLMetrics() {
	TTLQueryDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "ttl_query_duration",
			Help:      "Bucketed histogram of processing time (s) of handled TTL queries.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20), // 10ms ~ 1.45hour
		}, []string{LblSQLType, LblResult})

	TTLProcessedExpiredRowsCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "ttl_processed_expired_rows",
			Help:      "The count of expired rows processed in TTL jobs",
		}, []string{LblSQLType, LblResult})

	TTLJobStatus = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "ttl_job_status",
			Help:      "The jobs count in the specified status",
		}, []string{LblType})

	TTLTaskStatus = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "ttl_task_status",
			Help:      "The tasks count in the specified status",
		}, []string{LblType})

	TTLPhaseTime = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "ttl_phase_time",
			Help:      "The time spent in each phase",
		}, []string{LblType, LblPhase})

	TTLInsertRowsCount = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "ttl_insert_rows",
			Help:      "The count of TTL rows inserted",
		})

	TTLWatermarkDelay = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "ttl_watermark_delay",
			Help:      "Bucketed delay time in seconds for TTL tables.",
		}, []string{LblType, LblName})

	TTLEventCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "ttl_event_count",
			Help:      "Counter of ttl event.",
		}, []string{LblType})

	TTLSyncTimerCounter = TTLEventCounter.WithLabelValues("sync_one_timer")
	TTLFullRefreshTimersCounter = TTLEventCounter.WithLabelValues("full_refresh_timers")
}
