// Copyright 2018 PingCAP, Inc.
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

var (
	// ExecutorCounter records the number of expensive executors.
	ExecutorCounter *prometheus.CounterVec

	// StmtNodeCounter records the number of statement with the same type.
	StmtNodeCounter *prometheus.CounterVec

	// DbStmtNodeCounter records the number of statement with the same type and db.
	DbStmtNodeCounter *prometheus.CounterVec

	// ExecPhaseDuration records the duration of each execution phase.
	ExecPhaseDuration *prometheus.SummaryVec

	// OngoingTxnDurationHistogram records the duration of ongoing transactions.
	OngoingTxnDurationHistogram *prometheus.HistogramVec

	// MppCoordinatorStats records the number of mpp coordinator instances and related events
	MppCoordinatorStats *prometheus.GaugeVec

	// MppCoordinatorLatency records latencies of mpp coordinator operations.
	MppCoordinatorLatency *prometheus.HistogramVec
)

// InitExecutorMetrics initializes excutor metrics.
func InitExecutorMetrics() {
	ExecutorCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "expensive_total",
			Help:      "Counter of Expensive Executors.",
		}, []string{LblType},
	)

	StmtNodeCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "statement_total",
			Help:      "Counter of StmtNode.",
		}, []string{LblType, LblDb, LblResourceGroup})

	DbStmtNodeCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "statement_db_total",
			Help:      "Counter of StmtNode by Database.",
		}, []string{LblDb, LblType})

	ExecPhaseDuration = NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "phase_duration_seconds",
			Help:      "Summary of each execution phase duration.",
		}, []string{LblPhase, LblInternal})

	OngoingTxnDurationHistogram = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "ongoing_txn_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of ongoing transactions.",
			Buckets:   prometheus.ExponentialBuckets(60, 2, 15), // 60s ~ 273hours
		}, []string{LblType})

	MppCoordinatorStats = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "mpp_coordinator_stats",
			Help:      "Mpp Coordinator related stats",
		}, []string{LblType})

	MppCoordinatorLatency = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "mpp_coordinator_latency",
			Help:      "Bucketed histogram of processing time (ms) of mpp coordinator operations.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType})
}
