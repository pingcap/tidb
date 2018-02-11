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
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import "github.com/prometheus/client_golang/prometheus"

// Session metrics.
var (
	SessionExecuteParseDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_execute_parse_duration",
			Help:      "Bucketed histogram of processing time (s) in parse SQL.",
			Buckets:   prometheus.LinearBuckets(0.00004, 0.00001, 13),
		})
	SessionExecuteCompileDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_execute_compile_duration",
			Help:      "Bucketed histogram of processing time (s) in query optimize.",
			Buckets:   prometheus.LinearBuckets(0.00004, 0.00001, 13),
		})
	SessionExecuteRunDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_execute_run_duration",
			Help:      "Bucketed histogram of processing time (s) in running executor.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 13),
		})
	SchemaLeaseErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "schema_lease_error_counter",
			Help:      "Counter of schema lease error",
		}, []string{LblType})
	SessionRetry = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_retry",
			Help:      "Bucketed histogram of session retry count.",
			Buckets:   prometheus.LinearBuckets(0, 1, 10),
		})
	SessionRetryErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_retry_error",
			Help:      "Counter of session retry error.",
		}, []string{LblType})
	TransactionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "transaction_total",
			Help:      "Counter of transactions.",
		}, []string{LblType})

	SessionRestrictedSQLCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "session_restricted_sql_counter",
			Help:      "Counter of internal restricted sql.",
		})

	StatementPerTransaction = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "statement_per_transaction",
			Help:      "Buckated histogram of statements count in each transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 12),
		}, []string{LblType})

	TransactionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "transaction_duration_seconds",
			Help:      "Bucketed histogram of a transaction execution duration, including retry.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 16), // range 1ms ~ 64s
		}, []string{LblType})
)

// Label constants.
const (
	LblUnretryable = "unretryable"
	LblReachMax    = "reach_max"
	LblOK          = "ok"
	LblError       = "error"
	LblRollback    = "rollback"
	LblType        = "type"
)

func init() {
	prometheus.MustRegister(SessionExecuteParseDuration)
	prometheus.MustRegister(SessionExecuteCompileDuration)
	prometheus.MustRegister(SessionExecuteRunDuration)
	prometheus.MustRegister(SchemaLeaseErrorCounter)
	prometheus.MustRegister(SessionRetry)
	prometheus.MustRegister(SessionRetryErrorCounter)
	prometheus.MustRegister(TransactionCounter)
	prometheus.MustRegister(SessionRestrictedSQLCounter)
	prometheus.MustRegister(StatementPerTransaction)
	prometheus.MustRegister(TransactionDuration)
}
