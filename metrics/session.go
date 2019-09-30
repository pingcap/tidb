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
	SessionExecuteParseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "parse_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in parse SQL.",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 22), // 40us ~ 168s
		}, []string{LblSQLType})
	SessionExecuteCompileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "compile_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in query optimize.",
			// Build plan may execute the statement, or allocate table ID, so it might take a long time.
			Buckets: prometheus.ExponentialBuckets(0.00004, 2, 22), // 40us ~ 168s
		}, []string{LblSQLType})
	SessionExecuteRunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "execute_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in running executor.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 22), // 100us ~ 419s
		}, []string{LblSQLType})
	SchemaLeaseErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "schema_lease_error_total",
			Help:      "Counter of schema lease error",
		}, []string{LblType})
	SessionRetry = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "retry_num",
			Help:      "Bucketed histogram of session retry count.",
			Buckets:   prometheus.LinearBuckets(0, 1, 20), // 0 ~ 20
		})
	SessionRetryErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "retry_error_total",
			Help:      "Counter of session retry error.",
		}, []string{LblSQLType, LblType})
	TransactionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "transaction_total",
			Help:      "Counter of transactions.",
		}, []string{LblSQLType, LblType})

	SessionRestrictedSQLCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "restricted_sql_total",
			Help:      "Counter of internal restricted sql.",
		})

	StatementPerTransaction = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "transaction_statement_num",
			Help:      "Bucketed histogram of statements count in each transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16), // 1 ~ 65536
		}, []string{LblSQLType, LblType})

	TransactionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "transaction_duration_seconds",
			Help:      "Bucketed histogram of a transaction execution duration, including retry.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 1049s
		}, []string{LblSQLType, LblType})
)

// Label constants.
const (
	LblUnretryable = "unretryable"
	LblReachMax    = "reach_max"
	LblOK          = "ok"
	LblError       = "error"
	LblRollback    = "rollback"
	LblComRol      = "com_rol"
	LblType        = "type"
	LblResult      = "result"
	LblSQLType     = "sql_type"
	LblGeneral     = "general"
	LblInternal    = "internal"
	LblStore       = "store"
	LblAddress     = "address"
)
