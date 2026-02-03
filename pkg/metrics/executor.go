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
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
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

	// AffectedRowsCounter records the number of affected rows.
	AffectedRowsCounter *prometheus.CounterVec

	// AffectedRowsCounterInsert records the number of insert affected rows.
	AffectedRowsCounterInsert prometheus.Counter

	// AffectedRowsCounterUpdate records the number of update affected rows.
	AffectedRowsCounterUpdate prometheus.Counter

	// AffectedRowsCounterDelete records the number of delete affected rows.
	AffectedRowsCounterDelete prometheus.Counter

	// AffectedRowsCounterReplace records the number of replace affected rows.
	AffectedRowsCounterReplace prometheus.Counter

	// AffectedRowsCounterNTDMLUpdate records the number of NT-DML update affected rows.
	AffectedRowsCounterNTDMLUpdate prometheus.Counter
	// AffectedRowsCounterNTDMLDelete records the number of NT-DML delete affected rows.
	AffectedRowsCounterNTDMLDelete prometheus.Counter
	// AffectedRowsCounterNTDMLInsert records the number of NT-DML insert affected rows.
	AffectedRowsCounterNTDMLInsert prometheus.Counter
	// AffectedRowsCounterNTDMLReplace records the number of NT-DML replace affected rows.
	AffectedRowsCounterNTDMLReplace prometheus.Counter

	// ActiveActiveHardDeleteStmtCounter records the num of hard delete statements on a active-active table
	ActiveActiveHardDeleteStmtCounter prometheus.Counter
	// ActiveActiveWriteUnsafeOriginTsRowCounter records the num of unsafe _tidb_origin_ts write rows.
	ActiveActiveWriteUnsafeOriginTsRowCounter prometheus.Counter
	// ActiveActiveWriteUnsafeOriginTsStmtCounter records the num of unsafe _tidb_origin_ts statements.
	ActiveActiveWriteUnsafeOriginTsStmtCounter prometheus.Counter

	// SoftDeleteImplicitDeleteRows records the number of soft-deleted rows implicitly removed per DML statement.
	SoftDeleteImplicitDeleteRows *prometheus.HistogramVec
	// SoftDeleteImplicitDeleteRowsInsert records SoftDeleteImplicitDeleteRows with Insert label.
	SoftDeleteImplicitDeleteRowsInsert prometheus.Observer
	// SoftDeleteImplicitDeleteRowsLoadData records SoftDeleteImplicitDeleteRows with LoadData label.
	SoftDeleteImplicitDeleteRowsLoadData prometheus.Observer

	// NetworkTransmissionStats records the network transmission for queries
	NetworkTransmissionStats *prometheus.CounterVec

	// IndexLookUpExecutorDuration records the duration of index look up executor
	IndexLookUpExecutorDuration *prometheus.HistogramVec

	// IndexLookRowsCounter records the number of rows in index look up executor
	IndexLookRowsCounter *prometheus.CounterVec

	// IndexLookUpExecutorRowNumber records the number of rows scanned in one index look up executor
	IndexLookUpExecutorRowNumber *prometheus.HistogramVec

	// IndexLookUpCopTaskCount records the number of cop tasks in index look up executor
	IndexLookUpCopTaskCount *prometheus.CounterVec
)

// InitExecutorMetrics initializes excutor metrics.
func InitExecutorMetrics() {
	ExecutorCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "expensive_total",
			Help:      "Counter of Expensive Executors.",
		}, []string{LblType},
	)

	StmtNodeCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "statement_total",
			Help:      "Counter of StmtNode.",
		}, []string{LblType, LblDb, LblResourceGroup})

	DbStmtNodeCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "statement_db_total",
			Help:      "Counter of StmtNode by Database.",
		}, []string{LblDb, LblType})

	ExecPhaseDuration = metricscommon.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "phase_duration_seconds",
			Help:      "Summary of each execution phase duration.",
		}, []string{LblPhase, LblInternal})

	OngoingTxnDurationHistogram = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "ongoing_txn_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of ongoing transactions.",
			Buckets:   prometheus.ExponentialBuckets(60, 2, 15), // 60s ~ 273hours
		}, []string{LblType})

	MppCoordinatorStats = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "mpp_coordinator_stats",
			Help:      "Mpp Coordinator related stats",
		}, []string{LblType})

	MppCoordinatorLatency = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "mpp_coordinator_latency",
			Help:      "Bucketed histogram of processing time (ms) of mpp coordinator operations.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType})

	AffectedRowsCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "affected_rows",
			Help:      "Counters of server affected rows.",
		}, []string{LblSQLType})

	AffectedRowsCounterInsert = AffectedRowsCounter.WithLabelValues("Insert")
	AffectedRowsCounterUpdate = AffectedRowsCounter.WithLabelValues("Update")
	AffectedRowsCounterDelete = AffectedRowsCounter.WithLabelValues("Delete")
	AffectedRowsCounterReplace = AffectedRowsCounter.WithLabelValues("Replace")
	AffectedRowsCounterNTDMLUpdate = AffectedRowsCounter.WithLabelValues("NTDML-Update")
	AffectedRowsCounterNTDMLDelete = AffectedRowsCounter.WithLabelValues("NTDML-Delete")
	AffectedRowsCounterNTDMLInsert = AffectedRowsCounter.WithLabelValues("NTDML-Insert")
	AffectedRowsCounterNTDMLReplace = AffectedRowsCounter.WithLabelValues("NTDML-Replace")

	ActiveActiveHardDeleteStmtCounter = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "active_active_table_hard_delete_stmt_total",
			Help:      "hard delete statement count for a soft delete table",
		})

	ActiveActiveWriteUnsafeOriginTsRowCounter = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "write_unsafe_origin_ts_rows_total",
			Help:      "rows written with unsafe _tidb_origin_ts column",
		})

	ActiveActiveWriteUnsafeOriginTsStmtCounter = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "write_unsafe_origin_ts_stmt_total",
			Help:      "stmts written with unsafe _tidb_origin_ts column",
		})

	SoftDeleteImplicitDeleteRows = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "softdelete_implicit_delete_rows",
			Help:      "Bucketed histogram of soft-deleted rows implicitly removed per DML statement.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16), // 1 ~ 32768
		}, []string{LblSQLType})

	SoftDeleteImplicitDeleteRowsInsert = SoftDeleteImplicitDeleteRows.WithLabelValues("Insert")
	SoftDeleteImplicitDeleteRowsLoadData = SoftDeleteImplicitDeleteRows.WithLabelValues("LoadData")

	NetworkTransmissionStats = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "network_transmission",
			Help:      "Counter of network transmission bytes.",
		}, []string{LblType})

	IndexLookUpExecutorDuration = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "index_lookup_execute_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in running index-lookup executor.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 30), // 100us ~ 15h
		}, []string{LblType})

	IndexLookRowsCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "index_lookup_rows",
			Help:      "Counter of index lookup push-down rows.",
		}, []string{LblType})

	IndexLookUpExecutorRowNumber = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "index_lookup_row_number",
			Help:      "Row number for each index lookup executor",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		}, []string{LblType})

	IndexLookUpCopTaskCount = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "executor",
			Name:      "index_lookup_cop_task_count",
			Help:      "Counter for index lookup cop tasks",
		}, []string{LblType})
}
