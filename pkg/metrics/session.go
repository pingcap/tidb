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

import "github.com/prometheus/client_golang/prometheus"

// Session metrics.
var (
	AutoIDReqDuration                  prometheus.Histogram
	SessionExecuteParseDuration        *prometheus.HistogramVec
	SessionExecuteCompileDuration      *prometheus.HistogramVec
	SessionExecuteRunDuration          *prometheus.HistogramVec
	SchemaLeaseErrorCounter            *prometheus.CounterVec
	SessionRetry                       *prometheus.HistogramVec
	SessionRetryErrorCounter           *prometheus.CounterVec
	SessionRestrictedSQLCounter        prometheus.Counter
	StatementPerTransaction            *prometheus.HistogramVec
	TransactionDuration                *prometheus.HistogramVec
	StatementDeadlockDetectDuration    prometheus.Histogram
	StatementPessimisticRetryCount     prometheus.Histogram
	StatementLockKeysCount             prometheus.Histogram
	ValidateReadTSFromPDCount          prometheus.Counter
	NonTransactionalDMLCount           *prometheus.CounterVec
	TxnStatusEnteringCounter           *prometheus.CounterVec
	TxnDurationHistogram               *prometheus.HistogramVec
	LazyPessimisticUniqueCheckSetCount prometheus.Counter
	PessimisticDMLDurationByAttempt    *prometheus.HistogramVec
	ResourceGroupQueryTotalCounter     *prometheus.CounterVec
	FairLockingUsageCount              *prometheus.CounterVec
)

// InitSessionMetrics initializes session metrics.
func InitSessionMetrics() {
	AutoIDReqDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "meta",
			Name:      "autoid_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in parse SQL.",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 28), // 40us ~ 1.5h
		})

	SessionExecuteParseDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "parse_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in parse SQL.",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 28), // 40us ~ 1.5h
		}, []string{LblSQLType})

	SessionExecuteCompileDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "compile_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in query optimize.",
			// Build plan may execute the statement, or allocate table ID, so it might take a long time.
			Buckets: prometheus.ExponentialBuckets(0.00004, 2, 28), // 40us ~ 1.5h
		}, []string{LblSQLType})

	SessionExecuteRunDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "execute_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in running executor.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 30), // 100us ~ 15h
		}, []string{LblSQLType})

	SchemaLeaseErrorCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "schema_lease_error_total",
			Help:      "Counter of schema lease error",
		}, []string{LblType})

	SessionRetry = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "retry_num",
			Help:      "Bucketed histogram of session retry count.",
			Buckets:   prometheus.LinearBuckets(0, 1, 21), // 0 ~ 20
		}, []string{LblScope})

	SessionRetryErrorCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "retry_error_total",
			Help:      "Counter of session retry error.",
		}, []string{LblSQLType, LblType})

	SessionRestrictedSQLCounter = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "restricted_sql_total",
			Help:      "Counter of internal restricted sql.",
		})

	StatementPerTransaction = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "transaction_statement_num",
			Help:      "Bucketed histogram of statements count in each transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16), // 1 ~ 32768
		}, []string{LblTxnMode, LblType, LblScope})

	TransactionDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "transaction_duration_seconds",
			Help:      "Bucketed histogram of a transaction execution duration, including retry.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblTxnMode, LblType, LblScope})

	StatementDeadlockDetectDuration = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "statement_deadlock_detect_duration_seconds",
			Help:      "Bucketed histogram of a statement deadlock detect duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		},
	)

	StatementPessimisticRetryCount = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "statement_pessimistic_retry_count",
			Help:      "Bucketed histogram of statement pessimistic retry count",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16), // 1 ~ 32768
		})

	StatementLockKeysCount = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "statement_lock_keys_count",
			Help:      "Keys locking for a single statement",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 21), // 1 ~ 1048576
		})

	ValidateReadTSFromPDCount = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "validate_read_ts_from_pd_count",
			Help:      "Counter of validating read ts by getting a timestamp from PD",
		})

	NonTransactionalDMLCount = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "non_transactional_dml_count",
			Help:      "Counter of non-transactional delete",
		}, []string{LblType},
	)

	TxnStatusEnteringCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "txn_state_entering_count",
			Help:      "How many times transactions enter this state",
		}, []string{LblType},
	)

	TxnDurationHistogram = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "txn_state_seconds",
			Help:      "Bucketed histogram of different states of a transaction.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblHasLock})

	LazyPessimisticUniqueCheckSetCount = NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "lazy_pessimistic_unique_check_set_count",
			Help:      "Counter of setting tidb_constraint_check_in_place to false, note that it doesn't count the default value set by tidb config",
		},
	)

	PessimisticDMLDurationByAttempt = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "transaction_pessimistic_dml_duration_by_attempt",
			Help:      "Bucketed histogram of duration of pessimistic DMLs, distinguished by first attempt and retries",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType, LblPhase})

	ResourceGroupQueryTotalCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "resource_group_query_total",
			Help:      "Counter of the total number of queries for the resource group",
		}, []string{LblName, LblResourceGroup})

	FairLockingUsageCount = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "session",
			Name:      "transaction_fair_locking_usage",
			Help:      "The counter of statements and transactions in which fair locking is used or takes effect",
		}, []string{LblType})
}

// Label constants.
const (
	LblUnretryable    = "unretryable"
	LblReachMax       = "reach_max"
	LblOK             = "ok"
	LblError          = "error"
	LblCommit         = "commit"
	LblAbort          = "abort"
	LblRollback       = "rollback"
	LblType           = "type"
	LblDb             = "db"
	LblResult         = "result"
	LblSQLType        = "sql_type"
	LblCoprType       = "copr_type"
	LblGeneral        = "general"
	LblInternal       = "internal"
	LblTxnMode        = "txn_mode"
	LblPessimistic    = "pessimistic"
	LblOptimistic     = "optimistic"
	LblStore          = "store"
	LblAddress        = "address"
	LblBatchGet       = "batch_get"
	LblGet            = "get"
	LblLockKeys       = "lock_keys"
	LblInTxn          = "in_txn"
	LblVersion        = "version"
	LblHash           = "hash"
	LblCTEType        = "cte_type"
	LblAccountLock    = "account_lock"
	LblIdle           = "idle"
	LblRunning        = "executing_sql"
	LblLockWaiting    = "waiting_for_lock"
	LblCommitting     = "committing"
	LblRollingBack    = "rolling_back"
	LblHasLock        = "has_lock"
	LblPhase          = "phase"
	LblModule         = "module"
	LblRCReadCheckTS  = "read_check"
	LblRCWriteCheckTS = "write_check"
	LblResourceGroup  = "resource_group"

	LblName = "name"

	LblFairLockingTxnUsed       = "txn-used"
	LblFairLockingTxnEffective  = "txn-effective"
	LblFairLockingStmtUsed      = "stmt-used"
	LblFairLockingStmtEffective = "stmt-effective"
	LblScope                    = "scope"
)
