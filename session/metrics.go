// Copyright 2023 PingCAP, Inc.
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

package session

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	nonTransactionalDeleteCount prometheus.Counter
	nonTransactionalInsertCount prometheus.Counter
	nonTransactionalUpdateCount prometheus.Counter

	statementPerTransactionPessimisticOKInternal    prometheus.Observer
	statementPerTransactionPessimisticOKGeneral     prometheus.Observer
	statementPerTransactionPessimisticErrorInternal prometheus.Observer
	statementPerTransactionPessimisticErrorGeneral  prometheus.Observer
	statementPerTransactionOptimisticOKInternal     prometheus.Observer
	statementPerTransactionOptimisticOKGeneral      prometheus.Observer
	statementPerTransactionOptimisticErrorInternal  prometheus.Observer
	statementPerTransactionOptimisticErrorGeneral   prometheus.Observer
	transactionDurationPessimisticCommitInternal    prometheus.Observer
	transactionDurationPessimisticCommitGeneral     prometheus.Observer
	transactionDurationPessimisticAbortInternal     prometheus.Observer
	transactionDurationPessimisticAbortGeneral      prometheus.Observer
	transactionDurationOptimisticCommitInternal     prometheus.Observer
	transactionDurationOptimisticCommitGeneral      prometheus.Observer
	transactionDurationOptimisticAbortInternal      prometheus.Observer
	transactionDurationOptimisticAbortGeneral       prometheus.Observer
	transactionRetryInternal                        prometheus.Observer
	transactionRetryGeneral                         prometheus.Observer

	sessionExecuteCompileDurationInternal prometheus.Observer
	sessionExecuteCompileDurationGeneral  prometheus.Observer
	sessionExecuteParseDurationInternal   prometheus.Observer
	sessionExecuteParseDurationGeneral    prometheus.Observer

	telemetryCTEUsageRecurCTE       prometheus.Counter
	telemetryCTEUsageNonRecurCTE    prometheus.Counter
	telemetryCTEUsageNotCTE         prometheus.Counter
	telemetryMultiSchemaChangeUsage prometheus.Counter
	telemetryFlashbackClusterUsage  prometheus.Counter

	telemetryTablePartitionUsage                prometheus.Counter
	telemetryTablePartitionListUsage            prometheus.Counter
	telemetryTablePartitionRangeUsage           prometheus.Counter
	telemetryTablePartitionHashUsage            prometheus.Counter
	telemetryTablePartitionRangeColumnsUsage    prometheus.Counter
	telemetryTablePartitionRangeColumnsGt1Usage prometheus.Counter
	telemetryTablePartitionRangeColumnsGt2Usage prometheus.Counter
	telemetryTablePartitionRangeColumnsGt3Usage prometheus.Counter
	telemetryTablePartitionListColumnsUsage     prometheus.Counter
	telemetryTablePartitionMaxPartitionsUsage   prometheus.Counter
	telemetryTablePartitionCreateIntervalUsage  prometheus.Counter
	telemetryTablePartitionAddIntervalUsage     prometheus.Counter
	telemetryTablePartitionDropIntervalUsage    prometheus.Counter
	telemetryExchangePartitionUsage             prometheus.Counter
	telemetryTableCompactPartitionUsage         prometheus.Counter
	telemetryReorganizePartitionUsage           prometheus.Counter

	telemetryLockUserUsage          prometheus.Counter
	telemetryUnlockUserUsage        prometheus.Counter
	telemetryCreateOrAlterUserUsage prometheus.Counter

	telemetryIndexMerge        prometheus.Counter
	telemetryStoreBatchedUsage prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init session metrics vars.
func InitMetricsVars() {
	nonTransactionalDeleteCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "delete"})
	nonTransactionalInsertCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "insert"})
	nonTransactionalUpdateCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "update"})

	statementPerTransactionPessimisticOKInternal = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblOK, metrics.LblInternal)
	statementPerTransactionPessimisticOKGeneral = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblOK, metrics.LblGeneral)
	statementPerTransactionPessimisticErrorInternal = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblError, metrics.LblInternal)
	statementPerTransactionPessimisticErrorGeneral = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblError, metrics.LblGeneral)
	statementPerTransactionOptimisticOKInternal = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblOK, metrics.LblInternal)
	statementPerTransactionOptimisticOKGeneral = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblOK, metrics.LblGeneral)
	statementPerTransactionOptimisticErrorInternal = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblError, metrics.LblInternal)
	statementPerTransactionOptimisticErrorGeneral = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblError, metrics.LblGeneral)
	transactionDurationPessimisticCommitInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblCommit, metrics.LblInternal)
	transactionDurationPessimisticCommitGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblCommit, metrics.LblGeneral)
	transactionDurationPessimisticAbortInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblAbort, metrics.LblInternal)
	transactionDurationPessimisticAbortGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblAbort, metrics.LblGeneral)
	transactionDurationOptimisticCommitInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblCommit, metrics.LblInternal)
	transactionDurationOptimisticCommitGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblCommit, metrics.LblGeneral)
	transactionDurationOptimisticAbortInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblAbort, metrics.LblInternal)
	transactionDurationOptimisticAbortGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblAbort, metrics.LblGeneral)
	transactionRetryInternal = metrics.SessionRetry.WithLabelValues(metrics.LblInternal)
	transactionRetryGeneral = metrics.SessionRetry.WithLabelValues(metrics.LblGeneral)

	sessionExecuteCompileDurationInternal = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteCompileDurationGeneral = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblGeneral)
	sessionExecuteParseDurationInternal = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblInternal)
	sessionExecuteParseDurationGeneral = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblGeneral)

	telemetryCTEUsageRecurCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("recurCTE")
	telemetryCTEUsageNonRecurCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("nonRecurCTE")
	telemetryCTEUsageNotCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("notCTE")
	telemetryMultiSchemaChangeUsage = metrics.TelemetryMultiSchemaChangeCnt
	telemetryFlashbackClusterUsage = metrics.TelemetryFlashbackClusterCnt

	telemetryTablePartitionUsage = metrics.TelemetryTablePartitionCnt
	telemetryTablePartitionListUsage = metrics.TelemetryTablePartitionListCnt
	telemetryTablePartitionRangeUsage = metrics.TelemetryTablePartitionRangeCnt
	telemetryTablePartitionHashUsage = metrics.TelemetryTablePartitionHashCnt
	telemetryTablePartitionRangeColumnsUsage = metrics.TelemetryTablePartitionRangeColumnsCnt
	telemetryTablePartitionRangeColumnsGt1Usage = metrics.TelemetryTablePartitionRangeColumnsGt1Cnt
	telemetryTablePartitionRangeColumnsGt2Usage = metrics.TelemetryTablePartitionRangeColumnsGt2Cnt
	telemetryTablePartitionRangeColumnsGt3Usage = metrics.TelemetryTablePartitionRangeColumnsGt3Cnt
	telemetryTablePartitionListColumnsUsage = metrics.TelemetryTablePartitionListColumnsCnt
	telemetryTablePartitionMaxPartitionsUsage = metrics.TelemetryTablePartitionMaxPartitionsCnt
	telemetryTablePartitionCreateIntervalUsage = metrics.TelemetryTablePartitionCreateIntervalPartitionsCnt
	telemetryTablePartitionAddIntervalUsage = metrics.TelemetryTablePartitionAddIntervalPartitionsCnt
	telemetryTablePartitionDropIntervalUsage = metrics.TelemetryTablePartitionDropIntervalPartitionsCnt
	telemetryExchangePartitionUsage = metrics.TelemetryExchangePartitionCnt
	telemetryTableCompactPartitionUsage = metrics.TelemetryCompactPartitionCnt
	telemetryReorganizePartitionUsage = metrics.TelemetryReorganizePartitionCnt

	telemetryLockUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("lockUser")
	telemetryUnlockUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("unlockUser")
	telemetryCreateOrAlterUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("createOrAlterUser")

	telemetryIndexMerge = metrics.TelemetryIndexMergeUsage
	telemetryStoreBatchedUsage = metrics.TelemetryStoreBatchedQueryCnt
}
