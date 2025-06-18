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

package metrics

import (
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// session metrics vars
var (
	NonTransactionalDeleteCount prometheus.Counter
	NonTransactionalInsertCount prometheus.Counter
	NonTransactionalUpdateCount prometheus.Counter

	StatementPerTransactionPessimisticOKInternal    prometheus.Observer
	StatementPerTransactionPessimisticOKGeneral     prometheus.Observer
	StatementPerTransactionPessimisticErrorInternal prometheus.Observer
	StatementPerTransactionPessimisticErrorGeneral  prometheus.Observer
	StatementPerTransactionOptimisticOKInternal     prometheus.Observer
	StatementPerTransactionOptimisticOKGeneral      prometheus.Observer
	StatementPerTransactionOptimisticErrorInternal  prometheus.Observer
	StatementPerTransactionOptimisticErrorGeneral   prometheus.Observer
	TransactionDurationPessimisticCommitInternal    prometheus.Observer
	TransactionDurationPessimisticCommitGeneral     prometheus.Observer
	TransactionDurationPessimisticAbortInternal     prometheus.Observer
	TransactionDurationPessimisticAbortGeneral      prometheus.Observer
	TransactionDurationOptimisticCommitInternal     prometheus.Observer
	TransactionDurationOptimisticCommitGeneral      prometheus.Observer
	TransactionDurationOptimisticAbortInternal      prometheus.Observer
	TransactionDurationOptimisticAbortGeneral       prometheus.Observer
	TransactionRetryInternal                        prometheus.Observer
	TransactionRetryGeneral                         prometheus.Observer

	SessionExecuteCompileDurationInternal prometheus.Observer
	SessionExecuteCompileDurationGeneral  prometheus.Observer
	SessionExecuteParseDurationInternal   prometheus.Observer
	SessionExecuteParseDurationGeneral    prometheus.Observer

	TelemetryCTEUsageRecurCTE       prometheus.Counter
	TelemetryCTEUsageNonRecurCTE    prometheus.Counter
	TelemetryCTEUsageNotCTE         prometheus.Counter
	TelemetryMultiSchemaChangeUsage prometheus.Counter
	TelemetryFlashbackClusterUsage  prometheus.Counter

	TelemetryTablePartitionUsage                prometheus.Counter
	TelemetryTablePartitionListUsage            prometheus.Counter
	TelemetryTablePartitionRangeUsage           prometheus.Counter
	TelemetryTablePartitionHashUsage            prometheus.Counter
	TelemetryTablePartitionRangeColumnsUsage    prometheus.Counter
	TelemetryTablePartitionRangeColumnsGt1Usage prometheus.Counter
	TelemetryTablePartitionRangeColumnsGt2Usage prometheus.Counter
	TelemetryTablePartitionRangeColumnsGt3Usage prometheus.Counter
	TelemetryTablePartitionListColumnsUsage     prometheus.Counter
	TelemetryTablePartitionMaxPartitionsUsage   prometheus.Counter
	TelemetryTablePartitionCreateIntervalUsage  prometheus.Counter
	TelemetryTablePartitionAddIntervalUsage     prometheus.Counter
	TelemetryTablePartitionDropIntervalUsage    prometheus.Counter
	TelemetryExchangePartitionUsage             prometheus.Counter
	TelemetryTableCompactPartitionUsage         prometheus.Counter
	TelemetryReorganizePartitionUsage           prometheus.Counter

	TelemetryLockUserUsage          prometheus.Counter
	TelemetryUnlockUserUsage        prometheus.Counter
	TelemetryCreateOrAlterUserUsage prometheus.Counter

	TelemetryIndexMerge        prometheus.Counter
	TelemetryStoreBatchedUsage prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init session metrics vars.
func InitMetricsVars() {
	NonTransactionalDeleteCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "delete"})
	NonTransactionalInsertCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "insert"})
	NonTransactionalUpdateCount = metrics.NonTransactionalDMLCount.With(prometheus.Labels{metrics.LblType: "update"})

	StatementPerTransactionPessimisticOKInternal = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblOK, metrics.LblInternal)
	StatementPerTransactionPessimisticOKGeneral = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblOK, metrics.LblGeneral)
	StatementPerTransactionPessimisticErrorInternal = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblError, metrics.LblInternal)
	StatementPerTransactionPessimisticErrorGeneral = metrics.StatementPerTransaction.WithLabelValues(metrics.LblPessimistic, metrics.LblError, metrics.LblGeneral)
	StatementPerTransactionOptimisticOKInternal = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblOK, metrics.LblInternal)
	StatementPerTransactionOptimisticOKGeneral = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblOK, metrics.LblGeneral)
	StatementPerTransactionOptimisticErrorInternal = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblError, metrics.LblInternal)
	StatementPerTransactionOptimisticErrorGeneral = metrics.StatementPerTransaction.WithLabelValues(metrics.LblOptimistic, metrics.LblError, metrics.LblGeneral)
	TransactionDurationPessimisticCommitInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblCommit, metrics.LblInternal)
	TransactionDurationPessimisticCommitGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblCommit, metrics.LblGeneral)
	TransactionDurationPessimisticAbortInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblAbort, metrics.LblInternal)
	TransactionDurationPessimisticAbortGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblAbort, metrics.LblGeneral)
	TransactionDurationOptimisticCommitInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblCommit, metrics.LblInternal)
	TransactionDurationOptimisticCommitGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblCommit, metrics.LblGeneral)
	TransactionDurationOptimisticAbortInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblAbort, metrics.LblInternal)
	TransactionDurationOptimisticAbortGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblAbort, metrics.LblGeneral)
	TransactionRetryInternal = metrics.SessionRetry.WithLabelValues(metrics.LblInternal)
	TransactionRetryGeneral = metrics.SessionRetry.WithLabelValues(metrics.LblGeneral)

	SessionExecuteCompileDurationInternal = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblInternal)
	SessionExecuteCompileDurationGeneral = metrics.SessionExecuteCompileDuration.WithLabelValues(metrics.LblGeneral)
	SessionExecuteParseDurationInternal = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblInternal)
	SessionExecuteParseDurationGeneral = metrics.SessionExecuteParseDuration.WithLabelValues(metrics.LblGeneral)

	TelemetryCTEUsageRecurCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("recurCTE")
	TelemetryCTEUsageNonRecurCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("nonRecurCTE")
	TelemetryCTEUsageNotCTE = metrics.TelemetrySQLCTECnt.WithLabelValues("notCTE")
	TelemetryMultiSchemaChangeUsage = metrics.TelemetryMultiSchemaChangeCnt
	TelemetryFlashbackClusterUsage = metrics.TelemetryFlashbackClusterCnt

	TelemetryTablePartitionUsage = metrics.TelemetryTablePartitionCnt
	TelemetryTablePartitionListUsage = metrics.TelemetryTablePartitionListCnt
	TelemetryTablePartitionRangeUsage = metrics.TelemetryTablePartitionRangeCnt
	TelemetryTablePartitionHashUsage = metrics.TelemetryTablePartitionHashCnt
	TelemetryTablePartitionRangeColumnsUsage = metrics.TelemetryTablePartitionRangeColumnsCnt
	TelemetryTablePartitionRangeColumnsGt1Usage = metrics.TelemetryTablePartitionRangeColumnsGt1Cnt
	TelemetryTablePartitionRangeColumnsGt2Usage = metrics.TelemetryTablePartitionRangeColumnsGt2Cnt
	TelemetryTablePartitionRangeColumnsGt3Usage = metrics.TelemetryTablePartitionRangeColumnsGt3Cnt
	TelemetryTablePartitionListColumnsUsage = metrics.TelemetryTablePartitionListColumnsCnt
	TelemetryTablePartitionMaxPartitionsUsage = metrics.TelemetryTablePartitionMaxPartitionsCnt
	TelemetryTablePartitionCreateIntervalUsage = metrics.TelemetryTablePartitionCreateIntervalPartitionsCnt
	TelemetryTablePartitionAddIntervalUsage = metrics.TelemetryTablePartitionAddIntervalPartitionsCnt
	TelemetryTablePartitionDropIntervalUsage = metrics.TelemetryTablePartitionDropIntervalPartitionsCnt
	TelemetryExchangePartitionUsage = metrics.TelemetryExchangePartitionCnt
	TelemetryTableCompactPartitionUsage = metrics.TelemetryCompactPartitionCnt
	TelemetryReorganizePartitionUsage = metrics.TelemetryReorganizePartitionCnt

	TelemetryLockUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("lockUser")
	TelemetryUnlockUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("unlockUser")
	TelemetryCreateOrAlterUserUsage = metrics.TelemetryAccountLockCnt.WithLabelValues("createOrAlterUser")

	TelemetryIndexMerge = metrics.TelemetryIndexMergeUsage
	TelemetryStoreBatchedUsage = metrics.TelemetryStoreBatchedQueryCnt
}
