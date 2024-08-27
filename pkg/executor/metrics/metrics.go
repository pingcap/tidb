// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// phases
const (
	PhaseBuildLocking       = "build:locking"
	PhaseOpenLocking        = "open:locking"
	PhaseNextLocking        = "next:locking"
	PhaseLockLocking        = "lock:locking"
	PhaseBuildFinal         = "build:final"
	PhaseOpenFinal          = "open:final"
	PhaseNextFinal          = "next:final"
	PhaseLockFinal          = "lock:final"
	PhaseCommitPrewrite     = "commit:prewrite"
	PhaseCommitCommit       = "commit:commit"
	PhaseCommitWaitCommitTS = "commit:wait:commit-ts"
	PhaseCommitWaitLatestTS = "commit:wait:latest-ts"
	PhaseCommitWaitLatch    = "commit:wait:local-latch"
	PhaseCommitWaitBinlog   = "commit:wait:prewrite-binlog"
	PhaseWriteResponse      = "write-response"
)

// executor metrics vars
var (
	TotalQueryProcHistogramGeneral  prometheus.Observer
	TotalCopProcHistogramGeneral    prometheus.Observer
	TotalCopWaitHistogramGeneral    prometheus.Observer
	CopMVCCRatioHistogramGeneral    prometheus.Observer
	TotalQueryProcHistogramInternal prometheus.Observer
	TotalCopProcHistogramInternal   prometheus.Observer
	TotalCopWaitHistogramInternal   prometheus.Observer

	SelectForUpdateFirstAttemptDuration prometheus.Observer
	SelectForUpdateRetryDuration        prometheus.Observer
	DmlFirstAttemptDuration             prometheus.Observer
	DmlRetryDuration                    prometheus.Observer

	// FairLockingTxnUsedCount counts transactions where at least one statement has fair locking enabled.
	FairLockingTxnUsedCount prometheus.Counter
	// FairLockingStmtUsedCount counts statements that have fair locking enabled.
	FairLockingStmtUsedCount prometheus.Counter
	// FairLockingTxnEffectiveCount counts transactions where at least one statement has fair locking enabled,
	// and it takes effect (which is determined according to whether lock-with-conflict has occurred during execution).
	FairLockingTxnEffectiveCount prometheus.Counter
	// FairLockingStmtEffectiveCount counts statements where at least one statement has fair locking enabled,
	// and it takes effect (which is determined according to whether lock-with-conflict has occurred during execution).
	FairLockingStmtEffectiveCount prometheus.Counter

	ExecutorCounterMergeJoinExec            prometheus.Counter
	ExecutorCountHashJoinExec               prometheus.Counter
	ExecutorCounterHashAggExec              prometheus.Counter
	ExecutorStreamAggExec                   prometheus.Counter
	ExecutorCounterSortExec                 prometheus.Counter
	ExecutorCounterTopNExec                 prometheus.Counter
	ExecutorCounterNestedLoopApplyExec      prometheus.Counter
	ExecutorCounterIndexLookUpJoin          prometheus.Counter
	ExecutorCounterIndexLookUpExecutor      prometheus.Counter
	ExecutorCounterIndexMergeReaderExecutor prometheus.Counter

	SessionExecuteRunDurationInternal prometheus.Observer
	SessionExecuteRunDurationGeneral  prometheus.Observer
	TotalTiFlashQuerySuccCounter      prometheus.Counter

	// pre-define observers for non-internal queries
	ExecBuildLocking       prometheus.Observer
	ExecOpenLocking        prometheus.Observer
	ExecNextLocking        prometheus.Observer
	ExecLockLocking        prometheus.Observer
	ExecBuildFinal         prometheus.Observer
	ExecOpenFinal          prometheus.Observer
	ExecNextFinal          prometheus.Observer
	ExecLockFinal          prometheus.Observer
	ExecCommitPrewrite     prometheus.Observer
	ExecCommitCommit       prometheus.Observer
	ExecCommitWaitCommitTS prometheus.Observer
	ExecCommitWaitLatestTS prometheus.Observer
	ExecCommitWaitLatch    prometheus.Observer
	ExecCommitWaitBinlog   prometheus.Observer
	ExecWriteResponse      prometheus.Observer
	ExecUnknown            prometheus.Observer

	// pre-define observers for internal queries
	ExecBuildLockingInternal       prometheus.Observer
	ExecOpenLockingInternal        prometheus.Observer
	ExecNextLockingInternal        prometheus.Observer
	ExecLockLockingInternal        prometheus.Observer
	ExecBuildFinalInternal         prometheus.Observer
	ExecOpenFinalInternal          prometheus.Observer
	ExecNextFinalInternal          prometheus.Observer
	ExecLockFinalInternal          prometheus.Observer
	ExecCommitPrewriteInternal     prometheus.Observer
	ExecCommitCommitInternal       prometheus.Observer
	ExecCommitWaitCommitTSInternal prometheus.Observer
	ExecCommitWaitLatestTSInternal prometheus.Observer
	ExecCommitWaitLatchInternal    prometheus.Observer
	ExecCommitWaitBinlogInternal   prometheus.Observer
	ExecWriteResponseInternal      prometheus.Observer
	ExecUnknownInternal            prometheus.Observer

	TransactionDurationPessimisticRollbackInternal prometheus.Observer
	TransactionDurationPessimisticRollbackGeneral  prometheus.Observer
	TransactionDurationOptimisticRollbackInternal  prometheus.Observer
	TransactionDurationOptimisticRollbackGeneral   prometheus.Observer

	PhaseDurationObserverMap         map[string]prometheus.Observer
	PhaseDurationObserverMapInternal map[string]prometheus.Observer

	MppCoordinatorStatsTotalRegisteredNumber prometheus.Gauge
	MppCoordinatorStatsActiveNumber          prometheus.Gauge
	MppCoordinatorStatsOverTimeNumber        prometheus.Gauge
	MppCoordinatorStatsReportNotReceived     prometheus.Gauge

	MppCoordinatorLatencyRcvReport prometheus.Observer
)

func init() {
	InitMetricsVars()
	InitPhaseDurationObserverMap()
}

// InitMetricsVars init executor metrics vars.
func InitMetricsVars() {
	TotalQueryProcHistogramGeneral = metrics.TotalQueryProcHistogram.WithLabelValues(metrics.LblGeneral)
	TotalCopProcHistogramGeneral = metrics.TotalCopProcHistogram.WithLabelValues(metrics.LblGeneral)
	TotalCopWaitHistogramGeneral = metrics.TotalCopWaitHistogram.WithLabelValues(metrics.LblGeneral)
	CopMVCCRatioHistogramGeneral = metrics.CopMVCCRatioHistogram.WithLabelValues(metrics.LblGeneral)
	TotalQueryProcHistogramInternal = metrics.TotalQueryProcHistogram.WithLabelValues(metrics.LblInternal)
	TotalCopProcHistogramInternal = metrics.TotalCopProcHistogram.WithLabelValues(metrics.LblInternal)
	TotalCopWaitHistogramInternal = metrics.TotalCopWaitHistogram.WithLabelValues(metrics.LblInternal)

	SelectForUpdateFirstAttemptDuration = metrics.PessimisticDMLDurationByAttempt.WithLabelValues("select-for-update", "first-attempt")
	SelectForUpdateRetryDuration = metrics.PessimisticDMLDurationByAttempt.WithLabelValues("select-for-update", "retry")
	DmlFirstAttemptDuration = metrics.PessimisticDMLDurationByAttempt.WithLabelValues("dml", "first-attempt")
	DmlRetryDuration = metrics.PessimisticDMLDurationByAttempt.WithLabelValues("dml", "retry")

	FairLockingTxnUsedCount = metrics.FairLockingUsageCount.WithLabelValues(metrics.LblFairLockingTxnUsed)
	FairLockingStmtUsedCount = metrics.FairLockingUsageCount.WithLabelValues(metrics.LblFairLockingStmtUsed)
	FairLockingTxnEffectiveCount = metrics.FairLockingUsageCount.WithLabelValues(metrics.LblFairLockingTxnEffective)
	FairLockingStmtEffectiveCount = metrics.FairLockingUsageCount.WithLabelValues(metrics.LblFairLockingStmtEffective)

	ExecutorCounterMergeJoinExec = metrics.ExecutorCounter.WithLabelValues("MergeJoinExec")
	ExecutorCountHashJoinExec = metrics.ExecutorCounter.WithLabelValues("HashJoinExec")
	ExecutorCounterHashAggExec = metrics.ExecutorCounter.WithLabelValues("HashAggExec")
	ExecutorStreamAggExec = metrics.ExecutorCounter.WithLabelValues("StreamAggExec")
	ExecutorCounterSortExec = metrics.ExecutorCounter.WithLabelValues("SortExec")
	ExecutorCounterTopNExec = metrics.ExecutorCounter.WithLabelValues("TopNExec")
	ExecutorCounterNestedLoopApplyExec = metrics.ExecutorCounter.WithLabelValues("NestedLoopApplyExec")
	ExecutorCounterIndexLookUpJoin = metrics.ExecutorCounter.WithLabelValues("IndexLookUpJoin")
	ExecutorCounterIndexLookUpExecutor = metrics.ExecutorCounter.WithLabelValues("IndexLookUpExecutor")
	ExecutorCounterIndexMergeReaderExecutor = metrics.ExecutorCounter.WithLabelValues("IndexMergeReaderExecutor")

	SessionExecuteRunDurationInternal = metrics.SessionExecuteRunDuration.WithLabelValues(metrics.LblInternal)
	SessionExecuteRunDurationGeneral = metrics.SessionExecuteRunDuration.WithLabelValues(metrics.LblGeneral)
	TotalTiFlashQuerySuccCounter = metrics.TiFlashQueryTotalCounter.WithLabelValues("", metrics.LblOK)

	ExecBuildLocking = metrics.ExecPhaseDuration.WithLabelValues(PhaseBuildLocking, "0")
	ExecOpenLocking = metrics.ExecPhaseDuration.WithLabelValues(PhaseOpenLocking, "0")
	ExecNextLocking = metrics.ExecPhaseDuration.WithLabelValues(PhaseNextLocking, "0")
	ExecLockLocking = metrics.ExecPhaseDuration.WithLabelValues(PhaseLockLocking, "0")
	ExecBuildFinal = metrics.ExecPhaseDuration.WithLabelValues(PhaseBuildFinal, "0")
	ExecOpenFinal = metrics.ExecPhaseDuration.WithLabelValues(PhaseOpenFinal, "0")
	ExecNextFinal = metrics.ExecPhaseDuration.WithLabelValues(PhaseNextFinal, "0")
	ExecLockFinal = metrics.ExecPhaseDuration.WithLabelValues(PhaseLockFinal, "0")
	ExecCommitPrewrite = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitPrewrite, "0")
	ExecCommitCommit = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitCommit, "0")
	ExecCommitWaitCommitTS = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitWaitCommitTS, "0")
	ExecCommitWaitLatestTS = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitWaitLatestTS, "0")
	ExecCommitWaitLatch = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitWaitLatch, "0")
	ExecCommitWaitBinlog = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitWaitBinlog, "0")
	ExecWriteResponse = metrics.ExecPhaseDuration.WithLabelValues(PhaseWriteResponse, "0")
	ExecUnknown = metrics.ExecPhaseDuration.WithLabelValues("unknown", "0")

	ExecBuildLockingInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseBuildLocking, "1")
	ExecOpenLockingInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseOpenLocking, "1")
	ExecNextLockingInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseNextLocking, "1")
	ExecLockLockingInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseLockLocking, "1")
	ExecBuildFinalInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseBuildFinal, "1")
	ExecOpenFinalInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseOpenFinal, "1")
	ExecNextFinalInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseNextFinal, "1")
	ExecLockFinalInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseLockFinal, "1")
	ExecCommitPrewriteInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitPrewrite, "1")
	ExecCommitCommitInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitCommit, "1")
	ExecCommitWaitCommitTSInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitWaitCommitTS, "1")
	ExecCommitWaitLatestTSInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitWaitLatestTS, "1")
	ExecCommitWaitLatchInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitWaitLatch, "1")
	ExecCommitWaitBinlogInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseCommitWaitBinlog, "1")
	ExecWriteResponseInternal = metrics.ExecPhaseDuration.WithLabelValues(PhaseWriteResponse, "1")
	ExecUnknownInternal = metrics.ExecPhaseDuration.WithLabelValues("unknown", "1")

	TransactionDurationPessimisticRollbackInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblRollback, metrics.LblInternal)
	TransactionDurationPessimisticRollbackGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblRollback, metrics.LblGeneral)
	TransactionDurationOptimisticRollbackInternal = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblRollback, metrics.LblInternal)
	TransactionDurationOptimisticRollbackGeneral = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblRollback, metrics.LblGeneral)

	MppCoordinatorStatsTotalRegisteredNumber = metrics.MppCoordinatorStats.WithLabelValues("total")
	MppCoordinatorStatsActiveNumber = metrics.MppCoordinatorStats.WithLabelValues("active")
	MppCoordinatorStatsOverTimeNumber = metrics.MppCoordinatorStats.WithLabelValues("overTime")
	MppCoordinatorStatsReportNotReceived = metrics.MppCoordinatorStats.WithLabelValues("reportNotRcv")

	MppCoordinatorLatencyRcvReport = metrics.MppCoordinatorLatency.WithLabelValues("rcvReports")
}

// InitPhaseDurationObserverMap init observer map
func InitPhaseDurationObserverMap() {
	PhaseDurationObserverMap = map[string]prometheus.Observer{
		PhaseBuildLocking:       ExecBuildLocking,
		PhaseOpenLocking:        ExecOpenLocking,
		PhaseNextLocking:        ExecNextLocking,
		PhaseLockLocking:        ExecLockLocking,
		PhaseBuildFinal:         ExecBuildFinal,
		PhaseOpenFinal:          ExecOpenFinal,
		PhaseNextFinal:          ExecNextFinal,
		PhaseLockFinal:          ExecLockFinal,
		PhaseCommitPrewrite:     ExecCommitPrewrite,
		PhaseCommitCommit:       ExecCommitCommit,
		PhaseCommitWaitCommitTS: ExecCommitWaitCommitTS,
		PhaseCommitWaitLatestTS: ExecCommitWaitLatestTS,
		PhaseCommitWaitLatch:    ExecCommitWaitLatch,
		PhaseCommitWaitBinlog:   ExecCommitWaitBinlog,
		PhaseWriteResponse:      ExecWriteResponse,
	}
	PhaseDurationObserverMapInternal = map[string]prometheus.Observer{
		PhaseBuildLocking:       ExecBuildLockingInternal,
		PhaseOpenLocking:        ExecOpenLockingInternal,
		PhaseNextLocking:        ExecNextLockingInternal,
		PhaseLockLocking:        ExecLockLockingInternal,
		PhaseBuildFinal:         ExecBuildFinalInternal,
		PhaseOpenFinal:          ExecOpenFinalInternal,
		PhaseNextFinal:          ExecNextFinalInternal,
		PhaseLockFinal:          ExecLockFinalInternal,
		PhaseCommitPrewrite:     ExecCommitPrewriteInternal,
		PhaseCommitCommit:       ExecCommitCommitInternal,
		PhaseCommitWaitCommitTS: ExecCommitWaitCommitTSInternal,
		PhaseCommitWaitLatestTS: ExecCommitWaitLatestTSInternal,
		PhaseCommitWaitLatch:    ExecCommitWaitLatchInternal,
		PhaseCommitWaitBinlog:   ExecCommitWaitBinlogInternal,
		PhaseWriteResponse:      ExecWriteResponseInternal,
	}
}
