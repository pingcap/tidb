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
	"sync"

	timermetrics "github.com/pingcap/tidb/pkg/timer/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	tikvmetrics "github.com/tikv/client-go/v2/metrics"
	"go.uber.org/zap"
)

var (
	// PanicCounter measures the count of panics.
	PanicCounter *prometheus.CounterVec

	// MemoryUsage measures the usage gauge of memory.
	MemoryUsage *prometheus.GaugeVec
)

// metrics labels.
const (
	LabelSession    = "session"
	LabelDomain     = "domain"
	LabelDDLOwner   = "ddl-owner"
	LabelDDL        = "ddl"
	LabelDDLWorker  = "ddl-worker"
	LabelDistReorg  = "dist-reorg"
	LabelDDLSyncer  = "ddl-syncer"
	LabelGCWorker   = "gcworker"
	LabelAnalyze    = "analyze"
	LabelWorkerPool = "worker-pool"

	LabelBatchRecvLoop = "batch-recv-loop"
	LabelBatchSendLoop = "batch-send-loop"

	opSucc   = "ok"
	opFailed = "err"

	TiDB         = "tidb"
	LabelScope   = "scope"
	ScopeGlobal  = "global"
	ScopeSession = "session"
	Server       = "server"
	TiKVClient   = "tikvclient"
)

// RetLabel returns "ok" when err == nil and "err" when err != nil.
// This could be useful when you need to observe the operation result.
func RetLabel(err error) string {
	if err == nil {
		return opSucc
	}
	return opFailed
}

func init() {
	InitMetrics()
}

// InitMetrics is used to initialize metrics.
func InitMetrics() {
	InitBindInfoMetrics()
	InitDDLMetrics()
	InitDistSQLMetrics()
	InitDomainMetrics()
	InitExecutorMetrics()
	InitGCWorkerMetrics()
	InitLogBackupMetrics()
	InitMetaMetrics()
	InitOwnerMetrics()
	InitResourceManagerMetrics()
	InitServerMetrics()
	InitSessionMetrics()
	InitSliMetrics()
	InitStatsMetrics()
	InitTopSQLMetrics()
	InitTTLMetrics()
	InitDistTaskMetrics()
	InitResourceGroupMetrics()
	InitGlobalSortMetrics()
	timermetrics.InitTimerMetrics()

	PanicCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "panic_total",
			Help:      "Counter of panic.",
		}, []string{LblType})

	MemoryUsage = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "memory_usage",
			Help:      "Memory Usage",
		}, []string{LblModule, LblType})
}

// RegisterMetrics registers the metrics which are ONLY used in TiDB server.
func RegisterMetrics() {
	// use new go collector
	prometheus.DefaultRegisterer.Unregister(prometheus.NewGoCollector())
	prometheus.MustRegister(collectors.NewGoCollector(collectors.WithGoCollections(collectors.GoRuntimeMetricsCollection | collectors.GoRuntimeMemStatsCollection)))

	prometheus.MustRegister(AutoAnalyzeCounter)
	prometheus.MustRegister(AutoAnalyzeHistogram)
	prometheus.MustRegister(AutoIDHistogram)
	prometheus.MustRegister(BatchAddIdxHistogram)
	prometheus.MustRegister(CampaignOwnerCounter)
	prometheus.MustRegister(ConnGauge)
	prometheus.MustRegister(DisconnectionCounter)
	prometheus.MustRegister(PreparedStmtGauge)
	prometheus.MustRegister(CriticalErrorCounter)
	prometheus.MustRegister(DDLCounter)
	prometheus.MustRegister(BackfillTotalCounter)
	prometheus.MustRegister(BackfillProgressGauge)
	prometheus.MustRegister(DDLWorkerHistogram)
	prometheus.MustRegister(DDLJobTableDuration)
	prometheus.MustRegister(DDLRunningJobCount)
	prometheus.MustRegister(DeploySyncerHistogram)
	prometheus.MustRegister(DistSQLPartialCountHistogram)
	prometheus.MustRegister(DistSQLCoprCacheCounter)
	prometheus.MustRegister(DistSQLCoprClosestReadCounter)
	prometheus.MustRegister(DistSQLCoprRespBodySize)
	prometheus.MustRegister(DistSQLQueryHistogram)
	prometheus.MustRegister(DistSQLScanKeysHistogram)
	prometheus.MustRegister(DistSQLScanKeysPartialHistogram)
	prometheus.MustRegister(ExecuteErrorCounter)
	prometheus.MustRegister(ExecutorCounter)
	prometheus.MustRegister(GetTokenDurationHistogram)
	prometheus.MustRegister(NumOfMultiQueryHistogram)
	prometheus.MustRegister(HandShakeErrorCounter)
	prometheus.MustRegister(HandleJobHistogram)
	prometheus.MustRegister(SyncLoadCounter)
	prometheus.MustRegister(SyncLoadTimeoutCounter)
	prometheus.MustRegister(SyncLoadHistogram)
	prometheus.MustRegister(ReadStatsHistogram)
	prometheus.MustRegister(JobsGauge)
	prometheus.MustRegister(LoadPrivilegeCounter)
	prometheus.MustRegister(InfoCacheCounters)
	prometheus.MustRegister(LeaseExpireTime)
	prometheus.MustRegister(LoadSchemaCounter)
	prometheus.MustRegister(LoadSchemaDuration)
	prometheus.MustRegister(MetaHistogram)
	prometheus.MustRegister(NewSessionHistogram)
	prometheus.MustRegister(OwnerHandleSyncerHistogram)
	prometheus.MustRegister(PanicCounter)
	prometheus.MustRegister(PlanCacheCounter)
	prometheus.MustRegister(PlanCacheMissCounter)
	prometheus.MustRegister(PlanCacheInstanceMemoryUsage)
	prometheus.MustRegister(PlanCacheInstancePlanNumCounter)
	prometheus.MustRegister(PseudoEstimation)
	prometheus.MustRegister(PacketIOCounter)
	prometheus.MustRegister(QueryDurationHistogram)
	prometheus.MustRegister(QueryTotalCounter)
	prometheus.MustRegister(AffectedRowsCounter)
	prometheus.MustRegister(SchemaLeaseErrorCounter)
	prometheus.MustRegister(ServerEventCounter)
	prometheus.MustRegister(SessionExecuteCompileDuration)
	prometheus.MustRegister(SessionExecuteParseDuration)
	prometheus.MustRegister(SessionExecuteRunDuration)
	prometheus.MustRegister(SessionRestrictedSQLCounter)
	prometheus.MustRegister(SessionRetry)
	prometheus.MustRegister(SessionRetryErrorCounter)
	prometheus.MustRegister(StatementPerTransaction)
	prometheus.MustRegister(StatsInaccuracyRate)
	prometheus.MustRegister(StmtNodeCounter)
	prometheus.MustRegister(DbStmtNodeCounter)
	prometheus.MustRegister(ExecPhaseDuration)
	prometheus.MustRegister(OngoingTxnDurationHistogram)
	prometheus.MustRegister(MppCoordinatorStats)
	prometheus.MustRegister(MppCoordinatorLatency)
	prometheus.MustRegister(TimeJumpBackCounter)
	prometheus.MustRegister(TransactionDuration)
	prometheus.MustRegister(StatementDeadlockDetectDuration)
	prometheus.MustRegister(StatementPessimisticRetryCount)
	prometheus.MustRegister(StatementLockKeysCount)
	prometheus.MustRegister(ValidateReadTSFromPDCount)
	prometheus.MustRegister(UpdateSelfVersionHistogram)
	prometheus.MustRegister(WatchOwnerCounter)
	prometheus.MustRegister(GCActionRegionResultCounter)
	prometheus.MustRegister(GCConfigGauge)
	prometheus.MustRegister(GCHistogram)
	prometheus.MustRegister(GCJobFailureCounter)
	prometheus.MustRegister(GCRegionTooManyLocksCounter)
	prometheus.MustRegister(GCWorkerCounter)
	prometheus.MustRegister(TotalQueryProcHistogram)
	prometheus.MustRegister(TotalCopProcHistogram)
	prometheus.MustRegister(TotalCopWaitHistogram)
	prometheus.MustRegister(CopMVCCRatioHistogram)
	prometheus.MustRegister(HandleSchemaValidate)
	prometheus.MustRegister(MaxProcs)
	prometheus.MustRegister(GOGC)
	prometheus.MustRegister(ConnIdleDurationHistogram)
	prometheus.MustRegister(ServerInfo)
	prometheus.MustRegister(TokenGauge)
	prometheus.MustRegister(ConfigStatus)
	prometheus.MustRegister(TiFlashQueryTotalCounter)
	prometheus.MustRegister(TiFlashFailedMPPStoreState)
	prometheus.MustRegister(SmallTxnWriteDuration)
	prometheus.MustRegister(TxnWriteThroughput)
	prometheus.MustRegister(LoadSysVarCacheCounter)
	prometheus.MustRegister(TopSQLIgnoredCounter)
	prometheus.MustRegister(TopSQLReportDurationHistogram)
	prometheus.MustRegister(TopSQLReportDataHistogram)
	prometheus.MustRegister(PDAPIExecutionHistogram)
	prometheus.MustRegister(PDAPIRequestCounter)
	prometheus.MustRegister(CPUProfileCounter)
	prometheus.MustRegister(ReadFromTableCacheCounter)
	prometheus.MustRegister(LoadTableCacheDurationHistogram)
	prometheus.MustRegister(NonTransactionalDMLCount)
	prometheus.MustRegister(PessimisticDMLDurationByAttempt)
	prometheus.MustRegister(ResetAutoIDConnCounter)
	prometheus.MustRegister(ResourceGroupQueryTotalCounter)
	prometheus.MustRegister(MemoryUsage)
	prometheus.MustRegister(StatsCacheCounter)
	prometheus.MustRegister(StatsCacheGauge)
	prometheus.MustRegister(StatsHealthyGauge)
	prometheus.MustRegister(StatsDeltaLoadHistogram)
	prometheus.MustRegister(StatsDeltaUpdateHistogram)
	prometheus.MustRegister(TxnStatusEnteringCounter)
	prometheus.MustRegister(TxnDurationHistogram)
	prometheus.MustRegister(LastCheckpoint)
	prometheus.MustRegister(AdvancerOwner)
	prometheus.MustRegister(AdvancerTickDuration)
	prometheus.MustRegister(GetCheckpointBatchSize)
	prometheus.MustRegister(RegionCheckpointRequest)
	prometheus.MustRegister(RegionCheckpointFailure)
	prometheus.MustRegister(AutoIDReqDuration)
	prometheus.MustRegister(RegionCheckpointSubscriptionEvent)
	prometheus.MustRegister(RCCheckTSWriteConfilictCounter)
	prometheus.MustRegister(FairLockingUsageCount)
	prometheus.MustRegister(MemoryLimit)

	prometheus.MustRegister(TTLQueryDuration)
	prometheus.MustRegister(TTLProcessedExpiredRowsCounter)
	prometheus.MustRegister(TTLJobStatus)
	prometheus.MustRegister(TTLTaskStatus)
	prometheus.MustRegister(TTLPhaseTime)
	prometheus.MustRegister(TTLInsertRowsCount)
	prometheus.MustRegister(TTLWatermarkDelay)
	prometheus.MustRegister(TTLEventCounter)

	prometheus.MustRegister(timermetrics.TimerEventCounter)

	prometheus.MustRegister(EMACPUUsageGauge)
	prometheus.MustRegister(PoolConcurrencyCounter)

	prometheus.MustRegister(HistoricalStatsCounter)
	prometheus.MustRegister(PlanReplayerTaskCounter)
	prometheus.MustRegister(PlanReplayerRegisterTaskGauge)

	prometheus.MustRegister(DistTaskGauge)
	prometheus.MustRegister(DistTaskStartTimeGauge)
	prometheus.MustRegister(DistTaskUsedSlotsGauge)
	prometheus.MustRegister(RunawayCheckerCounter)
	prometheus.MustRegister(GlobalSortWriteToCloudStorageDuration)
	prometheus.MustRegister(GlobalSortWriteToCloudStorageRate)
	prometheus.MustRegister(GlobalSortReadFromCloudStorageDuration)
	prometheus.MustRegister(GlobalSortReadFromCloudStorageRate)
	prometheus.MustRegister(GlobalSortIngestWorkerCnt)
	prometheus.MustRegister(GlobalSortUploadWorkerCount)
	prometheus.MustRegister(AddIndexScanRate)

	prometheus.MustRegister(BindingCacheHitCounter)
	prometheus.MustRegister(BindingCacheMissCounter)
	prometheus.MustRegister(BindingCacheMemUsage)
	prometheus.MustRegister(BindingCacheMemLimit)
	prometheus.MustRegister(BindingCacheNumBindings)

	tikvmetrics.InitMetrics(TiDB, TiKVClient)
	tikvmetrics.RegisterMetrics()
	tikvmetrics.TiKVPanicCounter = PanicCounter // reset tidb metrics for tikv metrics
}

var mode struct {
	sync.Mutex
	isSimplified bool
}

// ToggleSimplifiedMode is used to register/unregister the metrics that unused by grafana.
func ToggleSimplifiedMode(simplified bool) {
	var unusedMetricsByGrafana = []prometheus.Collector{
		StatementDeadlockDetectDuration,
		ValidateReadTSFromPDCount,
		LoadTableCacheDurationHistogram,
		TxnWriteThroughput,
		SmallTxnWriteDuration,
		InfoCacheCounters,
		ReadFromTableCacheCounter,
		TiFlashQueryTotalCounter,
		TiFlashFailedMPPStoreState,
		CampaignOwnerCounter,
		NonTransactionalDMLCount,
		MemoryUsage,
		TokenGauge,
		tikvmetrics.TiKVRawkvSizeHistogram,
		tikvmetrics.TiKVRawkvCmdHistogram,
		tikvmetrics.TiKVReadThroughput,
		tikvmetrics.TiKVSmallReadDuration,
		tikvmetrics.TiKVBatchWaitOverLoad,
		tikvmetrics.TiKVBatchClientRecycle,
		tikvmetrics.TiKVRequestRetryTimesHistogram,
		tikvmetrics.TiKVStatusDuration,
	}
	mode.Lock()
	defer mode.Unlock()
	if mode.isSimplified == simplified {
		return
	}
	mode.isSimplified = simplified
	if simplified {
		for _, m := range unusedMetricsByGrafana {
			prometheus.Unregister(m)
		}
	} else {
		for _, m := range unusedMetricsByGrafana {
			err := prometheus.Register(m)
			if err != nil {
				logutil.BgLogger().Error("cannot register metrics", zap.Error(err))
				break
			}
		}
	}
}
