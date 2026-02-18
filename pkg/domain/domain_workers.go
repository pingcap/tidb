// Copyright 2015 PingCAP, Inc.
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

package domain

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	metrics2 "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/expensivequery"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memoryusagealarm"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/servermemorylimit"
	"github.com/pingcap/tidb/pkg/workloadlearning"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

func (do *Domain) TelemetryLoop(ctx sessionctx.Context) {
	ctx.GetSessionVars().InRestrictedSQL = true
	err := telemetry.InitialRun(ctx)
	if err != nil {
		logutil.BgLogger().Warn("Initial telemetry run failed", zap.Error(err))
	}

	reportTicker := time.NewTicker(telemetry.ReportInterval)
	subWindowTicker := time.NewTicker(telemetry.SubWindowSize)

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("TelemetryReportLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "TelemetryReportLoop", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-reportTicker.C:
				err := telemetry.ReportUsageData(ctx)
				if err != nil {
					logutil.BgLogger().Warn("TelemetryLoop retports usaged data failed", zap.Error(err))
				}
			case <-subWindowTicker.C:
				telemetry.RotateSubWindow()
			}
		}
	}, "TelemetryLoop")
}

// SetupPlanReplayerHandle setup plan replayer handle
func (do *Domain) SetupPlanReplayerHandle(collectorSctx sessionctx.Context, workersSctxs []sessionctx.Context) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	do.planReplayerHandle = &planReplayerHandle{}
	do.planReplayerHandle.planReplayerTaskCollectorHandle = &planReplayerTaskCollectorHandle{
		ctx:  ctx,
		sctx: collectorSctx,
	}
	taskCH := make(chan *PlanReplayerDumpTask, 16)
	taskStatus := &planReplayerDumpTaskStatus{}
	taskStatus.finishedTaskMu.finishedTask = map[replayer.PlanReplayerTaskKey]struct{}{}
	taskStatus.runningTaskMu.runningTasks = map[replayer.PlanReplayerTaskKey]struct{}{}

	do.planReplayerHandle.planReplayerTaskDumpHandle = &planReplayerTaskDumpHandle{
		taskCH: taskCH,
		status: taskStatus,
	}
	do.planReplayerHandle.planReplayerTaskDumpHandle.workers = make([]*planReplayerTaskDumpWorker, 0)
	for i := range workersSctxs {
		worker := &planReplayerTaskDumpWorker{
			ctx:    ctx,
			sctx:   workersSctxs[i],
			taskCH: taskCH,
			status: taskStatus,
		}
		do.planReplayerHandle.planReplayerTaskDumpHandle.workers = append(do.planReplayerHandle.planReplayerTaskDumpHandle.workers, worker)
	}
}

// RunawayManager returns the runaway manager.
func (do *Domain) RunawayManager() *runaway.Manager {
	return do.runawayManager
}

// ResourceGroupsController returns the resource groups controller.
func (do *Domain) ResourceGroupsController() *rmclient.ResourceGroupsController {
	return do.resourceGroupsController.Load()
}

// SetResourceGroupsController is only used in test.
func (do *Domain) SetResourceGroupsController(controller *rmclient.ResourceGroupsController) {
	do.resourceGroupsController.Store(controller)
}

// SetupHistoricalStatsWorker setups worker

// ExpensiveQueryHandle returns the expensive query handle.
func (do *Domain) ExpensiveQueryHandle() *expensivequery.Handle {
	return do.expensiveQueryHandle
}

// MemoryUsageAlarmHandle returns the memory usage alarm handle.
func (do *Domain) MemoryUsageAlarmHandle() *memoryusagealarm.Handle {
	return do.memoryUsageAlarmHandle
}

// ServerMemoryLimitHandle returns the expensive query handle.
func (do *Domain) ServerMemoryLimitHandle() *servermemorylimit.Handle {
	return do.serverMemoryLimitHandle
}


// LoadSigningCertLoop loads the signing cert periodically to make sure it's fresh new.
func (do *Domain) LoadSigningCertLoop(signingCert, signingKey string) {
	sessionstates.SetCertPath(signingCert)
	sessionstates.SetKeyPath(signingKey)

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Debug("loadSigningCertLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "LoadSigningCertLoop", nil, false)

		for {
			select {
			case <-time.After(sessionstates.LoadCertInterval):
				sessionstates.ReloadSigningCert()
			case <-do.exit:
				return
			}
		}
	}, "loadSigningCertLoop")
}

// ServerID gets serverID.
// StartTTLJobManager creates and starts the ttl job manager
func (do *Domain) StartTTLJobManager() {
	ttlJobManager := ttlworker.NewJobManager(do.ddl.GetID(), do.advancedSysSessionPool, do.store, do.etcdClient, do.ddl.OwnerManager().IsOwner)
	do.ttlJobManager.Store(ttlJobManager)
	ttlJobManager.Start()
}

// TTLJobManager returns the ttl job manager on this domain
func (do *Domain) TTLJobManager() *ttlworker.JobManager {
	return do.ttlJobManager.Load()
}

// StopAutoAnalyze stops (*Domain).autoAnalyzeWorker to launch new auto analyze jobs.
func (do *Domain) StopAutoAnalyze() {
	do.stopAutoAnalyze.Store(true)
}

// InitInstancePlanCache initializes the instance level plan cache for this Domain.
func (do *Domain) InitInstancePlanCache() {
	hardLimit := vardef.InstancePlanCacheMaxMemSize.Load()
	softLimit := float64(hardLimit) * (1 - vardef.InstancePlanCacheReservedPercentage.Load())
	do.instancePlanCache = NewInstancePlanCache(int64(softLimit), hardLimit)
	// use a separate goroutine to avoid the eviction blocking other operations.
	do.wg.Run(do.planCacheEvictTrigger, "planCacheEvictTrigger")
	do.wg.Run(do.planCacheMetricsAndVars, "planCacheMetricsAndVars")
}

// GetInstancePlanCache returns the instance level plan cache in this Domain.
func (do *Domain) GetInstancePlanCache() sessionctx.InstancePlanCache {
	return do.instancePlanCache
}

// planCacheMetricsAndVars updates metrics and variables for Instance Plan Cache periodically.
func (do *Domain) planCacheMetricsAndVars() {
	defer util.Recover(metrics.LabelDomain, "planCacheMetricsAndVars", nil, false)
	ticker := time.NewTicker(time.Second * 15) // 15s by default
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("planCacheMetricsAndVars exited.")
	}()

	for {
		select {
		case <-ticker.C:
			// update limits
			hardLimit := vardef.InstancePlanCacheMaxMemSize.Load()
			softLimit := int64(float64(hardLimit) * (1 - vardef.InstancePlanCacheReservedPercentage.Load()))
			curSoft, curHard := do.instancePlanCache.GetLimits()
			if curSoft != softLimit || curHard != hardLimit {
				do.instancePlanCache.SetLimits(softLimit, hardLimit)
			}

			// update the metrics
			size := do.instancePlanCache.Size()
			memUsage := do.instancePlanCache.MemUsage()
			metrics2.GetPlanCacheInstanceNumCounter(true).Set(float64(size))
			metrics2.GetPlanCacheInstanceMemoryUsage(true).Set(float64(memUsage))
		case <-do.exit:
			return
		}
	}
}

// planCacheEvictTrigger triggers the plan cache eviction periodically.
func (do *Domain) planCacheEvictTrigger() {
	defer util.Recover(metrics.LabelDomain, "planCacheEvictTrigger", nil, false)
	ticker := time.NewTicker(time.Second * 30) // 30s by default
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("planCacheEvictTrigger exited.")
	}()

	for {
		select {
		case <-ticker.C:
			// trigger the eviction
			begin := time.Now()
			enabled := vardef.EnableInstancePlanCache.Load()
			detailInfo, numEvicted := do.instancePlanCache.Evict(!enabled) // evict all if the plan cache is disabled
			metrics2.GetPlanCacheInstanceEvict().Set(float64(numEvicted))
			if numEvicted > 0 {
				logutil.BgLogger().Info("instance plan eviction",
					zap.String("detail", detailInfo),
					zap.Int64("num_evicted", int64(numEvicted)),
					zap.Duration("time_spent", time.Since(begin)))
			}
		case <-do.exit:
			return
		}
	}
}

// SetupWorkloadBasedLearningWorker sets up all of the workload based learning workers.
func (do *Domain) SetupWorkloadBasedLearningWorker() {
	wbLearningHandle := workloadlearning.NewWorkloadLearningHandle(do.sysSessionPool)
	wbCacheWorker := workloadlearning.NewWLCacheWorker(do.sysSessionPool)
	// Start the workload based learning worker to analyze the read workload by statement_summary.
	do.wg.Run(
		func() {
			do.readTableCostWorker(wbLearningHandle, wbCacheWorker)
		},
		"readTableCostWorker",
	)
	// TODO: Add more workers for other workload based learning tasks.
}

// readTableCostWorker is a background worker that periodically analyze the read path table cost by statement_summary.
func (do *Domain) readTableCostWorker(wbLearningHandle *workloadlearning.Handle, wbCacheWorker *workloadlearning.WLCacheWorker) {
	// Recover the panic and log the error when worker exit.
	defer util.Recover(metrics.LabelDomain, "readTableCostWorker", nil, false)
	readTableCostTicker := time.NewTicker(vardef.WorkloadBasedLearningInterval.Load())
	defer func() {
		readTableCostTicker.Stop()
		logutil.BgLogger().Info("readTableCostWorker exited.")
	}()
	for {
		select {
		case <-readTableCostTicker.C:
			if vardef.EnableWorkloadBasedLearning.Load() && do.statsOwner.IsOwner() {
				wbLearningHandle.HandleTableReadCost(do.InfoSchema())
				wbCacheWorker.UpdateTableReadCostCache()
			}
		case <-do.exit:
			return
		}
	}
}

