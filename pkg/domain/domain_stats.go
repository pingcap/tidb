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
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)
func (do *Domain) SetupHistoricalStatsWorker(ctx sessionctx.Context) {
	do.historicalStatsWorker = &HistoricalStatsWorker{
		tblCH: make(chan int64, 16),
		sctx:  ctx,
	}
}

// SetupDumpFileGCChecker setup sctx
func (do *Domain) SetupDumpFileGCChecker(ctx sessionctx.Context) {
	do.dumpFileGcChecker.setupSctx(ctx)
	do.dumpFileGcChecker.planReplayerTaskStatus = do.planReplayerHandle.status
}

// SetupExtractHandle setups extract handler
func (do *Domain) SetupExtractHandle(sctxs []sessionctx.Context) {
	do.extractTaskHandle = newExtractHandler(do.ctx, sctxs)
}

var planReplayerHandleLease atomic.Uint64

func init() {
	planReplayerHandleLease.Store(uint64(10 * time.Second))
	enableDumpHistoricalStats.Store(true)
}

// DisablePlanReplayerBackgroundJob4Test disable plan replayer handle for test
func DisablePlanReplayerBackgroundJob4Test() {
	planReplayerHandleLease.Store(0)
}

// DisableDumpHistoricalStats4Test disable historical dump worker for test
func DisableDumpHistoricalStats4Test() {
	enableDumpHistoricalStats.Store(false)
}

// StartPlanReplayerHandle start plan replayer handle job
func (do *Domain) StartPlanReplayerHandle() {
	lease := planReplayerHandleLease.Load()
	if lease < 1 {
		return
	}
	do.wg.Run(func() {
		logutil.BgLogger().Info("PlanReplayerTaskCollectHandle started")
		tikcer := time.NewTicker(time.Duration(lease))
		defer func() {
			tikcer.Stop()
			logutil.BgLogger().Info("PlanReplayerTaskCollectHandle exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskCollectHandle", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-tikcer.C:
				err := do.planReplayerHandle.CollectPlanReplayerTask()
				if err != nil {
					logutil.BgLogger().Warn("plan replayer handle collect tasks failed", zap.Error(err))
				}
			}
		}
	}, "PlanReplayerTaskCollectHandle")

	do.wg.Run(func() {
		logutil.BgLogger().Info("PlanReplayerTaskDumpHandle started")
		defer func() {
			logutil.BgLogger().Info("PlanReplayerTaskDumpHandle exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskDumpHandle", nil, false)

		for _, worker := range do.planReplayerHandle.planReplayerTaskDumpHandle.workers {
			go worker.run()
		}
		<-do.exit
		do.planReplayerHandle.planReplayerTaskDumpHandle.Close()
	}, "PlanReplayerTaskDumpHandle")
}

// GetPlanReplayerHandle returns plan replayer handle
func (do *Domain) GetPlanReplayerHandle() *planReplayerHandle {
	return do.planReplayerHandle
}

// GetExtractHandle returns extract handle
func (do *Domain) GetExtractHandle() *ExtractHandle {
	return do.extractTaskHandle
}

// GetDumpFileGCChecker returns dump file GC checker for plan replayer and plan trace
func (do *Domain) GetDumpFileGCChecker() *dumpFileGcChecker {
	return do.dumpFileGcChecker
}

// DumpFileGcCheckerLoop creates a goroutine that handles `exit` and `gc`.
func (do *Domain) DumpFileGcCheckerLoop() {
	do.wg.Run(func() {
		logutil.BgLogger().Info("dumpFileGcChecker started")
		gcTicker := time.NewTicker(do.dumpFileGcChecker.gcLease)
		defer func() {
			gcTicker.Stop()
			logutil.BgLogger().Info("dumpFileGcChecker exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "dumpFileGcCheckerLoop", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-gcTicker.C:
				do.dumpFileGcChecker.GCDumpFiles(do.ctx, time.Hour, time.Hour*24*7)
			}
		}
	}, "dumpFileGcChecker")
}

// GetHistoricalStatsWorker gets historical workers
func (do *Domain) GetHistoricalStatsWorker() *HistoricalStatsWorker {
	return do.historicalStatsWorker
}

// EnableDumpHistoricalStats used to control whether enable dump stats for unit test
var enableDumpHistoricalStats atomic.Bool

// StartHistoricalStatsWorker start historical workers running
func (do *Domain) StartHistoricalStatsWorker() {
	if !enableDumpHistoricalStats.Load() {
		return
	}
	do.wg.Run(func() {
		logutil.BgLogger().Info("HistoricalStatsWorker started")
		defer func() {
			logutil.BgLogger().Info("HistoricalStatsWorker exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "HistoricalStatsWorkerLoop", nil, false)

		for {
			select {
			case <-do.exit:
				close(do.historicalStatsWorker.tblCH)
				return
			case tblID, ok := <-do.historicalStatsWorker.tblCH:
				if !ok {
					return
				}
				err := do.historicalStatsWorker.DumpHistoricalStats(tblID, do.StatsHandle())
				if err != nil {
					logutil.BgLogger().Warn("dump historical stats failed", zap.Error(err), zap.Int64("tableID", tblID))
				}
			}
		}
	}, "HistoricalStatsWorker")
}

// StatsHandle returns the statistic handle.
func (do *Domain) StatsHandle() *handle.Handle {
	return do.statsHandle.Load()
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx context.Context) error {
	h, err := handle.NewHandle(
		ctx,
		do.statsLease,
		do.advancedSysSessionPool,
		&do.sysProcesses,
		do.ddlNotifier,
		do.NextConnID,
		do.ReleaseConnID,
	)
	if err != nil {
		return err
	}
	h.StartWorker()
	do.statsHandle.Store(h)
	return nil
}

// StatsUpdating checks if the stats worker is updating.
func (do *Domain) StatsUpdating() bool {
	return do.statsUpdating.Load() > 0
}

// SetStatsUpdating sets the value of stats updating.
func (do *Domain) SetStatsUpdating(val bool) {
	if val {
		do.statsUpdating.Store(1)
	} else {
		do.statsUpdating.Store(0)
	}
}

// LoadAndUpdateStatsLoop loads and updates stats info.
func (do *Domain) LoadAndUpdateStatsLoop(concurrency int) error {
	if err := do.UpdateTableStatsLoop(); err != nil {
		return err
	}
	do.StartLoadStatsSubWorkers(concurrency)
	return nil
}

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop.
// It will also start a goroutine to analyze tables automatically.
// It should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop() error {
	statsHandle, err := handle.NewHandle(
		do.ctx,
		do.statsLease,
		do.advancedSysSessionPool,
		&do.sysProcesses,
		do.ddlNotifier,
		do.NextConnID,
		do.ReleaseConnID,
	)
	if err != nil {
		return err
	}
	statsHandle.StartWorker()
	do.statsHandle.Store(statsHandle)
	do.ddl.RegisterStatsHandle(statsHandle)
	// Negative stats lease indicates that it is in test or in br binary mode, it does not need update.
	if do.statsLease >= 0 {
		do.wg.Run(do.loadStatsWorker, "loadStatsWorker")
	}
	variable.EnableStatsOwner = do.enableStatsOwner
	variable.DisableStatsOwner = do.disableStatsOwner
	do.statsOwner = do.NewOwnerManager(handle.StatsPrompt, handle.StatsOwnerKey)
	do.statsOwner.SetListener(owner.NewListenersWrapper(do.ddlNotifier))
	if config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load() {
		err := do.statsOwner.CampaignOwner()
		if err != nil {
			logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
			return err
		}
	}
	do.wg.Run(func() {
		do.indexUsageWorker()
	}, "indexUsageWorker")
	if do.statsLease <= 0 {
		// For statsLease > 0, `gcStatsWorker` handles the quit of stats owner.
		do.wg.Run(func() { quitStatsOwner(do, do.statsOwner) }, "quitStatsOwner")
		return nil
	}
	waitStartTask := func(do *Domain, fn func()) {
		select {
		case <-do.StatsHandle().InitStatsDone:
		case <-do.exit: // It may happen that before initStatsDone, tidb receive Ctrl+C
			return
		}
		fn()
	}
	do.SetStatsUpdating(true)
	// The asyncLoadHistogram/dumpColStatsUsageWorker/deltaUpdateTickerWorker doesn't require the stats initialization to be completed.
	// This is because thos workers' primary responsibilities are to update the change delta and handle DDL operations.
	// These tasks need to be in work mod as soon as possible to avoid the problem.
	do.wg.Run(do.asyncLoadHistogram, "asyncLoadHistogram")
	do.wg.Run(do.deltaUpdateTickerWorker, "deltaUpdateTickerWorker")
	do.wg.Run(do.dumpColStatsUsageWorker, "dumpColStatsUsageWorker")
	do.wg.Run(func() { waitStartTask(do, do.gcStatsWorker) }, "gcStatsWorker")

	// Wait for the stats worker to finish the initialization.
	// Otherwise, we may start the auto analyze worker before the stats cache is initialized.
	do.wg.Run(func() { waitStartTask(do, do.autoAnalyzeWorker) }, "autoAnalyzeWorker")
	do.wg.Run(func() { waitStartTask(do, do.analyzeJobsCleanupWorker) }, "analyzeJobsCleanupWorker")
	return nil
}

// enableStatsOwner enables this node to execute stats owner jobs.
// Since ownerManager.CampaignOwner will start a new goroutine to run ownerManager.campaignLoop,
// we should make sure that before invoking enableStatsOwner(), stats owner is DISABLE.
func (do *Domain) enableStatsOwner() error {
	if !do.statsOwner.IsOwner() {
		err := do.statsOwner.CampaignOwner()
		return errors.Trace(err)
	}
	return nil
}

// disableStatsOwner disable this node to execute stats owner.
// We should make sure that before invoking disableStatsOwner(), stats owner is ENABLE.
func (do *Domain) disableStatsOwner() error {
	// disable campaign by interrupting campaignLoop
	do.statsOwner.CampaignCancel()
	return nil
}

func quitStatsOwner(do *Domain, mgr owner.Manager) {
	<-do.exit
	mgr.Close()
}

// StartLoadStatsSubWorkers starts sub workers with new sessions to load stats concurrently.
func (do *Domain) StartLoadStatsSubWorkers(concurrency int) {
	statsHandle := do.StatsHandle()
	for range concurrency {
		do.wg.Add(1)
		go statsHandle.SubLoadWorker(do.exit, do.wg)
	}
	logutil.BgLogger().Info("start load stats sub workers", zap.Int("workerCount", concurrency))
}

// NewOwnerManager returns the owner manager for use outside of the domain.
func (do *Domain) NewOwnerManager(prompt, ownerKey string) owner.Manager {
	id := do.ddl.OwnerManager().ID()
	var statsOwner owner.Manager
	if do.etcdClient == nil {
		statsOwner = owner.NewMockManager(do.ctx, id, do.store, ownerKey)
	} else {
		statsOwner = owner.NewOwnerManager(do.ctx, do.etcdClient, prompt, id, ownerKey)
	}
	return statsOwner
}

func (do *Domain) initStats(ctx context.Context) {
	statsHandle := do.StatsHandle()
	// If skip-init-stats is configured, skip the heavy initial stats loading as well.
	// Still close InitStatsDone to unblock waiters that may depend on it.
	if config.GetGlobalConfig().Performance.SkipInitStats {
		close(statsHandle.InitStatsDone)
		statslogutil.StatsLogger().Info("Skipping initial stats due to skip-grant-table being set")
		return
	}

	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("panic when initiating stats", zap.Any("r", r),
				zap.Stack("stack"))
		}
		close(statsHandle.InitStatsDone)
	}()
	t := time.Now()
	liteInitStats := config.GetGlobalConfig().Performance.LiteInitStats
	var err error
	if liteInitStats {
		err = statsHandle.InitStatsLite(ctx)
	} else {
		err = statsHandle.InitStats(ctx, do.InfoSchema())
	}
	if err != nil {
		statslogutil.StatsLogger().Error("Init stats failed", zap.Bool("isLiteInitStats", liteInitStats), zap.Duration("duration", time.Since(t)), zap.Error(err))
	} else {
		statslogutil.StatsLogger().Info("Init stats succeed", zap.Bool("isLiteInitStats", liteInitStats), zap.Duration("duration", time.Since(t)))
	}
}

func (do *Domain) loadStatsWorker() {
	defer util.Recover(metrics.LabelDomain, "loadStatsWorker", nil, false)
	lease := do.statsLease
	if lease == 0 {
		lease = 3 * time.Second
	}
	loadTicker := time.NewTicker(lease)
	defer func() {
		loadTicker.Stop()
		logutil.BgLogger().Info("loadStatsWorker exited.")
	}()

	ctx, cancelFunc := context.WithCancel(context.Background())
	do.cancelFns.mu.Lock()
	do.cancelFns.fns = append(do.cancelFns.fns, cancelFunc)
	do.cancelFns.mu.Unlock()

	do.initStats(ctx)
	statsHandle := do.StatsHandle()
	var err error
	for {
		select {
		case <-loadTicker.C:
			err = statsHandle.Update(ctx, do.InfoSchema())
			if err != nil {
				logutil.BgLogger().Warn("update stats info failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) asyncLoadHistogram() {
	defer util.Recover(metrics.LabelDomain, "asyncLoadStats", nil, false)
	lease := do.statsLease
	if lease == 0 {
		lease = 3 * time.Second
	}
	cleanupTicker := time.NewTicker(lease)
	defer func() {
		cleanupTicker.Stop()
		logutil.BgLogger().Info("asyncLoadStats exited.")
	}()
	select {
	case <-do.StatsHandle().InitStatsDone:
	case <-do.exit: // It may happen that before initStatsDone, tidb receive Ctrl+C
		return
	}
	statsHandle := do.StatsHandle()
	var err error
	for {
		select {
		case <-cleanupTicker.C:
			err = statsHandle.LoadNeededHistograms(do.InfoSchema())
			if err != nil {
				statslogutil.StatsErrVerboseSampleLogger().Warn("Load histograms failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) indexUsageWorker() {
	defer util.Recover(metrics.LabelDomain, "indexUsageWorker", nil, false)
	gcStatsTicker := time.NewTicker(indexUsageGCDuration)
	handle := do.StatsHandle()
	defer func() {
		logutil.BgLogger().Info("indexUsageWorker exited.")
	}()
	for {
		select {
		case <-do.exit:
			return
		case <-gcStatsTicker.C:
			if err := handle.GCIndexUsage(); err != nil {
				statslogutil.StatsLogger().Error("gc index usage failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) gcStatsWorkerExitPreprocessing() {
	ch := make(chan struct{}, 1)
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		logutil.BgLogger().Info("gcStatsWorker ready to release owner")
		do.statsOwner.Close()
		ch <- struct{}{}
	}()
	if intest.InTest {
		// We should wait for statistics owner to close on exit.
		// Otherwise, the goroutine leak detection may fail.
		<-ch
		logutil.BgLogger().Info("gcStatsWorker exit preprocessing finished")
		return
	}
	select {
	case <-ch:
		logutil.BgLogger().Info("gcStatsWorker exit preprocessing finished")
		return
	case <-timeout.Done():
		logutil.BgLogger().Warn("gcStatsWorker exit preprocessing timeout, force exiting")
		return
	}
}

func (*Domain) deltaUpdateTickerWorkerExitPreprocessing(statsHandle *handle.Handle) {
	ch := make(chan struct{}, 1)
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		logutil.BgLogger().Info("deltaUpdateTicker is going to exit, start to flush stats")
		statsHandle.FlushStats()
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		logutil.BgLogger().Info("deltaUpdateTicker exit preprocessing finished")
		return
	case <-timeout.Done():
		logutil.BgLogger().Warn("deltaUpdateTicker exit preprocessing timeout, force exiting")
		return
	}
}

func (do *Domain) gcStatsWorker() {
	defer util.Recover(metrics.LabelDomain, "gcStatsWorker", nil, false)
	logutil.BgLogger().Info("gcStatsWorker started.")
	lease := do.statsLease
	gcStatsTicker := time.NewTicker(100 * lease)
	updateStatsHealthyTicker := time.NewTicker(20 * lease)
	readMemTicker := time.NewTicker(memory.ReadMemInterval)
	statsHandle := do.StatsHandle()
	defer func() {
		gcStatsTicker.Stop()
		readMemTicker.Stop()
		updateStatsHealthyTicker.Stop()
		do.SetStatsUpdating(false)
		logutil.BgLogger().Info("gcStatsWorker exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "gcStatsWorker", nil, false)

	for {
		select {
		case <-do.exit:
			do.gcStatsWorkerExitPreprocessing()
			return
		case <-gcStatsTicker.C:
			if !do.statsOwner.IsOwner() {
				continue
			}
			err := statsHandle.GCStats(do.InfoSchema(), do.GetSchemaLease())
			if err != nil {
				logutil.BgLogger().Warn("GC stats failed", zap.Error(err))
			}
			do.CheckAutoAnalyzeWindows()
		case <-readMemTicker.C:
			memory.ForceReadMemStats()
			do.StatsHandle().StatsCache.TriggerEvict()
		case <-updateStatsHealthyTicker.C:
			statsHandle.UpdateStatsHealthyMetrics()
		}
	}
}

func (do *Domain) dumpColStatsUsageWorker() {
	logutil.BgLogger().Info("dumpColStatsUsageWorker started.")
	// We need to have different nodes trigger tasks at different times to avoid the herd effect.
	randDuration := time.Duration(rand.Int63n(int64(time.Minute)))
	dumpDuration := 100*do.statsLease + randDuration
	dumpColStatsUsageTicker := time.NewTicker(dumpDuration)
	statsHandle := do.StatsHandle()
	defer func() {
		dumpColStatsUsageTicker.Stop()
		logutil.BgLogger().Info("dumpColStatsUsageWorker exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "dumpColStatsUsageWorker", nil, false)

	for {
		select {
		case <-do.exit:
			return
		case <-dumpColStatsUsageTicker.C:
			err := statsHandle.DumpColStatsUsageToKV()
			if err != nil {
				logutil.BgLogger().Warn("dump column stats usage failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) deltaUpdateTickerWorker() {
	defer util.Recover(metrics.LabelDomain, "deltaUpdateTickerWorker", nil, false)
	logutil.BgLogger().Info("deltaUpdateTickerWorker started.")
	lease := do.statsLease
	// We need to have different nodes trigger tasks at different times to avoid the herd effect.
	randDuration := time.Duration(rand.Int63n(int64(time.Minute)))
	updateDuration := 20*lease + randDuration
	failpoint.Inject("deltaUpdateDuration", func() {
		updateDuration = 20 * time.Second
	})

	deltaUpdateTicker := time.NewTicker(updateDuration)
	statsHandle := do.StatsHandle()
	for {
		select {
		case <-do.exit:
			do.deltaUpdateTickerWorkerExitPreprocessing(statsHandle)
			return
		case <-deltaUpdateTicker.C:
			err := statsHandle.DumpStatsDeltaToKV(false)
			if err != nil {
				logutil.BgLogger().Warn("dump stats delta failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) autoAnalyzeWorker() {
	defer util.Recover(metrics.LabelDomain, "autoAnalyzeWorker", nil, false)
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer func() {
		analyzeTicker.Stop()
		statslogutil.StatsLogger().Info("autoAnalyzeWorker exited.")
	}()
	for {
		select {
		case <-analyzeTicker.C:
			// In order to prevent tidb from being blocked by the auto analyze task during shutdown,
			// a stopautoanalyze is added here for judgment.
			//
			// The reason for checking of stopAutoAnalyze is following:
			// According to the issue#41318, if we don't check stopAutoAnalyze here, the autoAnalyzeWorker will be tricker
			// again when domain.exit is true.
			// The "case <-analyzeTicker.C" condition and "case <-do.exit" condition are satisfied at the same time
			// when the system is already executing the shutdown task.
			// At this time, the Go language will randomly select a case that meets the conditions to execute,
			// and there is a probability that a new autoanalyze task will be started again
			// when the system has already executed the shutdown.
			// Because the time interval of statsLease is much smaller than the execution speed of auto analyze.
			// Therefore, when the current auto analyze is completed,
			// the probability of this happening is very high that the ticker condition and exist condition will be met
			// at the same time.
			// This causes the auto analyze task to be triggered all the time and block the shutdown of tidb.
			if vardef.RunAutoAnalyze.Load() && !do.stopAutoAnalyze.Load() && do.statsOwner.IsOwner() {
				statsHandle.HandleAutoAnalyze()
			} else if !vardef.RunAutoAnalyze.Load() || !do.statsOwner.IsOwner() {
				// Once the auto analyze is disabled or this instance is not the owner,
				// we close the priority queue to release resources.
				// This would guarantee that when auto analyze is re-enabled or this instance becomes the owner again,
				// the priority queue would be re-initialized.
				statsHandle.ClosePriorityQueue()
			}
		case <-do.exit:
			return
		}
	}
}

// analyzeJobsCleanupWorker is a background worker that periodically performs two main tasks:
//
//  1. Garbage Collection: It removes outdated analyze jobs from the statistics handle.
//     This operation is performed every hour and only if the current instance is the owner.
//     Analyze jobs older than 7 days are considered outdated and are removed.
//
//  2. Cleanup: It cleans up corrupted analyze jobs.
//     A corrupted analyze job is one that is in a 'pending' or 'running' state,
//     but is associated with a TiDB instance that is either not currently running or has been restarted.
//     Also, if the analyze job is killed by the user, it is considered corrupted.
//     This operation is performed every 100 stats leases.
//     It first retrieves the list of current analyze processes, then removes any analyze job
//     that is not associated with a current process. Additionally, if the current instance is the owner,
//     it also cleans up corrupted analyze jobs on dead instances.
func (do *Domain) analyzeJobsCleanupWorker() {
	defer util.Recover(metrics.LabelDomain, "analyzeJobsCleanupWorker", nil, false)
	// For GC.
	const gcInterval = time.Hour
	const daysToKeep = 7
	gcTicker := time.NewTicker(gcInterval)
	// For clean up.
	// Default stats lease is 3 * time.Second.
	// So cleanupInterval is 100 * 3 * time.Second = 5 * time.Minute.
	var cleanupInterval = do.statsLease * 100
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer func() {
		gcTicker.Stop()
		cleanupTicker.Stop()
		logutil.BgLogger().Info("analyzeJobsCleanupWorker exited.")
	}()
	statsHandle := do.StatsHandle()
	for {
		select {
		case <-gcTicker.C:
			// Only the owner should perform this operation.
			if do.statsOwner.IsOwner() {
				updateTime := time.Now().AddDate(0, 0, -daysToKeep)
				err := statsHandle.DeleteAnalyzeJobs(updateTime)
				if err != nil {
					logutil.BgLogger().Warn("gc analyze history failed", zap.Error(err))
				}
			}
		case <-cleanupTicker.C:
			sm := do.InfoSyncer().GetSessionManager()
			if sm == nil {
				continue
			}
			analyzeProcessIDs := make(map[uint64]struct{}, 8)
			for _, process := range sm.ShowProcessList() {
				if isAnalyzeTableSQL(process.Info) {
					analyzeProcessIDs[process.ID] = struct{}{}
				}
			}

			err := statsHandle.CleanupCorruptedAnalyzeJobsOnCurrentInstance(analyzeProcessIDs)
			if err != nil {
				logutil.BgLogger().Warn("cleanup analyze jobs on current instance failed", zap.Error(err))
			}

			if do.statsOwner.IsOwner() {
				err = statsHandle.CleanupCorruptedAnalyzeJobsOnDeadInstances()
				if err != nil {
					logutil.BgLogger().Warn("cleanup analyze jobs on dead instances failed", zap.Error(err))
				}
			}
		case <-do.exit:
			return
		}
	}
}

func isAnalyzeTableSQL(sql string) bool {
	// Get rid of the comments.
	normalizedSQL := parser.Normalize(sql, "ON")
	return strings.HasPrefix(normalizedSQL, "analyze table")
}
