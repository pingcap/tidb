// Copyright 2024 PingCAP, Inc.
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

package priorityqueue

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/internal/heap"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

const notInitializedErrMsg = "priority queue not initialized"

const (
	lastAnalysisDurationRefreshInterval = time.Minute * 10
	dmlChangesFetchInterval             = time.Minute * 2
	failedJobRequeueInterval            = time.Minute * 5
)

// AnalysisPriorityQueueV2 is a priority queue for TableAnalysisJobs.
// Testing shows that keeping all jobs in memory is feasible.
// Memory usage for one million tables is approximately 300 to 500 MiB, which is acceptable.
// Typically, not many tables need to be analyzed simultaneously.
//
//nolint:fieldalignment
type AnalysisPriorityQueueV2 struct {
	statsHandle statstypes.StatsHandle
	calculator  *PriorityCalculator

	ctx    context.Context
	cancel context.CancelFunc
	wg     util.WaitGroupWrapper

	// syncFields is a substructure to hold fields protected by mu.
	syncFields struct {
		// mu is used to protect the following fields.
		mu    sync.RWMutex
		inner *heap.Heap[int64, AnalysisJob]
		// runningJobs is a map to store the running jobs. Used to avoid duplicate jobs.
		runningJobs map[int64]struct{}
		// lastDMLUpdateFetchTimestamp is the timestamp of the last DML update fetch.
		lastDMLUpdateFetchTimestamp uint64
		// failedJobs is a slice to store the failed jobs.
		failedJobs map[int64]struct{}
		// initialized is a flag to check if the queue is initialized.
		initialized bool
	}
}

// NewAnalysisPriorityQueueV2 creates a new AnalysisPriorityQueue2.
func NewAnalysisPriorityQueueV2(handle statstypes.StatsHandle) *AnalysisPriorityQueueV2 {
	ctx, cancel := context.WithCancel(context.Background())

	queue := &AnalysisPriorityQueueV2{
		statsHandle: handle,
		calculator:  NewPriorityCalculator(),
		ctx:         ctx,
		cancel:      cancel,
	}

	queue.syncFields.mu.Lock()
	queue.syncFields.runningJobs = make(map[int64]struct{})
	queue.syncFields.failedJobs = make(map[int64]struct{})
	queue.syncFields.mu.Unlock()

	return queue
}

// IsInitialized checks if the priority queue is initialized.
func (pq *AnalysisPriorityQueueV2) IsInitialized() bool {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()

	return pq.syncFields.initialized
}

// Initialize initializes the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueueV2) Initialize() error {
	pq.syncFields.mu.Lock()
	if pq.syncFields.initialized {
		statslogutil.StatsLogger().Warn("Priority queue already initialized")
		pq.syncFields.mu.Unlock()
		return nil
	}
	pq.syncFields.mu.Unlock()

	start := time.Now()
	defer func() {
		statslogutil.StatsLogger().Info("Priority queue initialized", zap.Duration("duration", time.Since(start)))
	}()

	pq.syncFields.mu.Lock()
	if err := pq.rebuildWithoutLock(); err != nil {
		pq.syncFields.mu.Unlock()
		pq.Close()
		return errors.Trace(err)
	}
	pq.syncFields.initialized = true
	pq.syncFields.mu.Unlock()

	// Start a goroutine to maintain the priority queue.
	pq.wg.Run(pq.run)
	return nil
}

// Rebuild rebuilds the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueueV2) Rebuild() error {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()

	if !pq.syncFields.initialized {
		return errors.New(notInitializedErrMsg)
	}

	return pq.rebuildWithoutLock()
}

// rebuildWithoutLock rebuilds the priority queue without holding the lock.
// Note: Please hold the lock before calling this function.
func (pq *AnalysisPriorityQueueV2) rebuildWithoutLock() error {
	keyFunc := func(job AnalysisJob) (int64, error) {
		return job.GetTableID(), nil
	}
	// We want the job with the highest weight to be at the top of the priority queue.
	lessFunc := func(a, b AnalysisJob) bool {
		return a.GetWeight() > b.GetWeight()
	}
	pq.syncFields.inner = heap.NewHeap(keyFunc, lessFunc)
	return statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		parameters := exec.GetAutoAnalyzeParameters(sctx)
		// We need to fetch the next check version with offset before fetching all tables and building analysis jobs.
		// Otherwise, we may miss some DML changes happened during the process because this operation takes time.
		// For example, 1m tables will take about 1min to fetch all tables and build analysis jobs.
		// This will guarantee that we will not miss any DML changes. But it may cause some DML changes to be processed twice.
		// It is acceptable since the DML changes operation is idempotent.
		nextCheckVersionWithOffset := pq.statsHandle.GetNextCheckVersionWithOffset()
		err := FetchAllTablesAndBuildAnalysisJobs(
			sctx,
			parameters,
			pq.statsHandle,
			pq.pushWithoutLock,
		)

		// Update the last fetch timestamp of DML updates.
		pq.syncFields.lastDMLUpdateFetchTimestamp = nextCheckVersionWithOffset
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	}, statsutil.FlagWrapTxn)
}

// run maintains the priority queue.
func (pq *AnalysisPriorityQueueV2) run() {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error("Priority queue panicked", zap.Any("recover", r), zap.Stack("stack"))
		}
	}()

	dmlChangesFetchInterval := time.NewTicker(dmlChangesFetchInterval)
	defer dmlChangesFetchInterval.Stop()
	timeRefreshInterval := time.NewTicker(lastAnalysisDurationRefreshInterval)
	defer timeRefreshInterval.Stop()
	failedJobRequeueInterval := time.NewTicker(failedJobRequeueInterval)
	defer failedJobRequeueInterval.Stop()

	for {
		select {
		case <-pq.ctx.Done():
			statslogutil.StatsLogger().Info("Priority queue stopped")
			return
		case <-dmlChangesFetchInterval.C:
			statslogutil.StatsLogger().Info("Start to fetch DML changes of jobs")
			pq.ProcessDMLChanges()
		case <-timeRefreshInterval.C:
			statslogutil.StatsLogger().Info("Start to refresh last analysis durations of jobs")
			pq.RefreshLastAnalysisDuration()
		case <-failedJobRequeueInterval.C:
			statslogutil.StatsLogger().Info("Start to request failed jobs")
			pq.RequeueFailedJobs()
		}
	}
}

// ProcessDMLChanges processes DML changes.
// Note: This function is thread-safe.
// Performance: To scan all table stats and process the DML changes, it takes about less than 100ms for 1m tables.
func (pq *AnalysisPriorityQueueV2) ProcessDMLChanges() {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()

	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		start := time.Now()
		defer func() {
			statslogutil.StatsLogger().Info("DML changes processed", zap.Duration("duration", time.Since(start)))
		}()

		parameters := exec.GetAutoAnalyzeParameters(sctx)
		// We need to fetch the next check version with offset before fetching new DML changes.
		// Otherwise, we may miss some DML changes happened during the process.
		newMaxVersion := pq.statsHandle.GetNextCheckVersionWithOffset()
		values := pq.statsHandle.Values()
		lastFetchTimestamp := pq.syncFields.lastDMLUpdateFetchTimestamp
		for _, value := range values {
			// We only process the tables that have been updated.
			// So here we only need to process the tables whose version is greater than the last fetch timestamp.
			if value.Version > lastFetchTimestamp {
				// TODO: consider locked tables and partitions. Consider move this check to
				err := pq.processTableStats(sctx, value, parameters)
				if err != nil {
					statslogutil.StatsLogger().Error(
						"Failed to process table stats",
						zap.Error(err),
						zap.Int64("tableID", value.PhysicalID),
					)
				}
			}
		}

		// Only update if we've seen a newer version
		if newMaxVersion > lastFetchTimestamp {
			statslogutil.StatsLogger().Info("Updating last fetch timestamp", zap.Uint64("new_max_version", newMaxVersion))
			pq.syncFields.lastDMLUpdateFetchTimestamp = newMaxVersion
		}
		return nil
	}, statsutil.FlagWrapTxn); err != nil {
		statslogutil.StatsLogger().Error("Failed to process DML changes", zap.Error(err))
	}
}

// Note: Please hold the lock before calling this function.
func (pq *AnalysisPriorityQueueV2) processTableStats(
	sctx sessionctx.Context,
	stats *statistics.Table,
	parameters map[string]string,
) error {
	// Check if the table is eligible for analysis first to avoid unnecessary work.
	if !stats.IsEligibleForAnalysis() {
		return nil
	}

	autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	// Get current timestamp from the session context.
	currentTs, err := getStartTs(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	jobFactory := NewAnalysisJobFactory(sctx, autoAnalyzeRatio, currentTs)
	// Check if the table is needed to be analyzed.
	// Note: Unanalyzed tables will also be considered.
	changePercent := jobFactory.CalculateChangePercentage(stats)
	if changePercent == 0 {
		return nil
	}
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())

	var job AnalysisJob
	// For dynamic partitioned tables, we need to recreate the job if the partition stats are updated.
	// This means we will always enter the tryCreateJob branch for these partitions.
	// Since we store the stats meta for each partition and the parent table, there may be redundant calculations.
	// This is acceptable for now, but in the future, we may consider separating the analysis job for each partition.
	job, ok, _ := pq.syncFields.inner.GetByKey(stats.PhysicalID)
	if !ok {
		job = pq.tryCreateJob(is, stats, pruneMode, jobFactory)
	} else {
		job = pq.tryUpdateJob(is, stats, job, jobFactory)
	}
	return pq.pushWithoutLock(job)
}
func (pq *AnalysisPriorityQueueV2) tryCreateJob(
	is infoschema.InfoSchema,
	stats *statistics.Table,
	pruneMode variable.PartitionPruneMode,
	jobFactory *AnalysisJobFactory,
) (job AnalysisJob) {
	if stats == nil {
		return nil
	}

	tableInfo, ok := pq.statsHandle.TableInfoByID(is, stats.PhysicalID)
	tableMeta := tableInfo.Meta()
	if !ok {
		statslogutil.StatsLogger().Warn(
			"Table info not found for table id",
			zap.Int64("tableID", stats.PhysicalID),
		)
		return nil
	}
	schemaName, ok := is.SchemaNameByTableID(tableMeta.ID)
	if !ok {
		statslogutil.StatsLogger().Warn(
			"Schema name not found for table id",
			zap.Int64("tableID", stats.PhysicalID),
		)
		return nil
	}
	partitionedTable := tableMeta.GetPartitionInfo()
	if partitionedTable == nil {
		job = jobFactory.CreateNonPartitionedTableAnalysisJob(
			schemaName.O,
			tableMeta,
			stats,
		)
	} else {
		partitionDefs := partitionedTable.Definitions
		if pruneMode == variable.Static {
			var partitionDef model.PartitionDefinition
			found := false
			// Find the specific partition definition.
			for _, def := range partitionDefs {
				if def.ID == stats.PhysicalID {
					partitionDef = def
					found = true
					break
				}
			}
			if !found {
				// This usually indicates that the stats are for the parent (global) table.
				// In static partition mode, we do not analyze the parent table.
				// TODO: add tests to verify this behavior.
				return nil
			}
			job = jobFactory.CreateStaticPartitionAnalysisJob(
				schemaName.O,
				tableMeta,
				partitionDef.ID,
				partitionDef.Name.O,
				stats,
			)
		} else {
			partitionStats := GetPartitionStats(pq.statsHandle, tableMeta, partitionDefs)
			job = jobFactory.CreateDynamicPartitionedTableAnalysisJob(
				schemaName.O,
				tableMeta,
				// Get global stats for dynamic partitioned table.
				pq.statsHandle.GetTableStatsForAutoAnalyze(tableMeta),
				partitionStats,
			)
		}
	}
	return job
}
func (pq *AnalysisPriorityQueueV2) tryUpdateJob(
	is infoschema.InfoSchema,
	stats *statistics.Table,
	oldJob AnalysisJob,
	jobFactory *AnalysisJobFactory,
) AnalysisJob {
	if stats == nil {
		return nil
	}
	intest.Assert(oldJob != nil)
	indicators := oldJob.GetIndicators()

	// For dynamic partitioned table, there is no way to only update the partition that has been changed.
	// So we recreate the job for dynamic partitioned table.
	if IsDynamicPartitionedTableAnalysisJob(oldJob) {
		tableInfo, ok := pq.statsHandle.TableInfoByID(is, stats.PhysicalID)
		if !ok {
			statslogutil.StatsLogger().Warn(
				"Table info not found during updating job",
				zap.Int64("tableID", stats.PhysicalID),
				zap.String("job", oldJob.String()),
			)
			return nil
		}
		tableMeta := tableInfo.Meta()
		partitionedTable := tableMeta.GetPartitionInfo()
		partitionDefs := partitionedTable.Definitions
		partitionStats := GetPartitionStats(pq.statsHandle, tableMeta, partitionDefs)
		schemaName, ok := is.SchemaNameByTableID(tableMeta.ID)
		if !ok {
			statslogutil.StatsLogger().Warn(
				"Schema name not found during updating job",
				zap.Int64("tableID", stats.PhysicalID),
				zap.String("job", oldJob.String()),
			)
			return nil
		}
		return jobFactory.CreateDynamicPartitionedTableAnalysisJob(
			schemaName.O,
			tableMeta,
			stats,
			partitionStats,
		)
	}
	// Otherwise, we update the indicators of the job.
	indicators.ChangePercentage = jobFactory.CalculateChangePercentage(stats)
	indicators.TableSize = jobFactory.CalculateTableSize(stats)
	oldJob.SetIndicators(indicators)
	return oldJob
}

// GetLastFetchTimestamp returns the last fetch timestamp of DML updates.
// Note: This function is thread-safe.
// Exported for testing.
func (pq *AnalysisPriorityQueueV2) GetLastFetchTimestamp() uint64 {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()

	return pq.syncFields.lastDMLUpdateFetchTimestamp
}

// RequeueFailedJobs requeues the failed jobs.
func (pq *AnalysisPriorityQueueV2) RequeueFailedJobs() {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()

	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		start := time.Now()
		defer func() {
			statslogutil.StatsLogger().Info("Failed jobs requeued", zap.Duration("duration", time.Since(start)))
		}()

		is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
		for tableID := range pq.syncFields.failedJobs {
			delete(pq.syncFields.failedJobs, tableID)
			tblInfo, ok := pq.statsHandle.TableInfoByID(is, tableID)
			if !ok {
				statslogutil.StatsLogger().Warn("Table info not found during requeueing failed jobs", zap.Int64("tableID", tableID))
				continue
			}
			err := pq.recreateAndPushJobForTable(sctx, tblInfo.Meta())
			if err != nil {
				statslogutil.StatsLogger().Error("Failed to recreate and push job for table", zap.Error(err), zap.Int64("tableID", tableID))
				continue
			}
		}
		return nil
	}, statsutil.FlagWrapTxn); err != nil {
		statslogutil.StatsLogger().Error("Failed to requeue failed jobs", zap.Error(err))
	}
}

// RefreshLastAnalysisDuration refreshes the last analysis duration of all jobs in the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueueV2) RefreshLastAnalysisDuration() {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()

	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		start := time.Now()
		defer func() {
			statslogutil.StatsLogger().Info("Last analysis duration refreshed", zap.Duration("duration", time.Since(start)))
		}()
		jobs := pq.syncFields.inner.List()
		currentTs, err := getStartTs(sctx)
		if err != nil {
			return errors.Trace(err)
		}
		jobFactory := NewAnalysisJobFactory(sctx, 0, currentTs)
		for _, job := range jobs {
			indicators := job.GetIndicators()
			tableStats, ok := pq.statsHandle.Get(job.GetTableID())
			if !ok {
				statslogutil.StatsLogger().Warn("Table stats not found during refreshing last analysis duration",
					zap.Int64("tableID", job.GetTableID()),
					zap.String("job", job.String()),
				)
				// TODO: Remove this after handling the DDL event.
				err := pq.syncFields.inner.Delete(job)
				if err != nil {
					statslogutil.StatsLogger().Error("Failed to delete job from priority queue",
						zap.Error(err),
						zap.String("job", job.String()),
					)
				}
			}
			indicators.LastAnalysisDuration = jobFactory.GetTableLastAnalyzeDuration(tableStats)
			job.SetIndicators(indicators)
			job.SetWeight(pq.calculator.CalculateWeight(job))
			if err := pq.syncFields.inner.Update(job); err != nil {
				statslogutil.StatsLogger().Error("Failed to add job to priority queue",
					zap.Error(err),
					zap.String("job", job.String()),
				)
			}
		}
		return nil
	}, statsutil.FlagWrapTxn); err != nil {
		statslogutil.StatsLogger().Error("Failed to refresh last analysis duration", zap.Error(err))
	}
}

// GetRunningJobs returns the running jobs.
// Note: This function is thread-safe.
// Exported for testing.
func (pq *AnalysisPriorityQueueV2) GetRunningJobs() map[int64]struct{} {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()

	runningJobs := make(map[int64]struct{}, len(pq.syncFields.runningJobs))
	for id := range pq.syncFields.runningJobs {
		runningJobs[id] = struct{}{}
	}
	return runningJobs
}

// Push pushes a job into the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueueV2) Push(job AnalysisJob) error {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	if !pq.syncFields.initialized {
		return errors.New(notInitializedErrMsg)
	}

	return pq.pushWithoutLock(job)
}
func (pq *AnalysisPriorityQueueV2) pushWithoutLock(job AnalysisJob) error {
	if job == nil {
		return nil
	}
	weight := pq.calculator.CalculateWeight(job)
	job.SetWeight(weight)
	// Skip the current running jobs.
	// Safety:
	// Let's say we have a job in the priority queue, and it is already running.
	// Then we will not add the same job to the priority queue again. Otherwise, we will analyze the same table twice.
	// If the job is finished, we will remove it from the running jobs.
	// Then the next time we process the DML changes, we will add the job to the priority queue.(if it is still needed)
	// In this process, we will not miss any DML changes of the table. Because when we try to delete the table from the current running jobs,
	// we guarantee that the job is finished and the stats cache is updated.(The last step of the analysis job is to update the stats cache).
	if _, ok := pq.syncFields.runningJobs[job.GetTableID()]; ok {
		return nil
	}
	// Skip the failed jobs.
	// Avoiding requeueing the failed jobs before the next failed job requeue interval.
	// Otherwise, we may requeue the same job multiple times in a short time.
	if _, ok := pq.syncFields.failedJobs[job.GetTableID()]; ok {
		return nil
	}
	return pq.syncFields.inner.Add(job)
}

// Pop pops a job from the priority queue and marks it as running.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueueV2) Pop() (AnalysisJob, error) {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	if !pq.syncFields.initialized {
		return nil, errors.New(notInitializedErrMsg)
	}

	job, err := pq.syncFields.inner.Pop()
	if err != nil {
		return nil, errors.Trace(err)
	}
	pq.syncFields.runningJobs[job.GetTableID()] = struct{}{}

	job.RegisterSuccessHook(func(j AnalysisJob) {
		pq.syncFields.mu.Lock()
		defer pq.syncFields.mu.Unlock()
		delete(pq.syncFields.runningJobs, j.GetTableID())
	})
	job.RegisterFailureHook(func(j AnalysisJob) {
		pq.syncFields.mu.Lock()
		defer pq.syncFields.mu.Unlock()
		// Mark the job as failed and remove it from the running jobs.
		delete(pq.syncFields.runningJobs, j.GetTableID())
		pq.syncFields.failedJobs[j.GetTableID()] = struct{}{}
	})
	return job, nil
}

// Peek peeks the top job from the priority queue.
func (pq *AnalysisPriorityQueueV2) Peek() (AnalysisJob, error) {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	if !pq.syncFields.initialized {
		return nil, errors.New(notInitializedErrMsg)
	}

	return pq.syncFields.inner.Peek()
}

// IsEmpty checks whether the priority queue is empty.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueueV2) IsEmpty() (bool, error) {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()
	if !pq.syncFields.initialized {
		return false, errors.New(notInitializedErrMsg)
	}

	return pq.syncFields.inner.IsEmpty(), nil
}

// Len returns the number of jobs in the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueueV2) Len() (int, error) {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()
	if !pq.syncFields.initialized {
		return 0, errors.New(notInitializedErrMsg)
	}

	return pq.syncFields.inner.Len(), nil
}

// Close closes the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueueV2) Close() {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	if !pq.syncFields.initialized {
		return
	}

	pq.cancel()
	pq.wg.Wait()
}
