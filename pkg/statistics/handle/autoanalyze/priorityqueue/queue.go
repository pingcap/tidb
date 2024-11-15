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
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const notInitializedErrMsg = "priority queue not initialized"

const (
	lastAnalysisDurationRefreshInterval = time.Minute * 10
	dmlChangesFetchInterval             = time.Minute * 2
	mustRetryJobRequeueInterval         = time.Minute * 5
)

// If the process takes longer than this threshold, we will log it as a slow log.
const slowLogThreshold = 150 * time.Millisecond

// Every 15 minutes, at most 1 log will be output.
var queueSamplerLogger = logutil.SampleLoggerFactory(15*time.Minute, 1, zap.String(logutil.LogFieldCategory, "stats"))

// pqHeap is an interface that wraps the methods of a priority queue heap.
type pqHeap interface {
	// getByKey returns the job by the given table ID.
	getByKey(tableID int64) (AnalysisJob, bool, error)
	// addOrUpdate adds a job to the heap or updates the job if it already exists.
	addOrUpdate(job AnalysisJob) error
	// update updates a job in the heap.
	update(job AnalysisJob) error
	// delete deletes a job from the heap.
	delete(job AnalysisJob) error
	// list returns all jobs in the heap.
	list() []AnalysisJob
	// pop pops the job with the highest priority from the heap.
	pop() (AnalysisJob, error)
	// peek peeks the job with the highest priority from the heap without removing it.
	peek() (AnalysisJob, error)
	// isEmpty returns true if the heap is empty.
	isEmpty() bool
	// len returns the number of jobs in the heap.
	len() int
}

// AnalysisPriorityQueue is a priority queue for TableAnalysisJobs.
// Testing shows that keeping all jobs in memory is feasible.
// Memory usage for one million tables is approximately 300 to 500 MiB, which is acceptable.
// Typically, not many tables need to be analyzed simultaneously.
//
//nolint:fieldalignment
type AnalysisPriorityQueue struct {
	ctx         context.Context
	statsHandle statstypes.StatsHandle
	calculator  *PriorityCalculator

	wg util.WaitGroupWrapper

	// syncFields is a substructure to hold fields protected by mu.
	syncFields struct {
		// mu is used to protect the following fields.
		mu sync.RWMutex
		// Because the Initialize and Close functions can be called concurrently,
		// so we need to protect the cancel function to avoid data race.
		cancel context.CancelFunc
		inner  pqHeap
		// runningJobs is a map to store the running jobs. Used to avoid duplicate jobs.
		runningJobs map[int64]struct{}
		// lastDMLUpdateFetchTimestamp is the timestamp of the last DML update fetch.
		lastDMLUpdateFetchTimestamp uint64
		// mustRetryJobs is a slice to store the must retry jobs.
		// For now, we have two types of jobs:
		// 1. The jobs that failed to be executed. We have to try it later.
		// 2. The jobs failed to enqueue due to the ongoing analysis,
		//    particularly for tables with new indexes created during this process.
		// We will requeue the must retry jobs periodically.
		mustRetryJobs map[int64]struct{}
		// initialized is a flag to check if the queue is initialized.
		initialized bool
	}
}

// NewAnalysisPriorityQueue creates a new AnalysisPriorityQueue2.
func NewAnalysisPriorityQueue(handle statstypes.StatsHandle) *AnalysisPriorityQueue {
	queue := &AnalysisPriorityQueue{
		statsHandle: handle,
		calculator:  NewPriorityCalculator(),
	}

	return queue
}

// IsInitialized checks if the priority queue is initialized.
func (pq *AnalysisPriorityQueue) IsInitialized() bool {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()

	return pq.syncFields.initialized
}

// Initialize initializes the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueue) Initialize() error {
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

	ctx, cancel := context.WithCancel(context.Background())
	pq.ctx = ctx
	pq.syncFields.cancel = cancel
	pq.syncFields.runningJobs = make(map[int64]struct{})
	pq.syncFields.mustRetryJobs = make(map[int64]struct{})
	pq.syncFields.initialized = true
	pq.syncFields.mu.Unlock()

	// Start a goroutine to maintain the priority queue.
	pq.wg.Run(pq.run)
	return nil
}

// Rebuild rebuilds the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueue) Rebuild() error {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()

	if !pq.syncFields.initialized {
		return errors.New(notInitializedErrMsg)
	}

	return pq.rebuildWithoutLock()
}

// rebuildWithoutLock rebuilds the priority queue without holding the lock.
// Note: Please hold the lock before calling this function.
func (pq *AnalysisPriorityQueue) rebuildWithoutLock() error {
	pq.syncFields.inner = newHeap()

	// We need to fetch the next check version with offset before fetching all tables and building analysis jobs.
	// Otherwise, we may miss some DML changes happened during the process because this operation takes time.
	// For example, 1m tables will take about 1min to fetch all tables and build analysis jobs.
	// This will guarantee that we will not miss any DML changes. But it may cause some DML changes to be processed twice.
	// It is acceptable since the DML changes operation is idempotent.
	nextCheckVersionWithOffset := pq.statsHandle.GetNextCheckVersionWithOffset()
	err := pq.fetchAllTablesAndBuildAnalysisJobs()
	if err != nil {
		return errors.Trace(err)
	}
	// Update the last fetch timestamp of DML updates.
	pq.syncFields.lastDMLUpdateFetchTimestamp = nextCheckVersionWithOffset

	return nil
}

// fetchAllTablesAndBuildAnalysisJobs builds analysis jobs for all eligible tables and partitions.
// Note: Please hold the lock before calling this function.
func (pq *AnalysisPriorityQueue) fetchAllTablesAndBuildAnalysisJobs() error {
	return statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		parameters := exec.GetAutoAnalyzeParameters(sctx)
		autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
		pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
		is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
		// Query locked tables once to minimize overhead.
		// Outdated lock info is acceptable as we verify table lock status pre-analysis.
		lockedTables, err := lockstats.QueryLockedTables(statsutil.StatsCtx, sctx)
		if err != nil {
			return err
		}
		// Get current timestamp from the session context.
		currentTs, err := statsutil.GetStartTS(sctx)
		if err != nil {
			return err
		}

		jobFactory := NewAnalysisJobFactory(sctx, autoAnalyzeRatio, currentTs)

		dbs := is.AllSchemaNames()
		for _, db := range dbs {
			// Ignore the memory and system database.
			if util.IsMemOrSysDB(db.L) {
				continue
			}

			tbls, err := is.SchemaTableInfos(context.Background(), db)
			if err != nil {
				return err
			}

			// We need to check every partition of every table to see if it needs to be analyzed.
			for _, tblInfo := range tbls {
				// If table locked, skip analyze all partitions of the table.
				if _, ok := lockedTables[tblInfo.ID]; ok {
					continue
				}

				if tblInfo.IsView() {
					continue
				}

				pi := tblInfo.GetPartitionInfo()
				if pi == nil {
					job := jobFactory.CreateNonPartitionedTableAnalysisJob(
						tblInfo,
						pq.statsHandle.GetTableStatsForAutoAnalyze(tblInfo),
					)
					err := pq.pushWithoutLock(job)
					if err != nil {
						return err
					}
					continue
				}

				// Only analyze the partition that has not been locked.
				partitionDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions))
				for _, def := range pi.Definitions {
					if _, ok := lockedTables[def.ID]; !ok {
						partitionDefs = append(partitionDefs, def)
					}
				}
				partitionStats := GetPartitionStats(pq.statsHandle, tblInfo, partitionDefs)
				// If the prune mode is static, we need to analyze every partition as a separate table.
				if pruneMode == variable.Static {
					for pIDAndName, stats := range partitionStats {
						job := jobFactory.CreateStaticPartitionAnalysisJob(
							tblInfo,
							pIDAndName.ID,
							stats,
						)
						err := pq.pushWithoutLock(job)
						if err != nil {
							return err
						}
					}
				} else {
					job := jobFactory.CreateDynamicPartitionedTableAnalysisJob(
						tblInfo,
						pq.statsHandle.GetPartitionStatsForAutoAnalyze(tblInfo, tblInfo.ID),
						partitionStats,
					)
					err := pq.pushWithoutLock(job)
					if err != nil {
						return err
					}
				}
			}
		}

		return nil
	}, statsutil.FlagWrapTxn)
}

// run maintains the priority queue.
func (pq *AnalysisPriorityQueue) run() {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error("Priority queue panicked", zap.Any("recover", r), zap.Stack("stack"))
		}
	}()

	dmlChangesFetchInterval := time.NewTicker(dmlChangesFetchInterval)
	defer dmlChangesFetchInterval.Stop()
	timeRefreshInterval := time.NewTicker(lastAnalysisDurationRefreshInterval)
	defer timeRefreshInterval.Stop()
	mustRetryJobRequeueInterval := time.NewTicker(mustRetryJobRequeueInterval)
	defer mustRetryJobRequeueInterval.Stop()

	for {
		select {
		case <-pq.ctx.Done():
			statslogutil.StatsLogger().Info("Priority queue stopped")
			return
		case <-dmlChangesFetchInterval.C:
			queueSamplerLogger().Info("Start to fetch DML changes of tables")
			pq.ProcessDMLChanges()
		case <-timeRefreshInterval.C:
			queueSamplerLogger().Info("Start to refresh last analysis durations of jobs")
			pq.RefreshLastAnalysisDuration()
		case <-mustRetryJobRequeueInterval.C:
			queueSamplerLogger().Info("Start to requeue must retry jobs")
			pq.RequeueMustRetryJobs()
		}
	}
}

// ProcessDMLChanges processes DML changes.
// Note: This function is thread-safe.
// Performance: To scan all table stats and process the DML changes, it takes about less than 100ms for 1m tables.
func (pq *AnalysisPriorityQueue) ProcessDMLChanges() {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()

	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			if duration > slowLogThreshold {
				queueSamplerLogger().Info("DML changes processed", zap.Duration("duration", duration))
			}
		}()

		parameters := exec.GetAutoAnalyzeParameters(sctx)
		// We need to fetch the next check version with offset before fetching new DML changes.
		// Otherwise, we may miss some DML changes happened during the process.
		newMaxVersion := pq.statsHandle.GetNextCheckVersionWithOffset()
		// Query locked tables once to minimize overhead.
		// Outdated lock info is acceptable as we verify table lock status pre-analysis.
		lockedTables, err := lockstats.QueryLockedTables(statsutil.StatsCtx, sctx)
		if err != nil {
			return err
		}
		values := pq.statsHandle.Values()
		lastFetchTimestamp := pq.syncFields.lastDMLUpdateFetchTimestamp
		for _, value := range values {
			// We only process the tables that have been updated.
			// So here we only need to process the tables whose version is greater than the last fetch timestamp.
			if value.Version > lastFetchTimestamp {
				err := pq.processTableStats(sctx, value, parameters, lockedTables)
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
			queueSamplerLogger().Info("Updating last fetch timestamp", zap.Uint64("new_max_version", newMaxVersion))
			pq.syncFields.lastDMLUpdateFetchTimestamp = newMaxVersion
		}
		return nil
	}, statsutil.FlagWrapTxn); err != nil {
		statslogutil.StatsLogger().Error("Failed to process DML changes", zap.Error(err))
	}
}

// Note: Please hold the lock before calling this function.
func (pq *AnalysisPriorityQueue) processTableStats(
	sctx sessionctx.Context,
	stats *statistics.Table,
	parameters map[string]string,
	lockedTables map[int64]struct{},
) error {
	// Check if the table is eligible for analysis first to avoid unnecessary work.
	if !stats.IsEligibleForAnalysis() {
		return nil
	}

	autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	// Get current timestamp from the session context.
	currentTs, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	jobFactory := NewAnalysisJobFactory(sctx, autoAnalyzeRatio, currentTs)
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())

	var job AnalysisJob
	// For dynamic partitioned tables, we need to recreate the job if the partition stats are updated.
	// This means we will always enter the tryCreateJob branch for these partitions.
	// Since we store the stats meta for each partition and the parent table, there may be redundant calculations.
	// This is acceptable for now, but in the future, we may consider separating the analysis job for each partition.
	job, ok, _ := pq.syncFields.inner.getByKey(stats.PhysicalID)
	if !ok {
		job = pq.tryCreateJob(is, stats, pruneMode, jobFactory, lockedTables)
	} else {
		// Skip analysis if the table is locked.
		// Dynamic partitioned tables are managed in the tryCreateJob branch.
		// Non-partitioned tables can be skipped entirely here.
		// For static partitioned tables, skip either the locked partition or the whole table if all partitions are locked.
		// For dynamic partitioned tables, if the parent table is locked, we skip the whole table here as well.
		if _, ok := lockedTables[stats.PhysicalID]; ok {
			// Clean up the job if the table is locked.
			err := pq.syncFields.inner.delete(job)
			if err != nil {
				statslogutil.StatsLogger().Error(
					"Failed to delete job from priority queue",
					zap.Error(err),
					zap.String("job", job.String()),
				)
			}
			return nil
		}
		job = pq.tryUpdateJob(is, stats, job, jobFactory)
	}
	return pq.pushWithoutLock(job)
}
func (pq *AnalysisPriorityQueue) tryCreateJob(
	is infoschema.InfoSchema,
	stats *statistics.Table,
	pruneMode variable.PartitionPruneMode,
	jobFactory *AnalysisJobFactory,
	lockedTables map[int64]struct{},
) (job AnalysisJob) {
	if stats == nil {
		return nil
	}

	tableInfo, ok := pq.statsHandle.TableInfoByID(is, stats.PhysicalID)
	if !ok {
		statslogutil.StatsLogger().Warn(
			"Table info not found for table id",
			zap.Int64("tableID", stats.PhysicalID),
		)
		return nil
	}
	tableMeta := tableInfo.Meta()
	partitionedTable := tableMeta.GetPartitionInfo()
	if partitionedTable == nil {
		// If the table is locked, we do not analyze it.
		if _, ok := lockedTables[tableMeta.ID]; ok {
			return nil
		}
		job = jobFactory.CreateNonPartitionedTableAnalysisJob(
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
			// If the partition is locked, we do not analyze it.
			if _, ok := lockedTables[partitionDef.ID]; ok {
				return nil
			}
			job = jobFactory.CreateStaticPartitionAnalysisJob(
				tableMeta,
				partitionDef.ID,
				stats,
			)
		} else {
			// If the table is locked, we do not analyze it.
			// Note: the table meta is the parent table meta.
			if _, ok := lockedTables[tableMeta.ID]; ok {
				return nil
			}

			// Only analyze the partition that has not been locked.
			// Special case for dynamic partitioned tables:
			// 1. Initially, neither the table nor any partitions are locked.
			// 2. Once partition p1 reaches the auto-analyze threshold, a job is created for the entire table.
			// 3. At this point, partition p1 is locked.
			// 4. There are no further partitions requiring analysis for this table because the only partition needing analysis is locked.
			//
			// Normally, we would remove the table's job in this scenario, but that is not handled here.
			// The primary responsibility of this function is to create jobs for tables needing analysis,
			// and deleting jobs falls outside its scope.
			//
			// This behavior is acceptable, as lock statuses will be validated before running the analysis.
			// So let keep it simple and ignore this edge case here.
			filteredPartitionDefs := make([]model.PartitionDefinition, 0, len(partitionDefs))
			for _, def := range partitionDefs {
				if _, ok := lockedTables[def.ID]; !ok {
					filteredPartitionDefs = append(filteredPartitionDefs, def)
				}
			}
			partitionStats := GetPartitionStats(pq.statsHandle, tableMeta, filteredPartitionDefs)
			job = jobFactory.CreateDynamicPartitionedTableAnalysisJob(
				tableMeta,
				// Get global stats for dynamic partitioned table.
				pq.statsHandle.GetTableStatsForAutoAnalyze(tableMeta),
				partitionStats,
			)
		}
	}
	return job
}
func (pq *AnalysisPriorityQueue) tryUpdateJob(
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
		return jobFactory.CreateDynamicPartitionedTableAnalysisJob(
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
func (pq *AnalysisPriorityQueue) GetLastFetchTimestamp() uint64 {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()

	return pq.syncFields.lastDMLUpdateFetchTimestamp
}

// RequeueMustRetryJobs requeues the must retry jobs.
func (pq *AnalysisPriorityQueue) RequeueMustRetryJobs() {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()

	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			if duration > slowLogThreshold {
				queueSamplerLogger().Info("Must retry jobs requeued", zap.Duration("duration", duration))
			}
		}()

		is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
		for tableID := range pq.syncFields.mustRetryJobs {
			// Note: Delete the job first to ensure it can be added back to the queue
			delete(pq.syncFields.mustRetryJobs, tableID)
			tblInfo, ok := pq.statsHandle.TableInfoByID(is, tableID)
			if !ok {
				statslogutil.StatsLogger().Warn("Table info not found during requeueing must retry jobs", zap.Int64("tableID", tableID))
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
		statslogutil.StatsLogger().Error("Failed to requeue must retry jobs", zap.Error(err))
	}
}

// RefreshLastAnalysisDuration refreshes the last analysis duration of all jobs in the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueue) RefreshLastAnalysisDuration() {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()

	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			if duration > slowLogThreshold {
				queueSamplerLogger().Info("Last analysis duration refreshed", zap.Duration("duration", duration))
			}
		}()
		jobs := pq.syncFields.inner.list()
		currentTs, err := statsutil.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}
		jobFactory := NewAnalysisJobFactory(sctx, 0, currentTs)
		// TODO: We can directly rebuild the priority queue instead of updating the indicators of each job.
		for _, job := range jobs {
			indicators := job.GetIndicators()
			tableStats, ok := pq.statsHandle.Get(job.GetTableID())
			if !ok {
				statslogutil.StatsLogger().Warn("Table stats not found during refreshing last analysis duration",
					zap.Int64("tableID", job.GetTableID()),
					zap.String("job", job.String()),
				)
				// TODO: Remove this after handling the DDL event.
				err := pq.syncFields.inner.delete(job)
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
			if err := pq.syncFields.inner.update(job); err != nil {
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
func (pq *AnalysisPriorityQueue) GetRunningJobs() map[int64]struct{} {
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
func (pq *AnalysisPriorityQueue) Push(job AnalysisJob) error {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	if !pq.syncFields.initialized {
		return errors.New(notInitializedErrMsg)
	}

	return pq.pushWithoutLock(job)
}
func (pq *AnalysisPriorityQueue) pushWithoutLock(job AnalysisJob) error {
	if job == nil {
		return nil
	}
	// Skip the must retry jobs.
	// Avoiding requeueing the must retry jobs before the next must retry job requeue interval.
	// Otherwise, we may requeue the same job multiple times in a short time.
	if _, ok := pq.syncFields.mustRetryJobs[job.GetTableID()]; ok {
		return nil
	}

	// Skip the current running jobs.
	// Safety:
	// Let's say we have a job in the priority queue, and it is already running.
	// Then we will not add the same job to the priority queue again. Otherwise, we will analyze the same table twice.
	// If the job is finished, we will remove it from the running jobs.
	// Then the next time we process the DML changes, we will add the job to the priority queue.(if it is still needed)
	// In this process, we will not miss any DML changes of the table. Because when we try to delete the table from the current running jobs,
	// we guarantee that the job is finished and the stats cache is updated.(The last step of the analysis job is to update the stats cache).
	if _, ok := pq.syncFields.runningJobs[job.GetTableID()]; ok {
		// Mark the job as must retry.
		// Because potentially the job can be analyzed in the near future.
		// For example, the table has new indexes added when the job is running.
		pq.syncFields.mustRetryJobs[job.GetTableID()] = struct{}{}
		return nil
	}
	// We apply a penalty to larger tables, which can potentially result in a negative weight.
	// To prevent this, we filter out any negative weights. Under normal circumstances, table sizes should not be negative.
	weight := pq.calculator.CalculateWeight(job)
	if weight <= 0 {
		statslogutil.SingletonStatsSamplerLogger().Warn(
			"Table gets a negative weight",
			zap.Float64("weight", weight),
			zap.Stringer("job", job),
		)
	}
	job.SetWeight(weight)
	return pq.syncFields.inner.addOrUpdate(job)
}

// Pop pops a job from the priority queue and marks it as running.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueue) Pop() (AnalysisJob, error) {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	if !pq.syncFields.initialized {
		return nil, errors.New(notInitializedErrMsg)
	}

	job, err := pq.syncFields.inner.pop()
	if err != nil {
		return nil, errors.Trace(err)
	}
	pq.syncFields.runningJobs[job.GetTableID()] = struct{}{}

	job.RegisterSuccessHook(func(j AnalysisJob) {
		pq.syncFields.mu.Lock()
		defer pq.syncFields.mu.Unlock()
		delete(pq.syncFields.runningJobs, j.GetTableID())
	})
	job.RegisterFailureHook(func(j AnalysisJob, needRetry bool) {
		pq.syncFields.mu.Lock()
		defer pq.syncFields.mu.Unlock()
		// Mark the job as failed and remove it from the running jobs.
		delete(pq.syncFields.runningJobs, j.GetTableID())
		if needRetry {
			pq.syncFields.mustRetryJobs[j.GetTableID()] = struct{}{}
		}
	})
	return job, nil
}

// Peek peeks the top job from the priority queue.
func (pq *AnalysisPriorityQueue) Peek() (AnalysisJob, error) {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	if !pq.syncFields.initialized {
		return nil, errors.New(notInitializedErrMsg)
	}

	return pq.syncFields.inner.peek()
}

// IsEmpty checks whether the priority queue is empty.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueue) IsEmpty() (bool, error) {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()
	if !pq.syncFields.initialized {
		return false, errors.New(notInitializedErrMsg)
	}

	return pq.syncFields.inner.isEmpty(), nil
}

// Len returns the number of jobs in the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueue) Len() (int, error) {
	pq.syncFields.mu.RLock()
	defer pq.syncFields.mu.RUnlock()
	if !pq.syncFields.initialized {
		return 0, errors.New(notInitializedErrMsg)
	}

	return pq.syncFields.inner.len(), nil
}

// Close closes the priority queue.
// Note: This function is thread-safe.
func (pq *AnalysisPriorityQueue) Close() {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	if !pq.syncFields.initialized {
		return
	}

	// It is possible that the priority queue is not initialized.
	if pq.syncFields.cancel != nil {
		pq.syncFields.cancel()
	}
	pq.wg.Wait()

	// Reset the initialized flag to allow the priority queue to be closed and re-initialized.
	pq.syncFields.initialized = false
	// The rest fields will be reset when the priority queue is initialized.
	// But we do it here for double safety.
	pq.syncFields.inner = nil
	pq.syncFields.runningJobs = nil
	pq.syncFields.mustRetryJobs = nil
	pq.syncFields.lastDMLUpdateFetchTimestamp = 0
	pq.syncFields.cancel = nil
}
