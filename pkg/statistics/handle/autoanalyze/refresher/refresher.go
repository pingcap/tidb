// Copyright 2024 PingCAP, Inc.
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

package refresher

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// Refresher provides methods to refresh stats info.
// NOTE: Refresher is not thread-safe.
type Refresher struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sysproctrack.Tracker
	// This will be refreshed every time we rebuild the priority queue.
	autoAnalysisTimeWindow priorityqueue.AutoAnalysisTimeWindow

	// Jobs is the priority queue of analysis jobs.
	// Exported for testing purposes.
	Jobs *priorityqueue.AnalysisPriorityQueue

	// worker is the worker that runs the analysis jobs.
	worker *worker
}

// NewRefresher creates a new Refresher and starts the goroutine.
func NewRefresher(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) *Refresher {
	maxConcurrency := int(variable.AutoAnalyzeConcurrency.Load())
	r := &Refresher{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		Jobs:           priorityqueue.NewAnalysisPriorityQueue(),
		worker:         NewWorker(statsHandle, sysProcTracker, maxConcurrency),
	}

	return r
}

// UpdateConcurrency updates the maximum concurrency for auto-analyze jobs
func (r *Refresher) UpdateConcurrency() {
	newConcurrency := int(variable.AutoAnalyzeConcurrency.Load())
	r.worker.UpdateConcurrency(newConcurrency)
}

// AnalyzeHighestPriorityTables picks tables with the highest priority and analyzes them.
func (r *Refresher) AnalyzeHighestPriorityTables() bool {
	if !r.autoAnalysisTimeWindow.IsWithinTimeWindow(time.Now()) {
		return false
	}

	se, err := r.statsHandle.SPool().Get()
	if err != nil {
		statslogutil.StatsLogger().Error("Failed to get session context", zap.Error(err))
		return false
	}
	defer r.statsHandle.SPool().Put(se)

	sctx := se.(sessionctx.Context)
	// Update the concurrency to the latest value.
	r.UpdateConcurrency()
	// Check remaining concurrency.
	maxConcurrency := r.worker.GetMaxConcurrency()
	currentRunningJobs := r.worker.GetRunningJobs()
	remainConcurrency := maxConcurrency - len(currentRunningJobs)
	if remainConcurrency <= 0 {
		statslogutil.SingletonStatsSamplerLogger().Info("No concurrency available")
		return false
	}

	analyzedCount := 0
	for r.Jobs.Len() > 0 && analyzedCount < remainConcurrency {
		job := r.Jobs.Pop()
		if _, isRunning := currentRunningJobs[job.GetTableID()]; isRunning {
			statslogutil.StatsLogger().Debug("Job already running, skipping", zap.Int64("tableID", job.GetTableID()))
			continue
		}
		if valid, failReason := job.IsValidToAnalyze(sctx); !valid {
			statslogutil.SingletonStatsSamplerLogger().Info(
				"Table not ready for analysis",
				zap.String("reason", failReason),
				zap.Stringer("job", job),
			)
			continue
		}

		statslogutil.StatsLogger().Info("Auto analyze triggered", zap.Stringer("job", job))

		submitted := r.worker.SubmitJob(job)
		intest.Assert(submitted, "Failed to submit job unexpectedly. "+
			"This should not occur as the concurrency limit was checked prior to job submission. "+
			"Please investigate potential race conditions or inconsistencies in the concurrency management logic.")
		if submitted {
			statslogutil.StatsLogger().Debug("Job submitted successfully",
				zap.Stringer("job", job),
				zap.Int("remainConcurrency", remainConcurrency),
				zap.Int("currentRunningJobs", len(currentRunningJobs)),
				zap.Int("maxConcurrency", maxConcurrency),
				zap.Int("analyzedCount", analyzedCount),
			)
			analyzedCount++
		} else {
			statslogutil.StatsLogger().Warn("Failed to submit job",
				zap.Stringer("job", job),
				zap.Int("remainConcurrency", remainConcurrency),
				zap.Int("currentRunningJobs", len(currentRunningJobs)),
				zap.Int("maxConcurrency", maxConcurrency),
				zap.Int("analyzedCount", analyzedCount),
			)
		}
	}

	if analyzedCount > 0 {
		statslogutil.StatsLogger().Debug("Auto analyze jobs submitted successfully", zap.Int("submittedCount", analyzedCount))
		return true
	}

	statslogutil.SingletonStatsSamplerLogger().Info("No tables to analyze")
	return false
}

// RebuildTableAnalysisJobQueue rebuilds the priority queue of analysis jobs.
func (r *Refresher) RebuildTableAnalysisJobQueue() error {
	// Reset the priority queue.
	r.Jobs = priorityqueue.NewAnalysisPriorityQueue()

	if err := statsutil.CallWithSCtx(
		r.statsHandle.SPool(),
		func(sctx sessionctx.Context) error {
			parameters := exec.GetAutoAnalyzeParameters(sctx)
			autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
			// Get the available time period for auto analyze and check if the current time is in the period.
			start, end, err := exec.ParseAutoAnalysisWindow(
				parameters[variable.TiDBAutoAnalyzeStartTime],
				parameters[variable.TiDBAutoAnalyzeEndTime],
			)
			if err != nil {
				statslogutil.StatsLogger().Error(
					"parse auto analyze period failed",
					zap.Error(err),
				)
				return err
			}
			// We will check it again when we try to execute the job.
			// So store the time window for later use.
			r.autoAnalysisTimeWindow = priorityqueue.NewAutoAnalysisTimeWindow(start, end)
			if !r.autoAnalysisTimeWindow.IsWithinTimeWindow(time.Now()) {
				return nil
			}
			calculator := priorityqueue.NewPriorityCalculator()
			pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
			is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
			// Query locked tables once to minimize overhead.
			// Outdated lock info is acceptable as we verify table lock status pre-analysis.
			lockedTables, err := lockstats.QueryLockedTables(sctx)
			if err != nil {
				return err
			}
			// Get current timestamp from the session context.
			currentTs, err := getStartTs(sctx)
			if err != nil {
				return err
			}

			jobFactory := priorityqueue.NewAnalysisJobFactory(sctx, autoAnalyzeRatio, currentTs)

			dbs := is.AllSchemaNames()
			for _, db := range dbs {
				// Sometimes the tables are too many. Auto-analyze will take too much time on it.
				// so we need to check the available time.
				if !r.autoAnalysisTimeWindow.IsWithinTimeWindow(time.Now()) {
					return nil
				}
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
							db.O,
							tblInfo,
							r.statsHandle.GetTableStatsForAutoAnalyze(tblInfo),
						)
						r.pushJob(job, calculator)
						continue
					}

					// Only analyze the partition that has not been locked.
					partitionDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions))
					for _, def := range pi.Definitions {
						if _, ok := lockedTables[def.ID]; !ok {
							partitionDefs = append(partitionDefs, def)
						}
					}
					partitionStats := priorityqueue.GetPartitionStats(r.statsHandle, tblInfo, partitionDefs)
					// If the prune mode is static, we need to analyze every partition as a separate table.
					if pruneMode == variable.Static {
						for pIDAndName, stats := range partitionStats {
							job := jobFactory.CreateStaticPartitionAnalysisJob(
								db.O,
								tblInfo,
								pIDAndName.ID,
								pIDAndName.Name,
								stats,
							)
							r.pushJob(job, calculator)
						}
					} else {
						job := jobFactory.CreateDynamicPartitionedTableAnalysisJob(
							db.O,
							tblInfo,
							r.statsHandle.GetPartitionStatsForAutoAnalyze(tblInfo, tblInfo.ID),
							partitionStats,
						)
						r.pushJob(job, calculator)
					}
				}
			}

			return nil
		},
		statsutil.FlagWrapTxn,
	); err != nil {
		return err
	}

	return nil
}

func (r *Refresher) pushJob(job priorityqueue.AnalysisJob, calculator *priorityqueue.PriorityCalculator) {
	if job == nil {
		return
	}
	// We apply a penalty to larger tables, which can potentially result in a negative weight.
	// To prevent this, we filter out any negative weights. Under normal circumstances, table sizes should not be negative.
	weight := calculator.CalculateWeight(job)
	if weight <= 0 {
		statslogutil.SingletonStatsSamplerLogger().Warn(
			"Table gets a negative weight",
			zap.Float64("weight", weight),
			zap.Stringer("job", job),
		)
	}
	job.SetWeight(weight)
	// Push the job onto the queue.
	r.Jobs.Push(job)
}

// WaitAutoAnalyzeFinishedForTest waits for the auto analyze job to be finished.
// Only used in the test.
func (r *Refresher) WaitAutoAnalyzeFinishedForTest() {
	r.worker.WaitAutoAnalyzeFinishedForTest()
}

// GetRunningJobs returns the currently running jobs.
// Only used in the test.
func (r *Refresher) GetRunningJobs() map[int64]struct{} {
	return r.worker.GetRunningJobs()
}

// Close stops all running jobs and releases resources.
func (r *Refresher) Close() {
	r.worker.Stop()
}

func getStartTs(sctx sessionctx.Context) (uint64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}
