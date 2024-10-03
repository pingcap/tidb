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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
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

	// jobs is the priority queue of analysis jobs.
	// Exported for testing purposes.
	jobs *priorityqueue.AnalysisPriorityQueueV2

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
		jobs:           priorityqueue.NewAnalysisPriorityQueueV2(statsHandle),
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
	se, err := r.statsHandle.SPool().Get()
	if err != nil {
		statslogutil.StatsLogger().Error("Failed to get session context", zap.Error(err))
		return false
	}
	defer r.statsHandle.SPool().Put(se)

	sctx := se.(sessionctx.Context)
	parameters := exec.GetAutoAnalyzeParameters(sctx)
	err = r.setAutoAnalysisTimeWindow(parameters)
	if err != nil {
		statslogutil.StatsLogger().Error("Set auto analyze time window failed", zap.Error(err))
		return false
	}
	if !r.isWithinTimeWindow() {
		return false
	}
	if !r.jobs.IsInitialized() {
		if err := r.jobs.Initialize(); err != nil {
			statslogutil.StatsLogger().Error("Failed to initialize AnalysisPriorityQueueV2", zap.Error(err))
			return false
		}
	}

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
	jobs, err := r.jobs.TopN(remainConcurrency * 10)
	if err != nil {
		return false
	}
	for _, job := range jobs {
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

		if analyzedCount >= remainConcurrency {
			break
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
	return r.jobs.Rebuild()
}

func (r *Refresher) setAutoAnalysisTimeWindow(
	parameters map[string]string,
) error {
	start, end, err := exec.ParseAutoAnalysisWindow(
		parameters[variable.TiDBAutoAnalyzeStartTime],
		parameters[variable.TiDBAutoAnalyzeEndTime],
	)
	if err != nil {
		return errors.Wrap(err, "parse auto analyze period failed")
	}
	r.autoAnalysisTimeWindow = priorityqueue.NewAutoAnalysisTimeWindow(start, end)
	return nil
}

// isWithinTimeWindow checks if the current time is within the auto analyze time window.
func (r *Refresher) isWithinTimeWindow() bool {
	return r.autoAnalysisTimeWindow.IsWithinTimeWindow(time.Now())
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

// ProcessDMLChangesForTest processes DML changes for the test.
// Only used in the test.
func (r *Refresher) ProcessDMLChangesForTest() {
	if r.jobs.IsInitialized() {
		r.jobs.ProcessDMLChanges()
	}
}

// GetQueueLength returns the length of the analysis job queue.
func (r *Refresher) GetQueueLength() int {
	l, err := r.jobs.Len()
	intest.Assert(err == nil, "Failed to get AnalysisPriorityQueueV2 length")
	return l
}

// Close stops all running jobs and releases resources.
func (r *Refresher) Close() {
	r.worker.Stop()
	if r.jobs != nil {
		r.jobs.Close()
	}
}
