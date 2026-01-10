// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"context"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

// JobMonitor monitors the status of import jobs.
type JobMonitor interface {
	WaitForJobs(ctx context.Context, jobs []*ImportJob) error
}

// DefaultJobMonitor is the default implementation of JobMonitor.
type DefaultJobMonitor struct {
	sdk             importsdk.SDK
	cpMgr           CheckpointManager
	pollInterval    time.Duration
	logInterval     time.Duration
	logger          log.Logger
	progressUpdater ProgressUpdater
}

type groupStats struct {
	runningCnt        int
	pendingCnt        int
	completedCnt      int
	failedCnt         int
	cancelledCnt      int
	totalImportedRows int64
}

// NewJobMonitor creates a new job monitor.
func NewJobMonitor(
	sdk importsdk.SDK,
	cpMgr CheckpointManager,
	pollInterval time.Duration,
	logInterval time.Duration,
	logger log.Logger,
	progressUpdater ProgressUpdater,
) JobMonitor {
	return &DefaultJobMonitor{
		sdk:             sdk,
		cpMgr:           cpMgr,
		pollInterval:    pollInterval,
		logInterval:     logInterval,
		logger:          logger,
		progressUpdater: progressUpdater,
	}
}

// WaitForJobs waits for all jobs to complete.
func (m *DefaultJobMonitor) WaitForJobs(ctx context.Context, jobs []*ImportJob) error {
	if len(jobs) == 0 {
		return nil
	}

	jobMap := make(map[int64]*ImportJob, len(jobs))
	jobTotalSize := make(map[int64]int64, len(jobs))
	jobFinishedSize := make(map[int64]int64, len(jobs))
	for _, job := range jobs {
		jobMap[job.JobID] = job
		if job.TableMeta != nil {
			jobTotalSize[job.JobID] = job.TableMeta.TotalSize
		}
	}

	groupKey := jobs[0].GroupKey
	finishedJobs := make(map[int64]struct{})
	var firstError error
	stats := &groupStats{}
	isGlobalSort := false

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	logTicker := time.NewTicker(m.logInterval)
	defer logTicker.Stop()

	m.logger.Info("waiting for all jobs to complete", zap.Int("totalJobs", len(jobs)), zap.String("groupKey", groupKey))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logTicker.C:
			m.logProgress(len(jobs), stats)
		case <-ticker.C:
			failpoint.Inject("SlowDownPolling", nil)
			// Get detailed status for each job
			statuses, err := m.sdk.GetJobsByGroup(ctx, groupKey)
			if err != nil {
				m.logger.Warn("failed to get group jobs status", zap.Error(err))
				continue
			}

			isGlobalSort = isGlobalSort || m.detectGlobalSort(statuses)
			stats = m.processJobStatuses(ctx, statuses, jobMap, jobTotalSize, jobFinishedSize, finishedJobs, &firstError, isGlobalSort)

			// Fast-fail: return error immediately if any job failed.
			if stats.failedCnt > 0 {
				if firstError == nil {
					firstError = errors.Errorf("job group %s has %d failed jobs", groupKey, stats.failedCnt)
				}
				return firstError
			}

			// Check if all jobs finished
			if len(finishedJobs) == len(jobs) {
				if firstError != nil {
					m.logger.Error("all jobs completed but with errors", zap.Int("total", len(jobs)), zap.Error(firstError))
					return firstError
				}
				m.logger.Info("all jobs completed successfully", zap.Int("total", len(jobs)))
				return nil
			}

			// Fast-fail: if we already detected an error, don't wait for remaining jobs
			if firstError != nil {
				m.logger.Info("exiting early due to failure", zap.Int("finished", len(finishedJobs)), zap.Int("total", len(jobs)))
				return firstError
			}
		}
	}
}

func (m *DefaultJobMonitor) logProgress(total int, stats *groupStats) {
	m.logger.Info("job group progress",
		zap.Int("total", total),
		zap.Int("pending", stats.pendingCnt),
		zap.Int("running", stats.runningCnt),
		zap.Int("completed", stats.completedCnt),
		zap.Int("failed", stats.failedCnt),
		zap.Int("cancelled", stats.cancelledCnt),
		zap.Int64("importedRows", stats.totalImportedRows),
	)
}

func (m *DefaultJobMonitor) parseHumanSize(jobID int64, sizeText string, warnMsg string) (int64, bool) {
	if sizeText == "" {
		return 0, false
	}
	size, err := units.FromHumanSize(sizeText)
	if err != nil {
		m.logger.Warn(warnMsg, zap.String("size", sizeText), zap.Int64("jobID", jobID), zap.Error(err))
		return 0, false
	}
	return size, true
}

func (m *DefaultJobMonitor) updateJobTotalSize(
	jobID int64,
	job *ImportJob,
	status *importsdk.JobStatus,
	jobTotalSize map[int64]int64,
) int64 {
	total := jobTotalSize[jobID]
	if job != nil && job.TableMeta != nil && job.TableMeta.TotalSize > 0 {
		total = max(total, job.TableMeta.TotalSize)
	}
	if total <= 0 {
		if size, ok := m.parseHumanSize(jobID, status.SourceFileSize, "failed to parse source file size"); ok {
			total = max(total, size)
		}
	}
	if total <= 0 {
		if size, ok := m.parseHumanSize(jobID, status.TotalSize, "failed to parse total size"); ok {
			total = max(total, size)
		}
	}
	if total > 0 && total != jobTotalSize[jobID] {
		jobTotalSize[jobID] = total
	}
	return total
}

type jobProgressPhase struct {
	phase string
	steps []string
}

func (*DefaultJobMonitor) detectGlobalSort(statuses []*importsdk.JobStatus) bool {
	for _, status := range statuses {
		switch status.Phase {
		case "global-sorting", "resolving-conflicts":
			return true
		}
		switch status.Step {
		case "encode", "merge-sort", "ingest", "collect-conflicts", "conflict-resolution":
			return true
		}
	}
	return false
}

func jobProgressPhases(isGlobalSort bool) []jobProgressPhase {
	if isGlobalSort {
		return []jobProgressPhase{
			{phase: "global-sorting", steps: []string{"encode", "merge-sort"}},
			{phase: "importing", steps: []string{"ingest"}},
			{phase: "resolving-conflicts", steps: []string{"collect-conflicts", "conflict-resolution"}},
			{phase: "validating", steps: []string{"post-process"}},
		}
	}
	return []jobProgressPhase{
		{phase: "importing", steps: []string{"import"}},
		{phase: "validating", steps: []string{"post-process"}},
	}
}

func (m *DefaultJobMonitor) stepRatio(status *importsdk.JobStatus) (float64, bool) {
	// `ProcessedSize/TotalSize` are progress metrics of the *current step*.
	// The meaning of bytes may vary by step (e.g. source file bytes, merged bytes, KV bytes),
	// so we only use the ratio as a generic progress indicator.
	stepProcessedBytes, ok1 := m.parseHumanSize(status.JobID, status.ProcessedSize, "failed to parse processed size")
	stepTotalBytes, ok2 := m.parseHumanSize(status.JobID, status.TotalSize, "failed to parse total size")
	if ok1 && ok2 && stepTotalBytes > 0 {
		ratio := float64(stepProcessedBytes) / float64(stepTotalBytes)
		ratio = min(ratio, 1)
		ratio = max(ratio, 0)
		return ratio, true
	}
	return 0, false
}

func findPhase(phases []jobProgressPhase, phase string) (int, bool) {
	for i, ph := range phases {
		if ph.phase == phase {
			return i, true
		}
	}
	return 0, false
}

func findStep(steps []string, step string) (int, bool) {
	for i, s := range steps {
		if s == step {
			return i, true
		}
	}
	return 0, false
}

func phaseFromStep(step string, isGlobalSort bool) string {
	if !isGlobalSort {
		switch step {
		case "import":
			return "importing"
		case "post-process":
			return "validating"
		}
		return ""
	}

	switch step {
	case "encode", "merge-sort":
		return "global-sorting"
	case "ingest":
		return "importing"
	case "collect-conflicts", "conflict-resolution":
		return "resolving-conflicts"
	case "post-process":
		return "validating"
	}
	return ""
}

func (m *DefaultJobMonitor) jobProgress(status *importsdk.JobStatus, isGlobalSort bool) float64 {
	phases := jobProgressPhases(isGlobalSort)
	if len(phases) == 0 {
		return 0
	}

	phase := status.Phase
	if phase == "" {
		phase = phaseFromStep(status.Step, isGlobalSort)
	}
	phaseIdx, ok := findPhase(phases, phase)
	if !ok {
		return 0
	}

	ratio, _ := m.stepRatio(status)
	stepIdx, ok := findStep(phases[phaseIdx].steps, status.Step)
	if !ok {
		stepIdx = 0
		ratio = 0
	}
	phaseProgress := (float64(stepIdx) + ratio) / float64(len(phases[phaseIdx].steps))
	phaseProgress = min(phaseProgress, 1)
	phaseProgress = max(phaseProgress, 0)

	progress := (float64(phaseIdx) + phaseProgress) / float64(len(phases))
	progress = min(progress, 1)
	progress = max(progress, 0)
	return progress
}

func (m *DefaultJobMonitor) estimateJobFinishedSize(
	status *importsdk.JobStatus,
	jobTotal int64,
	prevFinished int64,
	isGlobalSort bool,
) int64 {
	finished := prevFinished

	switch {
	case status.IsFinished():
		if jobTotal > 0 {
			finished = jobTotal
		}
	case status.IsFailed() || status.IsCancelled():
	default:
		if jobTotal <= 0 {
			break
		}
		progress := m.jobProgress(status, isGlobalSort)
		finished = max(finished, int64(float64(jobTotal)*progress))
	}

	if jobTotal > 0 {
		finished = min(finished, jobTotal)
	}
	return finished
}

func (m *DefaultJobMonitor) updateJobProgress(
	job *ImportJob,
	status *importsdk.JobStatus,
	jobTotalSize map[int64]int64,
	jobFinishedSize map[int64]int64,
	isGlobalSort bool,
) {
	jobID := status.JobID
	jobTotal := m.updateJobTotalSize(jobID, job, status, jobTotalSize)
	jobFinishedSize[jobID] = m.estimateJobFinishedSize(status, jobTotal, jobFinishedSize[jobID], isGlobalSort)
}

func (m *DefaultJobMonitor) processJobStatuses(
	ctx context.Context,
	statuses []*importsdk.JobStatus,
	jobMap map[int64]*ImportJob,
	jobTotalSize map[int64]int64,
	jobFinishedSize map[int64]int64,
	finishedJobs map[int64]struct{},
	firstError *error,
	isGlobalSort bool,
) *groupStats {
	stats := &groupStats{}

	for _, status := range statuses {
		job, ok := jobMap[status.JobID]
		if !ok {
			continue
		}

		m.updateJobProgress(job, status, jobTotalSize, jobFinishedSize, isGlobalSort)

		stats.totalImportedRows += status.ImportedRows

		// Count stats for tracked jobs
		switch {
		case status.IsFinished():
			stats.completedCnt++
		case status.IsFailed():
			stats.failedCnt++
		case status.IsCancelled():
			stats.cancelledCnt++
		case status.Status == "running":
			stats.runningCnt++
		default:
			stats.pendingCnt++
		}

		if _, finished := finishedJobs[status.JobID]; finished {
			continue
		}

		if status.IsCompleted() {
			finishedJobs[status.JobID] = struct{}{}

			// Set error if not success
			if !status.IsFinished() && *firstError == nil {
				if status.IsFailed() {
					*firstError = errors.Errorf("job %d failed: %s", status.JobID, status.ResultMessage)
				} else if status.IsCancelled() {
					*firstError = errors.Errorf("job %d was cancelled", status.JobID)
				}
			}

			// Record completion in checkpoint
			if err := m.recordCompletion(ctx, job, status); err != nil {
				m.logger.Error("failed to record job completion", zap.Int64("jobID", job.JobID), zap.Error(err))
				if *firstError == nil {
					*firstError = err
				}
				continue
			}

			m.logJobCompletion(job, status)
		}
	}

	var (
		totalSize    int64
		finishedSize int64
	)
	for jobID := range jobMap {
		totalSize += jobTotalSize[jobID]
		finishedSize += jobFinishedSize[jobID]
	}
	if m.progressUpdater != nil {
		m.progressUpdater.UpdateTotalSize(totalSize)
		m.progressUpdater.UpdateFinishedSize(finishedSize)
	}

	return stats
}

func (m *DefaultJobMonitor) logJobCompletion(job *ImportJob, status *importsdk.JobStatus) {
	logger := m.logger.With(
		zap.Int64("jobID", job.JobID),
		zap.String("database", job.TableMeta.Database),
		zap.String("table", job.TableMeta.Table),
	)
	if status.IsFinished() {
		logger.Info("job completed successfully", zap.Int64("importedRows", status.ImportedRows))
	} else if status.IsFailed() {
		logger.Error("job failed", zap.String("error", status.ResultMessage))
	} else if status.IsCancelled() {
		logger.Warn("job was cancelled")
	}
}

func (m *DefaultJobMonitor) recordCompletion(ctx context.Context, job *ImportJob, status *importsdk.JobStatus) error {
	checkpoint := &TableCheckpoint{
		TableName: common.UniqueTable(job.TableMeta.Database, job.TableMeta.Table),
		JobID:     job.JobID,
		GroupKey:  job.GroupKey,
	}

	if status.IsFinished() {
		checkpoint.Status = CheckpointStatusFinished
	} else {
		checkpoint.Status = CheckpointStatusFailed
		checkpoint.Message = status.ResultMessage
	}

	if err := m.cpMgr.Update(ctx, checkpoint); err != nil {
		return errors.Annotate(err, "update checkpoint")
	}

	return nil
}
