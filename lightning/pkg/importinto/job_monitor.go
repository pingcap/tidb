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
	sdk          importsdk.SDK
	cpMgr        CheckpointManager
	pollInterval time.Duration
	logInterval  time.Duration
	logger       log.Logger
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
) JobMonitor {
	return &DefaultJobMonitor{
		sdk:          sdk,
		cpMgr:        cpMgr,
		pollInterval: pollInterval,
		logInterval:  logInterval,
		logger:       logger,
	}
}

// WaitForJobs waits for all jobs to complete.
func (m *DefaultJobMonitor) WaitForJobs(ctx context.Context, jobs []*ImportJob) error {
	if len(jobs) == 0 {
		return nil
	}

	jobMap := make(map[int64]*ImportJob, len(jobs))
	for _, job := range jobs {
		jobMap[job.JobID] = job
	}

	groupKey := jobs[0].GroupKey
	finishedJobs := make(map[int64]struct{})
	var firstError error
	stats := &groupStats{}

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
			failpoint.Inject("SlowDownPolling", func() {
				time.Sleep(time.Second)
			})
			// Get detailed status for each job
			statuses, err := m.sdk.GetJobsByGroup(ctx, groupKey)
			if err != nil {
				m.logger.Warn("failed to get group jobs status", zap.Error(err))
				continue
			}

			stats = m.processJobStatuses(ctx, statuses, jobMap, finishedJobs, &firstError)

			// Fast-fail: if any job failed, cancel all and return error immediately
			if err := m.handleFailures(ctx, groupKey, statuses, stats, &firstError); err != nil {
				return err
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

func (m *DefaultJobMonitor) processJobStatuses(
	ctx context.Context,
	statuses []*importsdk.JobStatus,
	jobMap map[int64]*ImportJob,
	finishedJobs map[int64]struct{},
	firstError *error,
) *groupStats {
	stats := &groupStats{}
	for _, status := range statuses {
		job, ok := jobMap[status.JobID]
		if !ok {
			continue
		}

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
	return stats
}

func (m *DefaultJobMonitor) handleFailures(
	ctx context.Context,
	groupKey string,
	statuses []*importsdk.JobStatus,
	stats *groupStats,
	firstError *error,
) error {
	if stats.failedCnt > 0 {
		if *firstError == nil {
			*firstError = errors.Errorf("job group %s has %d failed jobs", groupKey, stats.failedCnt)
		}

		m.logger.Error("detected failures in job group, cancelling remaining jobs", zap.String("groupKey", groupKey), zap.Int("failedCount", stats.failedCnt))

		for _, status := range statuses {
			// Only cancel jobs that are not completed (finished, failed, or cancelled)
			if !status.IsCompleted() {
				m.logger.Info("cancelling job", zap.Int64("jobID", status.JobID))
				if err := m.sdk.CancelJob(ctx, status.JobID); err != nil {
					m.logger.Warn("failed to cancel job", zap.Int64("jobID", status.JobID), zap.Error(err))
				}
			}
		}
		return *firstError
	}
	return nil
}

func (m *DefaultJobMonitor) logJobCompletion(job *ImportJob, status *importsdk.JobStatus) {
	if status.IsFinished() {
		m.logger.Info("job completed successfully", zap.Int64("jobID", job.JobID), zap.String("database", job.TableMeta.Database), zap.String("table", job.TableMeta.Table), zap.Int64("importedRows", status.ImportedRows))
	} else if status.IsFailed() {
		m.logger.Error("job failed", zap.Int64("jobID", job.JobID), zap.String("database", job.TableMeta.Database), zap.String("table", job.TableMeta.Table), zap.String("error", status.ResultMessage))
	} else if status.IsCancelled() {
		m.logger.Warn("job was cancelled", zap.Int64("jobID", job.JobID), zap.String("database", job.TableMeta.Database), zap.String("table", job.TableMeta.Table))
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
