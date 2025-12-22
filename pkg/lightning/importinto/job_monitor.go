// Copyright 2025 PingCAP, Inc.
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
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/importsdk"
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

	// Build job map for quick lookup
	jobMap := make(map[int64]*ImportJob, len(jobs))
	for _, job := range jobs {
		jobMap[job.JobID] = job
	}

	groupKey := jobs[0].GroupKey
	finishedJobs := make(map[int64]struct{})
	var firstError error

	// Combined monitoring ticker
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	logTicker := time.NewTicker(m.logInterval)
	defer logTicker.Stop()

	m.logger.Info("waiting for all jobs to complete", zap.Int("totalJobs", len(jobs)), zap.String("groupKey", groupKey))

	var (
		runningCnt        int
		pendingCnt        int
		completedCnt      int
		failedCnt         int
		cancelledCnt      int
		totalImportedRows int64
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logTicker.C:
			m.logger.Info("job group progress",
				zap.Int("total", len(jobs)),
				zap.Int("pending", pendingCnt),
				zap.Int("running", runningCnt),
				zap.Int("completed", completedCnt),
				zap.Int("failed", failedCnt),
				zap.Int("cancelled", cancelledCnt),
				zap.Int64("importedRows", totalImportedRows),
			)
		case <-ticker.C:
			// Get detailed status for each job
			statuses, err := m.sdk.GetJobsByGroup(ctx, groupKey)
			if err != nil {
				m.logger.Warn("failed to get group jobs status", zap.Error(err))
				continue
			}

			// Reset counters
			runningCnt = 0
			pendingCnt = 0
			completedCnt = 0
			failedCnt = 0
			cancelledCnt = 0
			totalImportedRows = 0

			// Process each job status
			for _, status := range statuses {
				job, ok := jobMap[status.JobID]
				if !ok {
					continue
				}

				totalImportedRows += status.ImportedRows

				// Count stats for tracked jobs
				switch {
				case status.IsFinished():
					completedCnt++
				case status.IsFailed():
					failedCnt++
				case status.IsCancelled():
					cancelledCnt++
				case status.Status == "running":
					runningCnt++
				default:
					pendingCnt++
				}

				if _, finished := finishedJobs[status.JobID]; finished {
					continue
				}

				if status.IsCompleted() {
					finishedJobs[status.JobID] = struct{}{}

					// Record completion in checkpoint
					if err := m.recordCompletion(ctx, job, status); err != nil {
						m.logger.Error("failed to record job completion", zap.Int64("jobID", job.JobID), zap.Error(err))
						if firstError == nil {
							firstError = err
						}
						continue
					}

					m.logJobCompletion(job, status, &firstError)
				}
			}

			// Fast-fail: if any job failed, cancel all and return error immediately
			if failedCnt > 0 {
				if firstError == nil {
					firstError = fmt.Errorf("job group %s has %d failed jobs", groupKey, failedCnt)
				}

				m.logger.Error("detected failures in job group, cancelling remaining jobs", zap.String("groupKey", groupKey), zap.Int("failedCount", failedCnt))

				for _, status := range statuses {
					// Only cancel jobs that are not completed (finished, failed, or cancelled)
					if !status.IsCompleted() {
						m.logger.Info("cancelling job", zap.Int64("jobID", status.JobID))
						if err := m.sdk.CancelJob(ctx, status.JobID); err != nil {
							m.logger.Warn("failed to cancel job", zap.Int64("jobID", status.JobID), zap.Error(err))
						}
					}
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

func (m *DefaultJobMonitor) logJobCompletion(job *ImportJob, status *importsdk.JobStatus, firstError *error) {
	if status.IsFinished() {
		m.logger.Info("job completed successfully", zap.Int64("jobID", job.JobID), zap.String("database", job.TableMeta.Database), zap.String("table", job.TableMeta.Table), zap.Int64("importedRows", status.ImportedRows))
	} else if status.IsFailed() {
		m.logger.Error("job failed", zap.Int64("jobID", job.JobID), zap.String("database", job.TableMeta.Database), zap.String("table", job.TableMeta.Table), zap.String("error", status.ResultMessage))
		if *firstError == nil {
			*firstError = fmt.Errorf("job %d failed: %s", job.JobID, status.ResultMessage)
		}
	} else if status.IsCancelled() {
		m.logger.Warn("job was cancelled", zap.Int64("jobID", job.JobID), zap.String("database", job.TableMeta.Database), zap.String("table", job.TableMeta.Table))
		if *firstError == nil {
			*firstError = fmt.Errorf("job %d was cancelled", job.JobID)
		}
	}
}

func (m *DefaultJobMonitor) recordCompletion(ctx context.Context, job *ImportJob, status *importsdk.JobStatus) error {
	checkpoint := &TableCheckpoint{
		DBName:    job.TableMeta.Database,
		TableName: job.TableMeta.Table,
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
		return fmt.Errorf("update checkpoint: %w", err)
	}

	return nil
}
