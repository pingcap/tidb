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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultSubmitConcurrency is the default number of concurrent job submissions.
	DefaultSubmitConcurrency = 10
	// DefaultPollInterval is the default interval for polling job status.
	DefaultPollInterval = 5 * time.Second
	// DefaultLogInterval is the default interval for logging progress.
	DefaultLogInterval = 1 * time.Minute
	// defaultCancelGroupGracePeriod is the maximum time to wait for late-created jobs to become visible
	// when cancelling by group key.
	defaultCancelGroupGracePeriod = 5 * time.Second
	// defaultCancelGroupPollInterval is the interval for polling jobs by group key during cancellation.
	defaultCancelGroupPollInterval = 500 * time.Millisecond
)

// JobOrchestrator orchestrates the submission and monitoring of import jobs.
type JobOrchestrator interface {
	SubmitAndWait(ctx context.Context, tables []*importsdk.TableMeta) error
	Cancel(ctx context.Context) error
}

// DefaultJobOrchestrator is the default implementation of JobOrchestrator.
type DefaultJobOrchestrator struct {
	submitter          JobSubmitter
	cpMgr              CheckpointManager
	monitor            JobMonitor
	submitConcurrency  int
	logger             log.Logger
	sdk                importsdk.SDK
	cancelGracePeriod  time.Duration
	cancelPollInterval time.Duration

	activeJobs []*ImportJob
}

const cancelledByUserMessage = "cancelled by user"

// OrchestratorConfig configures the job orchestrator.
type OrchestratorConfig struct {
	Submitter          JobSubmitter
	CheckpointMgr      CheckpointManager
	SDK                importsdk.SDK
	Monitor            JobMonitor
	SubmitConcurrency  int
	PollInterval       time.Duration
	LogInterval        time.Duration
	CancelGracePeriod  time.Duration
	CancelPollInterval time.Duration
	Logger             log.Logger
	ProgressUpdater    ProgressUpdater
}

// NewJobOrchestrator creates a new job orchestrator.
func NewJobOrchestrator(cfg OrchestratorConfig) JobOrchestrator {
	submitConcurrency := cfg.SubmitConcurrency
	if submitConcurrency <= 0 {
		submitConcurrency = DefaultSubmitConcurrency
	}
	pollInterval := cfg.PollInterval
	if pollInterval == 0 {
		pollInterval = DefaultPollInterval
	}
	logInterval := cfg.LogInterval
	if logInterval == 0 {
		logInterval = DefaultLogInterval
	}

	cancelGracePeriod := cfg.CancelGracePeriod
	if cancelGracePeriod <= 0 {
		cancelGracePeriod = defaultCancelGroupGracePeriod
	}
	cancelPollInterval := cfg.CancelPollInterval
	if cancelPollInterval <= 0 {
		cancelPollInterval = defaultCancelGroupPollInterval
	}

	monitor := cfg.Monitor
	if monitor == nil {
		monitor = NewJobMonitor(cfg.SDK, cfg.CheckpointMgr, pollInterval, logInterval, cfg.Logger, cfg.ProgressUpdater)
	}

	return &DefaultJobOrchestrator{
		submitter:          cfg.Submitter,
		cpMgr:              cfg.CheckpointMgr,
		monitor:            monitor,
		submitConcurrency:  submitConcurrency,
		logger:             cfg.Logger,
		sdk:                cfg.SDK,
		cancelGracePeriod:  cancelGracePeriod,
		cancelPollInterval: cancelPollInterval,
	}
}

// SubmitAndWait submits all jobs and waits for their completion.
func (o *DefaultJobOrchestrator) SubmitAndWait(ctx context.Context, tables []*importsdk.TableMeta) error {
	// Phase 1: Submit all jobs
	jobs, err := o.submitAllJobs(ctx, tables)
	if err != nil {
		o.activeJobs = jobs
		if !common.IsContextCanceledError(err) {
			o.logger.Warn("job submission failed, cancelling submitted jobs", zap.Error(err), zap.Int("submitted", len(jobs)))
			cancelCtx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
			defer cancel()
			if cancelErr := o.Cancel(cancelCtx); cancelErr != nil {
				o.logger.Warn("failed to cancel jobs after submission error", zap.Error(cancelErr))
			}
		}
		return errors.Annotate(err, "submit jobs")
	}

	if len(jobs) == 0 {
		o.logger.Info("no jobs to execute")
		return nil
	}

	o.activeJobs = jobs

	o.logger.Info("all jobs submitted", zap.Int("count", len(jobs)))

	failpoint.Inject("FailAfterSubmission", func() {
		o.logger.Info("failpoint FailAfterSubmission triggered")
		failpoint.Return(errors.New("failpoint error after submission"))
	})

	// Phase 2: Wait for all jobs to complete (delegated to monitor)
	err = o.monitor.WaitForJobs(ctx, jobs)
	if err != nil && !common.IsContextCanceledError(err) {
		o.logger.Warn("job monitoring failed, cancelling remaining jobs", zap.Error(err))
		cancelCtx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
		defer cancel()
		if cancelErr := o.Cancel(cancelCtx); cancelErr != nil {
			o.logger.Warn("failed to cancel jobs after monitor error", zap.Error(cancelErr))
		}
	}
	return err
}

// Cancel cancels all active jobs.
func (o *DefaultJobOrchestrator) Cancel(ctx context.Context) error {
	groupKey := o.getGroupKey()
	if groupKey == "" {
		o.logger.Warn("no group key found, skip cancelling jobs")
		return nil
	}

	o.logger.Info("cancelling import jobs", zap.String("groupKey", groupKey))
	statusByID, cancelledJobs, err := o.cancelJobsInGroup(ctx, groupKey)

	updateErr := o.updateCheckpointsAfterCancel(ctx, groupKey, statusByID, cancelledJobs)
	if err == nil {
		err = updateErr
	}
	return err
}

func (o *DefaultJobOrchestrator) getGroupKey() string {
	if len(o.activeJobs) > 0 {
		return o.activeJobs[0].GroupKey
	}
	if o.submitter != nil {
		return o.submitter.GetGroupKey()
	}
	return ""
}

func (o *DefaultJobOrchestrator) cancelJobsInGroup(ctx context.Context, groupKey string) (map[int64]*importsdk.JobStatus, map[int64]struct{}, error) {
	var firstErr error
	statusByID := make(map[int64]*importsdk.JobStatus)
	cancelledJobs := make(map[int64]struct{})

	// Poll by group key for the grace period to cancel all running jobs.
	// This handles both existing jobs and late-appearing jobs from concurrent submissions.
	// SHOW IMPORT JOBS apperently may lag behind job creation.
	deadline := time.Now().Add(o.cancelGracePeriod)
	for {
		statuses, err := o.sdk.GetJobsByGroup(ctx, groupKey)
		if err != nil {
			o.logger.Warn("failed to get group jobs status", zap.Error(err), zap.String("groupKey", groupKey))
			if firstErr == nil {
				firstErr = err
			}
			break
		}

		for _, st := range statuses {
			statusByID[st.JobID] = st
			if st.IsCompleted() {
				continue
			}
			if _, ok := cancelledJobs[st.JobID]; ok {
				continue
			}
			if err := o.sdk.CancelJob(ctx, st.JobID); err != nil {
				o.logger.Warn("failed to cancel job", zap.Int64("jobID", st.JobID), zap.Error(err))
				if firstErr == nil {
					firstErr = err
				}
			} else {
				cancelledJobs[st.JobID] = struct{}{}
			}
		}

		if time.Now().After(deadline) {
			break
		}

		wait := o.cancelPollInterval
		if remaining := time.Until(deadline); wait > remaining {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, errors.Trace(ctx.Err())
		case <-timer.C:
		}
	}
	return statusByID, cancelledJobs, firstErr
}

func (o *DefaultJobOrchestrator) updateCheckpointsAfterCancel(
	ctx context.Context,
	groupKey string,
	statusByID map[int64]*importsdk.JobStatus,
	cancelledJobs map[int64]struct{},
) error {
	var firstErr error
	// Update checkpoints based on final job status.
	// Use activeJobs as the source for table names.
	for _, job := range o.activeJobs {
		if job == nil || job.TableMeta == nil || job.JobID <= 0 {
			continue
		}
		tableName := common.UniqueTable(job.TableMeta.Database, job.TableMeta.Table)
		st := statusByID[job.JobID]

		var cpStatus CheckpointStatus
		var cpMessage string
		switch {
		case st != nil && st.IsFinished():
			cpStatus = CheckpointStatusFinished
		case st != nil && st.IsFailed():
			cpStatus = CheckpointStatusFailed
			cpMessage = st.ResultMessage
		case st != nil && st.IsCancelled():
			cpStatus = CheckpointStatusFailed
			cpMessage = cancelledByUserMessage
		default:
			// Job was cancelled but status not yet updated, or job not found
			if _, ok := cancelledJobs[job.JobID]; !ok {
				continue
			}
			cpStatus = CheckpointStatusFailed
			cpMessage = cancelledByUserMessage
		}

		if err := o.cpMgr.Update(ctx, &TableCheckpoint{
			TableName: tableName,
			JobID:     job.JobID,
			Status:    cpStatus,
			Message:   cpMessage,
			GroupKey:  groupKey,
		}); err != nil {
			o.logger.Warn("failed to update checkpoint", zap.String("table", tableName), zap.Int64("jobID", job.JobID), zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (o *DefaultJobOrchestrator) submitAllJobs(ctx context.Context, tables []*importsdk.TableMeta) ([]*ImportJob, error) {
	var (
		mu   sync.Mutex
		jobs []*ImportJob
	)

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(o.submitConcurrency)

	// Note: In Go 1.22+, the loop variable 'table' is scoped to each iteration,
	// so it is safe to capture directly in the goroutine below.
	for _, table := range tables {
		if len(table.DataFiles) == 0 || table.TotalSize == 0 {
			o.logger.Info("skipping table with no data", zap.String("database", table.Database), zap.String("table", table.Table))
			continue
		}
		eg.Go(func() error {
			logger := o.logger.With(zap.String("database", table.Database), zap.String("table", table.Table))

			// Check if we can resume an existing job
			cp, err := o.cpMgr.Get(egCtx, common.UniqueTable(table.Database, table.Table))
			if err != nil {
				return errors.Annotatef(err, "get checkpoint for %s.%s", table.Database, table.Table)
			}

			if cp != nil && cp.Status == CheckpointStatusFinished {
				logger.Info("table already completed in previous run")
				return nil
			}

			var job *ImportJob
			if cp != nil && cp.JobID > 0 && cp.Status == CheckpointStatusRunning {
				// Resume existing running job
				logger.Info("resuming previously running job", zap.Int64("jobID", cp.JobID))
				job = &ImportJob{
					JobID:     cp.JobID,
					TableMeta: table,
					GroupKey:  o.submitter.GetGroupKey(),
				}
			} else {
				// Need to submit new job
				// This handles: no checkpoint, failed checkpoint, or cancelled checkpoint
				if cp != nil {
					logger.Info("previous job failed or cancelled, submitting new job",
						zap.String("previousStatus", cp.Status.String()),
						zap.Int64("previousJobID", cp.JobID),
					)
				} else {
					logger.Info("submitting new import job")
				}

				job, err = o.submitter.SubmitTable(egCtx, table)
				if err != nil {
					return errors.Annotatef(err, "submit table %s.%s", table.Database, table.Table)
				}

				cpCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelTimeout)
				defer cancel()
				if err := o.recordSubmission(cpCtx, job); err != nil {
					return errors.Annotatef(err, "record submission for %s.%s", table.Database, table.Table)
				}
			}

			mu.Lock()
			jobs = append(jobs, job)
			mu.Unlock()
			return nil
		})
	}

	err := eg.Wait()
	return jobs, err
}

func (o *DefaultJobOrchestrator) recordSubmission(ctx context.Context, job *ImportJob) error {
	return o.cpMgr.Update(ctx, &TableCheckpoint{
		TableName: common.UniqueTable(job.TableMeta.Database, job.TableMeta.Table),
		JobID:     job.JobID,
		Status:    CheckpointStatusRunning,
		GroupKey:  job.GroupKey,
	})
}
