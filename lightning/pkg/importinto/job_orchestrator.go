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
)

// JobOrchestrator orchestrates the submission and monitoring of import jobs.
type JobOrchestrator interface {
	SubmitAndWait(ctx context.Context, tables []*importsdk.TableMeta) error
	Cancel(ctx context.Context) error
}

// DefaultJobOrchestrator is the default implementation of JobOrchestrator.
type DefaultJobOrchestrator struct {
	submitter         JobSubmitter
	cpMgr             CheckpointManager
	monitor           JobMonitor
	submitConcurrency int
	logger            log.Logger
	sdk               importsdk.SDK

	activeJobs []*ImportJob
}

const cancelledByUserMessage = "cancelled by user"

// OrchestratorConfig configures the job orchestrator.
type OrchestratorConfig struct {
	Submitter         JobSubmitter
	CheckpointMgr     CheckpointManager
	SDK               importsdk.SDK
	Monitor           JobMonitor
	SubmitConcurrency int
	PollInterval      time.Duration
	LogInterval       time.Duration
	Logger            log.Logger
	ProgressUpdater   ProgressUpdater
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

	monitor := cfg.Monitor
	if monitor == nil {
		monitor = NewJobMonitor(cfg.SDK, cfg.CheckpointMgr, pollInterval, logInterval, cfg.Logger, cfg.ProgressUpdater)
	}

	return &DefaultJobOrchestrator{
		submitter:         cfg.Submitter,
		cpMgr:             cfg.CheckpointMgr,
		monitor:           monitor,
		submitConcurrency: submitConcurrency,
		logger:            cfg.Logger,
		sdk:               cfg.SDK,
	}
}

// SubmitAndWait submits all jobs and waits for their completion.
func (o *DefaultJobOrchestrator) SubmitAndWait(ctx context.Context, tables []*importsdk.TableMeta) error {
	// Phase 1: Submit all jobs
	jobs, err := o.submitAllJobs(ctx, tables)
	if err != nil {
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
	groupKey := ""
	if len(o.activeJobs) > 0 {
		groupKey = o.activeJobs[0].GroupKey
	}
	if groupKey == "" && o.submitter != nil {
		groupKey = o.submitter.GetGroupKey()
	}
	if groupKey == "" {
		o.logger.Warn("no group key found, skip cancelling jobs")
		return nil
	}

	statuses, statusErr := o.sdk.GetJobsByGroup(ctx, groupKey)
	if statusErr != nil {
		o.logger.Warn("failed to get group jobs status, will try best effort cancellation", zap.Error(statusErr), zap.String("groupKey", groupKey))
	}

	statusByID := make(map[int64]*importsdk.JobStatus, len(statuses))
	for _, st := range statuses {
		statusByID[st.JobID] = st
	}

	jobIDsToCancel := make(map[int64]struct{})
	if statusErr == nil {
		for _, st := range statuses {
			if !st.IsCompleted() {
				jobIDsToCancel[st.JobID] = struct{}{}
			}
		}
	}
	for _, job := range o.activeJobs {
		if job == nil || job.JobID <= 0 {
			continue
		}
		if st, ok := statusByID[job.JobID]; ok && st.IsCompleted() {
			continue
		}
		jobIDsToCancel[job.JobID] = struct{}{}
	}

	var cps []*TableCheckpoint
	if len(o.activeJobs) == 0 {
		var err error
		cps, err = o.cpMgr.GetCheckpoints(ctx)
		if err != nil {
			o.logger.Warn("failed to get checkpoints for cancellation", zap.Error(err), zap.String("groupKey", groupKey))
		} else {
			for _, cp := range cps {
				if cp.GroupKey != groupKey || cp.JobID <= 0 || cp.Status != CheckpointStatusRunning {
					continue
				}
				if st, ok := statusByID[cp.JobID]; ok && st.IsCompleted() {
					continue
				}
				jobIDsToCancel[cp.JobID] = struct{}{}
			}
		}
	}

	o.logger.Info("cancelling import jobs", zap.String("groupKey", groupKey), zap.Int("count", len(jobIDsToCancel)))

	var firstErr error
	cancelSucceeded := make(map[int64]struct{})
	for jobID := range jobIDsToCancel {
		if err := o.sdk.CancelJob(ctx, jobID); err != nil {
			o.logger.Warn("failed to cancel job", zap.Int64("jobID", jobID), zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		cancelSucceeded[jobID] = struct{}{}
	}

	updateCheckpoint := func(tableName string, jobID int64) {
		st := statusByID[jobID]
		var (
			cpStatus  CheckpointStatus
			cpMessage string
			shouldUpd bool
		)
		switch {
		case st != nil && st.IsFinished():
			cpStatus = CheckpointStatusFinished
			cpMessage = ""
			shouldUpd = true
		case st != nil && st.IsFailed():
			cpStatus = CheckpointStatusFailed
			cpMessage = st.ResultMessage
			shouldUpd = true
		case st != nil && st.IsCancelled():
			cpStatus = CheckpointStatusFailed
			cpMessage = cancelledByUserMessage
			shouldUpd = true
		default:
			if _, ok := cancelSucceeded[jobID]; ok {
				cpStatus = CheckpointStatusFailed
				cpMessage = cancelledByUserMessage
				shouldUpd = true
			}
		}
		if !shouldUpd {
			return
		}
		if err := o.cpMgr.Update(ctx, &TableCheckpoint{
			TableName: tableName,
			JobID:     jobID,
			Status:    cpStatus,
			Message:   cpMessage,
			GroupKey:  groupKey,
		}); err != nil {
			o.logger.Warn("failed to update checkpoint", zap.String("table", tableName), zap.Int64("jobID", jobID), zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	for _, job := range o.activeJobs {
		if job == nil || job.TableMeta == nil || job.JobID <= 0 {
			continue
		}
		updateCheckpoint(common.UniqueTable(job.TableMeta.Database, job.TableMeta.Table), job.JobID)
	}
	for _, cp := range cps {
		if cp.GroupKey != groupKey || cp.JobID <= 0 || cp.Status != CheckpointStatusRunning {
			continue
		}
		updateCheckpoint(cp.TableName, cp.JobID)
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

				if err := o.recordSubmission(egCtx, job); err != nil {
					return errors.Annotatef(err, "record submission for %s.%s", table.Database, table.Table)
				}
			}

			mu.Lock()
			jobs = append(jobs, job)
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (o *DefaultJobOrchestrator) recordSubmission(ctx context.Context, job *ImportJob) error {
	return o.cpMgr.Update(ctx, &TableCheckpoint{
		TableName: common.UniqueTable(job.TableMeta.Database, job.TableMeta.Table),
		JobID:     job.JobID,
		Status:    CheckpointStatusRunning,
		GroupKey:  job.GroupKey,
	})
}
