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
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

	mu         sync.Mutex
	activeJobs []*ImportJob
}

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
}

// NewJobOrchestrator creates a new job orchestrator.
func NewJobOrchestrator(cfg OrchestratorConfig) JobOrchestrator {
	submitConcurrency := cfg.SubmitConcurrency
	if submitConcurrency <= 0 {
		submitConcurrency = 10 // Default to 10 concurrent submissions
	}
	pollInterval := cfg.PollInterval
	if pollInterval == 0 {
		pollInterval = 2 * time.Second
	}
	logInterval := cfg.LogInterval
	if logInterval == 0 {
		logInterval = 5 * time.Minute // Default to 5 minutes
	}

	monitor := cfg.Monitor
	if monitor == nil {
		monitor = NewJobMonitor(cfg.SDK, cfg.CheckpointMgr, pollInterval, logInterval, cfg.Logger)
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
		return fmt.Errorf("submit jobs: %w", err)
	}

	if len(jobs) == 0 {
		o.logger.Info("no jobs to execute")
		return nil
	}

	o.mu.Lock()
	o.activeJobs = jobs
	o.mu.Unlock()

	o.logger.Info("all jobs submitted", zap.Int("count", len(jobs)))

	// Phase 2: Wait for all jobs to complete (delegated to monitor)
	return o.monitor.WaitForJobs(ctx, jobs)
}

// Cancel cancels all active jobs.
func (o *DefaultJobOrchestrator) Cancel(ctx context.Context) error {
	o.mu.Lock()
	jobs := o.activeJobs
	o.mu.Unlock()

	if len(jobs) == 0 {
		return nil
	}

	o.logger.Info("cancelling all active jobs", zap.Int("count", len(jobs)))

	// Get current status of all jobs in the group to avoid cancelling finished jobs
	groupKey := jobs[0].GroupKey
	statuses, err := o.sdk.GetJobsByGroup(ctx, groupKey)
	if err != nil {
		o.logger.Warn("failed to get group jobs status, will try to cancel all jobs", zap.Error(err))
	}

	finishedMap := make(map[int64]bool)
	for _, s := range statuses {
		if s.IsCompleted() {
			finishedMap[s.JobID] = true
		}
	}

	var firstErr error
	for _, job := range jobs {
		if finishedMap[job.JobID] {
			continue
		}
		if err := o.sdk.CancelJob(ctx, job.JobID); err != nil {
			o.logger.Warn("failed to cancel job", zap.Int64("jobID", job.JobID), zap.Error(err))
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
	sem := make(chan struct{}, o.submitConcurrency)

	for _, table := range tables {
		if len(table.DataFiles) == 0 || table.TotalSize == 0 {
			o.logger.Info("skipping table with no data", zap.String("database", table.Database), zap.String("table", table.Table))
			continue
		}
		eg.Go(func() error {
			// Acquire semaphore to limit concurrent submissions
			sem <- struct{}{}
			defer func() { <-sem }()

			// Check if we can resume an existing job
			cp, err := o.cpMgr.Get(table.Database, table.Table)
			if err != nil {
				return fmt.Errorf("get checkpoint for %s.%s: %w", table.Database, table.Table, err)
			}

			if cp != nil && cp.Status == CheckpointStatusFinished {
				o.logger.Info("table already completed in previous run",
					zap.String("database", table.Database),
					zap.String("table", table.Table),
				)
				return nil
			}

			var job *ImportJob
			if cp != nil && cp.JobID > 0 && cp.Status == CheckpointStatusRunning {
				// Resume existing running job
				o.logger.Info("resuming previously running job",
					zap.String("database", table.Database),
					zap.String("table", table.Table),
					zap.Int64("jobID", cp.JobID),
				)
				job = &ImportJob{
					JobID:     cp.JobID,
					TableMeta: table,
					GroupKey:  o.submitter.GetGroupKey(),
				}
			} else {
				// Need to submit new job
				// This handles: no checkpoint, failed checkpoint, or cancelled checkpoint
				if cp != nil {
					o.logger.Info("previous job failed or cancelled, submitting new job",
						zap.String("database", table.Database),
						zap.String("table", table.Table),
						zap.String("previousStatus", cp.Status.String()),
						zap.Int64("previousJobID", cp.JobID),
					)
				} else {
					o.logger.Info("submitting new import job",
						zap.String("database", table.Database),
						zap.String("table", table.Table),
					)
				}

				job, err = o.submitter.SubmitTable(egCtx, table)
				if err != nil {
					return fmt.Errorf("submit table %s.%s: %w", table.Database, table.Table, err)
				}

				if err := o.recordSubmission(egCtx, job); err != nil {
					return fmt.Errorf("record submission for %s.%s: %w", table.Database, table.Table, err)
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
		DBName:    job.TableMeta.Database,
		TableName: job.TableMeta.Table,
		JobID:     job.JobID,
		Status:    CheckpointStatusRunning,
		GroupKey:  job.GroupKey,
	})
}
