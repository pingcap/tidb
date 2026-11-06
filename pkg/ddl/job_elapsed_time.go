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

package ddl

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"go.uber.org/zap"
)

const jobElapsedTimeInterval = 10 * time.Second

// This loop must remain independent of scheduling and worker progress: both
// running jobs and jobs waiting for an available worker can exceed the limit.
func (s *jobScheduler) jobElapsedTimeLoop() {
	defer metrics.DDLUnfinishedJobMaxElapsed.Reset()
	ticker := time.NewTicker(jobElapsedTimeInterval)
	defer ticker.Stop()
	for {
		if s.schCtx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(s.schCtx, jobElapsedTimeInterval)
		err := s.updateJobElapsedTimeMetrics(ctx)
		cancel()
		if err != nil && s.schCtx.Err() == nil {
			logutil.SampleLogger().Warn("collect unfinished DDL elapsed time failed", zap.Error(err))
		}
		select {
		case <-s.schCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *jobScheduler) updateJobElapsedTimeMetrics(ctx context.Context) error {
	sessCtx, err := s.sessPool.Get()
	if err != nil {
		return err
	}
	defer s.sessPool.Put(sessCtx)
	se := sess.NewSession(sessCtx)
	if err = se.Begin(ctx); err != nil {
		return err
	}
	defer se.Rollback()
	txn, err := se.Txn()
	if err != nil {
		return err
	}
	// One snapshot and one TSO physical time keep pagination and pause accounting
	// consistent, including immediately after an owner change.
	now := model.TSConvert2Time(txn.StartTS())
	failpoint.InjectCall("mockDDLJobElapsedTimeNow", &now)
	maxima := make(map[string]float64)
	const batchSize = 100
	var lastID int64
	for {
		jobs, err := getJobsBySQL(ctx, se, fmt.Sprintf("job_id > %d order by job_id limit %d", lastID, batchSize))
		if err != nil {
			return err
		}
		for _, job := range jobs {
			updateMaxJobElapsedTime(maxima, job, now)
			lastID = job.ID
		}
		if len(jobs) < batchSize {
			break
		}
	}
	// Never publish a partial/failed scan, or values computed by a retired owner.
	if err = ctx.Err(); err != nil {
		return err
	}
	for tp := range s.jobElapsedTimeMetrics {
		if _, ok := maxima[tp]; !ok {
			metrics.DDLUnfinishedJobMaxElapsed.DeleteLabelValues(tp)
		}
	}
	for tp, elapsed := range maxima {
		metrics.DDLUnfinishedJobMaxElapsed.WithLabelValues(tp).Set(elapsed)
	}
	s.jobElapsedTimeMetrics = maxima
	return nil
}

func updateMaxJobElapsedTime(maxima map[string]float64, job *model.Job, now time.Time) {
	if job.ElapsedTime == nil || job.StartTS == 0 || job.IsPaused() ||
		job.IsSynced() || job.IsCancelled() || job.IsRollbackDone() {
		return
	}
	// Done jobs are still eligible while waiting for final schema synchronization.
	elapsed := now.Sub(model.TSConvert2Time(job.StartTS)) - time.Duration(job.ElapsedTime.PausedDurationMS)*time.Millisecond
	tp := job.Type.String()
	maxima[tp] = max(maxima[tp], elapsed.Seconds())
}
