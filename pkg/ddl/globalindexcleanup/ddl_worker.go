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

package globalindexcleanup

import (
	"context"
	goerrors "errors"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// CheckBackfillJobFinishInterval is the interval to check if the backfill job is finished.
const CheckBackfillJobFinishInterval = 300 * time.Millisecond

// UpdateBackfillJobRowCountInterval is the interval to update the row count of the backfill job.
const UpdateBackfillJobRowCountInterval = 3 * time.Second

// TaskKey generates a task key for the global index cleanup job.
func TaskKey(jobID int64) string {
	labels := make([]string, 0, 4)
	if kerneltype.IsNextGen() {
		labels = append(labels, keyspace.GetKeyspaceNameBySettings())
	}
	labels = append(labels, "ddl", proto.GlobalIndexCleanup.String(), strconv.FormatInt(jobID, 10))
	return strings.Join(labels, "/")
}

// ExecuteDistCleanupTask executes the distributed global index cleanup task.
// It returns (done, error) where done indicates if the cleanup is complete.
func ExecuteDistCleanupTask(
	ctx context.Context,
	store kv.Storage,
	job *model.Job,
	tblInfo *model.TableInfo,
	oldPartitionIDs []int64,
	globalIndexIDs []int64,
	isReorgRunnable func() error,
	updateRowCount func(count int64),
) (bool, error) {
	taskKey := TaskKey(job.ID)
	logger := logutil.DDLLogger().With(
		zap.Int64("job-id", job.ID),
		zap.String("task-key", taskKey),
	)

	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return false, err
	}

	// Check if task already exists.
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	if err != nil && !goerrors.Is(err, storage.ErrTaskNotFound) {
		return false, err
	}

	g, gCtx := errgroup.WithContext(ctx)
	done := make(chan struct{})

	var taskID int64
	if task != nil {
		// Task already exists.
		if task.State == proto.TaskStateSucceed {
			logger.Info("global index cleanup task already succeeded")
			return true, nil
		}
		taskID = task.ID
		logger.Info("resuming existing global index cleanup task", zap.Int64("task-id", taskID))

		g.Go(func() error {
			defer close(done)
			backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
			err := handle.RunWithRetry(gCtx, scheduler.RetrySQLTimes, backoffer, logger,
				func(context.Context) (bool, error) {
					return true, handle.ResumeTask(ctx, taskKey)
				},
			)
			if err != nil {
				return err
			}
			err = handle.WaitTaskDoneOrPaused(gCtx, task.ID)
			if rerr := isReorgRunnable(); rerr != nil {
				if dbterror.ErrPausedDDLJob.Equal(rerr) {
					logger.Warn("job paused by user", zap.Error(rerr))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(job.ID)
				}
			}
			return err
		})
	} else {
		// Create new task.
		concurrency := job.ReorgMeta.GetConcurrency()
		if concurrency <= 0 {
			concurrency = 4
		}

		taskMeta := &CleanupTaskMeta{
			Job:             *job.Clone(),
			TableInfo:       tblInfo,
			OldPartitionIDs: oldPartitionIDs,
			GlobalIndexIDs:  globalIndexIDs,
			Version:         1,
		}

		metaData, err := taskMeta.Marshal()
		if err != nil {
			return false, err
		}

		targetScope := job.ReorgMeta.TargetScope
		maxNodeCnt := job.ReorgMeta.MaxNodeCount

		logger.Info("submitting global index cleanup task",
			zap.Int("concurrency", concurrency),
			zap.String("target-scope", targetScope),
			zap.Int("max-node-count", maxNodeCnt),
			zap.Int64s("old-partition-ids", oldPartitionIDs),
			zap.Int64s("global-index-ids", globalIndexIDs),
		)

		task, err = handle.SubmitTask(gCtx, taskKey, proto.GlobalIndexCleanup, store.GetKeyspace(), concurrency, targetScope, maxNodeCnt, metaData)
		if err != nil {
			return false, err
		}
		taskID = task.ID

		g.Go(func() error {
			defer close(done)
			err := handle.WaitTaskDoneOrPaused(gCtx, task.ID)
			if rerr := isReorgRunnable(); rerr != nil {
				if dbterror.ErrPausedDDLJob.Equal(rerr) {
					logger.Warn("job paused by user", zap.Error(rerr))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(job.ID)
				}
			}
			return err
		})
	}

	// Monitor task progress.
	g.Go(func() error {
		checkFinishTk := time.NewTicker(CheckBackfillJobFinishInterval)
		defer checkFinishTk.Stop()
		updateRowCntTk := time.NewTicker(UpdateBackfillJobRowCountInterval)
		defer updateRowCntTk.Stop()

		for {
			select {
			case <-done:
				// Update final row count.
				if updateRowCount != nil {
					count, _ := taskManager.GetSubtaskRowCount(gCtx, taskID, proto.GlobalIndexCleanupStepScanAndDelete)
					updateRowCount(count)
				}
				return checkRunnableOrHandlePause(ctx, taskKey, isReorgRunnable)
			case <-checkFinishTk.C:
				if err := checkRunnableOrHandlePause(ctx, taskKey, isReorgRunnable); err != nil {
					return errors.Trace(err)
				}
			case <-updateRowCntTk.C:
				if updateRowCount != nil {
					count, _ := taskManager.GetSubtaskRowCount(gCtx, taskID, proto.GlobalIndexCleanupStepScanAndDelete)
					updateRowCount(count)
				}
			}
		}
	})

	err = g.Wait()
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func checkRunnableOrHandlePause(ctx context.Context, taskKey string, isReorgRunnable func() error) error {
	if err := isReorgRunnable(); err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			if perr := handle.PauseTask(ctx, taskKey); perr != nil {
				logutil.DDLLogger().Warn("pause task error", zap.String("task_key", taskKey), zap.Error(perr))
				return nil
			}
		}
		// Note: cleanup task does not support cancel once started.
		// So we don't call CancelTask here.
		if !dbterror.ErrCancelledDDLJob.Equal(err) {
			return errors.Trace(err)
		}
	}
	return nil
}

// ShouldUseDistTask returns true if the distributed task framework should be used.
func ShouldUseDistTask(job *model.Job) bool {
	if job.ReorgMeta == nil {
		return false
	}
	return job.ReorgMeta.IsDistReorg
}
