// Copyright 2023 PingCAP, Inc.
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

package handle

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	checkTaskFinishInterval = 300 * time.Millisecond
)

// SubmitGlobalTask submits a global task.
func SubmitGlobalTask(taskKey string, taskType proto.TaskType, concurrency int, taskMeta []byte) (*proto.Task, error) {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	globalTask, err := globalTaskManager.GetGlobalTaskByKey(taskKey)
	if err != nil {
		return nil, err
	}

	if globalTask == nil {
		taskID, err := globalTaskManager.AddNewGlobalTask(taskKey, taskType, concurrency, taskMeta)
		if err != nil {
			return nil, err
		}

		globalTask, err = globalTaskManager.GetGlobalTaskByID(taskID)
		if err != nil {
			return nil, err
		}

		if globalTask == nil {
			return nil, errors.Errorf("cannot find global task with ID %d", taskID)
		}
		metrics.UpdateMetricsForAddTask(globalTask)
	}
	return globalTask, nil
}

// WaitGlobalTask waits for a global task to finish.
func WaitGlobalTask(ctx context.Context, globalTask *proto.Task) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(checkTaskFinishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			found, err := globalTaskManager.GetTaskByIDWithHistory(globalTask.ID)
			if err != nil {
				logutil.Logger(ctx).Error("cannot get global task during waiting",
					zap.Int64("task-id", globalTask.ID),
					zap.Error(err))
				continue
			}
			if found == nil {
				return errors.Errorf("cannot find global task with ID %d", globalTask.ID)
			}

			switch found.State {
			case proto.TaskStateSucceed:
				return nil
			case proto.TaskStateReverted:
				logutil.BgLogger().Error("global task reverted", zap.Int64("task-id", globalTask.ID), zap.Error(found.Error))
				return found.Error
			case proto.TaskStatePaused:
				logutil.BgLogger().Error("global task paused", zap.Int64("task-id", globalTask.ID))
				return nil
			case proto.TaskStateFailed, proto.TaskStateCanceled:
				return errors.Errorf("task stopped with state %s, err %v", found.State, found.Error)
			}
		}
	}
}

// SubmitAndRunGlobalTask submits a global task and wait for it to finish.
func SubmitAndRunGlobalTask(ctx context.Context, taskKey string, taskType proto.TaskType, concurrency int, taskMeta []byte) error {
	globalTask, err := SubmitGlobalTask(taskKey, taskType, concurrency, taskMeta)
	if err != nil {
		return err
	}
	return WaitGlobalTask(ctx, globalTask)
}

// CancelGlobalTask cancels a global task.
func CancelGlobalTask(taskKey string) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	task, err := taskManager.GetGlobalTaskByKey(taskKey)
	if err != nil {
		return err
	}
	if task == nil {
		logutil.BgLogger().Info("task not exist", zap.String("taskKey", taskKey))

		return nil
	}
	return taskManager.CancelGlobalTask(task.ID)
}

// PauseTask pauses a task.
func PauseTask(taskKey string) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	found, err := taskManager.PauseTask(taskKey)
	if !found {
		logutil.BgLogger().Info("task not pausable", zap.String("taskKey", taskKey))
		return nil
	}
	return err
}

// ResumeTask resumes a task.
func ResumeTask(taskKey string) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	found, err := taskManager.ResumeTask(taskKey)
	if !found {
		logutil.BgLogger().Info("task not resumable", zap.String("taskKey", taskKey))
		return nil
	}
	return err
}

// RunWithRetry runs a function with retry, when retry exceed max retry time, it
// returns the last error met.
// if the function fails with err, it should return a bool to indicate whether
// the error is retryable.
// if context done, it will stop early and return ctx.Err().
func RunWithRetry(
	ctx context.Context,
	maxRetry int,
	backoffer backoff.Backoffer,
	logger *zap.Logger,
	f func(context.Context) (bool, error),
) error {
	var lastErr error
	for i := 0; i < maxRetry; i++ {
		retryable, err := f(ctx)
		if err == nil || !retryable {
			return err
		}
		lastErr = err
		logger.Warn("met retryable error", zap.Int("retry-count", i),
			zap.Int("max-retry", maxRetry), zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffer.Backoff(i)):
		}
	}
	return lastErr
}
