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

// SubmitTask submits a task.
func SubmitTask(ctx context.Context, taskKey string, taskType proto.TaskType, concurrency int, taskMeta []byte) (*proto.Task, error) {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	task, err := taskManager.GetTaskByKey(ctx, taskKey)
	if err != nil {
		return nil, err
	}

	if task == nil {
		taskID, err := taskManager.CreateTask(ctx, taskKey, taskType, concurrency, taskMeta)
		if err != nil {
			return nil, err
		}

		task, err = taskManager.GetTaskByID(ctx, taskID)
		if err != nil {
			return nil, err
		}

		if task == nil {
			return nil, errors.Errorf("cannot find task with ID %d", taskID)
		}
		metrics.UpdateMetricsForAddTask(task)
	}
	return task, nil
}

// WaitTask waits for a task done or paused.
// this API returns error if task failed or cancelled.
func WaitTask(ctx context.Context, id int64) error {
	logger := logutil.Logger(ctx).With(zap.Int64("task-id", id))
	found, err := waitTask(ctx, id, func(t *proto.Task) bool {
		return t.IsDone() || t.State == proto.TaskStatePaused
	})
	if err != nil {
		return err
	}

	switch found.State {
	case proto.TaskStateSucceed:
		return nil
	case proto.TaskStateReverted:
		logger.Error("task reverted", zap.Error(found.Error))
		return found.Error
	case proto.TaskStatePaused:
		logger.Error("task paused")
		return nil
	case proto.TaskStateFailed:
		return errors.Errorf("task stopped with state %s, err %v", found.State, found.Error)
	}
	return nil
}

// WaitTaskDoneByKey waits for a task done by task key.
func WaitTaskDoneByKey(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	if err != nil {
		return err
	}
	if task == nil {
		return errors.Errorf("cannot find task with key %s", taskKey)
	}
	_, err = waitTask(ctx, task.ID, func(t *proto.Task) bool {
		return t.IsDone()
	})
	return err
}

func waitTask(ctx context.Context, id int64, matchFn func(*proto.Task) bool) (*proto.Task, error) {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	ticker := time.NewTicker(checkTaskFinishInterval)
	defer ticker.Stop()

	logger := logutil.Logger(ctx).With(zap.Int64("task-id", id))
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			found, err := taskManager.GetTaskByIDWithHistory(ctx, id)
			if err != nil {
				logger.Error("cannot get task during waiting", zap.Error(err))
				continue
			}
			if found == nil {
				return nil, errors.Errorf("cannot find task with ID %d", id)
			}

			if matchFn(found) {
				return found, nil
			}
		}
	}
}

// SubmitAndWaitTask submits a task and wait for it to finish.
func SubmitAndWaitTask(ctx context.Context, taskKey string, taskType proto.TaskType, concurrency int, taskMeta []byte) error {
	task, err := SubmitTask(ctx, taskKey, taskType, concurrency, taskMeta)
	if err != nil {
		return err
	}
	return WaitTask(ctx, task.ID)
}

// CancelTask cancels a task.
func CancelTask(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKey(ctx, taskKey)
	if err != nil {
		return err
	}
	if task == nil {
		logutil.BgLogger().Info("task not exist", zap.String("taskKey", taskKey))

		return nil
	}
	return taskManager.CancelTask(ctx, task.ID)
}

// PauseTask pauses a task.
func PauseTask(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	found, err := taskManager.PauseTask(ctx, taskKey)
	if !found {
		logutil.BgLogger().Info("task not pausable", zap.String("taskKey", taskKey))
		return nil
	}
	return err
}

// ResumeTask resumes a task.
func ResumeTask(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	found, err := taskManager.ResumeTask(ctx, taskKey)
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
