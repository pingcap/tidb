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

	// TaskChangedCh used to speed up task schedule, such as when task is submitted
	// in the same node as the scheduler manager.
	// put it here to avoid cyclic import.
	TaskChangedCh = make(chan struct{}, 1)
)

// NotifyTaskChange is used to notify the scheduler manager that the task is changed,
// either a new task is submitted or a task is finished.
func NotifyTaskChange() {
	select {
	case TaskChangedCh <- struct{}{}:
	default:
	}
}

// GetCPUCountOfNode gets the CPU count of the managed node.
func GetCPUCountOfNode(ctx context.Context) (int, error) {
	manager, err := storage.GetTaskManager()
	if err != nil {
		return 0, err
	}
	return manager.GetCPUCountOfNode(ctx)
}

// SubmitTask submits a task.
func SubmitTask(ctx context.Context, taskKey string, taskType proto.TaskType, concurrency int, targetScope string, taskMeta []byte) (*proto.Task, error) {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	task, err := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	if err != nil && err != storage.ErrTaskNotFound {
		return nil, err
	}
	if task != nil {
		return nil, storage.ErrTaskAlreadyExists
	}

	taskID, err := taskManager.CreateTask(ctx, taskKey, taskType, concurrency, targetScope, taskMeta)
	if err != nil {
		return nil, err
	}

	task, err = taskManager.GetTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	metrics.UpdateMetricsForAddTask(&task.TaskBase)

	NotifyTaskChange()
	return task, nil
}

// WaitTaskDoneOrPaused waits for a task done or paused.
// this API returns error if task failed or cancelled.
func WaitTaskDoneOrPaused(ctx context.Context, id int64) error {
	logger := logutil.Logger(ctx).With(zap.Int64("task-id", id))
	_, err := WaitTask(ctx, id, func(t *proto.TaskBase) bool {
		return t.IsDone() || t.State == proto.TaskStatePaused
	})
	if err != nil {
		return err
	}
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	found, err := taskManager.GetTaskByIDWithHistory(ctx, id)
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
	_, err = WaitTask(ctx, task.ID, func(t *proto.TaskBase) bool {
		return t.IsDone()
	})
	return err
}

// WaitTask waits for a task until it meets the matchFn.
func WaitTask(ctx context.Context, id int64, matchFn func(base *proto.TaskBase) bool) (*proto.TaskBase, error) {
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
			task, err := taskManager.GetTaskBaseByIDWithHistory(ctx, id)
			if err != nil {
				logger.Error("cannot get task during waiting", zap.Error(err))
				continue
			}

			if matchFn(task) {
				return task, nil
			}
		}
	}
}

// CancelTask cancels a task.
func CancelTask(ctx context.Context, taskKey string) error {
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKey(ctx, taskKey)
	if err != nil {
		if err == storage.ErrTaskNotFound {
			logutil.BgLogger().Info("task not exist", zap.String("taskKey", taskKey))
			return nil
		}
		return err
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
