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
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	checkTaskFinishInterval = 300 * time.Millisecond
)

// SubmitGlobalTaskAndRun submits a global task and returns a channel that will be closed when the task is done.
func SubmitAndRunGlobalTask(ctx context.Context, taskKey, taskType string, concurrency int, taskMeta []byte) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	globalTask, err := globalTaskManager.GetGlobalTaskByKey(taskKey)
	if err != nil {
		return err
	}

	if globalTask == nil {
		taskID, err := globalTaskManager.AddNewGlobalTask(taskKey, taskType, concurrency, taskMeta)
		if err != nil {
			return err
		}

		globalTask, err = globalTaskManager.GetGlobalTaskByID(taskID)
		if err != nil {
			return err
		}

		if globalTask == nil {
			return errors.Errorf("cannot find global task with ID %d", taskID)
		}
	}

	ticker := time.NewTicker(checkTaskFinishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			found, err := globalTaskManager.GetGlobalTaskByID(globalTask.ID)
			if err != nil {
				return errors.Errorf("cannot get global task with ID %d, err %s", globalTask.ID, err.Error())
			}

			if found == nil {
				return errors.Errorf("cannot find global task with ID %d", globalTask.ID)
			}

			switch found.State {
			case proto.TaskStateSucceed:
				return nil
			case proto.TaskStateReverted:
				logutil.BgLogger().Error("global task reverted", zap.Int64("taskID", globalTask.ID), zap.String("error", string(found.Error)))
				return errors.New(string(found.Error))
			case proto.TaskStateFailed, proto.TaskStateCanceled:
				return errors.Errorf("task stopped with state %s, err %s", found.State, found.Error)
			}
		}
	}
}

func CancelGlobalTask(taskKey string) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	globalTask, err := globalTaskManager.GetGlobalTaskByKey(taskKey)
	if err != nil {
		return err
	}
	if globalTask == nil {
		return nil
	}
	return globalTaskManager.CancelGlobalTask(globalTask.ID)
}
