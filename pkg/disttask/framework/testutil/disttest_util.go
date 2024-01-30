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

package testutil

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// RegisterTaskMeta initialize mock components for dist task.
func RegisterTaskMeta(t *testing.T, ctrl *gomock.Controller, schedulerHandle scheduler.Extension, testContext *TestContext, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) {
	mockExtension := mock.NewMockExtension(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockStepExecutor := GetMockStepExecutor(ctrl)
	if runSubtaskFn == nil {
		mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, subtask *proto.Subtask) error {
				switch subtask.Step {
				case proto.StepOne, proto.StepTwo:
					testContext.CollectSubtask(subtask)
				default:
					panic("invalid step")
				}
				return nil
			}).AnyTimes()
	} else {
		mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(runSubtaskFn).AnyTimes()
	}
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true).AnyTimes()
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStepExecutor, nil).AnyTimes()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()
	registerTaskMetaInner(t, proto.TaskTypeExample, mockExtension, mockCleanupRountine, schedulerHandle)
}

func registerTaskMetaInner(t *testing.T, taskType proto.TaskType, mockExtension taskexecutor.Extension, mockCleanup scheduler.CleanUpRoutine, schedulerHandle scheduler.Extension) {
	t.Cleanup(func() {
		scheduler.ClearSchedulerFactory()
		scheduler.ClearSchedulerCleanUpFactory()
		taskexecutor.ClearTaskExecutors()
	})
	scheduler.RegisterSchedulerFactory(taskType,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			baseScheduler := scheduler.NewBaseScheduler(ctx, task, param)
			baseScheduler.Extension = schedulerHandle
			return baseScheduler
		})

	scheduler.RegisterSchedulerCleanUpFactory(taskType,
		func() scheduler.CleanUpRoutine {
			return mockCleanup
		})

	taskexecutor.RegisterTaskType(taskType,
		func(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable) taskexecutor.TaskExecutor {
			s := taskexecutor.NewBaseTaskExecutor(ctx, id, task, taskTable)
			s.Extension = mockExtension
			return s
		},
	)
}

// RegisterRollbackTaskMeta register rollback task meta.
func RegisterRollbackTaskMeta(t *testing.T, ctrl *gomock.Controller, mockScheduler scheduler.Extension, testContext *TestContext) {
	mockExtension := mock.NewMockExtension(ctrl)
	mockExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockExecutor.EXPECT().Init(gomock.Any()).Return(nil).AnyTimes()
	mockExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil).AnyTimes()
	mockExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subtask *proto.Subtask) error {
			testContext.CollectSubtask(subtask)
			return nil
		}).AnyTimes()
	mockExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true).AnyTimes()
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockExecutor, nil).AnyTimes()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()

	registerTaskMetaInner(t, proto.TaskTypeExample, mockExtension, mockCleanupRountine, mockScheduler)
}

// SubmitAndWaitTask schedule one task.
func SubmitAndWaitTask(ctx context.Context, t *testing.T, taskKey string) *proto.Task {
	_, err := handle.SubmitTask(ctx, taskKey, proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	return WaitTaskDoneOrPaused(ctx, t, taskKey)
}

// WaitTaskDoneOrPaused wait task done or paused.
func WaitTaskDoneOrPaused(ctx context.Context, t *testing.T, taskKey string) *proto.Task {
	taskMgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	gotTask, err := taskMgr.GetTaskByKeyWithHistory(ctx, taskKey)
	require.NoError(t, err)
	task, err := handle.WaitTask(ctx, gotTask.ID, func(task *proto.Task) bool {
		return task.IsDone() || task.State == proto.TaskStatePaused
	})
	require.NoError(t, err)
	return task
}
