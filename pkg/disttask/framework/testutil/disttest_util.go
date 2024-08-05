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

// RegisterTaskMetaWithDXFCtx initialize mock components for dist task.
func RegisterTaskMetaWithDXFCtx(c *TestDXFContext, schedulerExt scheduler.Extension, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) {
	RegisterTaskMeta(c.T, c.MockCtrl, schedulerExt, c.TestContext, runSubtaskFn)
}

// RegisterTaskMeta initialize mock components for dist task.
func RegisterTaskMeta(t testing.TB, ctrl *gomock.Controller, schedulerExt scheduler.Extension, testContext *TestContext, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) {
	executorExt := mock.NewMockExtension(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockStepExecutor := GetMockStepExecutor(ctrl)
	if runSubtaskFn == nil {
		mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, subtask *proto.Subtask) error {
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
	mockStepExecutor.EXPECT().RealtimeSummary().Return(nil).AnyTimes()
	executorExt.EXPECT().IsIdempotent(gomock.Any()).Return(true).AnyTimes()
	executorExt.EXPECT().GetStepExecutor(gomock.Any()).Return(mockStepExecutor, nil).AnyTimes()
	executorExt.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()
	registerTaskMetaInner(t, proto.TaskTypeExample, schedulerExt, executorExt, mockCleanupRountine)
}

func registerTaskMetaInner(t testing.TB, taskType proto.TaskType, schedulerExt scheduler.Extension, executorExt taskexecutor.Extension, mockCleanup scheduler.CleanUpRoutine) {
	t.Cleanup(func() {
		scheduler.ClearSchedulerFactory()
		scheduler.ClearSchedulerCleanUpFactory()
		taskexecutor.ClearTaskExecutors()
	})
	scheduler.RegisterSchedulerFactory(taskType,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			baseScheduler := scheduler.NewBaseScheduler(ctx, task, param)
			baseScheduler.Extension = schedulerExt
			return baseScheduler
		})

	scheduler.RegisterSchedulerCleanUpFactory(taskType,
		func() scheduler.CleanUpRoutine {
			return mockCleanup
		})

	taskexecutor.RegisterTaskType(taskType,
		func(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable) taskexecutor.TaskExecutor {
			s := taskexecutor.NewBaseTaskExecutor(ctx, id, task, taskTable)
			s.Extension = executorExt
			return s
		},
	)
}

// RegisterRollbackTaskMeta register rollback task meta.
func RegisterRollbackTaskMeta(t testing.TB, ctrl *gomock.Controller, schedulerExt scheduler.Extension, testContext *TestContext) {
	executorExt := mock.NewMockExtension(ctrl)
	stepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	stepExecutor.EXPECT().Init(gomock.Any()).Return(nil).AnyTimes()
	stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil).AnyTimes()
	stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subtask *proto.Subtask) error {
			testContext.CollectSubtask(subtask)
			return nil
		}).AnyTimes()
	stepExecutor.EXPECT().RealtimeSummary().Return(nil).AnyTimes()
	stepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	executorExt.EXPECT().IsIdempotent(gomock.Any()).Return(true).AnyTimes()
	executorExt.EXPECT().GetStepExecutor(gomock.Any()).Return(stepExecutor, nil).AnyTimes()
	executorExt.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()

	registerTaskMetaInner(t, proto.TaskTypeExample, schedulerExt, executorExt, mockCleanupRountine)
}

// SubmitAndWaitTask schedule one task.
func SubmitAndWaitTask(ctx context.Context, t testing.TB, taskKey string, targetScope string, concurrency int) *proto.TaskBase {
	_, err := handle.SubmitTask(ctx, taskKey, proto.TaskTypeExample, concurrency, targetScope, nil)
	require.NoError(t, err)
	return WaitTaskDoneOrPaused(ctx, t, taskKey)
}

// WaitTaskDoneOrPaused wait task done or paused.
func WaitTaskDoneOrPaused(ctx context.Context, t testing.TB, taskKey string) *proto.TaskBase {
	return waitTaskUntil(ctx, t, taskKey, func(task *proto.TaskBase) bool {
		return task.IsDone() || task.State == proto.TaskStatePaused
	})
}

// WaitTaskDone wait task done.
func WaitTaskDone(ctx context.Context, t testing.TB, taskKey string) *proto.TaskBase {
	return waitTaskUntil(ctx, t, taskKey, func(task *proto.TaskBase) bool {
		return task.IsDone()
	})
}

func waitTaskUntil(ctx context.Context, t testing.TB, taskKey string, fn func(task *proto.TaskBase) bool) *proto.TaskBase {
	taskMgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	gotTask, err := taskMgr.GetTaskBaseByKeyWithHistory(ctx, taskKey)
	require.NoError(t, err)
	task, err := handle.WaitTask(ctx, gotTask.ID, fn)
	require.NoError(t, err)
	return task
}
