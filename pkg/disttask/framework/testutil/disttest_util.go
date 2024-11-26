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
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// GetCommonTaskExecutorExt returns a common task executor extension.
func GetCommonTaskExecutorExt(ctrl *gomock.Controller, getStepExecFn func(*proto.Task) (execute.StepExecutor, error)) *mock.MockExtension {
	executorExt := mock.NewMockExtension(ctrl)
	executorExt.EXPECT().IsIdempotent(gomock.Any()).Return(true).AnyTimes()
	executorExt.EXPECT().GetStepExecutor(gomock.Any()).DoAndReturn(getStepExecFn).AnyTimes()
	executorExt.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()
	return executorExt
}

// GetCommonStepExecutor returns one mock subtaskExecutor.
func GetCommonStepExecutor(ctrl *gomock.Controller, step proto.Step, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) *mockexecute.MockStepExecutor {
	executor := mockexecute.NewMockStepExecutor(ctrl)
	executor.EXPECT().Init(gomock.Any()).Return(nil).AnyTimes()
	executor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(runSubtaskFn).AnyTimes()
	executor.EXPECT().GetStep().Return(step).AnyTimes()
	executor.EXPECT().Cleanup(gomock.Any()).Return(nil).AnyTimes()
	executor.EXPECT().RealtimeSummary().Return(nil).AnyTimes()
	return executor
}

// GetCommonCleanUpRoutine returns a common cleanup routine.
func GetCommonCleanUpRoutine(ctrl *gomock.Controller) scheduler.CleanUpRoutine {
	r := mock.NewMockCleanUpRoutine(ctrl)
	r.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return r
}

// RegisterExampleTask register example task.
func RegisterExampleTask(
	t testing.TB,
	schedulerExt scheduler.Extension,
	executorExt taskexecutor.Extension,
	mockCleanup scheduler.CleanUpRoutine,
) {
	registerTaskType(t, proto.TaskTypeExample, schedulerExt, executorExt, mockCleanup)
}

func registerTaskType(t testing.TB, taskType proto.TaskType, schedulerExt scheduler.Extension, executorExt taskexecutor.Extension, mockCleanup scheduler.CleanUpRoutine) {
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
		},
	)

	scheduler.RegisterSchedulerCleanUpFactory(taskType, func() scheduler.CleanUpRoutine {
		return mockCleanup
	})

	taskexecutor.RegisterTaskType(taskType,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			s := taskexecutor.NewBaseTaskExecutor(ctx, task, param)
			s.Extension = executorExt
			return s
		},
	)
}

// RegisterTaskTypeForRollback register rollback task meta.
func RegisterTaskTypeForRollback(t testing.TB, ctrl *gomock.Controller, schedulerExt scheduler.Extension, testContext *TestContext) {
	subtaskRunFn := func(_ context.Context, subtask *proto.Subtask) error {
		testContext.CollectSubtask(subtask)
		return nil
	}
	executorExt := GetCommonTaskExecutorExt(ctrl, func(task *proto.Task) (execute.StepExecutor, error) {
		return GetCommonStepExecutor(ctrl, task.Step, subtaskRunFn), nil
	})
	RegisterExampleTask(t, schedulerExt, executorExt, GetCommonCleanUpRoutine(ctrl))
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
