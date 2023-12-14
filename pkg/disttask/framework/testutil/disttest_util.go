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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/hook"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// RegisterTaskMeta initialize mock components for dist task.
func RegisterTaskMeta(t *testing.T, ctrl *gomock.Controller, schedulerHandle scheduler.Extension, testContext *TestContext, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) {
	mockExtension := mock.NewMockExtension(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockSubtaskExecutor := GetMockSubtaskExecutor(ctrl)
	if runSubtaskFn == nil {
		mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, subtask *proto.Subtask) error {
				switch subtask.Step {
				case proto.StepOne:
					testContext.M.Store("0", "0")
				case proto.StepTwo:
					testContext.M.Store("1", "1")
				default:
					panic("invalid step")
				}
				return nil
			}).AnyTimes()
	} else {
		mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(runSubtaskFn).AnyTimes()
	}
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true).AnyTimes()
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()
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
		func(ctx context.Context, taskMgr scheduler.TaskManager, serverID string, task *proto.Task) scheduler.Scheduler {
			baseScheduler := scheduler.NewBaseScheduler(ctx, taskMgr, serverID, task)
			baseScheduler.Extension = schedulerHandle
			return baseScheduler
		})

	scheduler.RegisterSchedulerCleanUpFactory(taskType,
		func() scheduler.CleanUpRoutine {
			return mockCleanup
		})

	taskexecutor.RegisterTaskType(taskType,
		func(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable) taskexecutor.TaskExecutor {
			s := taskexecutor.NewBaseTaskExecutor(ctx, id, task.ID, taskTable)
			s.Extension = mockExtension
			return s
		},
	)
}

// RegisterRollbackTaskMeta register rollback task meta.
func RegisterRollbackTaskMeta(t *testing.T, ctrl *gomock.Controller, mockScheduler scheduler.Extension, testContext *TestContext) {
	mockExtension := mock.NewMockExtension(ctrl)
	mockExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockExecutor.EXPECT().Init(gomock.Any()).Return(nil).AnyTimes()
	mockExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil).AnyTimes()
	mockExecutor.EXPECT().Rollback(gomock.Any()).DoAndReturn(
		func(_ context.Context) error {
			testContext.RollbackCnt.Add(1)
			return nil
		},
	).AnyTimes()
	mockExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Subtask) error {
			testContext.M.Store("1", "1")
			return nil
		}).AnyTimes()
	mockExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true).AnyTimes()
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockExecutor, nil).AnyTimes()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()

	registerTaskMetaInner(t, proto.TaskTypeExample, mockExtension, mockCleanupRountine, mockScheduler)
	testContext.RollbackCnt.Store(0)
}

// DispatchTask schedule one task.
func DispatchTask(ctx context.Context, t *testing.T, taskKey string) *proto.Task {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	_, err = mgr.CreateTask(ctx, taskKey, proto.TaskTypeExample, 8, nil)
	require.NoError(t, err)
	return WaitTaskExit(ctx, t, taskKey)
}

// WaitTaskExit wait until the task exit.
func WaitTaskExit(ctx context.Context, t *testing.T, taskKey string) *proto.Task {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	var task *proto.Task
	start := time.Now()
	for {
		if time.Since(start) > 10*time.Minute {
			require.FailNow(t, "timeout")
		}

		time.Sleep(time.Second)
		task, err = mgr.GetTaskByKeyWithHistory(ctx, taskKey)
		require.NoError(t, err)
		require.NotNil(t, task)
		if task.State != proto.TaskStatePending && task.State != proto.TaskStateRunning && task.State != proto.TaskStateCancelling && task.State != proto.TaskStateReverting && task.State != proto.TaskStatePausing {
			break
		}
	}
	return task
}

// DispatchTaskAndCheckSuccess schedule one task and check if it is succeed.
func DispatchTaskAndCheckSuccess(ctx context.Context, t *testing.T, taskKey string, testContext *TestContext, checkResultFn func(t *testing.T, testContext *TestContext)) {
	task := DispatchTask(ctx, t, taskKey)
	require.Equal(t, proto.TaskStateSucceed, task.State)
	if checkResultFn == nil {
		v, ok := testContext.M.Load("1")
		require.Equal(t, true, ok)
		require.Equal(t, "1", v)
		v, ok = testContext.M.Load("0")
		require.Equal(t, true, ok)
		require.Equal(t, "0", v)
		return
	}
	checkResultFn(t, testContext)
	testContext.M = sync.Map{}
}

// DispatchAndCancelTask schedule one task then cancel it.
func DispatchAndCancelTask(ctx context.Context, t *testing.T, taskKey string, testContext *TestContext) {
	// init hook.
	hk := &hook.TestCallback{}
	hk.OnSubtaskFinishedBeforeExported = func(subtask *proto.Subtask) error {
		mgr, err := storage.GetTaskManager()
		if err != nil {
			logutil.BgLogger().Error("get task manager failed", zap.Error(err))
			return err
		}

		err = mgr.CancelTask(ctx, subtask.TaskID)
		if err != nil {
			logutil.BgLogger().Error("cancel task failed", zap.Error(err))
			return err
		}
		return nil
	}
	taskexecutor.RegisterHook(proto.TaskTypeExample, func() hook.Callback {
		return hk
	})
	task := DispatchTask(ctx, t, taskKey)
	require.Equal(t, proto.TaskStateReverted, task.State)
	testContext.M.Range(func(key, value interface{}) bool {
		testContext.M.Delete(key)
		return true
	})
}

// DispatchTaskAndCheckState schedule one task and check the task state.
func DispatchTaskAndCheckState(ctx context.Context, t *testing.T, taskKey string, testContext *TestContext, state proto.TaskState) {
	task := DispatchTask(ctx, t, taskKey)
	require.Equal(t, state, task.State)
	testContext.M.Range(func(key, value interface{}) bool {
		testContext.M.Delete(key)
		return true
	})
}

// DispatchMultiTasksAndOneFail schedulees multiple tasks and force one task failed.
// TODO(ywqzzy): run tasks with multiple types.
func DispatchMultiTasksAndOneFail(ctx context.Context, t *testing.T, num int, testContext *TestContext) {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	tasks := make([]*proto.Task, num)
	// init hook.
	cnt := 0
	hk := &hook.TestCallback{}
	hk.OnSubtaskFinishedBeforeExported = func(_ *proto.Subtask) error {
		if cnt == 0 {
			cnt++
			return errors.New("MockExecutorRunErr")
		}
		return nil
	}
	taskexecutor.RegisterHook(proto.TaskTypeExample, func() hook.Callback {
		return hk
	})

	for i := 0; i < num; i++ {
		_, err = mgr.CreateTask(ctx, fmt.Sprintf("key%d", i), proto.TaskTypeExample, 8, nil)
		require.NoError(t, err)
	}
	for i := 0; i < num; i++ {
		tasks[i] = WaitTaskExit(ctx, t, fmt.Sprintf("key%d", i))
	}

	failCount := 0
	for _, task := range tasks {
		if task.State == proto.TaskStateReverted {
			failCount++
		}
	}
	require.Equal(t, 1, failCount)

	testContext.M.Range(func(key, value interface{}) bool {
		testContext.M.Delete(key)
		return true
	})
}
