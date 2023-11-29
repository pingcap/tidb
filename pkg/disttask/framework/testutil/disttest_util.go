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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// initialize mock components for dist task.
func RegisterTaskMeta(t *testing.T, ctrl *gomock.Controller, dispatcherHandle dispatcher.Extension, testContext *TestContext) {
	mockExtension := mock.NewMockExtension(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockSubtaskExecutor := GetMockSubtaskExecutor(ctrl)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, subtask *proto.Subtask) error {
			switch subtask.Step {
			case proto.StepOne:
				testContext.m.Store("0", "0")
			case proto.StepTwo:
				testContext.m.Store("1", "1")
			default:
				panic("invalid step")
			}
			return nil
		}).AnyTimes()
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()
	registerTaskMetaInner(t, proto.TaskTypeExample, mockExtension, mockCleanupRountine, dispatcherHandle)
}

func RegisterTaskMetaForExample2(t *testing.T, ctrl *gomock.Controller, dispatcherHandle dispatcher.Extension, testContext *TestContext) {
	mockExtension := mock.NewMockExtension(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockSubtaskExecutor := GetMockSubtaskExecutor(ctrl)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, subtask *proto.Subtask) error {
			switch subtask.Step {
			case proto.StepOne:
				testContext.m.Store("2", "2")
			case proto.StepTwo:
				testContext.m.Store("3", "3")
			default:
				panic("invalid step")
			}
			return nil
		}).AnyTimes()
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()
	registerTaskMetaInner(t, proto.TaskTypeExample2, mockExtension, mockCleanupRountine, dispatcherHandle)
}

func RegisterTaskMetaForExample3(t *testing.T, ctrl *gomock.Controller, dispatcherHandle dispatcher.Extension, testContext *TestContext) {
	mockExtension := mock.NewMockExtension(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockSubtaskExecutor := GetMockSubtaskExecutor(ctrl)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, subtask *proto.Subtask) error {
			switch subtask.Step {
			case proto.StepOne:
				testContext.m.Store("4", "4")
			case proto.StepTwo:
				testContext.m.Store("5", "5")
			default:
				panic("invalid step")
			}
			return nil
		}).AnyTimes()
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()
	registerTaskMetaInner(t, proto.TaskTypeExample3, mockExtension, mockCleanupRountine, dispatcherHandle)
}

func registerTaskMetaInner(t *testing.T, taskType proto.TaskType, mockExtension scheduler.Extension, mockCleanup dispatcher.CleanUpRoutine, dispatcherHandle dispatcher.Extension) {
	t.Cleanup(func() {
		dispatcher.ClearDispatcherFactory()
		dispatcher.ClearDispatcherCleanUpFactory()
		scheduler.ClearSchedulers()
	})
	dispatcher.RegisterDispatcherFactory(taskType,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			baseDispatcher := dispatcher.NewBaseDispatcher(ctx, taskMgr, serverID, task)
			baseDispatcher.Extension = dispatcherHandle
			return baseDispatcher
		})

	dispatcher.RegisterDispatcherCleanUpFactory(taskType,
		func() dispatcher.CleanUpRoutine {
			return mockCleanup
		})

	scheduler.RegisterTaskType(taskType,
		func(ctx context.Context, id string, task *proto.Task, taskTable scheduler.TaskTable) scheduler.Scheduler {
			s := scheduler.NewBaseScheduler(ctx, id, task.ID, taskTable)
			s.Extension = mockExtension
			return s
		},
	)
}

func RegisterRollbackTaskMeta(t *testing.T, ctrl *gomock.Controller, mockDispatcher dispatcher.Extension, testContext *TestContext) {
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
			testContext.m.Store("1", "1")
			return nil
		}).AnyTimes()
	mockExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockExecutor, nil).AnyTimes()

	registerTaskMetaInner(t, proto.TaskTypeExample, mockExtension, mockCleanupRountine, mockDispatcher)
	testContext.RollbackCnt.Store(0)
}

func DispatchTask(t *testing.T, ctx context.Context, taskKey string) *proto.Task {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	_, err = mgr.AddNewGlobalTask(ctx, taskKey, proto.TaskTypeExample, 8, nil)
	require.NoError(t, err)
	return WaitTaskExit(t, ctx, taskKey)
}

func WaitTaskExit(t *testing.T, ctx context.Context, taskKey string) *proto.Task {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	var task *proto.Task
	start := time.Now()
	for {
		if time.Since(start) > 10*time.Minute {
			require.FailNow(t, "timeout")
		}

		time.Sleep(time.Second)
		task, err = mgr.GetGlobalTaskByKeyWithHistory(ctx, taskKey)
		require.NoError(t, err)
		require.NotNil(t, task)
		if task.State != proto.TaskStatePending && task.State != proto.TaskStateRunning && task.State != proto.TaskStateCancelling && task.State != proto.TaskStateReverting && task.State != proto.TaskStatePausing {
			break
		}
	}
	return task
}

func DispatchTaskAndCheckSuccess(t *testing.T, ctx context.Context, taskKey string, testContext *TestContext) {
	task := DispatchTask(t, ctx, taskKey)
	require.Equal(t, proto.TaskStateSucceed, task.State)
	v, ok := testContext.m.Load("1")
	require.Equal(t, true, ok)
	require.Equal(t, "1", v)
	v, ok = testContext.m.Load("0")
	require.Equal(t, true, ok)
	require.Equal(t, "0", v)
	testContext.m = sync.Map{}
}

func DispatchAndCancelTask(t *testing.T, ctx context.Context, taskKey string, testContext *TestContext) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockExecutorRunCancel", "1*return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockExecutorRunCancel"))
	}()
	task := DispatchTask(t, ctx, taskKey)
	require.Equal(t, proto.TaskStateReverted, task.State)
	testContext.m.Range(func(key, value interface{}) bool {
		testContext.m.Delete(key)
		return true
	})
}

func DispatchTaskAndCheckState(t *testing.T, ctx context.Context, taskKey string, testContext *TestContext, state proto.TaskState) {
	task := DispatchTask(t, ctx, taskKey)
	require.Equal(t, state, task.State)
	testContext.m.Range(func(key, value interface{}) bool {
		testContext.m.Delete(key)
		return true
	})
}

func DispatchMultiTasksAndOneFail(t *testing.T, ctx context.Context, num int, testContexts []*TestContext) {
	var tasks []*proto.Task
	var taskID []int64
	var start []time.Time
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	taskID = make([]int64, num)
	start = make([]time.Time, num)
	tasks = make([]*proto.Task, num)

	for i := 0; i < num; i++ {
		if i == 0 {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockExecutorRunErr", "1*return(true)"))
			taskID[0], err = mgr.AddNewGlobalTask(ctx, "key0", "Example", 8, nil)
			require.NoError(t, err)
			start[0] = time.Now()
			var task *proto.Task
			for {
				if time.Since(start[0]) > 2*time.Minute {
					require.FailNow(t, "timeout")
				}
				time.Sleep(time.Second)
				task, err = mgr.GetTaskByIDWithHistory(ctx, taskID[0])
				require.NoError(t, err)
				require.NotNil(t, task)
				tasks[0] = task
				if task.State != proto.TaskStatePending && task.State != proto.TaskStateRunning && task.State != proto.TaskStateCancelling && task.State != proto.TaskStateReverting {
					break
				}
			}
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockExecutorRunErr"))
		} else {
			taskID[i], err = mgr.AddNewGlobalTask(ctx, fmt.Sprintf("key%d", i), proto.Int2Type(i+2), 8, nil)
			require.NoError(t, err)
			start[i] = time.Now()
		}
	}

	for i := 1; i < num; i++ {
		var task *proto.Task
		for {
			if time.Since(start[i]) > 2*time.Minute {
				require.FailNow(t, "timeout")
			}
			time.Sleep(time.Second)
			task, err = mgr.GetTaskByIDWithHistory(ctx, taskID[i])
			require.NoError(t, err)
			require.NotNil(t, task)
			tasks[i] = task

			if task.State != proto.TaskStatePending && task.State != proto.TaskStateRunning && task.State != proto.TaskStateCancelling && task.State != proto.TaskStateReverting {
				break
			}
		}
	}
	v, ok := testContexts[0].m.Load("0")
	require.Equal(t, false, ok)
	require.Equal(t, nil, v)
	v, ok = testContexts[0].m.Load("1")
	require.Equal(t, false, ok)
	require.Equal(t, nil, v)
	v, ok = testContexts[1].m.Load("2")
	require.Equal(t, true, ok)
	require.Equal(t, "2", v)
	v, ok = testContexts[1].m.Load("3")
	require.Equal(t, true, ok)
	require.Equal(t, "3", v)
	v, ok = testContexts[2].m.Load("4")
	require.Equal(t, true, ok)
	require.Equal(t, "4", v)
	v, ok = testContexts[2].m.Load("5")
	require.Equal(t, true, ok)
	require.Equal(t, "5", v)

	require.Equal(t, proto.TaskStateReverted, tasks[0].State)
	require.Equal(t, proto.TaskStateSucceed, tasks[1].State)
	require.Equal(t, proto.TaskStateSucceed, tasks[2].State)

	for _, text_context := range testContexts {
		text_context.m.Range(func(key, value interface{}) bool {
			text_context.m.Delete(key)
			return true
		})
	}
}
