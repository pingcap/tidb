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

package taskexecutor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	sqlsvrapimock "github.com/pingcap/tidb/pkg/domain/sqlsvrapi/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type storeWithKS struct {
	kv.Storage
	ks string
}

func (s *storeWithKS) GetKeyspace() string {
	return s.ks
}

type sessionWithSQLServer struct {
	*utilmock.Context
	server sqlsvrapi.Server
}

func (s *sessionWithSQLServer) GetSQLServer() sqlsvrapi.Server {
	return s.server
}

func newRuntimeWithStore(ctrl *gomock.Controller, store kv.Storage) *sqlsvrapimock.MockRuntime {
	return newMockRuntime(ctrl, store, nil)
}

func newRuntimeHandle(ctrl *gomock.Controller, store kv.Storage) *sqlsvrapimock.MockKSRuntimeHandle {
	runtimeHandle := sqlsvrapimock.NewMockKSRuntimeHandle(ctrl)
	runtimeHandle.EXPECT().Store().Return(store).AnyTimes()
	runtimeHandle.EXPECT().SysSessionPool().Return(nil).AnyTimes()
	return runtimeHandle
}

func expectRuntimeFromNewSession(ctrl *gomock.Controller, taskTable *mock.MockTaskTable, runtime sqlsvrapi.Runtime) {
	server := sqlsvrapimock.NewMockServer(ctrl)
	server.EXPECT().GetRuntime().Return(runtime).AnyTimes()
	taskTable.EXPECT().WithNewSession(gomock.Any()).DoAndReturn(func(fn func(sessionctx.Context) error) error {
		return fn(&sessionWithSQLServer{
			Context: utilmock.NewContext(),
			server:  server,
		})
	}).AnyTimes()
}

func TestManageTaskExecutor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskTable := mock.NewMockTaskTable(ctrl)
	m, err := NewManager(context.Background(), &storeWithKS{}, "test", mockTaskTable, proto.NodeResourceForTest)
	require.NoError(t, err)

	// add executor 1
	executor1 := mock.NewMockTaskExecutor(ctrl)
	executor1.EXPECT().GetTaskBase().Return(&proto.TaskBase{ID: 1})
	m.addTaskExecutor(executor1)
	require.Len(t, m.mu.taskExecutors, 1)
	require.True(t, m.isExecutorStarted(1))
	require.True(t, ctrl.Satisfied())
	// add executor 2
	executor2 := mock.NewMockTaskExecutor(ctrl)
	executor2.EXPECT().GetTaskBase().Return(&proto.TaskBase{ID: 2})
	m.addTaskExecutor(executor2)
	require.True(t, m.isExecutorStarted(2))
	require.True(t, ctrl.Satisfied())
	// delete executor 1
	executor1.EXPECT().GetTaskBase().Return(&proto.TaskBase{ID: 1})
	m.delTaskExecutor(executor1)
	require.False(t, m.isExecutorStarted(1))
	require.True(t, ctrl.Satisfied())

	// cancel executor 2
	executor2.EXPECT().Cancel()
	m.cancelTaskExecutors([]*proto.TaskBase{{ID: 2}})
	require.True(t, ctrl.Satisfied())
	// cancel running subtask of 2
	executor2.EXPECT().CancelRunningSubtask()
	m.cancelRunningSubtaskOf(2)
	require.True(t, ctrl.Satisfied())
	// handle pause
	executor1.EXPECT().GetTaskBase().Return(&proto.TaskBase{ID: 1})
	executor1.EXPECT().Cancel().Times(2)
	m.addTaskExecutor(executor1)
	mockTaskTable.EXPECT().PauseSubtasks(m.ctx, "test", int64(1)).Return(nil)
	require.NoError(t, m.handlePausingTask(1))
	mockTaskTable.EXPECT().PauseSubtasks(m.ctx, "test", int64(1)).Return(errors.New("pause failed"))
	require.ErrorContains(t, m.handlePausingTask(1), "pause failed")
	require.True(t, ctrl.Satisfied())

	// handle reverting
	executor1.EXPECT().GetTaskBase().Return(&proto.TaskBase{ID: 1})
	executor1.EXPECT().CancelRunningSubtask()
	m.addTaskExecutor(executor1)
	mockTaskTable.EXPECT().CancelSubtask(m.ctx, "test", int64(1)).Return(nil)
	require.NoError(t, m.handleRevertingTask(1))
	require.True(t, ctrl.Satisfied())
}

func TestHandleExecutableTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalExecutor := mock.NewMockTaskExecutor(ctrl)
	ctx := context.Background()

	id := "test"
	taskID := int64(1)
	task := &proto.TaskBase{ID: taskID, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type", RequiredSlots: 6}
	mockInternalExecutor.EXPECT().GetTaskBase().Return(task).AnyTimes()

	m, err := NewManager(ctx, &storeWithKS{}, id, mockTaskTable, proto.NodeResourceForTest)
	require.NoError(t, err)
	m.slotManager.available.Store(16)
	expectRuntimeFromNewSession(ctrl, mockTaskTable, newRuntimeWithStore(ctrl, m.store))

	// no task
	m.handleExecutableTasks(nil)

	// type not found
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), taskID).Return(&proto.Task{TaskBase: *task}, nil)
	mockTaskTable.EXPECT().FailSubtask(m.ctx, id, taskID, gomock.Any())
	m.startTaskExecutor(task)
	require.True(t, ctrl.Satisfied())

	RegisterTaskType("type",
		func(ctx context.Context, task *proto.Task, param Param) TaskExecutor {
			return mockInternalExecutor
		})

	// executor init failed non retryable
	executorErr := errors.New("executor init failed")
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(executorErr)
	mockInternalExecutor.EXPECT().IsRetryableError(executorErr).Return(false)
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(&proto.Task{TaskBase: *task}, nil)
	mockTaskTable.EXPECT().FailSubtask(m.ctx, id, taskID, executorErr)
	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task}})
	require.Equal(t, true, ctrl.Satisfied())

	// executor init failed retryable
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(executorErr)
	mockInternalExecutor.EXPECT().IsRetryableError(executorErr).Return(true)
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(&proto.Task{TaskBase: *task}, nil)
	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task}})
	require.Equal(t, true, ctrl.Satisfied())

	ch := make(chan struct{})
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutor.EXPECT().Run().DoAndReturn(func() {
		<-ch
	})
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(&proto.Task{TaskBase: *task}, nil)
	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task}})
	require.Eventually(t, func() bool {
		return ctrl.Satisfied()
	}, 5*time.Second, 100*time.Millisecond)
	require.Equal(t, 10, m.slotManager.availableSlots())
	require.True(t, m.isExecutorStarted(taskID))
	close(ch)
	mockInternalExecutor.EXPECT().Close()
	m.executorWG.Wait()
	require.True(t, ctrl.Satisfied())
	require.Equal(t, 16, m.slotManager.availableSlots())
	require.False(t, m.isExecutorStarted(taskID))
}

type crossKeyspaceStartCase struct {
	ctrl      *gomock.Controller
	taskTable *mock.MockTaskTable
	manager   *Manager
	task      *proto.Task
	taskStore *storeWithKS
	server    *sqlsvrapimock.MockServer
}

func newCrossKeyspaceStartCase(t *testing.T, taskID int64, taskKey string) *crossKeyspaceStartCase {
	t.Helper()
	ClearTaskExecutors()
	t.Cleanup(ClearTaskExecutors)

	ctrl := gomock.NewController(t)
	taskTable := mock.NewMockTaskTable(ctrl)
	m, err := NewManager(context.Background(), &storeWithKS{ks: "SYSTEM"}, "exec-1", taskTable, proto.NodeResourceForTest)
	require.NoError(t, err)
	task := &proto.Task{TaskBase: proto.TaskBase{
		ID:            taskID,
		Key:           taskKey,
		Type:          proto.TaskTypeExample,
		RequiredSlots: 1,
		Keyspace:      "user_ks",
	}}
	server := sqlsvrapimock.NewMockServer(ctrl)
	taskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	taskTable.EXPECT().WithNewSession(gomock.Any()).DoAndReturn(func(fn func(sessionctx.Context) error) error {
		return fn(&sessionWithSQLServer{Context: utilmock.NewContext(), server: server})
	})

	return &crossKeyspaceStartCase{
		ctrl:      ctrl,
		taskTable: taskTable,
		manager:   m,
		task:      task,
		taskStore: &storeWithKS{ks: task.Keyspace},
		server:    server,
	}
}

func (tc *crossKeyspaceStartCase) expectRuntimeAcquiredAndReleased() *sqlsvrapimock.MockKSRuntimeHandle {
	runtimeHandle := newRuntimeHandle(tc.ctrl, tc.taskStore)
	runtimeHandle.EXPECT().Release()
	tc.server.EXPECT().AcquireKSRuntime(tc.task.Keyspace, tc.holderID()).Return(runtimeHandle, nil)
	return runtimeHandle
}

func (tc *crossKeyspaceStartCase) holderID() string {
	return fmt.Sprintf("DXF/executor/%d", tc.task.ID)
}

func TestStartTaskExecutorCrossKeyspaceRuntime(t *testing.T) {
	t.Run("acquires runtime and releases it when executor exits", func(t *testing.T) {
		tc := newCrossKeyspaceStartCase(t, 201, "cross-ks-executor")
		runtimeHandle := tc.expectRuntimeAcquiredAndReleased()

		executor := mock.NewMockTaskExecutor(tc.ctrl)
		runCh := make(chan struct{})
		RegisterTaskType(proto.TaskTypeExample, func(_ context.Context, gotTask *proto.Task, param Param) TaskExecutor {
			require.Same(t, tc.task, gotTask)
			require.Same(t, runtimeHandle, param.TaskRuntime)
			require.Same(t, tc.taskStore, param.TaskRuntime.Store())
			return executor
		})
		executor.EXPECT().Init(gomock.Any()).Return(nil)
		executor.EXPECT().Run().Do(func() {
			close(runCh)
		})
		executor.EXPECT().GetTaskBase().Return(&tc.task.TaskBase).AnyTimes()
		executor.EXPECT().Close()

		require.True(t, tc.manager.startTaskExecutor(&tc.task.TaskBase))
		require.Eventually(t, func() bool {
			select {
			case <-runCh:
				return true
			default:
				return false
			}
		}, 5*time.Second, 100*time.Millisecond)
		tc.manager.executorWG.Wait()
	})

	t.Run("releases runtime when executor initialization fails", func(t *testing.T) {
		tc := newCrossKeyspaceStartCase(t, 202, "cross-ks-executor-init-fail")
		runtimeHandle := tc.expectRuntimeAcquiredAndReleased()

		executor := mock.NewMockTaskExecutor(tc.ctrl)
		initErr := errors.New("init failed")
		RegisterTaskType(proto.TaskTypeExample, func(_ context.Context, gotTask *proto.Task, param Param) TaskExecutor {
			require.Same(t, tc.task, gotTask)
			require.Same(t, runtimeHandle, param.TaskRuntime)
			require.Same(t, tc.taskStore, param.TaskRuntime.Store())
			return executor
		})
		executor.EXPECT().Init(gomock.Any()).Return(initErr)
		executor.EXPECT().IsRetryableError(initErr).Return(false)
		tc.taskTable.EXPECT().FailSubtask(tc.manager.ctx, tc.manager.id, tc.task.ID, initErr).Return(nil)

		require.False(t, tc.manager.startTaskExecutor(&tc.task.TaskBase))
	})

	t.Run("stops when runtime acquisition fails", func(t *testing.T) {
		tc := newCrossKeyspaceStartCase(t, 203, "cross-ks-executor-acquire-fail")
		acquireErr := errors.New("acquire failed")
		tc.server.EXPECT().AcquireKSRuntime(tc.task.Keyspace, tc.holderID()).Return(nil, acquireErr)
		factoryCalled := false
		RegisterTaskType(proto.TaskTypeExample, func(context.Context, *proto.Task, Param) TaskExecutor {
			factoryCalled = true
			require.FailNow(t, "task executor factory should not be called when runtime acquisition fails")
			return nil
		})

		require.False(t, tc.manager.startTaskExecutor(&tc.task.TaskBase))
		require.False(t, factoryCalled)
		require.False(t, tc.manager.isExecutorStarted(tc.task.ID))
		require.Equal(t, proto.NodeResourceForTest.TotalCPU, tc.manager.slotManager.availableSlots())
	})

	t.Run("releases runtime when task factory is missing", func(t *testing.T) {
		tc := newCrossKeyspaceStartCase(t, 204, "cross-ks-executor-missing-factory")
		tc.expectRuntimeAcquiredAndReleased()

		tc.taskTable.EXPECT().FailSubtask(tc.manager.ctx, tc.manager.id, tc.task.ID, gomock.Any()).Return(nil)

		require.False(t, tc.manager.startTaskExecutor(&tc.task.TaskBase))
		require.False(t, tc.manager.isExecutorStarted(tc.task.ID))
		require.Equal(t, proto.NodeResourceForTest.TotalCPU, tc.manager.slotManager.availableSlots())
	})
}

func TestManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalExecutors := map[int64]*mock.MockTaskExecutor{
		1: mock.NewMockTaskExecutor(ctrl),
		2: mock.NewMockTaskExecutor(ctrl),
		3: mock.NewMockTaskExecutor(ctrl),
	}
	RegisterTaskType("type",
		func(ctx context.Context, task *proto.Task, param Param) TaskExecutor {
			return mockInternalExecutors[task.ID]
		})
	id := "test"

	m, err := NewManager(context.Background(), &storeWithKS{}, id, mockTaskTable, proto.NodeResourceForTest)
	require.NoError(t, err)
	expectRuntimeFromNewSession(ctrl, mockTaskTable, newRuntimeWithStore(ctrl, m.store))

	task1 := &proto.TaskBase{ID: 1, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type"}
	task2 := &proto.TaskBase{ID: 2, State: proto.TaskStateReverting, Step: proto.StepOne, Type: "type"}
	task3 := &proto.TaskBase{ID: 3, State: proto.TaskStatePausing, Step: proto.StepOne, Type: "type"}

	mockTaskTable.EXPECT().InitMeta(m.ctx, "test", "").Return(nil).Times(1)
	mockTaskTable.EXPECT().GetTaskExecInfoByExecID(m.ctx, m.id).
		Return([]*storage.TaskExecInfo{{TaskBase: task1}, {TaskBase: task2}, {TaskBase: task3}}, nil)
	mockTaskTable.EXPECT().GetTaskExecInfoByExecID(m.ctx, m.id).Return(nil, nil).AnyTimes()
	// task1
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(&proto.Task{TaskBase: *task1}, nil)
	mockInternalExecutors[task1.ID].EXPECT().GetTaskBase().Return(task1).Times(2)
	mockInternalExecutors[task1.ID].EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutors[task1.ID].EXPECT().Run()
	mockInternalExecutors[task1.ID].EXPECT().Close()
	// task2
	mockTaskTable.EXPECT().CancelSubtask(m.ctx, m.id, task2.ID)
	// task3
	mockTaskTable.EXPECT().PauseSubtasks(m.ctx, id, task3.ID).Return(nil).AnyTimes()

	require.NoError(t, m.InitMeta())
	require.NoError(t, m.Start())
	require.Eventually(t, func() bool {
		return ctrl.Satisfied()
	}, 5*time.Second, 100*time.Millisecond)
	m.Stop()
}

func TestManagerHandleTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalExecutor := mock.NewMockTaskExecutor(ctrl)
	RegisterTaskType("type",
		func(ctx context.Context, task *proto.Task, param Param) TaskExecutor {
			return mockInternalExecutor
		})
	id := "test"

	m, err := NewManager(context.Background(), &storeWithKS{}, id, mockTaskTable, proto.NodeResourceForTest)
	require.NoError(t, err)
	m.slotManager.available.Store(16)
	expectRuntimeFromNewSession(ctrl, mockTaskTable, newRuntimeWithStore(ctrl, m.store))

	// failed to get tasks
	mockTaskTable.EXPECT().GetTaskExecInfoByExecID(m.ctx, m.id).
		Return(nil, errors.New("mock err"))
	require.Len(t, m.mu.taskExecutors, 0)
	m.handleTasks()
	require.Len(t, m.mu.taskExecutors, 0)
	require.True(t, ctrl.Satisfied())

	// handle pausing tasks
	mockTaskTable.EXPECT().GetTaskExecInfoByExecID(m.ctx, m.id).
		Return([]*storage.TaskExecInfo{{TaskBase: &proto.TaskBase{ID: 1, State: proto.TaskStatePausing}}}, nil)
	mockTaskTable.EXPECT().PauseSubtasks(m.ctx, id, int64(1)).Return(nil)
	m.handleTasks()
	require.True(t, ctrl.Satisfied())

	ch := make(chan error)
	defer close(ch)
	task1 := &proto.TaskBase{ID: 1, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type", RequiredSlots: 1}

	mockInternalExecutor.EXPECT().GetTaskBase().Return(task1).AnyTimes()
	// handle pending tasks
	mockTaskTable.EXPECT().GetTaskExecInfoByExecID(m.ctx, m.id).
		Return([]*storage.TaskExecInfo{{TaskBase: task1}}, nil)
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(&proto.Task{TaskBase: *task1}, nil)
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutor.EXPECT().Run().DoAndReturn(func() error {
		return <-ch
	})
	m.handleTasks()
	require.Eventually(t, func() bool {
		return ctrl.Satisfied()
	}, 5*time.Second, 100*time.Millisecond)
	require.True(t, m.isExecutorStarted(task1.ID))

	// handle task1 again, no effects
	mockTaskTable.EXPECT().GetTaskExecInfoByExecID(m.ctx, m.id).
		Return([]*storage.TaskExecInfo{{TaskBase: task1}}, nil)
	m.handleTasks()
	require.True(t, ctrl.Satisfied())

	// task1 changed to reverting, executor will keep running, but context canceled
	task1.State = proto.TaskStateReverting
	mockTaskTable.EXPECT().GetTaskExecInfoByExecID(m.ctx, m.id).
		Return([]*storage.TaskExecInfo{{TaskBase: task1}}, nil)
	mockInternalExecutor.EXPECT().CancelRunningSubtask()
	mockTaskTable.EXPECT().CancelSubtask(m.ctx, m.id, task1.ID)
	m.handleTasks()
	require.True(t, ctrl.Satisfied())
	require.True(t, m.isExecutorStarted(task1.ID))

	// finish task1, executor will be closed
	task1.State = proto.TaskStateReverted
	mockInternalExecutor.EXPECT().Close()
	ch <- nil
	require.Eventually(t, func() bool {
		return ctrl.Satisfied()
	}, 5*time.Second, 100*time.Millisecond)
	require.False(t, m.isExecutorStarted(task1.ID))

	m.executorWG.Wait()
}

func TestSlotManagerInManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalExecutors := map[int64]*mock.MockTaskExecutor{
		1: mock.NewMockTaskExecutor(ctrl),
		2: mock.NewMockTaskExecutor(ctrl),
		3: mock.NewMockTaskExecutor(ctrl),
	}
	RegisterTaskType("type",
		func(ctx context.Context, task *proto.Task, param Param) TaskExecutor {
			return mockInternalExecutors[task.ID]
		})
	id := "test"

	m, err := NewManager(context.Background(), &storeWithKS{}, id, mockTaskTable, proto.NodeResourceForTest)
	require.NoError(t, err)
	m.slotManager.available.Store(10)
	expectRuntimeFromNewSession(ctrl, mockTaskTable, newRuntimeWithStore(ctrl, m.store))

	var (
		task1 = &proto.TaskBase{
			ID:            1,
			State:         proto.TaskStateRunning,
			RequiredSlots: 10,
			Step:          proto.StepOne,
			Type:          "type",
		}
		task2 = &proto.TaskBase{
			ID:            2,
			State:         proto.TaskStateRunning,
			RequiredSlots: 1,
			Step:          proto.StepOne,
			Type:          "type",
		}
		task3 = &proto.TaskBase{
			ID:            3,
			State:         proto.TaskStateRunning,
			RequiredSlots: 1,
			Priority:      -1,
			Step:          proto.StepOne,
			Type:          "type",
		}
	)
	mockInternalExecutors[task1.ID].EXPECT().GetTaskBase().Return(task1).AnyTimes()
	mockInternalExecutors[task2.ID].EXPECT().GetTaskBase().Return(task2).AnyTimes()
	mockInternalExecutors[task3.ID].EXPECT().GetTaskBase().Return(task3).AnyTimes()

	// init error, allocated slot will be released
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(&proto.Task{TaskBase: *task1}, nil)
	mockInternalExecutors[task1.ID].EXPECT().Init(gomock.Any()).Return(errors.New("some error"))
	mockInternalExecutors[task1.ID].EXPECT().IsRetryableError(gomock.Any()).Return(true)
	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task1}})
	require.True(t, ctrl.Satisfied())
	require.False(t, m.isExecutorStarted(task1.ID))
	require.Equal(t, 10, m.slotManager.availableSlots())
	require.Empty(t, m.slotManager.executorTasks)

	ch := make(chan error)
	defer close(ch)

	// ******** Test task1 alloc success ********
	// 1. task1 alloc success
	// 2. task2 alloc failed
	// 3. task1 run success

	// mock inside startTaskExecutor
	mockInternalExecutors[task1.ID].EXPECT().Init(gomock.Any()).Return(nil)
	// task1 start running
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(&proto.Task{TaskBase: *task1}, nil)
	mockInternalExecutors[task1.ID].EXPECT().Run().DoAndReturn(func() error {
		return <-ch
	})

	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task1}, {TaskBase: task2}})
	// task1 alloc resource success
	require.Eventually(t, func() bool {
		return ctrl.Satisfied()
	}, 2*time.Second, 300*time.Millisecond)
	require.True(t, m.isExecutorStarted(task1.ID))
	require.Equal(t, 0, m.slotManager.availableSlots())
	require.Len(t, m.slotManager.executorTasks, 1)
	// task1 succeed
	mockInternalExecutors[task1.ID].EXPECT().Close()
	ch <- nil
	m.executorWG.Wait()
	require.Equal(t, 10, m.slotManager.availableSlots())
	require.False(t, m.isExecutorStarted(task1.ID))
	require.Len(t, m.slotManager.executorTasks, 0)
	require.True(t, ctrl.Satisfied())

	// ******** Test task preemption ********
	// task1 start running
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(&proto.Task{TaskBase: *task1}, nil)
	mockInternalExecutors[task1.ID].EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutors[task1.ID].EXPECT().Run().DoAndReturn(func() error {
		return <-ch
	})

	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task1}, {TaskBase: task2}})
	// task1 alloc resource success
	require.Eventually(t, func() bool {
		return ctrl.Satisfied()
	}, 2*time.Second, 300*time.Millisecond)
	require.True(t, m.isExecutorStarted(task1.ID))
	require.Equal(t, 0, m.slotManager.availableSlots())
	require.Len(t, m.slotManager.executorTasks, 1)

	// 2. task1 is preempted by task3, task1 start to pausing
	// 3. task3 is waiting for task1 to be released, and task2 can't be allocated
	// the priority of task3 is higher than task2, so task3 is in front of task2
	mockInternalExecutors[task1.ID].EXPECT().Cancel()
	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task3}, {TaskBase: task2}})
	require.True(t, ctrl.Satisfied())
	require.Equal(t, 0, m.slotManager.availableSlots())
	require.Len(t, m.slotManager.executorTasks, 1)
	require.True(t, m.isExecutorStarted(task1.ID))

	// 4. task1 is released, task3 alloc success, start to run
	mockInternalExecutors[task1.ID].EXPECT().Close()
	ch <- context.Canceled
	m.executorWG.Wait()
	require.Equal(t, 10, m.slotManager.availableSlots())
	require.Len(t, m.slotManager.executorTasks, 0)
	require.False(t, m.isExecutorStarted(task1.ID))
	require.True(t, ctrl.Satisfied())

	// 5. available is enough, task3/task2 alloc success,
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task2.ID).Return(&proto.Task{TaskBase: *task2}, nil)
	mockInternalExecutors[task2.ID].EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutors[task2.ID].EXPECT().Run().DoAndReturn(func() error {
		return <-ch
	})
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task3.ID).Return(&proto.Task{TaskBase: *task3}, nil)
	mockInternalExecutors[task3.ID].EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutors[task3.ID].EXPECT().Run().DoAndReturn(func() error {
		return <-ch
	})

	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task3}, {TaskBase: task1}, {TaskBase: task2}})
	require.Eventually(t, func() bool {
		return ctrl.Satisfied()
	}, 2*time.Second, 300*time.Millisecond)
	require.Equal(t, 8, m.slotManager.availableSlots())
	require.Len(t, m.slotManager.executorTasks, 2)
	require.True(t, m.isExecutorStarted(task2.ID))
	require.True(t, m.isExecutorStarted(task3.ID))

	// 6. task3/task2 run success
	mockInternalExecutors[task2.ID].EXPECT().Close()
	mockInternalExecutors[task3.ID].EXPECT().Close()
	ch <- nil
	ch <- nil
	m.executorWG.Wait()
	require.Equal(t, 10, m.slotManager.availableSlots())
	require.Equal(t, 0, len(m.slotManager.executorTasks))
	require.True(t, ctrl.Satisfied())

	// task rank: task3(1), task1(4), task2(1)
	task1.RequiredSlots = 4
	// task3 exchange to 8 slots, task1 cannot start, and we will skip task2 too.
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task3.ID).Return(&proto.Task{TaskBase: *task3}, nil)
	mockInternalExecutors[task3.ID].EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutors[task3.ID].EXPECT().Run().DoAndReturn(func() error {
		return <-ch
	})
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(&proto.Task{TaskBase: *task1}, nil)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/beforeCallStartTaskExecutor",
		func(task *proto.TaskBase) {
			if task.ID == task1.ID {
				newTask3 := *task3
				newTask3.RequiredSlots = 8
				require.True(t, m.slotManager.exchange(&newTask3))
				require.Equal(t, 2, m.slotManager.availableSlots())
			}
		},
	)
	m.handleExecutableTasks([]*storage.TaskExecInfo{{TaskBase: task3}, {TaskBase: task1}, {TaskBase: task2}})
	require.Eventually(t, func() bool {
		return ctrl.Satisfied()
	}, 2*time.Second, 300*time.Millisecond)
	require.Equal(t, 2, m.slotManager.availableSlots())
	require.Len(t, m.slotManager.executorTasks, 1)
	require.EqualValues(t, 8, m.slotManager.executorTasks[0].RequiredSlots)
	require.True(t, m.isExecutorStarted(task3.ID))
	// finish
	mockInternalExecutors[task3.ID].EXPECT().Close()
	ch <- nil
	m.executorWG.Wait()
	require.Equal(t, 10, m.slotManager.availableSlots())
	require.Equal(t, 0, len(m.slotManager.executorTasks))
	require.True(t, ctrl.Satisfied())
}

func TestStartTaskExecutorResolveTaskRuntimeFromTaskKeyspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	t.Cleanup(func() {
		ClearTaskExecutors()
	})

	const (
		instanceKS = "instance_ks"
		taskKS     = "task_ks"
	)
	task := &proto.Task{
		TaskBase: proto.TaskBase{
			ID:            1,
			Keyspace:      taskKS,
			Type:          "resolve-store",
			Step:          proto.StepOne,
			State:         proto.TaskStateRunning,
			RequiredSlots: 1,
		},
	}
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	m, err := NewManager(context.Background(), &storeWithKS{ks: instanceKS}, "test", mockTaskTable, proto.NodeResourceForTest)
	require.NoError(t, err)

	taskStore := &storeWithKS{ks: taskKS}
	runtimeHandle := newRuntimeHandle(ctrl, taskStore)
	runtimeHandle.EXPECT().Release()
	server := sqlsvrapimock.NewMockServer(ctrl)
	server.EXPECT().AcquireKSRuntime(taskKS, "DXF/executor/1").Return(runtimeHandle, nil)

	mockExecutor := mock.NewMockTaskExecutor(ctrl)
	var gotStore kv.Storage
	RegisterTaskType(task.Type, func(_ context.Context, _ *proto.Task, param Param) TaskExecutor {
		gotStore = param.TaskRuntime.Store()
		return mockExecutor
	})
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockTaskTable.EXPECT().WithNewSession(gomock.Any()).DoAndReturn(func(fn func(sessionctx.Context) error) error {
		return fn(&sessionWithSQLServer{Context: utilmock.NewContext(), server: server})
	})
	mockExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	runCh := make(chan struct{})
	mockExecutor.EXPECT().GetTaskBase().Return(&task.TaskBase).AnyTimes()
	mockExecutor.EXPECT().Run().DoAndReturn(func() {
		<-runCh
	})
	mockExecutor.EXPECT().Close()
	defer func() {
		close(runCh)
		m.executorWG.Wait()
	}()

	require.True(t, m.startTaskExecutor(&task.TaskBase))
	require.Same(t, taskStore, gotStore)
}

func TestStartTaskExecutorResolveTaskRuntimeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		instanceKS = "instance_ks"
		taskKS     = "task_ks"
	)
	task := &proto.Task{
		TaskBase: proto.TaskBase{
			ID:            2,
			Keyspace:      taskKS,
			Type:          "resolve-store-error",
			Step:          proto.StepOne,
			State:         proto.TaskStateRunning,
			RequiredSlots: 1,
		},
	}
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	m, err := NewManager(context.Background(), &storeWithKS{ks: instanceKS}, "test", mockTaskTable, proto.NodeResourceForTest)
	require.NoError(t, err)

	runtimeErr := errors.New("ks runtime not found")
	server := sqlsvrapimock.NewMockServer(ctrl)
	server.EXPECT().AcquireKSRuntime(taskKS, "DXF/executor/2").Return(nil, runtimeErr)
	factoryCalled := false
	RegisterTaskType(task.Type, func(context.Context, *proto.Task, Param) TaskExecutor {
		factoryCalled = true
		return nil
	})

	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockTaskTable.EXPECT().WithNewSession(gomock.Any()).DoAndReturn(func(fn func(sessionctx.Context) error) error {
		return fn(&sessionWithSQLServer{Context: utilmock.NewContext(), server: server})
	})

	require.False(t, m.startTaskExecutor(&task.TaskBase))
	require.False(t, factoryCalled)
	require.False(t, m.isExecutorStarted(task.ID))
	require.Equal(t, proto.NodeResourceForTest.TotalCPU, m.slotManager.availableSlots())
}

func TestManagerInitMeta(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskTable := mock.NewMockTaskTable(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &Manager{
		taskTable: mockTaskTable,
		ctx:       ctx,
		logger:    logutil.BgLogger(),
	}
	mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	require.NoError(t, m.InitMeta())
	require.True(t, ctrl.Satisfied())
	gomock.InOrder(
		mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mock err")),
		mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
	)
	require.NoError(t, m.InitMeta())
	require.True(t, ctrl.Satisfied())

	reduceRetrySQLTimes(t, 1)
	mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mock err"))
	require.ErrorContains(t, m.InitMeta(), "mock err")
	require.True(t, ctrl.Satisfied())

	cancel()
	mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mock err"))
	require.ErrorIs(t, m.InitMeta(), context.Canceled)
	require.True(t, ctrl.Satisfied())
}
