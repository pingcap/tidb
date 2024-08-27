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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBuildManager(t *testing.T) {
	m, err := NewManager(context.Background(), "test", nil)
	require.NoError(t, err)
	require.NotNil(t, m)

	bak := memory.MemTotal
	defer func() {
		memory.MemTotal = bak
	}()
	memory.MemTotal = func() (uint64, error) {
		return 0, errors.New("mock error")
	}
	_, err = NewManager(context.Background(), "test", nil)
	require.ErrorContains(t, err, "mock error")

	memory.MemTotal = func() (uint64, error) {
		return 0, nil
	}
	_, err = NewManager(context.Background(), "test", nil)
	require.ErrorContains(t, err, "invalid cpu or memory")
}

func TestManageTaskExecutor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTaskTable := mock.NewMockTaskTable(ctrl)
	m, err := NewManager(context.Background(), "test", mockTaskTable)
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
	task := &proto.TaskBase{ID: taskID, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type", Concurrency: 6}
	mockInternalExecutor.EXPECT().GetTaskBase().Return(task).AnyTimes()

	m, err := NewManager(ctx, id, mockTaskTable)
	require.NoError(t, err)
	m.slotManager.available.Store(16)

	// no task
	m.handleExecutableTasks(nil)

	// type not found
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), taskID).Return(&proto.Task{TaskBase: *task}, nil)
	mockTaskTable.EXPECT().FailSubtask(m.ctx, id, taskID, gomock.Any())
	m.startTaskExecutor(task)
	require.True(t, ctrl.Satisfied())

	RegisterTaskType("type",
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor {
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
	mockInternalExecutor.EXPECT().Run(gomock.Any()).DoAndReturn(func(*proto.StepResource) {
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
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor {
			return mockInternalExecutors[task.ID]
		})
	id := "test"

	m, err := NewManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)

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
	mockInternalExecutors[task1.ID].EXPECT().Run(gomock.Any())
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
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor {
			return mockInternalExecutor
		})
	id := "test"

	m, err := NewManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)
	m.slotManager.available.Store(16)

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
	task1 := &proto.TaskBase{ID: 1, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type", Concurrency: 1}

	mockInternalExecutor.EXPECT().GetTaskBase().Return(task1).AnyTimes()
	// handle pending tasks
	mockTaskTable.EXPECT().GetTaskExecInfoByExecID(m.ctx, m.id).
		Return([]*storage.TaskExecInfo{{TaskBase: task1}}, nil)
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(&proto.Task{TaskBase: *task1}, nil)
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutor.EXPECT().Run(gomock.Any()).DoAndReturn(func(_ *proto.StepResource) error {
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
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor {
			return mockInternalExecutors[task.ID]
		})
	id := "test"

	m, err := NewManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)
	m.slotManager.available.Store(10)

	var (
		task1 = &proto.TaskBase{
			ID:          1,
			State:       proto.TaskStateRunning,
			Concurrency: 10,
			Step:        proto.StepOne,
			Type:        "type",
		}
		task2 = &proto.TaskBase{
			ID:          2,
			State:       proto.TaskStateRunning,
			Concurrency: 1,
			Step:        proto.StepOne,
			Type:        "type",
		}
		task3 = &proto.TaskBase{
			ID:          3,
			State:       proto.TaskStateRunning,
			Concurrency: 1,
			Priority:    -1,
			Step:        proto.StepOne,
			Type:        "type",
		}
	)
	mockInternalExecutors[task1.ID].EXPECT().GetTaskBase().Return(task1).AnyTimes()
	mockInternalExecutors[task2.ID].EXPECT().GetTaskBase().Return(task2).AnyTimes()
	mockInternalExecutors[task3.ID].EXPECT().GetTaskBase().Return(task3).AnyTimes()

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
	mockInternalExecutors[task1.ID].EXPECT().Run(gomock.Any()).DoAndReturn(func(_ *proto.StepResource) error {
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

	// ******** Test task occupation ********
	// task1 start running
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(&proto.Task{TaskBase: *task1}, nil)
	mockInternalExecutors[task1.ID].EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutors[task1.ID].EXPECT().Run(gomock.Any()).DoAndReturn(func(_ *proto.StepResource) error {
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
	mockInternalExecutors[task2.ID].EXPECT().Run(gomock.Any()).DoAndReturn(func(_ *proto.StepResource) error {
		return <-ch
	})
	mockTaskTable.EXPECT().GetTaskByID(gomock.Any(), task3.ID).Return(&proto.Task{TaskBase: *task3}, nil)
	mockInternalExecutors[task3.ID].EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutors[task3.ID].EXPECT().Run(gomock.Any()).DoAndReturn(func(_ *proto.StepResource) error {
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

	bak := scheduler.RetrySQLTimes
	t.Cleanup(func() {
		scheduler.RetrySQLTimes = bak
	})
	scheduler.RetrySQLTimes = 1
	mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mock err"))
	require.ErrorContains(t, m.InitMeta(), "mock err")
	require.True(t, ctrl.Satisfied())

	cancel()
	mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mock err"))
	require.ErrorIs(t, m.InitMeta(), context.Canceled)
	require.True(t, ctrl.Satisfied())
}
