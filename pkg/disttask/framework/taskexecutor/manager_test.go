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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func getPoolRunFn() (*sync.WaitGroup, func(f func()) error) {
	wg := &sync.WaitGroup{}
	return wg, func(f func()) error {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
		return nil
	}
}

func TestManageTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	b := NewManagerBuilder()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	m, err := b.BuildManager(context.Background(), "test", mockTaskTable)
	require.NoError(t, err)
	tasks := []*proto.Task{{ID: 1}, {ID: 2}}
	newTasks := m.filterAlreadyHandlingTasks(tasks)
	require.Equal(t, tasks, newTasks)

	m.addHandlingTask(1)
	tasks = []*proto.Task{{ID: 1}, {ID: 2}}
	newTasks = m.filterAlreadyHandlingTasks(tasks)
	require.Equal(t, []*proto.Task{{ID: 2}}, newTasks)

	m.addHandlingTask(2)
	tasks = []*proto.Task{{ID: 1}, {ID: 2}}
	newTasks = m.filterAlreadyHandlingTasks(tasks)
	require.Equal(t, []*proto.Task{}, newTasks)

	m.removeHandlingTask(1)
	tasks = []*proto.Task{{ID: 1}, {ID: 2}}
	newTasks = m.filterAlreadyHandlingTasks(tasks)
	require.Equal(t, []*proto.Task{{ID: 1}}, newTasks)

	ctx1, cancel1 := context.WithCancelCause(context.Background())
	m.registerCancelFunc(2, cancel1)
	m.cancelAllRunningTasks()
	require.Equal(t, context.Canceled, ctx1.Err())

	m.addHandlingTask(2)
	ctx1, cancel1 = context.WithCancelCause(context.Background())
	m.registerCancelFunc(2, cancel1)
	m.cancelTaskExecutors([]*proto.Task{{ID: 2}})
	require.Equal(t, context.Canceled, ctx1.Err())

	// test cancel.
	m.addHandlingTask(1)
	ctx2, cancel2 := context.WithCancelCause(context.Background())
	m.registerCancelFunc(1, cancel2)
	ctx3, cancel3 := context.WithCancelCause(context.Background())
	m.registerCancelFunc(2, cancel3)
	m.onCanceledTasks(context.Background(), []*proto.Task{{ID: 1}})
	require.Equal(t, context.Canceled, ctx2.Err())
	require.NoError(t, ctx3.Err())

	// test pause.
	m.addHandlingTask(3)
	ctx4, cancel4 := context.WithCancelCause(context.Background())
	m.registerCancelFunc(1, cancel4)
	mockTaskTable.EXPECT().PauseSubtasks(m.ctx, "test", int64(1)).Return(nil)
	require.NoError(t, m.onPausingTasks([]*proto.Task{{ID: 1}}))
	require.Equal(t, context.Canceled, ctx4.Err())
}

func TestOnRunnableTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalExecutor := mock.NewMockTaskExecutor(ctrl)
	mockPool := mock.NewMockPool(ctrl)
	ctx := context.Background()

	b := NewManagerBuilder()
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	id := "test"
	taskID := int64(1)
	task := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type"}

	m, err := b.BuildManager(ctx, id, mockTaskTable)
	require.NoError(t, err)

	// no task
	m.onRunnableTasks(nil)

	// type not found
	mockTaskTable.EXPECT().UpdateErrorToSubtask(m.ctx, id, taskID, gomock.Any())
	m.onRunnableTask(task)

	RegisterTaskType("type",
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor {
			return mockInternalExecutor
		})

	// executor init failed non retryable
	executorErr := errors.New("executor init failed")
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(executorErr)
	mockInternalExecutor.EXPECT().IsRetryableError(executorErr).Return(false)
	mockTaskTable.EXPECT().UpdateErrorToSubtask(m.ctx, id, taskID, executorErr)
	m.onRunnableTask(task)
	m.removeHandlingTask(taskID)
	require.Equal(t, true, ctrl.Satisfied())

	// executor init failed retryable
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(executorErr)
	mockInternalExecutor.EXPECT().IsRetryableError(executorErr).Return(true)
	m.onRunnableTask(task)
	m.removeHandlingTask(taskID)

	// get subtask failed
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID, proto.StepOne,
		unfinishedSubtaskStates).
		Return(false, errors.New("get subtask failed"))
	mockInternalExecutor.EXPECT().Close()
	m.onRunnableTasks([]*proto.Task{task})

	// no subtask
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID, proto.StepOne,
		unfinishedSubtaskStates).Return(false, nil)
	m.onRunnableTasks([]*proto.Task{task})

	// pool error
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID, proto.StepOne,
		unfinishedSubtaskStates).Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).Return(errors.New("pool error"))
	m.onRunnableTasks([]*proto.Task{task})

	// StepOne succeed
	wg, runFn := getPoolRunFn()
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID, proto.StepOne,
		unfinishedSubtaskStates).Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID).Return(task, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID, proto.StepOne,
		unfinishedSubtaskStates).Return(true, nil)
	mockInternalExecutor.EXPECT().Run(gomock.Any(), task).Return(nil)

	// StepTwo failed
	task1 := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID).Return(task1, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID, proto.StepTwo,
		unfinishedSubtaskStates).Return(true, nil)
	mockInternalExecutor.EXPECT().Run(gomock.Any(), task1).Return(errors.New("run err"))

	task2 := &proto.Task{ID: taskID, State: proto.TaskStateReverting, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID).Return(task2, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID, proto.StepTwo,
		unfinishedSubtaskStates).Return(true, nil)
	mockInternalExecutor.EXPECT().Rollback(gomock.Any(), task2).Return(nil)

	task3 := &proto.Task{ID: taskID, State: proto.TaskStateReverted, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID).Return(task3, nil)

	m.onRunnableTasks([]*proto.Task{task})

	wg.Wait()
}

func TestManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalExecutor := mock.NewMockTaskExecutor(ctrl)
	mockPool := mock.NewMockPool(ctrl)
	b := NewManagerBuilder()
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	RegisterTaskType("type",
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor {
			return mockInternalExecutor
		})
	id := "test"

	m, err := b.BuildManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)

	taskID1 := int64(1)
	taskID2 := int64(2)
	taskID3 := int64(3)
	task1 := &proto.Task{ID: taskID1, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type"}
	task2 := &proto.Task{ID: taskID2, State: proto.TaskStateReverting, Step: proto.StepOne, Type: "type"}
	task3 := &proto.Task{ID: taskID3, State: proto.TaskStatePausing, Step: proto.StepOne, Type: "type"}

	mockTaskTable.EXPECT().InitMeta(m.ctx, "test", "").Return(nil).Times(1)
	mockTaskTable.EXPECT().GetTasksInStates(m.ctx, proto.TaskStateRunning, proto.TaskStateReverting).
		Return([]*proto.Task{task1, task2}, nil).AnyTimes()
	mockTaskTable.EXPECT().GetTasksInStates(m.ctx, proto.TaskStateReverting).
		Return([]*proto.Task{task2}, nil).AnyTimes()
	mockTaskTable.EXPECT().GetTasksInStates(m.ctx, proto.TaskStatePausing).
		Return([]*proto.Task{task3}, nil).AnyTimes()
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	// task1
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	wg, runFn := getPoolRunFn()
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID1).Return(task1, nil).AnyTimes()
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockInternalExecutor.EXPECT().Run(gomock.Any(), task1).Return(nil)

	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(false, nil).AnyTimes()
	mockInternalExecutor.EXPECT().Close()
	// task2
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID2, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID2).Return(task2, nil).AnyTimes()
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID2, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalExecutor.EXPECT().Rollback(gomock.Any(), task2).Return(nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID2, proto.StepOne,
		unfinishedSubtaskStates).
		Return(false, nil).AnyTimes()
	mockInternalExecutor.EXPECT().Close()
	// task3
	mockTaskTable.EXPECT().PauseSubtasks(m.ctx, id, taskID3).Return(nil).AnyTimes()

	// for taskExecutor pool
	mockPool.EXPECT().ReleaseAndWait().Do(func() {
		wg.Wait()
	})

	require.NoError(t, m.InitMeta())
	require.NoError(t, m.Start())
	time.Sleep(5 * time.Second)
	m.Stop()
}

func TestSlotManagerInManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalExecutor := mock.NewMockTaskExecutor(ctrl)
	mockPool := mock.NewMockPool(ctrl)
	b := NewManagerBuilder()
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	RegisterTaskType("type",
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor {
			return mockInternalExecutor
		})
	id := "test"

	m, err := b.BuildManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)
	m.slotManager.available = 10

	var (
		taskID1 = int64(1)
		taskID2 = int64(2)

		task1 = &proto.Task{
			ID:          taskID1,
			State:       proto.TaskStateRunning,
			Concurrency: 10,
			Step:        proto.StepOne,
			Type:        "type",
		}
		task2 = &proto.Task{
			ID:          taskID2,
			State:       proto.TaskStateRunning,
			Concurrency: 1,
			Step:        proto.StepOne,
			Type:        "type",
		}
	)

	ch := make(chan error)
	defer close(ch)
	wg, runFn := getPoolRunFn()

	// ******** Test task1 alloc success ********
	// 1. task1 alloc success
	// 2. task2 alloc failed
	// 3. task1 run success
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID2, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)

	// mock inside onRunnableTask
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID1).Return(task1, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	// task1 start running
	mockInternalExecutor.EXPECT().Run(gomock.Any(), task1).DoAndReturn(func(_ context.Context, _ *proto.Task) error {
		return <-ch
	})

	m.onRunnableTasks([]*proto.Task{task1, task2})
	// task1 alloc resource success
	require.Eventually(t, func() bool {
		if m.slotManager.available != 0 || len(m.slotManager.executorTasks) != 1 ||
			m.slotManager.executorTasks[0].ID != task1.ID {
			return false
		}
		return ctrl.Satisfied()
	}, 2*time.Second, 300*time.Millisecond)
	ch <- nil

	// task1 succeed
	task1.State = proto.TaskStateSucceed
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID1).Return(task1, nil)
	mockInternalExecutor.EXPECT().Close()
	wg.Wait()
	require.Equal(t, 10, m.slotManager.available)
	require.Equal(t, 0, len(m.slotManager.executorTasks))
	require.True(t, ctrl.Satisfied())

	// ******** Test task occupation ********
	task1.State = proto.TaskStateRunning
	var (
		taskID3 = int64(3)
		task3   = &proto.Task{
			ID:          taskID3,
			State:       proto.TaskStateRunning,
			Concurrency: 1,
			Priority:    -1,
			Step:        proto.StepOne,
			Type:        "type",
		}
	)
	// 1. task1 alloc success
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID2, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)

	// mock inside onRunnableTask
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID1).Return(task1, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	// task1 start running
	mockInternalExecutor.EXPECT().Run(gomock.Any(), task1).DoAndReturn(func(_ context.Context, _ *proto.Task) error {
		return <-ch
	})

	m.onRunnableTasks([]*proto.Task{task1, task2})
	// task1 alloc resource success
	require.Eventually(t, func() bool {
		if m.slotManager.available != 0 || len(m.slotManager.executorTasks) != 1 ||
			m.slotManager.executorTasks[0].ID != task1.ID {
			return false
		}
		return ctrl.Satisfied()
	}, 2*time.Second, 300*time.Millisecond)

	// 2. task1 is preempted by task3, task1 start to pausing
	// 3. task3 is waiting for task1 to be released, and task2 can't be allocated
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID3, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)

	// the priority of task3 is higher than task2, so task3 is in front of task2
	m.onRunnableTasks([]*proto.Task{task3, task2})
	require.Equal(t, 0, m.slotManager.available)
	require.Equal(t, []*proto.Task{task1}, m.slotManager.executorTasks)
	require.True(t, ctrl.Satisfied())

	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID1).Return(task1, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockInternalExecutor.EXPECT().Close()

	// 4. task1 is released, task3 alloc success, start to run
	ch <- context.Canceled
	wg.Wait()
	require.Equal(t, 10, m.slotManager.available)
	require.Len(t, m.slotManager.executorTasks, 0)
	require.True(t, ctrl.Satisfied())

	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID3, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID1, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID2, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)

	// mock inside onRunnableTask
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID3).Return(task3, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID3, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockInternalExecutor.EXPECT().Run(gomock.Any(), task3).DoAndReturn(func(_ context.Context, _ *proto.Task) error {
		return <-ch
	})

	// 5. available is enough, task2 alloc success,
	mockInternalExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID2).Return(task2, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(m.ctx, id, taskID2, proto.StepOne,
		unfinishedSubtaskStates).
		Return(true, nil)
	mockInternalExecutor.EXPECT().Run(gomock.Any(), task2).DoAndReturn(func(_ context.Context, _ *proto.Task) error {
		return <-ch
	})

	m.onRunnableTasks([]*proto.Task{task3, task1, task2})
	time.Sleep(2 * time.Second)
	require.Eventually(t, func() bool {
		if m.slotManager.available != 8 || len(m.slotManager.executorTasks) != 2 {
			return false
		}
		return ctrl.Satisfied()
	}, 2*time.Second, 300*time.Millisecond)

	// 6. task3/task2 run success
	task3.State = proto.TaskStateSucceed
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID3).Return(task3, nil)
	mockInternalExecutor.EXPECT().Close()
	task2.State = proto.TaskStateSucceed
	mockTaskTable.EXPECT().GetTaskByID(m.ctx, taskID2).Return(task2, nil)
	mockInternalExecutor.EXPECT().Close()
	ch <- nil
	ch <- nil
	wg.Wait()
	require.Equal(t, 10, m.slotManager.available)
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
		logCtx:    ctx,
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

	bak := retrySQLTimes
	t.Cleanup(func() {
		retrySQLTimes = bak
	})
	retrySQLTimes = 1
	mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mock err"))
	require.ErrorContains(t, m.InitMeta(), "mock err")
	require.True(t, ctrl.Satisfied())

	cancel()
	mockTaskTable.EXPECT().InitMeta(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("mock err"))
	require.ErrorIs(t, m.InitMeta(), context.Canceled)
	require.True(t, ctrl.Satisfied())
}
