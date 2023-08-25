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

package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/disttask/framework/mock"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/resourcemanager/util"
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
	b := NewManagerBuilder()
	m, err := b.BuildManager(context.Background(), "test", nil)
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

	ctx1, cancel1 := context.WithCancel(context.Background())
	m.registerCancelFunc(2, cancel1)
	m.cancelAllRunningTasks()
	require.Equal(t, context.Canceled, ctx1.Err())

	m.addHandlingTask(1)
	ctx2, cancel2 := context.WithCancel(context.Background())
	m.registerCancelFunc(1, cancel2)
	ctx3, cancel3 := context.WithCancel(context.Background())
	m.registerCancelFunc(2, cancel3)
	m.onCanceledTasks(context.Background(), []*proto.Task{{ID: 1}})
	require.Equal(t, context.Canceled, ctx2.Err())
	require.NoError(t, ctx3.Err())
}

func TestOnRunnableTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalScheduler := mock.NewMockInternalScheduler(ctrl)
	mockPool := mock.NewMockPool(ctrl)

	b := NewManagerBuilder()
	b.setSchedulerFactory(func(ctx context.Context, id string, taskID int64, taskTable TaskTable, pool Pool) InternalScheduler {
		return mockInternalScheduler
	})
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	id := "test"
	taskID := int64(1)
	task := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type"}

	m, err := b.BuildManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)

	// no task
	m.onRunnableTasks(context.Background(), nil)

	// unknown task type
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	m.subtaskExecutorPools["type"] = mockPool

	// get subtask failed
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).
		Return(false, errors.New("get subtask failed"))
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// no subtask
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, nil)
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// pool error
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).Return(errors.New("pool error"))
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// step 0 succeed
	wg, runFn := getPoolRunFn()
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockInternalScheduler.EXPECT().Start()
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task).Return(nil)

	// step 1 canceled
	task1 := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task1, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepTwo,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task1).Return(errors.New("run err"))

	task2 := &proto.Task{ID: taskID, State: proto.TaskStateReverting, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task2, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepTwo,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil)
	mockInternalScheduler.EXPECT().Rollback(gomock.Any(), task2).Return(nil)

	task3 := &proto.Task{ID: taskID, State: proto.TaskStateReverted, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task3, nil)
	mockInternalScheduler.EXPECT().Stop()

	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	wg.Wait()
}

func TestManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalScheduler := mock.NewMockInternalScheduler(ctrl)
	mockPool := mock.NewMockPool(ctrl)
	b := NewManagerBuilder()
	b.setSchedulerFactory(func(ctx context.Context, id string, taskID int64, taskTable TaskTable, pool Pool) InternalScheduler {
		return mockInternalScheduler
	})
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	RegisterTaskType("type", WithPoolSize(1))
	RegisterSubtaskExectorConstructor("type", proto.StepOne, func(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error) {
		return mock.NewMockSubtaskExecutor(ctrl), nil
	})
	id := "test"
	taskID1 := int64(1)
	taskID2 := int64(2)
	task1 := &proto.Task{ID: taskID1, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type"}
	task2 := &proto.Task{ID: taskID2, State: proto.TaskStateReverting, Step: proto.StepOne, Type: "type"}

	mockTaskTable.EXPECT().GetGlobalTasksInStates(proto.TaskStateRunning, proto.TaskStateReverting).
		Return([]*proto.Task{task1, task2}, nil).AnyTimes()
	mockTaskTable.EXPECT().GetGlobalTasksInStates(proto.TaskStateReverting).
		Return([]*proto.Task{task2}, nil).AnyTimes()
	// task1
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).
		Return(true, nil)
	wg, runFn := getPoolRunFn()
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockInternalScheduler.EXPECT().Start()
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID1).Return(task1, nil).AnyTimes()
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).
		Return(true, nil)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task1).Return(nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).
		Return(false, nil).AnyTimes()
	mockInternalScheduler.EXPECT().Stop()
	// task2
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).
		Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockInternalScheduler.EXPECT().Start()
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID2).Return(task2, nil).AnyTimes()
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).
		Return(true, nil)
	mockInternalScheduler.EXPECT().Rollback(gomock.Any(), task2).Return(nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).
		Return(false, nil).AnyTimes()
	mockInternalScheduler.EXPECT().Stop()
	// once for scheduler pool, once for subtask pool
	mockPool.EXPECT().ReleaseAndWait().Do(func() {
		wg.Wait()
	}).Times(2)
	m, err := b.BuildManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)
	m.Start()
	time.Sleep(5 * time.Second)
	m.Stop()
}
