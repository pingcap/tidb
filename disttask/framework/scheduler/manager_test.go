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
	"testing"
	"time"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestManageTask(t *testing.T) {
	b := NewManagerBuilder()
	m, err := b.BuildManager(context.Background(), "test", nil, "mock")
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
	mockTaskTable := &MockTaskTable{}
	mockInternalScheduler := &MockInternalScheduler{}
	mockPool := &MockPool{}

	b := NewManagerBuilder()
	b.setSchedulerFactory(func(ctx context.Context, id string, ddlid string, taskID int64, taskTable TaskTable, pool Pool) InternalScheduler {
		return mockInternalScheduler
	})
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	id := "test"
	taskID := int64(1)
	task := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: 0, Type: "type"}

	m, err := b.BuildManager(context.Background(), id, mockTaskTable, "mock")
	require.NoError(t, err)

	// no task
	m.onRunnableTasks(context.Background(), nil)

	// unknown task type
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	m.subtaskExecutorPools["type"] = mockPool

	// get subtask failed
	mockTaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, errors.New("get subtask failed")).Once()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// no subtask
	mockTaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, nil).Once()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// pool error
	mockTaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockPool.On("Run", mock.Anything).Return(errors.New("pool error")).Once()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// step 0 succeed
	mockTaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockPool.On("Run", mock.Anything).Return(nil).Once()
	mockInternalScheduler.On("Start").Once()
	mockTaskTable.On("GetGlobalTaskByID", taskID).Return(task, nil).Once()
	mockTaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockInternalScheduler.On("Run", mock.Anything, task).Return(nil).Once()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// step 1 canceled
	task1 := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: 1}
	mockTaskTable.On("GetGlobalTaskByID", taskID).Return(task1, nil).Once()
	mockTaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockInternalScheduler.On("Run", mock.Anything, task1).Return(errors.New("run errr")).Once()

	task2 := &proto.Task{ID: taskID, State: proto.TaskStateReverting, Step: 1}
	mockTaskTable.On("GetGlobalTaskByID", taskID).Return(task2, nil).Once()
	mockTaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockInternalScheduler.On("Rollback", mock.Anything, task2).Return(nil).Once()

	task3 := &proto.Task{ID: taskID, State: proto.TaskStateReverted, Step: 1}
	mockTaskTable.On("GetGlobalTaskByID", taskID).Return(task3, nil).Once()
	mockInternalScheduler.On("Stop").Return(nil).Once()

	time.Sleep(5 * time.Second)
	mockTaskTable.AssertExpectations(t)
	mockTaskTable.AssertExpectations(t)
	mockInternalScheduler.AssertExpectations(t)
	mockPool.AssertExpectations(t)
}

func TestManager(t *testing.T) {
	// TODO(gmhdbjd): use real subtask table instead of mock
	mockTaskTable := &MockTaskTable{}
	mockInternalScheduler := &MockInternalScheduler{}
	mockPool := &MockPool{}
	b := NewManagerBuilder()
	b.setSchedulerFactory(func(ctx context.Context, id string, ddlid string, taskID int64, taskTable TaskTable, pool Pool) InternalScheduler {
		return mockInternalScheduler
	})
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	RegisterSubtaskExectorConstructor("type", func(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error) {
		return &MockSubtaskExecutor{}, nil
	}, func(opts *subtaskExecutorRegisterOptions) {
		opts.PoolSize = 1
	})
	id := "test"
	taskID1 := int64(1)
	taskID2 := int64(2)
	task1 := &proto.Task{ID: taskID1, State: proto.TaskStateRunning, Step: 0, Type: "type"}
	task2 := &proto.Task{ID: taskID2, State: proto.TaskStateReverting, Step: 0, Type: "type"}

	mockTaskTable.On("GetGlobalTasksInStates", proto.TaskStateRunning, proto.TaskStateReverting).Return([]*proto.Task{task1, task2}, nil)
	mockTaskTable.On("GetGlobalTasksInStates", proto.TaskStateReverting).Return([]*proto.Task{task2}, nil)
	// task1
	mockTaskTable.On("HasSubtasksInStates", id, taskID1, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockPool.On("Run", mock.Anything).Return(nil).Once()
	mockInternalScheduler.On("Start").Once()
	mockTaskTable.On("GetGlobalTaskByID", taskID1).Return(task1, nil)
	mockTaskTable.On("HasSubtasksInStates", id, taskID1, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockInternalScheduler.On("Run", mock.Anything, task1).Return(nil).Once()
	mockTaskTable.On("HasSubtasksInStates", id, taskID1, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, nil)
	mockInternalScheduler.On("Stop").Once()
	// task2
	mockTaskTable.On("HasSubtasksInStates", id, taskID2, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockPool.On("Run", mock.Anything).Return(nil).Once()
	mockInternalScheduler.On("Start").Once()
	mockTaskTable.On("GetGlobalTaskByID", taskID2).Return(task2, nil)
	mockTaskTable.On("HasSubtasksInStates", id, taskID2, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockInternalScheduler.On("Rollback", mock.Anything, task2).Return(nil).Once()
	mockTaskTable.On("HasSubtasksInStates", id, taskID2, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, nil)
	mockInternalScheduler.On("Stop").Once()
	mockPool.On("ReleaseAndWait").Twice()
	RegisterSubtaskExectorConstructor("type", func(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error) {
		return &MockSubtaskExecutor{}, nil
	}, func(opts *subtaskExecutorRegisterOptions) {
		opts.PoolSize = 1
	})
	m, err := b.BuildManager(context.Background(), id, mockTaskTable, "mock")
	require.NoError(t, err)
	m.Start()
	time.Sleep(5 * time.Second)
	m.Stop()
	time.Sleep(5 * time.Second)
	mockTaskTable.AssertExpectations(t)
	mockTaskTable.AssertExpectations(t)
	mockInternalScheduler.AssertExpectations(t)
	mockPool.AssertExpectations(t)
}
