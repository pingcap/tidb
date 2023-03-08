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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestManageTask(t *testing.T) {
	m := NewManager(context.Background(), "test", nil, nil)
	tasks := []*proto.Task{{ID: 1}, {ID: 2}}
	new_tasks := m.filterAlreadyHandlingTasks(tasks)
	require.Equal(t, tasks, new_tasks)

	m.addHandlingTask(1)
	tasks = []*proto.Task{{ID: 1}, {ID: 2}}
	new_tasks = m.filterAlreadyHandlingTasks(tasks)
	require.Equal(t, []*proto.Task{{ID: 2}}, new_tasks)

	m.addHandlingTask(2)
	tasks = []*proto.Task{{ID: 1}, {ID: 2}}
	new_tasks = m.filterAlreadyHandlingTasks(tasks)
	require.Equal(t, []*proto.Task{}, new_tasks)

	m.removeHandlingTask(1)
	tasks = []*proto.Task{{ID: 1}, {ID: 2}}
	new_tasks = m.filterAlreadyHandlingTasks(tasks)
	require.Equal(t, []*proto.Task{{ID: 1}}, new_tasks)

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
	mockSubtaskTable := &MockSubtaskTable{}
	mockInternalScheduler := &MockInternalScheduler{}
	mockPool := &MockPool{}
	newScheduler = func(ctx context.Context, id string, taskID int64, subtaskTable SubtaskTable, pool Pool) InternalScheduler {
		return mockInternalScheduler
	}
	newPool = func(concurrency int) Pool {
		return mockPool
	}
	defer func() {
		newScheduler = NewInternalScheduler
		newPool = NewMockPool
	}()
	id := "test"
	taskID := int64(1)
	task := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: 0, Type: "type"}

	m := NewManager(context.Background(), id, mockTaskTable, mockSubtaskTable)

	// no task
	m.onRunnableTasks(context.Background(), nil)

	// unknown task type
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	m.subtaskExecutorPools["type"] = mockPool

	// get subtask failed
	mockSubtaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, errors.New("get subtask failed")).Once()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// no subtask
	mockSubtaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, nil).Once()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// pool error
	mockSubtaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockPool.On("Run", mock.Anything).Return(errors.New("pool error")).Once()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// step 0 succeed
	mockSubtaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockPool.On("Run", mock.Anything).Return(nil).Once()
	mockInternalScheduler.On("Start").Once()
	mockTaskTable.On("GetTaskByID", taskID).Return(task, nil).Once()
	mockSubtaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockInternalScheduler.On("Run", mock.Anything, task).Return(nil).Once()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// step 1 canceled
	task1 := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: 1}
	mockTaskTable.On("GetTaskByID", taskID).Return(task1, nil).Once()
	mockSubtaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockInternalScheduler.On("Run", mock.Anything, task1).Return(errors.New("run errr")).Once()

	task2 := &proto.Task{ID: taskID, State: proto.TaskStateReverting, Step: 1}
	mockTaskTable.On("GetTaskByID", taskID).Return(task2, nil).Once()
	mockSubtaskTable.On("HasSubtasksInStates", id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Once()
	mockInternalScheduler.On("Rollback", mock.Anything, task2).Return(nil).Once()

	task3 := &proto.Task{ID: taskID, State: proto.TaskStateReverted, Step: 1}
	mockTaskTable.On("GetTaskByID", taskID).Return(task3, nil).Once()
	mockInternalScheduler.On("Stop").Return(nil).Once()

	time.Sleep(5 * time.Second)
	mockTaskTable.AssertExpectations(t)
	mockSubtaskTable.AssertExpectations(t)
	mockInternalScheduler.AssertExpectations(t)
	mockPool.AssertExpectations(t)
}
