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

	gomock "github.com/golang/mock/gomock"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/stretchr/testify/require"
)

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
	mockTaskTable := &MockTaskTable{}
	mockInternalScheduler := &MockInternalScheduler{}
	mockPool := &MockPool{}

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
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, errors.New("get subtask failed")).Times(1)
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// no subtask
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, nil).Times(1)
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// pool error
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockPool.EXPECT().Run(gomock.Any()).Return(errors.New("pool error")).Times(1)
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// step 0 succeed
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockPool.EXPECT().Run(gomock.Any()).Return(nil).Times(1)
	mockInternalScheduler.EXPECT().Start().Times(1)
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task, nil).Times(1)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task).Return(nil).Times(1)
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// step 1 canceled
	task1 := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task1, nil).Times(1)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task1).Return(errors.New("run err")).Times(1)

	task2 := &proto.Task{ID: taskID, State: proto.TaskStateReverting, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task2, nil).Times(1)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockInternalScheduler.EXPECT().Rollback(gomock.Any(), task2).Return(nil).Times(1)

	task3 := &proto.Task{ID: taskID, State: proto.TaskStateReverted, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task3, nil).Times(1)
	mockInternalScheduler.EXPECT().Stop().Times(1)

	time.Sleep(5 * time.Second)
}

func TestManager(t *testing.T) {
	// TODO(gmhdbjd): use real subtask table instead of mock
	mockTaskTable := &MockTaskTable{}
	mockInternalScheduler := &MockInternalScheduler{}
	mockPool := &MockPool{}
	b := NewManagerBuilder()
	b.setSchedulerFactory(func(ctx context.Context, id string, taskID int64, taskTable TaskTable, pool Pool) InternalScheduler {
		return mockInternalScheduler
	})
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	RegisterTaskType("type", WithPoolSize(1))
	RegisterSubtaskExectorConstructor("type", proto.StepOne, func(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error) {
		return &MockSubtaskExecutor{}, nil
	})
	id := "test"
	taskID1 := int64(1)
	taskID2 := int64(2)
	task1 := &proto.Task{ID: taskID1, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type"}
	task2 := &proto.Task{ID: taskID2, State: proto.TaskStateReverting, Step: proto.StepOne, Type: "type"}

	mockTaskTable.EXPECT().GetGlobalTasksInStates(proto.TaskStateRunning, proto.TaskStateReverting).Return([]*proto.Task{task1, task2}, nil).Times(1)
	mockTaskTable.EXPECT().GetGlobalTasksInStates(proto.TaskStateReverting).Return([]*proto.Task{task2}, nil).Times(1)
	// task1
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockPool.EXPECT().Run(gomock.Any()).Return(nil).Times(1)
	mockInternalScheduler.EXPECT().Start().Times(1)
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID1).Return(task1, nil).Times(1)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task1).Return(nil).Times(1)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, nil).Times(1)
	mockInternalScheduler.EXPECT().Stop().Times(1)
	// task2
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockPool.EXPECT().Run(gomock.Any()).Return(nil).Times(1)
	mockInternalScheduler.EXPECT().Start().Times(1)
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID2).Return(task2, nil).Times(1)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(true, nil).Times(1)
	mockInternalScheduler.EXPECT().Rollback(gomock.Any(), task2).Return(nil).Times(1)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, []interface{}{proto.TaskStatePending, proto.TaskStateRevertPending}).Return(false, nil).Times(1)
	mockInternalScheduler.EXPECT().Stop().Times(1)
	// once for scheduler pool, once for subtask pool
	mockPool.EXPECT().ReleaseAndWait().Times(2)
	m, err := b.BuildManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)
	m.Start()
	time.Sleep(5 * time.Second)
	m.Stop()
	time.Sleep(5 * time.Second)
}
