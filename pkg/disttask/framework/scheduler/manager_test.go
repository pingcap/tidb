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

	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/spool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var unfinishedSubtaskStates = []interface{}{
	proto.TaskStatePending, proto.TaskStateRevertPending,
	proto.TaskStateRunning, proto.TaskStateReverting,
}

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
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
	mockTaskTable.EXPECT().PauseSubtasks("test", int64(1)).Return(nil)
	m.onPausingTasks([]*proto.Task{{ID: 1}})
	require.Equal(t, context.Canceled, ctx4.Err())
}

func TestOnRunnableTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalScheduler := mock.NewMockScheduler(ctrl)
	mockPool := mock.NewMockPool(ctrl)

	b := NewManagerBuilder()
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

	RegisterTaskType("type",
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) Scheduler {
			return mockInternalScheduler
		})

	// get subtask failed
	mockInternalScheduler.EXPECT().Init(gomock.Any()).Return(nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		unfinishedSubtaskStates...).
		Return(false, errors.New("get subtask failed"))
	mockInternalScheduler.EXPECT().Close()
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// no subtask
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		unfinishedSubtaskStates...).Return(false, nil)
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// pool error
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		unfinishedSubtaskStates...).Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).Return(errors.New("pool error"))
	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	// StepOne succeed
	wg, runFn := getPoolRunFn()
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		unfinishedSubtaskStates...).Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepOne,
		unfinishedSubtaskStates...).Return(true, nil)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task).Return(nil)

	// StepTwo failed
	task1 := &proto.Task{ID: taskID, State: proto.TaskStateRunning, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task1, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepTwo,
		unfinishedSubtaskStates...).Return(true, nil)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task1).Return(errors.New("run err"))

	task2 := &proto.Task{ID: taskID, State: proto.TaskStateReverting, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task2, nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID, proto.StepTwo,
		unfinishedSubtaskStates...).Return(true, nil)
	mockInternalScheduler.EXPECT().Rollback(gomock.Any(), task2).Return(nil)

	task3 := &proto.Task{ID: taskID, State: proto.TaskStateReverted, Step: proto.StepTwo}
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID).Return(task3, nil)

	m.onRunnableTasks(context.Background(), []*proto.Task{task})

	wg.Wait()
}

func TestManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	mockInternalScheduler := mock.NewMockScheduler(ctrl)
	mockPool := mock.NewMockPool(ctrl)
	b := NewManagerBuilder()
	b.setPoolFactory(func(name string, size int32, component util.Component, options ...spool.Option) (Pool, error) {
		return mockPool, nil
	})
	RegisterTaskType("type",
		func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) Scheduler {
			return mockInternalScheduler
		})
	id := "test"
	taskID1 := int64(1)
	taskID2 := int64(2)
	taskID3 := int64(3)
	task1 := &proto.Task{ID: taskID1, State: proto.TaskStateRunning, Step: proto.StepOne, Type: "type"}
	task2 := &proto.Task{ID: taskID2, State: proto.TaskStateReverting, Step: proto.StepOne, Type: "type"}
	task3 := &proto.Task{ID: taskID3, State: proto.TaskStatePausing, Step: proto.StepOne, Type: "type"}

	mockTaskTable.EXPECT().StartManager("test", "").Return(nil).Times(1)
	mockTaskTable.EXPECT().GetGlobalTasksInStates(proto.TaskStateRunning, proto.TaskStateReverting).
		Return([]*proto.Task{task1, task2}, nil).AnyTimes()
	mockTaskTable.EXPECT().GetGlobalTasksInStates(proto.TaskStateReverting).
		Return([]*proto.Task{task2}, nil).AnyTimes()
	mockTaskTable.EXPECT().GetGlobalTasksInStates(proto.TaskStatePausing).
		Return([]*proto.Task{task3}, nil).AnyTimes()
	mockInternalScheduler.EXPECT().Init(gomock.Any()).Return(nil)
	// task1
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, proto.StepOne,
		unfinishedSubtaskStates...).
		Return(true, nil)
	wg, runFn := getPoolRunFn()
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID1).Return(task1, nil).AnyTimes()
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, proto.StepOne,
		unfinishedSubtaskStates...).
		Return(true, nil)
	mockInternalScheduler.EXPECT().Run(gomock.Any(), task1).Return(nil)

	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID1, proto.StepOne,
		unfinishedSubtaskStates...).
		Return(false, nil).AnyTimes()
	mockInternalScheduler.EXPECT().Close()
	// task2
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, proto.StepOne,
		unfinishedSubtaskStates...).
		Return(true, nil)
	mockPool.EXPECT().Run(gomock.Any()).DoAndReturn(runFn)
	mockTaskTable.EXPECT().GetGlobalTaskByID(taskID2).Return(task2, nil).AnyTimes()
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, proto.StepOne,
		unfinishedSubtaskStates...).
		Return(true, nil)
	mockInternalScheduler.EXPECT().Init(gomock.Any()).Return(nil)
	mockInternalScheduler.EXPECT().Rollback(gomock.Any(), task2).Return(nil)
	mockTaskTable.EXPECT().HasSubtasksInStates(id, taskID2, proto.StepOne,
		unfinishedSubtaskStates...).
		Return(false, nil).AnyTimes()
	mockInternalScheduler.EXPECT().Close()
	// task3
	mockTaskTable.EXPECT().PauseSubtasks(id, taskID3).Return(nil).AnyTimes()

	// for scheduler pool
	mockPool.EXPECT().ReleaseAndWait().Do(func() {
		wg.Wait()
	})
	m, err := b.BuildManager(context.Background(), id, mockTaskTable)
	require.NoError(t, err)
	require.NoError(t, m.Start())
	time.Sleep(5 * time.Second)
	m.Stop()
}
