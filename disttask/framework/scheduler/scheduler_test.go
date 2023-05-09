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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSchedulerRun(t *testing.T) {
	tp := "test_scheduler_run"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	mockSubtaskTable := &MockTaskTable{}
	mockPool := &MockPool{}
	mockScheduler := &MockScheduler{}
	mockSubtaskExecutor := &MockSubtaskExecutor{}

	// 1. no scheduler constructor
	schedulerRegisterErr := errors.Errorf("constructor of scheduler for type %s not found", tp)
	scheduler := NewInternalScheduler(ctx, "id", 1, mockSubtaskTable, mockPool)
	err := scheduler.Run(runCtx, &proto.Task{Type: tp})
	require.EqualError(t, err, schedulerRegisterErr.Error())

	RegisterSchedulerConstructor(tp, func(task []byte, step int64) (Scheduler, error) {
		return mockScheduler, nil
	})

	// 2. init subtask exec env failed
	initErr := errors.New("init error")
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(initErr).Once()
	err = scheduler.Run(runCtx, &proto.Task{Type: tp})
	require.EqualError(t, err, initErr.Error())

	// 3. pool error
	poolErr := errors.New("pool error")
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(poolErr).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(nil).Once()
	err = scheduler.Run(runCtx, &proto.Task{Type: tp})
	require.EqualError(t, err, poolErr.Error())

	// 4. get subtask failed
	getSubtaskErr := errors.New("get subtask error")
	cleanupErr := errors.New("clean up error")
	var taskID int64 = 1
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(nil, getSubtaskErr).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(cleanupErr).Once()
	err = scheduler.Run(runCtx, &proto.Task{Type: tp, ID: taskID})
	require.EqualError(t, err, getSubtaskErr.Error())

	// 5. update subtask state failed
	updateSubtaskErr := errors.New("update subtask error")
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateRunning).Return(updateSubtaskErr).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(nil).Once()
	err = scheduler.Run(runCtx, &proto.Task{Type: tp, ID: taskID})
	require.EqualError(t, err, updateSubtaskErr.Error())

	// 6. no subtask executor constructor
	subtaskExecutorRegisterErr := errors.Errorf("constructor of subtask executor for type %s not found", tp)
	var concurrency uint64 = 10
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateRunning).Return(nil).Once()
	mockScheduler.On("SplitSubtask", mock.Anything, mock.Anything).Return([]proto.MinimalTask{MockMinimalTask{}}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateFailed).Return(nil).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(nil).Once()
	err = scheduler.Run(runCtx, &proto.Task{Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, subtaskExecutorRegisterErr.Error())

	RegisterSubtaskExectorConstructor(tp, func(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error) {
		return mockSubtaskExecutor, nil
	})

	// 7. run subtask failed
	runSubtaskErr := errors.New("run subtask error")
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateRunning).Return(nil).Once()
	mockScheduler.On("SplitSubtask", mock.Anything, mock.Anything).Return([]proto.MinimalTask{MockMinimalTask{}}, nil).Once()
	mockSubtaskExecutor.On("Run", mock.Anything).Return(runSubtaskErr).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateFailed).Return(nil).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(nil).Once()
	err = scheduler.Run(runCtx, &proto.Task{Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, runSubtaskErr.Error())

	// 8. run subtask success
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateRunning).Return(nil).Once()
	mockScheduler.On("SplitSubtask", mock.Anything, mock.Anything).Return([]proto.MinimalTask{MockMinimalTask{}}, nil).Once()
	mockSubtaskExecutor.On("Run", mock.Anything).Return(nil).Once()
	mockScheduler.On("OnSubtaskFinished", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateSucceed).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(nil, nil).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(nil).Once()
	err = scheduler.Run(runCtx, &proto.Task{Type: tp, ID: taskID, Concurrency: concurrency})
	require.NoError(t, err)

	mockSubtaskTable.AssertExpectations(t)
	mockScheduler.AssertExpectations(t)
	mockSubtaskExecutor.AssertExpectations(t)
	mockPool.AssertExpectations(t)

	// 9. run subtask concurrently
	RegisterSchedulerConstructor(tp, func(task []byte, step int64) (Scheduler, error) {
		return mockScheduler, nil
	}, WithConcurrentSubtask())
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", int64(1), proto.TaskStateRunning).Return(nil).Once()
	mockScheduler.On("SplitSubtask", mock.Anything, mock.Anything).Return([]proto.MinimalTask{MockMinimalTask{}}, nil).Once()
	mockSubtaskExecutor.On("Run", mock.Anything).Return(nil).Once()
	mockScheduler.On("OnSubtaskFinished", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", int64(1), proto.TaskStateSucceed).Return(nil).Once()

	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 2, Type: tp}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", int64(2), proto.TaskStateRunning).Return(nil).Once()
	mockScheduler.On("SplitSubtask", mock.Anything, mock.Anything).Return([]proto.MinimalTask{MockMinimalTask{}}, nil).Once()
	mockSubtaskExecutor.On("Run", mock.Anything).Return(nil).Once()
	mockScheduler.On("OnSubtaskFinished", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", int64(2), proto.TaskStateSucceed).Return(nil).Once()

	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(nil, nil).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(nil).Once()
	err = scheduler.Run(runCtx, &proto.Task{Type: tp, ID: taskID, Concurrency: concurrency})
	require.NoError(t, err)

	// 10. cancel
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateRunning).Return(nil).Once()
	mockScheduler.On("SplitSubtask", mock.Anything, mock.Anything).Return([]proto.MinimalTask{MockMinimalTask{}, MockMinimalTask{}}, nil).Once()
	mockSubtaskExecutor.On("Run", mock.Anything).Return(nil).Once()
	mockSubtaskExecutor.On("Run", mock.Anything).Return(context.Canceled).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateCanceled).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(nil, nil).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(nil).Once()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = scheduler.Run(runCtx, &proto.Task{Type: tp, ID: taskID, Concurrency: concurrency})
		require.EqualError(t, err, context.Canceled.Error())
	}()
	time.Sleep(time.Second)
	runCancel()
	wg.Wait()

	mockSubtaskTable.AssertExpectations(t)
	mockScheduler.AssertExpectations(t)
	mockSubtaskExecutor.AssertExpectations(t)
	mockPool.AssertExpectations(t)
}

func TestSchedulerRollback(t *testing.T) {
	tp := "test_scheduler_rollback"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	mockSubtaskTable := &MockTaskTable{}
	mockPool := &MockPool{}
	mockScheduler := &MockScheduler{}

	// 1. no scheduler constructor
	schedulerRegisterErr := errors.Errorf("constructor of scheduler for type %s not found", tp)
	scheduler := NewInternalScheduler(ctx, "id", 1, mockSubtaskTable, mockPool)
	mockSubtaskTable.On("GetSubtaskInStates", "id", int64(1), []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil).Once()
	err := scheduler.Rollback(runCtx, &proto.Task{ID: 1, Type: tp})
	require.EqualError(t, err, schedulerRegisterErr.Error())

	RegisterSchedulerConstructor(tp, func(task []byte, step int64) (Scheduler, error) {
		return mockScheduler, nil
	})

	// 2. get subtask failed
	getSubtaskErr := errors.New("get subtask error")
	var taskID int64 = 1
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStateRevertPending}).Return(nil, getSubtaskErr).Once()
	err = scheduler.Rollback(runCtx, &proto.Task{Type: tp, ID: taskID})
	require.EqualError(t, err, getSubtaskErr.Error())

	// 3. no subtask
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStateRevertPending}).Return(nil, nil).Once()
	err = scheduler.Rollback(runCtx, &proto.Task{Type: tp, ID: taskID})
	require.NoError(t, err)

	// 4. update subtask error
	updateSubtaskErr := errors.New("update subtask error")
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStateRevertPending}).Return(&proto.Subtask{ID: 1}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateReverting).Return(updateSubtaskErr).Once()
	err = scheduler.Rollback(runCtx, &proto.Task{Type: tp, ID: taskID})
	require.EqualError(t, err, updateSubtaskErr.Error())

	// rollback failed
	rollbackErr := errors.New("rollback error")
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStateRevertPending}).Return(&proto.Subtask{ID: 1}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateReverting).Return(nil).Once()
	mockScheduler.On("Rollback", mock.Anything).Return(rollbackErr).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateRevertFailed).Return(nil).Once()
	err = scheduler.Rollback(runCtx, &proto.Task{Type: tp, ID: taskID})
	require.EqualError(t, err, rollbackErr.Error())

	// rollback success
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(&proto.Subtask{ID: 1}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", int64(1), proto.TaskStateCanceled).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(&proto.Subtask{ID: 2}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", int64(2), proto.TaskStateCanceled).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStateRevertPending}).Return(&proto.Subtask{ID: 3}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", int64(3), proto.TaskStateReverting).Return(nil).Once()
	mockScheduler.On("Rollback", mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", int64(3), proto.TaskStateReverted).Return(nil).Once()
	err = scheduler.Rollback(runCtx, &proto.Task{Type: tp, ID: taskID})
	require.NoError(t, err)

	mockSubtaskTable.AssertExpectations(t)
	mockScheduler.AssertExpectations(t)
	mockPool.AssertExpectations(t)
}

func TestScheduler(t *testing.T) {
	tp := "test_scheduler"
	var taskID int64 = 1
	var concurrency uint64 = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	mockSubtaskTable := &MockTaskTable{}
	mockPool := &MockPool{}
	mockScheduler := &MockScheduler{}
	mockSubtaskExecutor := &MockSubtaskExecutor{}

	RegisterSchedulerConstructor(tp, func(task []byte, step int64) (Scheduler, error) {
		return mockScheduler, nil
	})
	RegisterSubtaskExectorConstructor(tp, func(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error) {
		return mockSubtaskExecutor, nil
	})

	scheduler := NewInternalScheduler(ctx, "id", 1, mockSubtaskTable, mockPool)
	scheduler.Start()
	defer scheduler.Stop()

	// run failed
	runSubtaskErr := errors.New("run subtask error")
	mockScheduler.On("InitSubtaskExecEnv", mock.Anything).Return(nil).Once()
	mockPool.On("RunWithConcurrency", mock.Anything, mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateRunning).Return(nil).Once()
	mockScheduler.On("SplitSubtask", mock.Anything, mock.Anything).Return([]proto.MinimalTask{MockMinimalTask{}}, nil).Once()
	mockSubtaskExecutor.On("Run", mock.Anything).Return(runSubtaskErr).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateFailed).Return(nil).Once()
	mockScheduler.On("CleanupSubtaskExecEnv", mock.Anything).Return(nil).Once()
	err := scheduler.Run(runCtx, &proto.Task{Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, runSubtaskErr.Error())

	// rollback success
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil).Once()
	mockSubtaskTable.On("GetSubtaskInStates", "id", taskID, []interface{}{proto.TaskStateRevertPending}).Return(&proto.Subtask{ID: 1, Type: tp}, nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateReverting).Return(nil).Once()
	mockScheduler.On("Rollback", mock.Anything).Return(nil).Once()
	mockSubtaskTable.On("UpdateSubtaskStateAndError", taskID, proto.TaskStateReverted).Return(nil).Once()
	err = scheduler.Rollback(runCtx, &proto.Task{Type: tp, ID: taskID})
	require.NoError(t, err)

	mockSubtaskTable.AssertExpectations(t)
	mockScheduler.AssertExpectations(t)
	mockSubtaskExecutor.AssertExpectations(t)
	mockPool.AssertExpectations(t)
}
