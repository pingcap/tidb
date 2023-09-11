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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func getRunWithConcurrencyFn() (*sync.WaitGroup, func(funcs chan func(), _ uint32) error) {
	poolWg := &sync.WaitGroup{}
	_ = poolWg
	return poolWg, func(funcs chan func(), _ uint32) error {
		go func() {
			for f := range funcs {
				fl := f
				poolWg.Add(1)
				go func() {
					defer poolWg.Done()
					fl()
				}()
			}
		}()
		return nil
	}
}

// MockMinimalTask is a mock of MinimalTask.
type MockMinimalTask struct{}

// IsMinimalTask implements MinimalTask.IsMinimalTask.
func (MockMinimalTask) IsMinimalTask() {}

// String is used to implement the fmt.Stringer interface.
func (MockMinimalTask) String() string {
	return "mock minimal task"
}

func TestSchedulerRun(t *testing.T) {
	tp := "test_scheduler_run"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockPool := mock.NewMockPool(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockMiniTaskExecutor := mockexecute.NewMockMiniTaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// check cancel loop will call it once.
	mockSubtaskTable.EXPECT().IsSchedulerCanceled(gomock.Any(), gomock.Any()).Return(false, nil).Times(1)

	// 1. no scheduler constructor
	schedulerRegisterErr := errors.Errorf("constructor of scheduler for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, schedulerRegisterErr)
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable, mockPool)
	scheduler.Extension = mockExtension
	// UpdateErrorToSubtask won't return such errors, but since the error is not handled,
	// it's saved by UpdateErrorToSubtask.
	// here we use this to check the returned error of s.run.
	forwardErrFn := func(_ string, _ int64, err error) error {
		return err
	}
	mockSubtaskTable.EXPECT().UpdateErrorToSubtask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(forwardErrFn).AnyTimes()
	err := scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, schedulerRegisterErr.Error())

	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	// 2. init subtask exec env failed
	initErr := errors.New("init error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, initErr.Error())

	poolWg, runWithConcurrencyFn := getRunWithConcurrencyFn()

	// 3. pool error
	poolErr := errors.New("pool error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockPool.EXPECT().RunWithConcurrency(gomock.Any(), gomock.Any()).Return(poolErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, poolErr.Error())

	var taskID int64 = 1

	// 4. no subtask executor constructor
	subtaskExecutorRegisterErr := errors.Errorf("constructor of subtask executor for key not found")
	mockExtension.EXPECT().GetMiniTaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, subtaskExecutorRegisterErr)
	var concurrency uint64 = 10
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockPool.EXPECT().RunWithConcurrency(gomock.Any(), gomock.Any()).DoAndReturn(runWithConcurrencyFn)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().SplitSubtask(gomock.Any(), gomock.Any()).Return([]proto.MinimalTask{MockMinimalTask{}}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, subtaskExecutorRegisterErr.Error())

	mockExtension.EXPECT().GetMiniTaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockMiniTaskExecutor, nil).AnyTimes()

	// 5. run subtask failed
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockPool.EXPECT().RunWithConcurrency(gomock.Any(), gomock.Any()).DoAndReturn(runWithConcurrencyFn)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().SplitSubtask(gomock.Any(), gomock.Any()).Return([]proto.MinimalTask{MockMinimalTask{}}, nil)
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, runSubtaskErr.Error())

	// 6. run subtask success
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockPool.EXPECT().RunWithConcurrency(gomock.Any(), gomock.Any()).DoAndReturn(runWithConcurrencyFn)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().SplitSubtask(gomock.Any(), gomock.Any()).Return([]proto.MinimalTask{MockMinimalTask{}}, nil)
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return([]byte(""), nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.NoError(t, err)

	// 7. run subtask one by one
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockPool.EXPECT().RunWithConcurrency(gomock.Any(), gomock.Any()).DoAndReturn(runWithConcurrencyFn)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(int64(1)).Return(nil)
	mockSubtaskExecutor.EXPECT().SplitSubtask(gomock.Any(), gomock.Any()).Return([]proto.MinimalTask{MockMinimalTask{}}, nil)
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return([]byte(""), nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(1), gomock.Any()).Return(nil)

	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 2, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(int64(2)).Return(nil)
	mockSubtaskExecutor.EXPECT().SplitSubtask(gomock.Any(), gomock.Any()).Return([]proto.MinimalTask{MockMinimalTask{}}, nil)
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return([]byte(""), nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(2), gomock.Any()).Return(nil)

	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(nil, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.NoError(t, err)
	// 8. cancel
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockPool.EXPECT().RunWithConcurrency(gomock.Any(), gomock.Any()).DoAndReturn(runWithConcurrencyFn)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().SplitSubtask(gomock.Any(), gomock.Any()).Return([]proto.MinimalTask{MockMinimalTask{}, MockMinimalTask{}}, nil)
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(nil)
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(context.Canceled)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateCanceled, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
		require.EqualError(t, err, context.Canceled.Error())
	}()
	time.Sleep(time.Second)
	runCancel()
	wg.Wait()
	// 9. run subtask one by one, on error, we should wait all minimal task finished before call Cleanup
	syncCh := make(chan struct{})
	lastMinimalTaskFinishTime, cleanupTime := time.Time{}, time.Time{}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/waitUntilError", `return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/waitUntilError"))
	})
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockPool.EXPECT().RunWithConcurrency(gomock.Any(), gomock.Any()).DoAndReturn(runWithConcurrencyFn)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().SplitSubtask(gomock.Any(), gomock.Any()).Return([]proto.MinimalTask{MockMinimalTask{}, MockMinimalTask{}}, nil)
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(nil).Do(func(_ context.Context) {
		<-syncCh
		lastMinimalTaskFinishTime = time.Now()
	})
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(context.Canceled)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateCanceled, gomock.Any()).Return(nil)
	// GetSubtaskInStates should not be called again, we should break on first check in the foo loop of scheduler.Run
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil).Do(func(_ context.Context) {
		cleanupTime = time.Now()
	})
	scheduler = NewBaseScheduler(ctx, "id", 1, mockSubtaskTable, mockPool)
	scheduler.Extension = mockExtension
	runCtx2, runCancel2 := context.WithCancel(ctx)
	defer runCancel2()
	wg = sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		err = scheduler.Run(runCtx2, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
		require.EqualError(t, err, context.Canceled.Error())
	}()
	go func() {
		defer wg.Done()
		time.Sleep(3 * time.Second)
		syncCh <- struct{}{}
	}()
	wg.Wait()
	runCancel2()
	require.False(t, lastMinimalTaskFinishTime.IsZero())
	require.False(t, cleanupTime.IsZero())
	require.Greater(t, cleanupTime, lastMinimalTaskFinishTime)
	poolWg.Wait()
}

func TestSchedulerRollback(t *testing.T) {
	tp := "test_scheduler_rollback"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockPool := mock.NewMockPool(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// 1. no scheduler constructor
	schedulerRegisterErr := errors.Errorf("constructor of scheduler for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, schedulerRegisterErr)
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable, mockPool)
	scheduler.Extension = mockExtension
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", int64(1), proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil)
	err := scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, ID: 1, Type: tp})
	require.EqualError(t, err, schedulerRegisterErr.Error())

	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	// 2. get subtask failed
	getSubtaskErr := errors.New("get subtask error")
	var taskID int64 = 1
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStateRevertPending}).Return(nil, getSubtaskErr)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, getSubtaskErr.Error())

	// 3. no subtask
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStateRevertPending}).Return(nil, nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// 4. update subtask error
	updateSubtaskErr := errors.New("update subtask error")
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStateRevertPending}).Return(&proto.Subtask{ID: 1}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateReverting, nil).Return(updateSubtaskErr)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, updateSubtaskErr.Error())

	// rollback failed
	rollbackErr := errors.New("rollback error")
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStateRevertPending}).Return(&proto.Subtask{ID: 1}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(rollbackErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateRevertFailed, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, rollbackErr.Error())

	// rollback success
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(&proto.Subtask{ID: 1}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(1), proto.TaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(&proto.Subtask{ID: 2}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(2), proto.TaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStateRevertPending}).Return(&proto.Subtask{ID: 3}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(3), proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(3), proto.TaskStateReverted, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
}

func TestScheduler(t *testing.T) {
	tp := "test_scheduler"
	var taskID int64 = 1
	var concurrency uint64 = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockPool := mock.NewMockPool(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockMiniTaskExecutor := mockexecute.NewMockMiniTaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	mockExtension.EXPECT().GetMiniTaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockMiniTaskExecutor, nil).AnyTimes()
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable, mockPool)
	scheduler.Extension = mockExtension

	poolWg, runWithConcurrencyFn := getRunWithConcurrencyFn()

	// run failed
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockPool.EXPECT().RunWithConcurrency(gomock.Any(), gomock.Any()).DoAndReturn(runWithConcurrencyFn)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", proto.StepOne, taskID,
		[]interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().SplitSubtask(gomock.Any(), gomock.Any()).Return([]proto.MinimalTask{MockMinimalTask{}}, nil)
	mockMiniTaskExecutor.EXPECT().Run(gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err := scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, runSubtaskErr.Error())

	// rollback success
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStatePending, proto.TaskStateRunning}).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne,
		[]interface{}{proto.TaskStateRevertPending}).Return(&proto.Subtask{ID: 1, Type: tp}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateReverted, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
	poolWg.Wait()
}
