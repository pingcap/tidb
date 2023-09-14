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

	"github.com/pingcap/tidb/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSchedulerRun(t *testing.T) {
	tp := "test_scheduler_run"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// we don't test cancelCheck here, but in case the test is slow and trigger it.
	mockSubtaskTable.EXPECT().IsSchedulerCanceled(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	// 1. no scheduler constructor
	schedulerRegisterErr := errors.Errorf("constructor of scheduler for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, schedulerRegisterErr)
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
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

	var taskID int64 = 1
	var concurrency uint64 = 10

	// 5. run subtask failed
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, runSubtaskErr.Error())

	// 6. run subtask success
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.NoError(t, err)

	// 7. run subtask one by one
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(int64(1)).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(1), gomock.Any()).Return(nil)

	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 2, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(int64(2)).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(2), gomock.Any()).Return(nil)

	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(nil, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.NoError(t, err)
	// 8. cancel
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", taskID, proto.StepOne, []interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(context.Canceled)
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
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// 1. no scheduler constructor
	schedulerRegisterErr := errors.Errorf("constructor of scheduler for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, schedulerRegisterErr)
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
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
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
	scheduler.Extension = mockExtension

	// run failed
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtaskInStates("id", proto.StepOne, taskID,
		[]interface{}{proto.TaskStatePending}).Return(&proto.Subtask{ID: 1, Type: tp, Step: proto.StepOne}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
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
}
