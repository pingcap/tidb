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
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	unfinishedNormalSubtaskStates = []interface{}{
		proto.TaskStatePending, proto.TaskStateRunning,
	}
	unfinishedRevertSubtaskStates = []interface{}{
		proto.TaskStateRevertPending, proto.TaskStateReverting,
	}
)

func TestSchedulerRun(t *testing.T) {
	var tp proto.TaskType = "test_scheduler_run"
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
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, schedulerRegisterErr).Times(2)
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
	scheduler.Extension = mockExtension
	err := scheduler.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, schedulerRegisterErr.Error())
	mockSubtaskTable.EXPECT().UpdateErrorToSubtask(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	err = scheduler.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.NoError(t, err)

	// 2. init subtask exec env failed
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	initErr := errors.New("init error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	err = scheduler.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, initErr.Error())

	var taskID int64 = 1
	var concurrency uint64 = 10
	task := &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}

	// 3. run subtask failed
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)

	err = scheduler.Run(runCtx, task)
	require.EqualError(t, err, runSubtaskErr.Error())

	// 4. run subtask success
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.NoError(t, err)

	// 5. run subtask one by one
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(
		[]*proto.Subtask{
			{ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending},
			{ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending},
		}, nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(int64(1)).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(1), gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(int64(2)).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(int64(2), gomock.Any()).Return(nil)
	// third round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.NoError(t, err)

	// run previous left subtask in running state again, but the subtask is not
	// idempotent, so fail it.
	subtaskID := int64(2)
	theSubtask := &proto.Subtask{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.TaskStateRunning}
	mockSubtaskTable.EXPECT().GetSubtasksInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{theSubtask}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(subtaskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.ErrorContains(t, err, "subtask in running state and is not idempotent")

	// run previous left subtask in running state again, but the subtask idempotent,
	// run it again.
	theSubtask = &proto.Subtask{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.TaskStateRunning}
	mockSubtaskTable.EXPECT().GetSubtasksInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{theSubtask}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(subtaskID, gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetGlobalTaskByID(gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.NoError(t, err)

	// 6. cancel
	mockSubtaskTable.EXPECT().GetSubtasksInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(ErrCancelSubtask)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateCanceled, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.EqualError(t, err, ErrCancelSubtask.Error())

	// 7. RunSubtask return context.Canceled
	mockSubtaskTable.EXPECT().GetSubtasksInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(context.Canceled)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = scheduler.Run(runCtx, task)
	require.EqualError(t, err, context.Canceled.Error())

	runCancel()
}

func TestSchedulerRollback(t *testing.T) {
	var tp proto.TaskType = "test_scheduler_rollback"
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
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", int64(1), proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	err := scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, ID: 1, Type: tp})
	require.EqualError(t, err, schedulerRegisterErr.Error())

	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	// 2. get subtask failed
	getSubtaskErr := errors.New("get subtask error")
	var taskID int64 = 1
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(nil, getSubtaskErr)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, getSubtaskErr.Error())

	// 3. no subtask
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(nil, nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// 4. rollback failed
	rollbackErr := errors.New("rollback error")
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 1, State: proto.TaskStateRevertPending}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(rollbackErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateRevertFailed, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, rollbackErr.Error())

	// 5. rollback success
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{ID: 1}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(1), proto.TaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{ID: 2}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(2), proto.TaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 3, State: proto.TaskStateRevertPending}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(3), proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(3), proto.TaskStateReverted, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// rollback again for previous left subtask in TaskStateReverting state
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 3, State: proto.TaskStateReverting}, nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(int64(3), proto.TaskStateReverted, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
}

func TestSchedulerPause(t *testing.T) {
	var tp proto.TaskType = "test_scheduler_pause"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// pause success.
	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
	scheduler.Extension = mockExtension
	mockSubtaskTable.EXPECT().PauseSubtasks("id", int64(1)).Return(nil)
	require.NoError(t, scheduler.Pause(runCtx, &proto.Task{Step: proto.StepOne, ID: 1, Type: tp}))

	// pause error.
	pauseErr := errors.New("pause error")
	mockSubtaskTable.EXPECT().PauseSubtasks("id", int64(1)).Return(pauseErr)
	err := scheduler.Pause(runCtx, &proto.Task{Step: proto.StepOne, ID: 1, Type: tp})
	require.EqualError(t, err, pauseErr.Error())
}

func TestScheduler(t *testing.T) {
	var tp proto.TaskType = "test_scheduler"
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
	mockSubtaskTable.EXPECT().IsSchedulerCanceled(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	scheduler := NewBaseScheduler(ctx, "id", 1, mockSubtaskTable)
	scheduler.Extension = mockExtension

	// 1. run failed.
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.TaskStatePending}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(taskID).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err := scheduler.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, runSubtaskErr.Error())

	// 2. rollback success.
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates("id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 1, Type: tp, State: proto.TaskStateRevertPending}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(taskID, proto.TaskStateReverted, nil).Return(nil)
	err = scheduler.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
}
