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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	unfinishedNormalSubtaskStates = []interface{}{
		proto.SubtaskStatePending, proto.SubtaskStateRunning,
	}
	unfinishedRevertSubtaskStates = []interface{}{
		proto.SubtaskStateRevertPending, proto.SubtaskStateReverting,
	}
)

func TestTaskExecutorRun(t *testing.T) {
	var tp proto.TaskType = "test_task_executor_run"
	var taskID int64 = 1
	var concurrency = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).AnyTimes()

	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id",
		taskID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{{ID: 1}}, nil).AnyTimes()

	task1 := &proto.Task{Step: proto.StepOne, Type: tp}
	// 1. no taskExecutor constructor
	taskExecutorRegisterErr := errors.Errorf("constructor of taskExecutor for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, taskExecutorRegisterErr).Times(2)
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task1, mockSubtaskTable)
	taskExecutor.Extension = mockExtension
	err := taskExecutor.runStep(runCtx, task1)
	require.EqualError(t, err, taskExecutorRegisterErr.Error())
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	err = taskExecutor.Run(runCtx, task1)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 2. init subtask exec env failed
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	initErr := errors.New("init error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	err = taskExecutor.runStep(runCtx, task1)
	require.EqualError(t, err, initErr.Error())
	require.True(t, ctrl.Satisfied())

	task := &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}
	// 3. run subtask failed
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)

	err = taskExecutor.Run(runCtx, task)
	require.EqualError(t, err, runSubtaskErr.Error())
	require.True(t, ctrl.Satisfied())

	// 4. run subtask success
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 5. run subtask one by one
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(
		[]*proto.Subtask{
			{ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"},
			{ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"},
		}, nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), int64(1), "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), int64(2), "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(2), gomock.Any()).Return(nil)
	// third round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// run previous left subtask in running state again, but the subtask is not
	// idempotent, so fail it.
	subtaskID := int64(2)
	theSubtask := &proto.Subtask{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.SubtaskStateRunning, ExecID: "id"}
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{theSubtask}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", subtaskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.ErrorContains(t, err, "subtask in running state and is not idempotent")
	require.True(t, ctrl.Satisfied())

	// run previous left subtask in running state again, but the subtask idempotent,
	// run it again.
	theSubtask = &proto.Subtask{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.SubtaskStateRunning, ExecID: "id"}
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{theSubtask}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtaskID, gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 6. cancel
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(ErrCancelSubtask)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.SubtaskStateCanceled, gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.EqualError(t, err, ErrCancelSubtask.Error())
	require.True(t, ctrl.Satisfied())

	// 7. RunSubtask return context.Canceled
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(context.Canceled)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.EqualError(t, err, context.Canceled.Error())
	require.True(t, ctrl.Satisfied())

	// 8. grpc cancel
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	grpcErr := status.Error(codes.Canceled, "test cancel")
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(grpcErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.EqualError(t, err, grpcErr.Error())
	require.True(t, ctrl.Satisfied())

	// 9. annotate grpc cancel
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	grpcErr = status.Error(codes.Canceled, "test cancel")
	annotatedError := errors.Annotatef(
		grpcErr,
		" %s",
		"test annotate",
	)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(annotatedError)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.EqualError(t, err, annotatedError.Error())
	require.True(t, ctrl.Satisfied())

	// 10. subtask owned by other executor
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(storage.ErrSubtaskNotFound)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.Run(runCtx, task)
	require.NoError(t, err)
	runCancel()
	require.True(t, ctrl.Satisfied())
}

func TestTaskExecutorRollback(t *testing.T) {
	var tp proto.TaskType = "test_executor_rollback"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// 1. no taskExecutor constructor
	task1 := &proto.Task{Step: proto.StepOne, ID: 1, Type: tp}
	taskExecutorRegisterErr := errors.Errorf("constructor of taskExecutor for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, taskExecutorRegisterErr)
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task1, mockSubtaskTable)
	taskExecutor.Extension = mockExtension
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", int64(1), proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	err := taskExecutor.Rollback(runCtx, task1)
	require.EqualError(t, err, taskExecutorRegisterErr.Error())

	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	// 2. get subtask failed
	getSubtaskErr := errors.New("get subtask error")
	var taskID int64 = 1
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(nil, getSubtaskErr)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, getSubtaskErr.Error())

	// 3. no subtask
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(nil, nil)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// 4. rollback failed
	rollbackErr := errors.New("rollback error")
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 1, State: proto.SubtaskStateRevertPending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", taskID, proto.SubtaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(rollbackErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", taskID, proto.SubtaskStateRevertFailed, nil).Return(nil)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, rollbackErr.Error())

	// 5. rollback success
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{ID: 1, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(1), proto.SubtaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{ID: 2, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(2), proto.SubtaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 3, State: proto.SubtaskStateRevertPending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(3), proto.SubtaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(3), proto.SubtaskStateReverted, nil).Return(nil)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// rollback again for previous left subtask in TaskStateReverting state
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 3, State: proto.SubtaskStateReverting, ExecID: "id"}, nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(3), proto.SubtaskStateReverted, nil).Return(nil)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
}

func TestTaskExecutor(t *testing.T) {
	var tp proto.TaskType = "test_task_executor"
	var taskID int64 = 1
	var concurrency = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()
	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id",
		taskID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{{ID: 1}}, nil).AnyTimes()

	task := &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// 1. run failed.
	runSubtaskErr := errors.New("run subtask error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	subtasks := []*proto.Subtask{
		{ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"},
	}
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err := taskExecutor.runStep(runCtx, task)
	require.EqualError(t, err, runSubtaskErr.Error())
	require.True(t, ctrl.Satisfied())

	// 2. rollback success.
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedRevertSubtaskStates...).Return(&proto.Subtask{ID: 1, Type: tp, State: proto.SubtaskStateRevertPending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", taskID, proto.SubtaskStateReverting, nil).Return(nil)
	mockSubtaskExecutor.EXPECT().Rollback(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", taskID, proto.SubtaskStateReverted, nil).Return(nil)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 3. run one subtask, then task moved to history(ErrTaskNotFound).
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(nil, storage.ErrTaskNotFound)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.runStep(runCtx, task)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())
}

func TestRunStepCurrentSubtaskScheduledAway(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	task := &proto.Task{Step: proto.StepOne, Type: "type", ID: 1, Concurrency: 1}
	subtasks := []*proto.Subtask{
		{ID: 1, Type: "type", Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "tidb1"},
	}
	ctx := context.Background()
	taskExecutor := NewBaseTaskExecutor(ctx, "tidb1", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "tidb1",
		task.ID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
	// mock for runStep
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "tidb1", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "tidb1", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task.ID, "tidb1").Return(nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subtask *proto.Subtask) error {
		<-ctx.Done()
		return ctx.Err()
	})
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	require.ErrorIs(t, taskExecutor.runStep(ctx, task), context.Canceled)
	require.True(t, ctrl.Satisfied())
}

func TestCheckBalanceSubtask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	ctx := context.Background()
	task := &proto.Task{Step: proto.StepOne, Type: "type", ID: 1, Concurrency: 1}
	taskExecutor := NewBaseTaskExecutor(ctx, "tidb1", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// context canceled
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	taskExecutor.checkBalanceSubtask(canceledCtx)

	bak := checkBalanceSubtaskInterval
	t.Cleanup(func() {
		checkBalanceSubtaskInterval = bak
	})
	checkBalanceSubtaskInterval = 100 * time.Millisecond

	// subtask scheduled away
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return(nil, errors.New("error"))
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
	runCtx, cancelCause := context.WithCancelCause(ctx)
	taskExecutor.registerCancelFunc(cancelCause)
	require.NoError(t, runCtx.Err())
	taskExecutor.checkBalanceSubtask(ctx)
	require.ErrorIs(t, runCtx.Err(), context.Canceled)
	require.True(t, ctrl.Satisfied())

	subtasks := []*proto.Subtask{{ID: 1, ExecID: "tidb1"}}
	// in-idempotent running subtask
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return(subtasks, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "tidb1",
		subtasks[0].ID, proto.SubtaskStateFailed, ErrNonIdempotentSubtask).Return(nil)
	mockExtension.EXPECT().IsIdempotent(subtasks[0]).Return(false)
	taskExecutor.checkBalanceSubtask(ctx)
	require.True(t, ctrl.Satisfied())

	// current running subtask is skipped
	require.Zero(t, taskExecutor.currSubtaskID.Load())
	taskExecutor.currSubtaskID.Store(1)
	subtasks = []*proto.Subtask{{ID: 1, ExecID: "tidb1"}, {ID: 2, ExecID: "tidb1"}}
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return(subtasks, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
	mockSubtaskTable.EXPECT().RunningSubtasksBack2Pending(gomock.Any(), []*proto.Subtask{{ID: 2, ExecID: "tidb1"}}).Return(nil)
	// used to break the loop
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return(nil, nil)
	taskExecutor.checkBalanceSubtask(ctx)
	require.True(t, ctrl.Satisfied())
}

func TestExecutorErrHandling(t *testing.T) {
	var tp proto.TaskType = "test_task_executor"
	var taskID int64 = 1
	var concurrency = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockSubtaskExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	task := &proto.Task{Step: proto.StepOne, Type: tp}
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// 1. GetSubtaskExecutor meet retryable error.
	getSubtaskExecutorErr := errors.New("get executor err")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, getSubtaskExecutorErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 2. GetSubtaskExecutor meet non retryable error.
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, getSubtaskExecutorErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), getSubtaskExecutorErr)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 3. Init meet retryable error.
	initErr := errors.New("executor init err")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 4. Init meet non retryable error.
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), initErr)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 5. GetSubtasksByStepAndStates meet retryable error.
	getSubtasksByStepAndStatesErr := errors.New("get subtasks err")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(
		gomock.Any(),
		taskExecutor.id,
		gomock.Any(),
		proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, getSubtasksByStepAndStatesErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 6. GetSubtasksByStepAndStates meet non retryable error.
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(
		gomock.Any(),
		taskExecutor.id,
		gomock.Any(),
		proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, getSubtasksByStepAndStatesErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), getSubtasksByStepAndStatesErr)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 7. Cleanup meet retryable error.
	cleanupErr := errors.New("cleanup err")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(
		gomock.Any(),
		taskExecutor.id,
		gomock.Any(),
		proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(cleanupErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 8. Cleanup meet non retryable error.
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(
		gomock.Any(),
		taskExecutor.id,
		gomock.Any(),
		proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(cleanupErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), cleanupErr)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 9. runSummaryCollectLoop meet retryable error.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockSummaryCollectErr", "return()"))
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 10. runSummaryCollectLoop meet non retryable error.
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), gomock.Any())
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockSummaryCollectErr"))

	// 11. meet cancel before register cancel.
	runCtx1, runCancel1 := context.WithCancel(ctx)
	runCancel1()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.Run(runCtx1, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 12. meet ErrCancelSubtask before register cancel.
	runCtx2, runCancel2 := context.WithCancelCause(ctx)
	runCancel2(ErrCancelSubtask)
	mockSubtaskTable.EXPECT().CancelSubtask(runCtx2, taskExecutor.id, gomock.Any())
	require.NoError(t, taskExecutor.Run(runCtx2, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())

	// 13. subtask succeed.
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByStepAndStates(
		gomock.Any(),
		taskExecutor.id,
		gomock.Any(),
		proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	require.NoError(t, taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}))
	require.True(t, ctrl.Satisfied())
}
