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

	"github.com/pingcap/errors"
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

	// we don't test cancelCheck here, but in case the test is slow and trigger it.
	mockSubtaskTable.EXPECT().IsTaskExecutorCanceled(ctx, gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	// 1. no taskExecutor constructor
	taskExecutorRegisterErr := errors.Errorf("constructor of taskExecutor for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, taskExecutorRegisterErr).Times(2)
	taskExecutor := NewBaseTaskExecutor(ctx, "id", 1, mockSubtaskTable)
	taskExecutor.Extension = mockExtension
	err := taskExecutor.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, taskExecutorRegisterErr.Error())
	mockSubtaskTable.EXPECT().UpdateErrorToSubtask(runCtx, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	err = taskExecutor.Run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.NoError(t, err)

	// 2. init subtask exec env failed
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()

	initErr := errors.New("init error")
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	err = taskExecutor.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp})
	require.EqualError(t, err, initErr.Error())

	var taskID int64 = 1
	var concurrency = 10
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
	taskExecutorRegisterErr := errors.Errorf("constructor of taskExecutor for key not found")
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, taskExecutorRegisterErr)
	taskExecutor := NewBaseTaskExecutor(ctx, "id", 1, mockSubtaskTable)
	taskExecutor.Extension = mockExtension
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", int64(1), proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	err := taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, ID: 1, Type: tp})
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
	mockSubtaskTable.EXPECT().IsTaskExecutorCanceled(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil).AnyTimes()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()

	taskExecutor := NewBaseTaskExecutor(ctx, "id", 1, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// 1. run failed.
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
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err := taskExecutor.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.EqualError(t, err, runSubtaskErr.Error())

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

	// 3. run one subtask, then task moved to history(ErrTaskNotFound).
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
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(nil, storage.ErrTaskNotFound)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.run(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency})
	require.NoError(t, err)
}
