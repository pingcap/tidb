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
	mockStepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).AnyTimes()

	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id",
		taskID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{{ID: 1}}, nil).AnyTimes()

	task1 := &proto.Task{Step: proto.StepOne, Type: tp}
	// 1. no taskExecutor constructor
	taskExecutorRegisterErr := errors.Errorf("constructor of taskExecutor for key not found")
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, taskExecutorRegisterErr).Times(2)
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task1, mockSubtaskTable)
	taskExecutor.Extension = mockExtension
	err := taskExecutor.runStep(runCtx, task1, nil)
	require.EqualError(t, err, taskExecutorRegisterErr.Error())
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	err = taskExecutor.RunStep(runCtx, task1, nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 2. init subtask exec env failed
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStepExecutor, nil).AnyTimes()

	initErr := errors.New("init error")
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	err = taskExecutor.runStep(runCtx, task1, nil)
	require.EqualError(t, err, initErr.Error())
	require.True(t, ctrl.Satisfied())

	task := &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}
	// 3. run subtask failed
	runSubtaskErr := errors.New("run subtask error")
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)

	err = taskExecutor.RunStep(runCtx, task, nil)
	require.EqualError(t, err, runSubtaskErr.Error())
	require.True(t, ctrl.Satisfied())

	// 4. run subtask success
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 5. run subtask one by one
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
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
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), int64(2), "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(2), gomock.Any()).Return(nil)
	// third round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// run previous left subtask in running state again, but the subtask is not
	// idempotent, so fail it.
	subtaskID := int64(2)
	theSubtask := &proto.Subtask{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.SubtaskStateRunning, ExecID: "id"}
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{theSubtask}, nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", subtaskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
	require.ErrorContains(t, err, "subtask in running state and is not idempotent")
	require.True(t, ctrl.Satisfied())

	// run previous left subtask in running state again, but the subtask idempotent,
	// run it again.
	theSubtask = &proto.Subtask{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.SubtaskStateRunning, ExecID: "id"}
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{theSubtask}, nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtaskID, gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 6. cancel
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(ErrCancelSubtask)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.SubtaskStateCanceled, gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
	require.EqualError(t, err, ErrCancelSubtask.Error())
	require.True(t, ctrl.Satisfied())

	// 7. RunSubtask return context.Canceled
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(context.Canceled)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
	require.EqualError(t, err, context.Canceled.Error())
	require.True(t, ctrl.Satisfied())

	// 8. grpc cancel
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	grpcErr := status.Error(codes.Canceled, "test cancel")
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(grpcErr)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
	require.EqualError(t, err, grpcErr.Error())
	require.True(t, ctrl.Satisfied())

	// 9. annotate grpc cancel
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
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
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(annotatedError)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
	require.EqualError(t, err, annotatedError.Error())
	require.True(t, ctrl.Satisfied())

	// 10. subtask owned by other executor
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return([]*proto.Subtask{{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(storage.ErrSubtaskNotFound)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(&proto.Task{ID: taskID, Step: proto.StepTwo}, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(runCtx, task, nil)
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
	mockStepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	// 1. no taskExecutor constructor
	task1 := &proto.Task{Step: proto.StepOne, ID: 1, Type: tp}
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task1, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStepExecutor, nil).AnyTimes()

	// 2. get subtask failed
	getSubtaskErr := errors.New("get subtask error")
	var taskID int64 = 1
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, getSubtaskErr)
	err := taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.EqualError(t, err, getSubtaskErr.Error())

	// 3. no subtask
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// 5. rollback success
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{ID: 1, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(1), proto.SubtaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{ID: 2, ExecID: "id"}, nil)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(runCtx, "id", int64(2), proto.SubtaskStateCanceled, nil).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)

	// rollback again for previous left subtask in TaskStateReverting state
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
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
	mockStepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStepExecutor, nil).AnyTimes()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()
	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id",
		taskID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{{ID: 1}}, nil).AnyTimes()

	task := &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// 1. run failed.
	runSubtaskErr := errors.New("run subtask error")
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	subtasks := []*proto.Subtask{
		{ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"},
	}
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err := taskExecutor.runStep(runCtx, task, nil)
	require.EqualError(t, err, runSubtaskErr.Error())
	require.True(t, ctrl.Satisfied())

	// 2. rollback success.
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(runCtx, "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	err = taskExecutor.Rollback(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID})
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 3. run one subtask, then task moved to history(ErrTaskNotFound).
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(nil, storage.ErrTaskNotFound)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.runStep(runCtx, task, nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// no subtask to run, should exit the loop after some time.
	checkIntervalBak := checkInterval
	maxIntervalBak := maxCheckInterval
	defer func() {
		checkInterval = checkIntervalBak
		maxCheckInterval = maxIntervalBak
	}()
	maxCheckInterval, checkInterval = time.Millisecond, time.Millisecond
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil).Times(8)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(task, nil).Times(8)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.runStep(runCtx, task, nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// no-subtask check counter should be reset after a subtask is run.
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	// no subtask to run for 4 times.
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil).Times(4)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(task, nil).Times(4)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil).Times(8)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(task, nil).Times(8)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.runStep(runCtx, task, nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())
}

func TestRunStepCurrentSubtaskScheduledAway(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockStepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	task := &proto.Task{Step: proto.StepOne, Type: "example", ID: 1, Concurrency: 1}
	subtasks := []*proto.Subtask{
		{ID: 1, Type: "example", Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "tidb1"},
	}
	ctx := context.Background()
	taskExecutor := NewBaseTaskExecutor(ctx, "tidb1", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
		task.ID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
	// mock for runStep
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStepExecutor, nil)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "tidb1", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task.ID, "tidb1").Return(nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subtask *proto.Subtask) error {
		<-ctx.Done()
		return ctx.Err()
	})
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	require.ErrorIs(t, taskExecutor.runStep(ctx, task, nil), context.Canceled)
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
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return(nil, errors.New("error"))
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
	runCtx, cancelCause := context.WithCancelCause(ctx)
	taskExecutor.registerCancelFunc(cancelCause)
	require.NoError(t, runCtx.Err())
	taskExecutor.checkBalanceSubtask(ctx)
	require.ErrorIs(t, runCtx.Err(), context.Canceled)
	require.True(t, ctrl.Satisfied())

	subtasks := []*proto.Subtask{{ID: 1, ExecID: "tidb1"}}
	// in-idempotent running subtask
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
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
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return(subtasks, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
	mockSubtaskTable.EXPECT().RunningSubtasksBack2Pending(gomock.Any(), []*proto.Subtask{{ID: 2, ExecID: "tidb1"}}).Return(nil)
	// used to break the loop
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
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
	mockSubtaskExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	task := &proto.Task{Step: proto.StepOne, Type: tp}
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// 1. GetStepExecutor meet retryable error.
	getSubtaskExecutorErr := errors.New("get executor err")
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, getSubtaskExecutorErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 2. GetStepExecutor meet non retryable error.
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, getSubtaskExecutorErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), getSubtaskExecutorErr)
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 3. Init meet retryable error.
	initErr := errors.New("executor init err")
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 4. Init meet non retryable error.
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), initErr)
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 5. GetSubtasksByStepAndStates meet retryable error.
	getSubtasksByExecIDAndStepAndStatesErr := errors.New("get subtasks err")
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(
		gomock.Any(),
		taskExecutor.id,
		gomock.Any(),
		proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, getSubtasksByExecIDAndStepAndStatesErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 6. GetSubtasksByExecIDAndStepAndStates meet non retryable error.
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(
		gomock.Any(),
		taskExecutor.id,
		gomock.Any(),
		proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, getSubtasksByExecIDAndStepAndStatesErr)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), getSubtasksByExecIDAndStepAndStatesErr)
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 7. Cleanup meet retryable error.
	cleanupErr := errors.New("cleanup err")
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(
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
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 8. Cleanup meet non retryable error.
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(
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
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 9. runSummaryCollectLoop meet retryable error.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockSummaryCollectErr", "return()"))
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 10. runSummaryCollectLoop meet non retryable error.
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(runCtx, taskExecutor.id, gomock.Any(), gomock.Any())
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockSummaryCollectErr"))

	// 11. meet cancel before register cancel.
	runCtx1, runCancel1 := context.WithCancel(ctx)
	runCancel1()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.RunStep(runCtx1, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 12. meet ErrCancelSubtask before register cancel.
	runCtx2, runCancel2 := context.WithCancelCause(ctx)
	runCancel2(ErrCancelSubtask)
	mockSubtaskTable.EXPECT().CancelSubtask(runCtx2, taskExecutor.id, gomock.Any())
	require.NoError(t, taskExecutor.RunStep(runCtx2, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())

	// 13. subtask succeed.
	mockExtension.EXPECT().GetStepExecutor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(
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
	require.NoError(t, taskExecutor.RunStep(runCtx, &proto.Task{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}, nil))
	require.True(t, ctrl.Satisfied())
}
