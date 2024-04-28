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
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	unfinishedNormalSubtaskStates = []any{
		proto.SubtaskStatePending, proto.SubtaskStateRunning,
	}
)

func TestTaskExecutorRun(t *testing.T) {
	var tp proto.TaskType = "test_task_executor_run"
	var concurrency = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockStepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()

	task1 := &proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne, Type: tp, ID: 1, Concurrency: concurrency}}
	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id",
		task1.ID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{{SubtaskBase: proto.SubtaskBase{ID: 1}}}, nil).AnyTimes()
	// mock GetTaskByID at beginning of runStep
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task1.ID).Return(task1, nil).AnyTimes()

	// 1. no taskExecutor constructor
	taskExecutorRegisterErr := errors.Errorf("constructor of taskExecutor for key not found")
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(nil, taskExecutorRegisterErr).Times(2)
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task1, mockSubtaskTable)
	taskExecutor.Extension = mockExtension
	err := taskExecutor.runStep(nil)
	require.EqualError(t, err, taskExecutorRegisterErr.Error())
	mockSubtaskTable.EXPECT().FailSubtask(taskExecutor.ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	err = taskExecutor.RunStep(nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 2. init subtask exec env failed
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(mockStepExecutor, nil).AnyTimes()

	initErr := errors.New("init error")
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	err = taskExecutor.runStep(nil)
	require.EqualError(t, err, initErr.Error())
	require.True(t, ctrl.Satisfied())

	// 3. run subtask failed
	runSubtaskErr := errors.New("run subtask error")
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task1.ID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", task1.ID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)

	err = taskExecutor.RunStep(nil)
	require.EqualError(t, err, runSubtaskErr.Error())
	require.True(t, ctrl.Satisfied())

	// 4. run subtask success
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task1.ID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 5. run subtask one by one
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), int64(1), "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 2, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), int64(2), "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(2), gomock.Any()).Return(nil)
	// third round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// run previous left subtask in running state again, but the subtask is not
	// idempotent, so fail it.
	subtaskID := int64(2)
	theSubtask := &proto.Subtask{SubtaskBase: proto.SubtaskBase{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.SubtaskStateRunning, ExecID: "id"}}
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", subtaskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.ErrorContains(t, err, "subtask in running state and is not idempotent")
	require.True(t, ctrl.Satisfied())

	// run previous left subtask in running state again, but the subtask idempotent,
	// run it again.
	theSubtask = &proto.Subtask{SubtaskBase: proto.SubtaskBase{ID: subtaskID, Type: tp, Step: proto.StepOne, State: proto.SubtaskStateRunning, ExecID: "id"}}
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	// first round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtaskID, gomock.Any()).Return(nil)
	// second round of the run loop
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// 6. cancel
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task1.ID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(ErrCancelSubtask)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", task1.ID, proto.SubtaskStateCanceled, gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.EqualError(t, err, ErrCancelSubtask.Error())
	require.True(t, ctrl.Satisfied())

	// 7. RunSubtask return context.Canceled
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task1.ID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(context.Canceled)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.EqualError(t, err, context.Canceled.Error())
	require.True(t, ctrl.Satisfied())

	// 8. grpc cancel
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task1.ID, "id").Return(nil)
	grpcErr := status.Error(codes.Canceled, "test cancel")
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(grpcErr)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.EqualError(t, err, grpcErr.Error())
	require.True(t, ctrl.Satisfied())

	// 9. annotate grpc cancel
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task1.ID, "id").Return(nil)
	grpcErr = status.Error(codes.Canceled, "test cancel")
	annotatedError := errors.Annotatef(
		grpcErr,
		" %s",
		"test annotate",
	)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(annotatedError)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.EqualError(t, err, annotatedError.Error())
	require.True(t, ctrl.Satisfied())

	// 10. subtask owned by other executor
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task1.ID, "id").Return(storage.ErrSubtaskNotFound)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.RunStep(nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())

	// task not found when Run
	mockSubtaskTable.EXPECT().GetTaskBaseByID(gomock.Any(), task1.ID).Return(nil, storage.ErrTaskNotFound)
	taskExecutor.Run(nil)
	require.True(t, ctrl.Satisfied())
	// task Succeed inside Run
	task1.State = proto.TaskStateSucceed
	mockSubtaskTable.EXPECT().GetTaskBaseByID(gomock.Any(), task1.ID).Return(&task1.TaskBase, nil)
	taskExecutor.Run(nil)
	require.True(t, ctrl.Satisfied())

	task1.State = proto.TaskStateRunning
	ReduceCheckInterval(t)

	// GetTaskBaseByID error, should continue
	mockSubtaskTable.EXPECT().GetTaskBaseByID(gomock.Any(), task1.ID).Return(nil, errors.New("mock err"))
	// HasSubtasksInStates error, should continue
	mockSubtaskTable.EXPECT().GetTaskBaseByID(gomock.Any(), task1.ID).Return(&task1.TaskBase, nil)
	mockSubtaskTable.EXPECT().HasSubtasksInStates(gomock.Any(), "id", task1.ID, task1.Step,
		unfinishedNormalSubtaskStates...).Return(false, errors.New("failed to check"))
	// no subtask to run, should exit the loop after some time.
	mockSubtaskTable.EXPECT().GetTaskBaseByID(gomock.Any(), task1.ID).Return(&task1.TaskBase, nil).Times(8)
	mockSubtaskTable.EXPECT().HasSubtasksInStates(gomock.Any(), "id", task1.ID, task1.Step,
		unfinishedNormalSubtaskStates...).Return(false, nil).Times(8)
	taskExecutor.Run(nil)
	require.True(t, ctrl.Satisfied())

	// no-subtask check counter should be reset after a subtask is run.
	// loop 4 times without subtask, then 1 time with subtask.
	mockSubtaskTable.EXPECT().GetTaskBaseByID(gomock.Any(), task1.ID).Return(&task1.TaskBase, nil).Times(4)
	mockSubtaskTable.EXPECT().HasSubtasksInStates(gomock.Any(), "id", task1.ID, task1.Step,
		unfinishedNormalSubtaskStates...).Return(false, nil).Times(4)
	mockSubtaskTable.EXPECT().GetTaskBaseByID(gomock.Any(), task1.ID).Return(&task1.TaskBase, nil)
	mockSubtaskTable.EXPECT().HasSubtasksInStates(gomock.Any(), "id", task1.ID, task1.Step,
		unfinishedNormalSubtaskStates...).Return(true, nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task1.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	// should loop for another 8 times
	mockSubtaskTable.EXPECT().GetTaskBaseByID(gomock.Any(), task1.ID).Return(&task1.TaskBase, nil).Times(8)
	mockSubtaskTable.EXPECT().HasSubtasksInStates(gomock.Any(), "id", task1.ID, task1.Step,
		unfinishedNormalSubtaskStates...).Return(false, nil).Times(8)
	taskExecutor.Run(nil)
	require.True(t, ctrl.Satisfied())

	taskExecutor.Cancel()
	taskExecutor.Run(nil)
	require.True(t, ctrl.Satisfied())
}

func TestTaskExecutor(t *testing.T) {
	var tp proto.TaskType = "test_task_executor"
	var taskID int64 = 1
	var concurrency = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockStepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", taskID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(mockStepExecutor, nil).AnyTimes()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()
	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id",
		taskID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{{SubtaskBase: proto.SubtaskBase{ID: 1}}}, nil).AnyTimes()

	task := &proto.Task{TaskBase: proto.TaskBase{Step: proto.StepOne, Type: tp, ID: taskID, Concurrency: concurrency}}
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// 1. run failed.
	runSubtaskErr := errors.New("run subtask error")
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	subtasks := []*proto.Subtask{
		{SubtaskBase: proto.SubtaskBase{ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}},
	}
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err := taskExecutor.runStep(nil)
	require.EqualError(t, err, runSubtaskErr.Error())
	require.True(t, ctrl.Satisfied())

	// 2. run one subtask, then task moved to history(ErrTaskNotFound).
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), taskID, "id").Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	err = taskExecutor.runStep(nil)
	require.NoError(t, err)
	require.True(t, ctrl.Satisfied())
}

func TestRunStepCurrentSubtaskScheduledAway(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockStepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	task := &proto.Task{TaskBase: proto.TaskBase{Step: proto.StepOne, Type: "example", ID: 1, Concurrency: 1}}
	subtasks := []*proto.Subtask{
		{SubtaskBase: proto.SubtaskBase{ID: 1, Type: "example", Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "tidb1"}},
	}
	ctx := context.Background()
	taskExecutor := NewBaseTaskExecutor(ctx, "tidb1", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// mock for checkBalanceSubtask
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
		task.ID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
	// mock for runStep
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(mockStepExecutor, nil)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "tidb1", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(subtasks[0], nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task.ID, "tidb1").Return(nil)
	mockStepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subtask *proto.Subtask) error {
		<-ctx.Done()
		return ctx.Err()
	})
	mockStepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	require.ErrorIs(t, taskExecutor.runStep(nil), context.Canceled)
	require.True(t, ctrl.Satisfied())
}

func TestCheckBalanceSubtask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	ctx := context.Background()
	task := &proto.Task{TaskBase: proto.TaskBase{Step: proto.StepOne, Type: "type", ID: 1, Concurrency: 1}}
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
	taskExecutor.registerRunStepCancelFunc(cancelCause)
	require.NoError(t, runCtx.Err())
	taskExecutor.checkBalanceSubtask(ctx)
	require.ErrorIs(t, runCtx.Err(), context.Canceled)
	require.True(t, ctrl.Satisfied())

	subtasks := []*proto.Subtask{{SubtaskBase: proto.SubtaskBase{ID: 1, ExecID: "tidb1"}}}
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
	subtasks = []*proto.Subtask{{SubtaskBase: proto.SubtaskBase{ID: 1, ExecID: "tidb1"}}, {SubtaskBase: proto.SubtaskBase{ID: 2, ExecID: "tidb1"}}}
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return(subtasks, nil)
	mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
	mockSubtaskTable.EXPECT().RunningSubtasksBack2Pending(gomock.Any(), []*proto.SubtaskBase{{ID: 2, ExecID: "tidb1"}}).Return(nil)
	// used to break the loop
	mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
		task.ID, task.Step, proto.SubtaskStateRunning).Return(nil, nil)
	taskExecutor.checkBalanceSubtask(ctx)
	require.True(t, ctrl.Satisfied())
}

func TestExecutorErrHandling(t *testing.T) {
	var tp proto.TaskType = "test_task_executor"
	var concurrency = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockSubtaskExecutor := mockexecute.NewMockStepExecutor(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)
	task := &proto.Task{TaskBase: proto.TaskBase{Step: proto.StepOne, Type: tp, ID: 1, Concurrency: concurrency}}
	taskExecutor := NewBaseTaskExecutor(ctx, "id", task, mockSubtaskTable)
	taskExecutor.Extension = mockExtension

	// GetStepExecutor meet retryable error.
	getSubtaskExecutorErr := errors.New("get executor err")
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(nil, getSubtaskExecutorErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.RunStep(nil))
	require.True(t, ctrl.Satisfied())

	// GetStepExecutor meet non retryable error.
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(nil, getSubtaskExecutorErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(taskExecutor.ctx, taskExecutor.id, gomock.Any(), getSubtaskExecutorErr)
	require.NoError(t, taskExecutor.RunStep(nil))
	require.True(t, ctrl.Satisfied())

	// Init meet retryable error.
	initErr := errors.New("executor init err")
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.RunStep(nil))
	require.True(t, ctrl.Satisfied())

	// Init meet non retryable error.
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(taskExecutor.ctx, taskExecutor.id, gomock.Any(), initErr)
	require.NoError(t, taskExecutor.RunStep(nil))
	require.True(t, ctrl.Satisfied())

	// Cleanup meet retryable error.
	cleanupErr := errors.New("cleanup err")
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task.ID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(cleanupErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(true)
	require.NoError(t, taskExecutor.RunStep(nil))
	require.True(t, ctrl.Satisfied())

	// Cleanup meet non retryable error.
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task.ID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(cleanupErr)
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false)
	mockSubtaskTable.EXPECT().FailSubtask(taskExecutor.ctx, taskExecutor.id, gomock.Any(), cleanupErr)
	require.NoError(t, taskExecutor.RunStep(nil))
	require.True(t, ctrl.Satisfied())

	// subtask succeed.
	mockSubtaskTable.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	mockExtension.EXPECT().GetStepExecutor(gomock.Any()).Return(mockSubtaskExecutor, nil)
	mockSubtaskExecutor.EXPECT().Init(gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(&proto.Subtask{SubtaskBase: proto.SubtaskBase{
		ID: 1, Type: tp, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}, nil)
	mockSubtaskTable.EXPECT().StartSubtask(gomock.Any(), task.ID, "id").Return(nil)
	mockSubtaskExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskExecutor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().FinishSubtask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockSubtaskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", task.ID, proto.StepOne,
		unfinishedNormalSubtaskStates...).Return(nil, nil)
	mockSubtaskExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
	require.NoError(t, taskExecutor.RunStep(nil))
	require.True(t, ctrl.Satisfied())
}

func TestInject(t *testing.T) {
	e := &EmptyStepExecutor{}
	r := &proto.StepResource{CPU: proto.NewAllocatable(1)}
	execute.SetFrameworkInfo(e, r)
	got := e.GetResource()
	require.Equal(t, r, got)
}
