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
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

<<<<<<< HEAD
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
=======
func TestTaskExecutorRun(t *testing.T) {
	t.Run("context done when run", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskExecutor.cancel()
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task not found when run", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task state is not running when run", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())

		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.revertingTask1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task state become 'modifying' when run, keeps running", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		modifyingTask1 := *e.task1
		modifyingTask1.State = proto.TaskStateModifying
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&modifyingTask1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(nil, nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("retry on error of GetTaskByID", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, errors.New("some err"))
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, errors.New("some err"))
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("retry on error of GetFirstSubtaskInStates", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		for i := 0; i < 3; i++ {
			e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
			e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
				unfinishedNormalSubtaskStates...).Return(nil, errors.New("some err"))
		}
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("get step executor failed", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		taskExecutorRegisterErr := errors.Errorf("constructor of taskExecutor for key not found")
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(nil, taskExecutorRegisterErr)
		e.taskTable.EXPECT().FailSubtask(gomock.Any(), e.taskExecutor.execID, e.task1.ID, taskExecutorRegisterErr).Return(nil)
		// used to break the loop, below too
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("non retryable step executor Init error", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		initErr := errors.New("init error")
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
		e.taskExecExt.EXPECT().IsRetryableError(gomock.Any()).Return(false)
		e.taskTable.EXPECT().FailSubtask(gomock.Any(), e.taskExecutor.execID, e.task1.ID, initErr).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("retryable step executor Init error", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		initErr := errors.New("init error")
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(initErr)
		e.taskExecExt.EXPECT().IsRetryableError(gomock.Any()).Return(true)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("run one subtask failed with non-retryable error", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()

		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		runSubtaskErr := errors.New("run subtask error")
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
		e.taskExecExt.EXPECT().IsRetryableError(gomock.Any()).Return(false)
		e.taskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", e.task1.ID, proto.SubtaskStateFailed, gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("run subtask panic, fail the entire task", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()

		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), e.pendingSubtask1).DoAndReturn(
			func(context.Context, *proto.Subtask) error {
				panic("run subtask panic")
			},
		)
		e.taskTable.EXPECT().FailSubtask(gomock.Any(), "id", e.task1.ID, gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, _ int64, err error) error {
				require.ErrorContains(t, err, "run subtask panic")
				return nil
			},
		)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("run one subtask failed with retryable error, success after retry 3 times", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()

		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		runSubtaskErr := errors.New("run subtask error")
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
		e.taskExecExt.EXPECT().IsRetryableError(gomock.Any()).Return(true)
		// already started by prev StartSubtask
		runningSubtask := *e.pendingSubtask1
		runningSubtask.State = proto.SubtaskStateRunning
		for i := 0; i < 3; i++ {
			e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
			e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
				unfinishedNormalSubtaskStates...).Return(&runningSubtask, nil)
			e.stepExecutor.EXPECT().GetStep().Return(proto.StepOne)
			e.taskExecExt.EXPECT().IsIdempotent(gomock.Any()).Return(true)
			if i < 2 {
				e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(runSubtaskErr)
				e.taskExecExt.EXPECT().IsRetryableError(gomock.Any()).Return(true)
			} else {
				e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
				e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", e.pendingSubtask1.ID, gomock.Any()).Return(nil)
			}
		}
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("subtask scheduled away during running, keep running next subtask", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		// mock for checkBalanceSubtask, returns empty subtask list
		e.taskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id",
			e.task1.ID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
		// this subtask is scheduled away during running
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, subtask *proto.Subtask) error {
			<-ctx.Done()
			return ctx.Err()
		})
		e.taskExecExt.EXPECT().IsRetryableError(gomock.Any()).Return(true)
		// keep running next subtask
		nextSubtask := &proto.Subtask{SubtaskBase: proto.SubtaskBase{
			ID: 2, Type: e.task1.Type, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(nextSubtask, nil)
		e.stepExecutor.EXPECT().GetStep().Return(proto.StepOne)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), nextSubtask.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), nextSubtask).Return(nil)
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", nextSubtask.ID, gomock.Any()).Return(nil)
		// exit
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("run one subtask success", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("run subtasks one by one, and exit due to no subtask to run for a while", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		for i := 0; i < 5; i++ {
			subtaskID := int64(i + 1)
			e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
			theSubtask := &proto.Subtask{SubtaskBase: proto.SubtaskBase{
				ID: subtaskID, Type: e.task1.Type, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}
			e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
				unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
			if i == 0 {
				e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
				e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
			} else {
				e.stepExecutor.EXPECT().GetStep().Return(proto.StepOne)
			}
			e.taskTable.EXPECT().StartSubtask(gomock.Any(), subtaskID, "id").Return(nil)
			e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), theSubtask).Return(nil)
			e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtaskID, gomock.Any()).Return(nil)
		}
		// exit due to no subtask to run for a while
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil).Times(8)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(nil, nil).Times(8)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("run subtasks step by step", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		idAlloc := int64(1)
		var currStepExecStep proto.Step
		for i, s := range []struct {
			step  proto.Step
			count int
		}{
			{proto.StepOne, 5},
			{proto.StepTwo, 1},
			{proto.StepThree, 3},
		} {
			taskOfStep := *e.task1
			taskOfStep.Step = s.step
			for j := 0; j < s.count; j++ {
				subtaskID := idAlloc
				idAlloc++
				e.taskTable.EXPECT().GetTaskByID(gomock.Any(), taskOfStep.ID).Return(&taskOfStep, nil)
				theSubtask := &proto.Subtask{SubtaskBase: proto.SubtaskBase{
					ID: subtaskID, Type: taskOfStep.Type, Step: s.step, State: proto.SubtaskStatePending, ExecID: "id"}}
				e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", taskOfStep.ID, s.step,
					unfinishedNormalSubtaskStates...).Return(theSubtask, nil)
				if j == 0 {
					if i != 0 {
						e.stepExecutor.EXPECT().GetStep().Return(currStepExecStep)
						e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
					}
					e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
					e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
					currStepExecStep = s.step
				} else {
					e.stepExecutor.EXPECT().GetStep().Return(currStepExecStep)
				}
				e.taskTable.EXPECT().StartSubtask(gomock.Any(), subtaskID, "id").Return(nil)
				e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), theSubtask).Return(nil)
				e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtaskID, gomock.Any()).Return(nil)
			}
		}
		// end the loop
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("step executor cleanup failed, keeps running", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), e.pendingSubtask1).Return(nil)
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", int64(1), gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		step2Subtask := &proto.Subtask{SubtaskBase: proto.SubtaskBase{
			ID: 2, Type: e.task1.Type, Step: proto.StepTwo, State: proto.SubtaskStatePending, ExecID: "id"}}
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(step2Subtask, nil)
		e.stepExecutor.EXPECT().GetStep().Return(e.pendingSubtask1.Step)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(errors.New("some error"))
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), step2Subtask.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), step2Subtask).Return(nil)
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", step2Subtask.ID, gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(errors.New("some error 2"))
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("run previous left non-idempotent subtask in running state, fail it.", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		subtaskID := int64(2)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.runningSubtask2, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskExecExt.EXPECT().IsIdempotent(gomock.Any()).Return(false)
		e.taskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", subtaskID, proto.SubtaskStateFailed, ErrNonIdempotentSubtask).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("run previous left idempotent subtask in running state, run it again.", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		subtaskID := int64(2)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.runningSubtask2, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		// first round of the run loop
		e.taskExecExt.EXPECT().IsIdempotent(gomock.Any()).Return(true)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtaskID, gomock.Any()).Return(nil)
		// second round of the run loop
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("subtask cancelled during running", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *proto.Subtask) error {
			e.taskExecutor.CancelRunningSubtask()
			return ErrCancelSubtask
		})
		e.taskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "id", e.task1.ID, proto.SubtaskStateCanceled, nil).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task executor cancelled for graceful shutdown during subtask running", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *proto.Subtask) error {
			e.taskExecutor.Cancel()
			return context.Canceled
		})
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("subtask scheduled away right before we start it", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(storage.ErrSubtaskNotFound)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("start subtask failed after retry, will try again", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		reduceRetrySQLTimes(t, 1)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(errors.New("some error"))
		// second round of the run loop
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.stepExecutor.EXPECT().GetStep().Return(proto.StepOne)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), e.pendingSubtask1).Return(nil)
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", e.pendingSubtask1.ID, gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("no subtask to run, should exit the loop after some time", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil).Times(8)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(nil, nil).Times(8)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("no-subtask check counter should be reset after a subtask is run.", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		// loop 4 times without subtask, then 1 time with subtask.
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil).Times(4)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(nil, nil).Times(4)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).Return(nil)
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", e.pendingSubtask1.ID, gomock.Any()).Return(nil)
		// loop for 8 times without subtask, and exit
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil).Times(8)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(nil, nil).Times(8)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task concurrency became smaller", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		concurrencyIs4Task1 := *e.task1
		concurrencyIs4Task1.Concurrency = 4
		concurrencyIs2Task1 := *e.task1
		concurrencyIs2Task1.Concurrency = 2
		slotMgr := e.taskExecutor.slotMgr
		slotMgr.alloc(&e.task1.TaskBase)
		require.Equal(t, 6, slotMgr.availableSlots())
		// without step executor, concurrency modified to 4
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&concurrencyIs4Task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne, unfinishedNormalSubtaskStates...).DoAndReturn(
			func(context.Context, string, int64, proto.Step, ...proto.SubtaskState) (*proto.Subtask, error) {
				require.Equal(t, 12, slotMgr.availableSlots())
				require.Equal(t, 4, e.taskExecutor.GetTaskBase().Concurrency)
				return nil, nil
			})
		// with step executor, concurrency modified to 2
		e.mockForCheckBalanceSubtask()
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&concurrencyIs4Task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), e.pendingSubtask1).DoAndReturn(func(context.Context, *proto.Subtask) error {
			require.EqualValues(t, 4, e.stepExecutor.GetResource().CPU.Capacity())
			return nil
		})
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", e.pendingSubtask1.ID, gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&concurrencyIs2Task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne, unfinishedNormalSubtaskStates...).DoAndReturn(
			func(context.Context, string, int64, proto.Step, ...proto.SubtaskState) (*proto.Subtask, error) {
				require.Equal(t, 14, slotMgr.availableSlots())
				require.Equal(t, 2, e.taskExecutor.GetTaskBase().Concurrency)
				require.EqualValues(t, 2, e.stepExecutor.GetResource().CPU.Capacity())
				return nil, nil
			})
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task concurrency become larger, but not enough slots for exchange", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		concurrencyIs14Task1 := *e.task1
		concurrencyIs14Task1.Concurrency = 14
		slotMgr := e.taskExecutor.slotMgr
		slotMgr.alloc(&e.task1.TaskBase)
		// alloc for another task executor
		slotMgr.alloc(&proto.TaskBase{ID: 2, Concurrency: 4})
		require.Equal(t, 2, slotMgr.availableSlots())
		// without step executor, concurrency modified to 4
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&concurrencyIs14Task1, nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
		require.Equal(t, 2, slotMgr.availableSlots())
	})

	t.Run("task concurrency become larger, exchange success", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		concurrencyIs14Task1 := *e.task1
		concurrencyIs14Task1.Concurrency = 14
		slotMgr := e.taskExecutor.slotMgr
		slotMgr.alloc(&e.task1.TaskBase)
		require.Equal(t, 6, slotMgr.availableSlots())
		e.mockForCheckBalanceSubtask()
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), e.pendingSubtask1).DoAndReturn(func(context.Context, *proto.Subtask) error {
			require.EqualValues(t, 10, e.stepExecutor.GetResource().CPU.Capacity())
			return nil
		})
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", e.pendingSubtask1.ID, gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&concurrencyIs14Task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne, unfinishedNormalSubtaskStates...).DoAndReturn(
			func(context.Context, string, int64, proto.Step, ...proto.SubtaskState) (*proto.Subtask, error) {
				require.Equal(t, 2, slotMgr.availableSlots())
				require.Equal(t, 14, e.taskExecutor.GetTaskBase().Concurrency)
				require.EqualValues(t, 14, e.stepExecutor.GetResource().CPU.Capacity())
				return nil, nil
			})
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task meta modified, no step executor", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		newMetaTask1 := *e.task1
		newMetaTask1.Meta = []byte("modified")
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&newMetaTask1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(nil, nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task meta/concurrency modified, with step executor, same step, notify failed, will recreate step executor", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		slotMgr := e.taskExecutor.slotMgr
		slotMgr.alloc(&e.task1.TaskBase)
		require.Equal(t, 6, slotMgr.availableSlots())
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), e.pendingSubtask1).DoAndReturn(func(context.Context, *proto.Subtask) error {
			require.EqualValues(t, 10, e.stepExecutor.GetResource().CPU.Capacity())
			require.Nil(t, e.taskExecutor.task.Load().Meta)
			return nil
		})
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", e.pendingSubtask1.ID, gomock.Any()).Return(nil)
		// second round
		newMetaTask1 := *e.task1
		newMetaTask1.Concurrency = 3
		newMetaTask1.Meta = []byte("modified")
		subtask2 := &proto.Subtask{SubtaskBase: proto.SubtaskBase{
			ID: 2, Type: e.task1.Type, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&newMetaTask1, nil)
		e.stepExecutor.EXPECT().GetStep().Return(e.task1.Step)
		e.stepExecutor.EXPECT().TaskMetaModified(&newMetaTask1).Return(errors.New("some error"))
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).DoAndReturn(func(context.Context) error {
			// still the old one
			require.Equal(t, 10, e.taskExecutor.GetTaskBase().Concurrency)
			require.Nil(t, e.taskExecutor.task.Load().Meta)
			return nil
		})
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&newMetaTask1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(subtask2, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), subtask2.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), subtask2).DoAndReturn(func(context.Context, *proto.Subtask) error {
			require.Equal(t, 13, slotMgr.availableSlots())
			require.Equal(t, 3, e.taskExecutor.GetTaskBase().Concurrency)
			require.Equal(t, []byte("modified"), e.taskExecutor.task.Load().Meta)
			require.EqualValues(t, 3, e.stepExecutor.GetResource().CPU.Capacity())
			return nil
		})
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtask2.ID, gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task meta/concurrency modified, with step executor, same step, notify success", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		slotMgr := e.taskExecutor.slotMgr
		slotMgr.alloc(&e.task1.TaskBase)
		require.Equal(t, 6, slotMgr.availableSlots())
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), e.pendingSubtask1).DoAndReturn(func(context.Context, *proto.Subtask) error {
			require.EqualValues(t, 10, e.stepExecutor.GetResource().CPU.Capacity())
			require.Nil(t, e.taskExecutor.task.Load().Meta)
			return nil
		})
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", e.pendingSubtask1.ID, gomock.Any()).Return(nil)
		// second round
		newMetaTask1 := *e.task1
		newMetaTask1.Concurrency = 3
		newMetaTask1.Meta = []byte("modified")
		subtask2 := &proto.Subtask{SubtaskBase: proto.SubtaskBase{
			ID: 2, Type: e.task1.Type, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}}
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&newMetaTask1, nil)
		e.stepExecutor.EXPECT().GetStep().Return(e.task1.Step)
		e.stepExecutor.EXPECT().TaskMetaModified(&newMetaTask1).Return(nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(subtask2, nil)
		e.stepExecutor.EXPECT().GetStep().Return(e.task1.Step)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), subtask2.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), subtask2).DoAndReturn(func(context.Context, *proto.Subtask) error {
			require.Equal(t, 13, slotMgr.availableSlots())
			require.Equal(t, 3, e.taskExecutor.GetTaskBase().Concurrency)
			require.Equal(t, []byte("modified"), e.taskExecutor.task.Load().Meta)
			require.EqualValues(t, 3, e.stepExecutor.GetResource().CPU.Capacity())
			return nil
		})
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", subtask2.ID, gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task meta/concurrency modified, with step executor, but also switch to next step", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.mockForCheckBalanceSubtask()
		slotMgr := e.taskExecutor.slotMgr
		slotMgr.alloc(&e.task1.TaskBase)
		require.Equal(t, 6, slotMgr.availableSlots())
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), e.pendingSubtask1.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), e.pendingSubtask1).DoAndReturn(func(context.Context, *proto.Subtask) error {
			require.EqualValues(t, 10, e.stepExecutor.GetResource().CPU.Capacity())
			require.Nil(t, e.taskExecutor.task.Load().Meta)
			return nil
		})
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", e.pendingSubtask1.ID, gomock.Any()).Return(nil)
		// second round
		newMetaTask1 := *e.task1
		newMetaTask1.Concurrency = 6
		newMetaTask1.Meta = []byte("modified")
		newMetaTask1.Step = proto.StepTwo
		step2Subtask2 := &proto.Subtask{SubtaskBase: proto.SubtaskBase{
			ID: 2, Type: e.task1.Type, Step: proto.StepTwo, State: proto.SubtaskStatePending, ExecID: "id"}}
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(&newMetaTask1, nil)
		e.stepExecutor.EXPECT().GetStep().Return(e.task1.Step)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepTwo,
			unfinishedNormalSubtaskStates...).Return(step2Subtask2, nil)
		e.stepExecutor.EXPECT().GetStep().Return(e.task1.Step)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(e.stepExecutor, nil)
		e.stepExecutor.EXPECT().Init(gomock.Any()).Return(nil)
		e.taskTable.EXPECT().StartSubtask(gomock.Any(), step2Subtask2.ID, "id").Return(nil)
		e.stepExecutor.EXPECT().RunSubtask(gomock.Any(), step2Subtask2).DoAndReturn(func(context.Context, *proto.Subtask) error {
			require.Equal(t, 10, slotMgr.availableSlots())
			require.Equal(t, 6, e.taskExecutor.GetTaskBase().Concurrency)
			require.Equal(t, []byte("modified"), e.taskExecutor.task.Load().Meta)
			require.EqualValues(t, 6, e.stepExecutor.GetResource().CPU.Capacity())
			return nil
		})
		e.taskTable.EXPECT().FinishSubtask(gomock.Any(), "id", step2Subtask2.ID, gomock.Any()).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.stepExecutor.EXPECT().Cleanup(gomock.Any()).Return(nil)
		e.taskExecutor.Run()
		require.True(t, e.ctrl.Satisfied())
>>>>>>> c199ddfcdf9 (disttask: cancel subtask context if scheduled away (#58615))
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

<<<<<<< HEAD
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
=======
	t.Run("context cancelled", func(t *testing.T) {
		// context canceled
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		taskExecutor.checkBalanceSubtask(canceledCtx, nil)
	})

	t.Run("subtask scheduled away", func(t *testing.T) {
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return(nil, errors.New("error"))
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
		runCtx, cancel := context.WithCancel(ctx)
		require.NoError(t, runCtx.Err())
		taskExecutor.checkBalanceSubtask(ctx, cancel)
		require.ErrorIs(t, runCtx.Err(), context.Canceled)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("non-idempotent running subtask", func(t *testing.T) {
		subtasks := []*proto.Subtask{{SubtaskBase: proto.SubtaskBase{ID: 1, ExecID: "tidb1"}}}
		// in-idempotent running subtask
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return(subtasks, nil)
		mockExtension.EXPECT().IsIdempotent(subtasks[0]).Return(false)
		mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "tidb1",
			subtasks[0].ID, proto.SubtaskStateFailed, ErrNonIdempotentSubtask).Return(nil)
		taskExecutor.checkBalanceSubtask(ctx, nil)
		require.True(t, ctrl.Satisfied())

		// if we failed to change state of non-idempotent subtask, will retry
		reduceRetrySQLTimes(t, 1)
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return(subtasks, nil)
		mockExtension.EXPECT().IsIdempotent(subtasks[0]).Return(false)
		mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "tidb1",
			subtasks[0].ID, proto.SubtaskStateFailed, ErrNonIdempotentSubtask).
			Return(errors.New("some error"))
		// retry part
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return(subtasks, nil)
		mockExtension.EXPECT().IsIdempotent(subtasks[0]).Return(false)
		mockSubtaskTable.EXPECT().UpdateSubtaskStateAndError(gomock.Any(), "tidb1",
			subtasks[0].ID, proto.SubtaskStateFailed, ErrNonIdempotentSubtask).Return(nil)
		taskExecutor.checkBalanceSubtask(ctx, nil)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("current running subtask is skipped", func(t *testing.T) {
		require.Zero(t, taskExecutor.currSubtaskID.Load())
		taskExecutor.currSubtaskID.Store(1)
		subtasks := []*proto.Subtask{{SubtaskBase: proto.SubtaskBase{ID: 1, ExecID: "tidb1"}}, {SubtaskBase: proto.SubtaskBase{ID: 2, ExecID: "tidb1"}}}
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return(subtasks, nil)
		mockExtension.EXPECT().IsIdempotent(gomock.Any()).Return(true)
		mockSubtaskTable.EXPECT().RunningSubtasksBack2Pending(gomock.Any(), []*proto.SubtaskBase{{ID: 2, ExecID: "tidb1"}}).Return(nil)
		// used to break the loop
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return(nil, nil)
		taskExecutor.checkBalanceSubtask(ctx, nil)
		require.True(t, ctrl.Satisfied())
	})
>>>>>>> c199ddfcdf9 (disttask: cancel subtask context if scheduled away (#58615))
}

func TestInject(t *testing.T) {
	e := &EmptyStepExecutor{}
	r := &proto.StepResource{CPU: proto.NewAllocatable(1)}
	execute.SetFrameworkInfo(e, r)
	got := e.GetResource()
	require.Equal(t, r, got)
}

func throwError() error {
	return errors.New("mock error")
}

func callOnError(taskExecutor *BaseTaskExecutor) {
	taskExecutor.onError(throwError())
}

func throwErrorNoTrace() error {
	return errors.NewNoStackError("mock error")
}

func callOnErrorNoTrace(taskExecutor *BaseTaskExecutor) {
	taskExecutor.onError(throwErrorNoTrace())
}

func TestExecutorOnErrorLog(t *testing.T) {
	taskExecutor := &BaseTaskExecutor{}

	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	taskExecutor.logger = observedLogger

	{
		callOnError(taskExecutor)
		require.GreaterOrEqual(t, observedLogs.Len(), 1)
		errLog := observedLogs.TakeAll()[0]
		contextMap := errLog.ContextMap()
		require.Contains(t, contextMap, "error stack")
		errStack := contextMap["error stack"]
		require.IsType(t, "", errStack)
		errStackStr := errStack.(string)
		require.Regexpf(t, `mock error[\n\t ]*`+
			`github\.com/pingcap/tidb/pkg/disttask/framework/taskexecutor\.throwError`,
			errStackStr,
			"got err stack: %s", errStackStr)
	}

	{
		callOnErrorNoTrace(taskExecutor)
		require.GreaterOrEqual(t, observedLogs.Len(), 1)
		errLog := observedLogs.TakeAll()[0]
		contextMap := errLog.ContextMap()
		require.NotContains(t, contextMap, "error stack")
	}
}
