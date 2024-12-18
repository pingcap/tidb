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
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	unfinishedNormalSubtaskStates = []any{
		proto.SubtaskStatePending, proto.SubtaskStateRunning,
	}
)

func reduceRetrySQLTimes(t *testing.T, target int) {
	retryCntBak := scheduler.RetrySQLTimes
	t.Cleanup(func() {
		scheduler.RetrySQLTimes = retryCntBak
	})
	scheduler.RetrySQLTimes = target
}

type taskExecutorRunEnv struct {
	ctrl            *gomock.Controller
	taskTable       *mock.MockTaskTable
	stepExecutor    *mockexecute.MockStepExecutor
	taskExecExt     *mock.MockExtension
	taskExecutor    *BaseTaskExecutor
	task1           *proto.Task
	succeedTask1    *proto.Task
	revertingTask1  *proto.Task
	pendingSubtask1 *proto.Subtask
	runningSubtask2 *proto.Subtask
}

func newTaskExecutorRunEnv(t *testing.T) *taskExecutorRunEnv {
	ctrl := gomock.NewController(t)
	taskTable := mock.NewMockTaskTable(ctrl)
	stepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	taskExecExt := mock.NewMockExtension(ctrl)

	task1 := proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
		Type: proto.TaskTypeExample, ID: 1, Concurrency: 10}}
	taskExecutor := NewBaseTaskExecutor(context.Background(), "id", &task1, taskTable)
	taskExecutor.Extension = taskExecExt

	t.Cleanup(func() {
		ctrl.Finish()
	})
	ReduceCheckInterval(t)

	succeedTask1 := task1
	succeedTask1.State = proto.TaskStateSucceed
	revertingTask1 := task1
	revertingTask1.State = proto.TaskStateReverting
	return &taskExecutorRunEnv{
		ctrl:           ctrl,
		taskTable:      taskTable,
		stepExecutor:   stepExecutor,
		taskExecExt:    taskExecExt,
		taskExecutor:   taskExecutor,
		task1:          &task1,
		succeedTask1:   &succeedTask1,
		revertingTask1: &revertingTask1,
		pendingSubtask1: &proto.Subtask{SubtaskBase: proto.SubtaskBase{
			ID: 1, Type: task1.Type, Step: proto.StepOne, State: proto.SubtaskStatePending, ExecID: "id"}},
		runningSubtask2: &proto.Subtask{SubtaskBase: proto.SubtaskBase{
			ID: 2, Type: task1.Type, Step: proto.StepOne, State: proto.SubtaskStateRunning, ExecID: "id"}},
	}
}

func (e *taskExecutorRunEnv) mockForCheckBalanceSubtask() {
	e.taskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(
		gomock.Any(), "id", e.task1.ID, proto.StepOne, proto.SubtaskStateRunning).
		DoAndReturn(func(context.Context, string, int64, proto.Step, ...proto.SubtaskState) ([]*proto.Subtask, error) {
			return []*proto.Subtask{{SubtaskBase: proto.SubtaskBase{ID: e.taskExecutor.currSubtaskID.Load()}}}, nil
		}).AnyTimes()
}

func TestTaskExecutorRun(t *testing.T) {
	t.Run("context done when run", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskExecutor.cancel()
		e.taskExecutor.Run(nil)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task not found when run", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, storage.ErrTaskNotFound)
		e.taskExecutor.Run(nil)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("task state is not running when run", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run(nil)
		require.True(t, e.ctrl.Satisfied())

		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.revertingTask1, nil)
		e.taskExecutor.Run(nil)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("retry on error of GetTaskByID", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, errors.New("some err"))
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(nil, errors.New("some err"))
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("get step executor failed", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(e.pendingSubtask1, nil)
		taskExecutorRegisterErr := errors.Errorf("constructor of taskExecutor for key not found")
		e.taskExecExt.EXPECT().GetStepExecutor(gomock.Any()).Return(nil, taskExecutorRegisterErr)
		e.taskTable.EXPECT().FailSubtask(gomock.Any(), e.taskExecutor.id, e.task1.ID, taskExecutorRegisterErr).Return(nil)
		// used to break the loop, below too
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run(nil)
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
		e.taskTable.EXPECT().FailSubtask(gomock.Any(), e.taskExecutor.id, e.task1.ID, initErr).Return(nil)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.succeedTask1, nil)
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("subtask scheduled away during running, keep running next subtask", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		// mock for checkBalanceSubtask, returns empty subtask list
		e.taskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "id",
			e.task1.ID, proto.StepOne, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
		// this subtask is scheduled awsy during running
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
		require.True(t, e.ctrl.Satisfied())
	})

	t.Run("no subtask to run, should exit the loop after some time", func(t *testing.T) {
		e := newTaskExecutorRunEnv(t)
		e.taskTable.EXPECT().GetTaskByID(gomock.Any(), e.task1.ID).Return(e.task1, nil).Times(8)
		e.taskTable.EXPECT().GetFirstSubtaskInStates(gomock.Any(), "id", e.task1.ID, proto.StepOne,
			unfinishedNormalSubtaskStates...).Return(nil, nil).Times(8)
		e.taskExecutor.Run(nil)
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
		e.taskExecutor.Run(nil)
		require.True(t, e.ctrl.Satisfied())
	})
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

	bak := checkBalanceSubtaskInterval
	retryIntBak := scheduler.RetrySQLInterval
	t.Cleanup(func() {
		checkBalanceSubtaskInterval = bak
		scheduler.RetrySQLInterval = retryIntBak
	})
	checkBalanceSubtaskInterval = 100 * time.Millisecond
	scheduler.RetrySQLInterval = time.Millisecond

	t.Run("context cancelled", func(t *testing.T) {
		// context canceled
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		taskExecutor.checkBalanceSubtask(canceledCtx)
	})

	t.Run("subtask scheduled away", func(t *testing.T) {
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return(nil, errors.New("error"))
		mockSubtaskTable.EXPECT().GetSubtasksByExecIDAndStepAndStates(gomock.Any(), "tidb1",
			task.ID, task.Step, proto.SubtaskStateRunning).Return([]*proto.Subtask{}, nil)
		runCtx, cancelCause := context.WithCancelCause(ctx)
		taskExecutor.mu.runtimeCancel = cancelCause
		require.NoError(t, runCtx.Err())
		taskExecutor.checkBalanceSubtask(ctx)
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
		taskExecutor.checkBalanceSubtask(ctx)
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
		taskExecutor.checkBalanceSubtask(ctx)
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
		taskExecutor.checkBalanceSubtask(ctx)
		require.True(t, ctrl.Satisfied())
	})
}

func TestInject(t *testing.T) {
	e := &EmptyStepExecutor{}
	r := &proto.StepResource{CPU: proto.NewAllocatable(1)}
	execute.SetFrameworkInfo(e, proto.StepOne, r)
	got := e.GetResource()
	require.Equal(t, r, got)
}
