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

func setDetectParamModifyInterval(t *testing.T, newInterval time.Duration) {
	bak := DetectParamModifyInterval
	t.Cleanup(func() {
		DetectParamModifyInterval = bak
	})
	DetectParamModifyInterval = newInterval
}

// StepExecWrap wraps mock step executor and StepExecFrameworkInfo, so we can
// check modified resource.
type stepExecWrap struct {
	// methods of mock overrides base.
	*mockexecute.MockStepExecutor
	// to avoid ambiguous methods
	StepExecFrameworkInfo execute.StepExecFrameworkInfo
}

var _ execute.StepExecutor = (*stepExecWrap)(nil)

func (w *stepExecWrap) GetResource() *proto.StepResource {
	return w.StepExecFrameworkInfo.GetResource()
}

func (w *stepExecWrap) SetResource(resource *proto.StepResource) {
	w.StepExecFrameworkInfo.SetResource(resource)
}

type taskExecutorRunEnv struct {
	ctrl            *gomock.Controller
	taskTable       *mock.MockTaskTable
	stepExecutor    *stepExecWrap
	taskExecExt     *mock.MockExtension
	taskExecutor    *BaseTaskExecutor
	task1           *proto.Task
	succeedTask1    *proto.Task
	revertingTask1  *proto.Task
	pendingSubtask1 *proto.Subtask
	runningSubtask2 *proto.Subtask
}

func newTaskExecutorRunEnv(t *testing.T) *taskExecutorRunEnv {
	env := newTaskExecutorRunEnv0(t)
	// DetectParamModify loop is harder to mock for Run, so we prevent it from running.
	setDetectParamModifyInterval(t, time.Hour)
	return env
}

func newTaskExecutorRunEnv0(t *testing.T) *taskExecutorRunEnv {
	ctrl := gomock.NewController(t)
	taskTable := mock.NewMockTaskTable(ctrl)
	stepExecutor := mockexecute.NewMockStepExecutor(ctrl)
	taskExecExt := mock.NewMockExtension(ctrl)

	task1 := proto.Task{TaskBase: proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
		Type: proto.TaskTypeExample, ID: 1, Concurrency: 10}}
	taskExecutor := NewBaseTaskExecutor(context.Background(), &task1, Param{
		taskTable: taskTable,
		slotMgr:   newSlotManager(16),
		nodeRc:    proto.NodeResourceForTest,
		execID:    "id",
	})
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
		stepExecutor:   &stepExecWrap{MockStepExecutor: stepExecutor},
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
		for range 3 {
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
		for i := range 3 {
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
		for i := range 5 {
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
			for j := range s.count {
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
		e.stepExecutor.EXPECT().TaskMetaModified(gomock.Any(), newMetaTask1.Meta).Return(errors.New("some error"))
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
		e.stepExecutor.EXPECT().TaskMetaModified(gomock.Any(), newMetaTask1.Meta).Return(nil)
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
	})
}

func TestDetectAndHandleParamModify(t *testing.T) {
	newTestEnv := func(t *testing.T, dur time.Duration) *taskExecutorRunEnv {
		env := newTaskExecutorRunEnv0(t)
		setDetectParamModifyInterval(t, dur)
		return env
	}

	t.Run("loop: break on context cancel", func(t *testing.T) {
		env := newTestEnv(t, time.Hour)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		env.taskExecutor.detectAndHandleParamModifyLoop(ctx)
	})

	t.Run("loop: retry on get task fail", func(t *testing.T) {
		env := newTestEnv(t, time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for range 3 {
			env.taskTable.EXPECT().GetTaskByID(gomock.Any(), env.task1.ID).Return(nil, errors.New("some err"))
		}
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), env.task1.ID).DoAndReturn(func(context.Context, int64) (*proto.Task, error) {
			cancel()
			return nil, ctx.Err()
		})
		env.taskExecutor.detectAndHandleParamModifyLoop(ctx)
	})

	t.Run("no param modify", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), env.task1.ID).Return(env.task1, nil)
		oldTask := env.taskExecutor.task.Load()
		require.NoError(t, env.taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, oldTask, env.taskExecutor.task.Load())
	})

	t.Run("concurrency become smaller, but failed to reduce resource usage", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 4
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
		require.NoError(t, taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, oldTask, taskExecutor.task.Load())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
	})

	t.Run("concurrency become smaller, apply successfully", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 4
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).Return(nil)
		require.NoError(t, taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, &latestTask, taskExecutor.task.Load())
		require.Equal(t, 12, taskExecutor.slotMgr.availableSlots())
	})

	t.Run("concurrency become larger, but not enough slots for exchange", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		taskExecutor.slotMgr.alloc(&proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
			Type: proto.TaskTypeExample, ID: 2, Concurrency: 4})
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 14
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		require.NoError(t, taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, oldTask, taskExecutor.task.Load())
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
	})

	t.Run("concurrency become larger, but failed to notify application", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		taskExecutor.slotMgr.alloc(&proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
			Type: proto.TaskTypeExample, ID: 2, Concurrency: 4})
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 12
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
		require.NoError(t, taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, oldTask, taskExecutor.task.Load())
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
	})

	t.Run("concurrency become larger, apply successfully", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		taskExecutor.slotMgr.alloc(&proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
			Type: proto.TaskTypeExample, ID: 2, Concurrency: 4})
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 12
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).Return(nil)
		require.NoError(t, taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, &latestTask, taskExecutor.task.Load())
		require.Equal(t, 0, taskExecutor.slotMgr.availableSlots())
	})

	t.Run("task meta modified, but failed to notify", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Meta = []byte("modified")
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().TaskMetaModified(gomock.Any(), latestTask.Meta).Return(errors.New("some error"))
		require.ErrorContains(t, taskExecutor.detectAndHandleParamModify(context.Background()), "failed to apply task param modification")
		require.Equal(t, oldTask, taskExecutor.task.Load())
	})

	t.Run("task meta modified, apply successfully", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Meta = []byte("modified")
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().TaskMetaModified(gomock.Any(), latestTask.Meta).Return(nil)
		require.NoError(t, taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, &latestTask, taskExecutor.task.Load())
	})

	t.Run("both meta and concurrency modified, both failed to apply", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		taskExecutor.slotMgr.alloc(&proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
			Type: proto.TaskTypeExample, ID: 2, Concurrency: 4})
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 12
		latestTask.Meta = []byte("modified")
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
		env.stepExecutor.EXPECT().TaskMetaModified(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
		require.ErrorContains(t, taskExecutor.detectAndHandleParamModify(context.Background()), "failed to apply task param modification")
		require.Equal(t, oldTask, taskExecutor.task.Load())
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
	})

	t.Run("both meta and concurrency modified, apply concurrency success, but failed to apply meta", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		taskExecutor.slotMgr.alloc(&proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
			Type: proto.TaskTypeExample, ID: 2, Concurrency: 4})
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 12
		latestTask.Meta = []byte("modified")
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).Return(nil)
		env.stepExecutor.EXPECT().TaskMetaModified(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
		require.ErrorContains(t, taskExecutor.detectAndHandleParamModify(context.Background()), "failed to apply task param modification")
		require.Equal(t, latestTask.Concurrency, taskExecutor.task.Load().Concurrency)
		require.Empty(t, taskExecutor.task.Load().Meta)
		oldTask.Concurrency = latestTask.Concurrency
		require.Equal(t, oldTask, taskExecutor.task.Load())
		require.Equal(t, 0, taskExecutor.slotMgr.availableSlots())
	})

	t.Run("both meta and concurrency modified, apply meta success, but failed to apply concurrency", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		taskExecutor.slotMgr.alloc(&proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
			Type: proto.TaskTypeExample, ID: 2, Concurrency: 4})
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 12
		latestTask.Meta = []byte("modified")
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
		env.stepExecutor.EXPECT().TaskMetaModified(gomock.Any(), gomock.Any()).Return(nil)
		require.NoError(t, taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, []byte("modified"), taskExecutor.task.Load().Meta)
		require.Equal(t, oldTask.Concurrency, taskExecutor.task.Load().Concurrency)
		oldTask.Meta = latestTask.Meta
		require.Equal(t, oldTask, taskExecutor.task.Load())
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
	})

	t.Run("both meta and concurrency modified, both apply success", func(t *testing.T) {
		env := newTaskExecutorRunEnv0(t)
		taskExecutor := env.taskExecutor
		taskExecutor.stepExec = env.stepExecutor
		taskExecutor.slotMgr.alloc(taskExecutor.GetTaskBase())
		require.Equal(t, 6, taskExecutor.slotMgr.availableSlots())
		taskExecutor.slotMgr.alloc(&proto.TaskBase{State: proto.TaskStateRunning, Step: proto.StepOne,
			Type: proto.TaskTypeExample, ID: 2, Concurrency: 4})
		require.Equal(t, 2, taskExecutor.slotMgr.availableSlots())
		oldTask := taskExecutor.task.Load()
		latestTask := *oldTask
		latestTask.Concurrency = 12
		latestTask.Meta = []byte("modified")
		env.taskTable.EXPECT().GetTaskByID(gomock.Any(), latestTask.ID).Return(&latestTask, nil)
		env.stepExecutor.EXPECT().ResourceModified(gomock.Any(), gomock.Any()).Return(nil)
		env.stepExecutor.EXPECT().TaskMetaModified(gomock.Any(), gomock.Any()).Return(nil)
		require.NoError(t, taskExecutor.detectAndHandleParamModify(context.Background()))
		require.Equal(t, &latestTask, taskExecutor.task.Load())
		require.Equal(t, 0, taskExecutor.slotMgr.availableSlots())
	})
}

func TestCheckBalanceSubtask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSubtaskTable := mock.NewMockTaskTable(ctrl)
	mockExtension := mock.NewMockExtension(ctrl)

	ctx := context.Background()
	task := &proto.Task{TaskBase: proto.TaskBase{Step: proto.StepOne, Type: "type", ID: 1, Concurrency: 1}}
	taskExecutor := NewBaseTaskExecutor(ctx, task, Param{
		taskTable: mockSubtaskTable,
		slotMgr:   newSlotManager(16),
		nodeRc:    proto.NodeResourceForTest,
		execID:    "tidb1",
	})
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
}

func TestInject(t *testing.T) {
	e := &BaseStepExecutor{}
	r := &proto.StepResource{CPU: proto.NewAllocatable(1)}
	execute.SetFrameworkInfo(e, proto.StepOne, r)
	got := e.GetResource()
	require.Equal(t, r, got)
}
