// Copyright 2024 PingCAP, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	schmock "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func createScheduler(task *proto.Task, allocatedSlots bool, taskMgr TaskManager, ctrl *gomock.Controller) *BaseScheduler {
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "scheduler")
	nodeMgr := NewNodeManager()
	sch := NewBaseScheduler(ctx, task, Param{
		taskMgr:        taskMgr,
		nodeMgr:        nodeMgr,
		slotMgr:        newSlotManager(),
		allocatedSlots: allocatedSlots,
	})
	return sch
}

func TestSchedulerOnNextStage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)
	schExt := schmock.NewMockExtension(ctrl)
	task := proto.Task{
		TaskBase: proto.TaskBase{
			ID:    1,
			State: proto.TaskStatePending,
			Step:  proto.StepInit,
		},
	}
	cloneTask := task
	sch := createScheduler(&cloneTask, true, taskMgr, ctrl)
	sch.Extension = schExt

	// test next step is done
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepDone)
	schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("done err"))
	require.ErrorContains(t, sch.Switch2NextStep(), "done err")
	require.True(t, ctrl.Satisfied())
	require.Equal(t, proto.StepInit, sch.GetTask().Step)
	taskClone2 := task
	sch.task.Store(&taskClone2)
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepDone)
	schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	taskMgr.EXPECT().SucceedTask(gomock.Any(), gomock.Any()).Return(nil)
	require.NoError(t, sch.Switch2NextStep())
	require.True(t, ctrl.Satisfied())

	// GetEligibleInstances err
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, errors.New("GetEligibleInstances err"))
	require.ErrorContains(t, sch.Switch2NextStep(), "GetEligibleInstances err")
	require.True(t, ctrl.Satisfied())
	// GetEligibleInstances no instance
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)
	require.ErrorContains(t, sch.Switch2NextStep(), "no available TiDB node to dispatch subtasks")
	require.True(t, ctrl.Satisfied())

	serverNodes := []string{":4000"}
	// OnNextSubtasksBatch err
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(serverNodes, nil)
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("OnNextSubtasksBatch err"))
	schExt.EXPECT().IsRetryableErr(gomock.Any()).Return(true)
	require.ErrorContains(t, sch.Switch2NextStep(), "OnNextSubtasksBatch err")
	require.True(t, ctrl.Satisfied())

	bak := kv.TxnTotalSizeLimit.Load()
	t.Cleanup(func() {
		kv.TxnTotalSizeLimit.Store(bak)
	})

	// dispatch in batch
	subtaskMetas := [][]byte{
		[]byte(`{"xx": "1"}`),
		[]byte(`{"xx": "2"}`),
	}
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(serverNodes, nil)
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(subtaskMetas, nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, nil)
	taskMgr.EXPECT().SwitchTaskStepInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	kv.TxnTotalSizeLimit.Store(1)
	require.NoError(t, sch.Switch2NextStep())
	require.True(t, ctrl.Satisfied())
	// met unstable subtasks
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(serverNodes, nil)
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(subtaskMetas, nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, nil)
	taskMgr.EXPECT().SwitchTaskStepInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.Annotatef(storage.ErrUnstableSubtasks, "expected %d, got %d",
			2, 100))
	kv.TxnTotalSizeLimit.Store(1)
	startTime := time.Now()
	err := sch.Switch2NextStep()
	require.ErrorIs(t, err, storage.ErrUnstableSubtasks)
	require.ErrorContains(t, err, "expected 2, got 100")
	require.WithinDuration(t, startTime, time.Now(), 10*time.Second)
	require.True(t, ctrl.Satisfied())

	// dispatch in one txn
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepOne)
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(serverNodes, nil)
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(subtaskMetas, nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, nil)
	taskMgr.EXPECT().SwitchTaskStep(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	kv.TxnTotalSizeLimit.Store(config.DefTxnTotalSizeLimit)
	require.NoError(t, sch.Switch2NextStep())
	require.True(t, ctrl.Satisfied())
}

func TestGetEligibleNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	mockSch := mock.NewMockScheduler(ctrl)
	mockSch.EXPECT().GetTask().Return(&proto.Task{TaskBase: proto.TaskBase{ID: 1}}).AnyTimes()

	mockSch.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock err"))
	_, err := getEligibleNodes(ctx, mockSch, []string{":4000"})
	require.ErrorContains(t, err, "mock err")
	require.True(t, ctrl.Satisfied())

	mockSch.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return([]string{":4000"}, nil)
	nodes, err := getEligibleNodes(ctx, mockSch, []string{":4000", ":4001"})
	require.NoError(t, err)
	require.Equal(t, []string{":4000"}, nodes)
	require.True(t, ctrl.Satisfied())

	mockSch.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil)
	nodes, err = getEligibleNodes(ctx, mockSch, []string{":4000", ":4001"})
	require.NoError(t, err)
	require.Equal(t, []string{":4000", ":4001"}, nodes)
	require.True(t, ctrl.Satisfied())
}

func TestSchedulerIsStepSucceed(t *testing.T) {
	s := &BaseScheduler{}
	require.True(t, s.isStepSucceed(nil))
	require.True(t, s.isStepSucceed(map[proto.SubtaskState]int64{}))
	require.True(t, s.isStepSucceed(map[proto.SubtaskState]int64{
		proto.SubtaskStateSucceed: 1,
	}))
	for _, state := range []proto.SubtaskState{
		proto.SubtaskStateCanceled,
		proto.SubtaskStateFailed,
	} {
		require.False(t, s.isStepSucceed(map[proto.SubtaskState]int64{
			state: 1,
		}))
	}
}

func TestSchedulerNotAllocateSlots(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)

	// scheduler not allocated slots, task from paused to resuming. Should exit the scheduler.
	task := proto.Task{
		TaskBase: proto.TaskBase{
			ID:          int64(1),
			Concurrency: 1,
			Type:        proto.TaskTypeExample,
			State:       proto.TaskStatePaused,
		},
	}
	cloneTask := task
	sch := createScheduler(&cloneTask, false, taskMgr, ctrl)
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.TaskBase, error) {
		cloneTask.State = proto.TaskStateResuming
		return &cloneTask.TaskBase, nil
	})
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())

	// scheduler not allocated slots, task from paused to running. Should exit the scheduler.
	task.State = proto.TaskStatePaused
	cloneTask = task
	sch = createScheduler(&cloneTask, false, taskMgr, ctrl)
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.TaskBase, error) {
		cloneTask.State = proto.TaskStateRunning
		return &cloneTask.TaskBase, nil
	})
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())

	// scheduler not allocated slots, but won't exit the scheduler.
	task.State = proto.TaskStateReverting
	cloneTask = task

	sch = createScheduler(&cloneTask, false, taskMgr, ctrl)
	schExt := schmock.NewMockExtension(ctrl)
	sch.Extension = schExt
	schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.TaskBase, error) {
		return &cloneTask.TaskBase, nil
	})

	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, cloneTask.Step).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStatePending: 0,
		proto.SubtaskStateRunning: 0}, nil)
	taskMgr.EXPECT().RevertedTask(gomock.Any(), cloneTask.ID).Return(nil)
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.TaskBase, error) {
		cloneTask.State = proto.TaskStateReverted
		return &cloneTask.TaskBase, nil
	})
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())

	task.State = proto.TaskStatePausing
	cloneTask = task
	sch = createScheduler(&cloneTask, false, taskMgr, ctrl)
	schExt = schmock.NewMockExtension(ctrl)
	sch.Extension = schExt
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.TaskBase, error) {
		return &cloneTask.TaskBase, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, cloneTask.Step).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStatePending: 0,
		proto.SubtaskStateRunning: 0}, nil)
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.TaskBase, error) {
		cloneTask.State = proto.TaskStatePaused
		return &cloneTask.TaskBase, nil
	})
	taskMgr.EXPECT().PausedTask(gomock.Any(), cloneTask.ID).Return(nil)
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())
}

func TestSchedulerRefreshTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)

	ctx := context.Background()
	task := proto.Task{
		TaskBase: proto.TaskBase{
			ID:    1,
			State: proto.TaskStateRunning,
			Step:  proto.StepOne,
		},
		Meta: []byte("aaa"),
	}
	schTask := task
	scheduler := NewBaseScheduler(ctx, &schTask, Param{taskMgr: taskMgr})

	// get task base error
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), task.ID).Return(nil, errors.New("get task err"))
	require.ErrorContains(t, scheduler.refreshTaskIfNeeded(), "get task err")
	require.True(t, ctrl.Satisfied())
	// state/step not changed, no need to refresh
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), task.ID).Return(&task.TaskBase, nil)
	require.NoError(t, scheduler.refreshTaskIfNeeded())
	require.Equal(t, *scheduler.GetTask(), task)
	require.True(t, ctrl.Satisfied())
	// get task by id failed
	tmpTask := task
	tmpTask.State = proto.TaskStateCancelling
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), task.ID).Return(&tmpTask.TaskBase, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(nil, errors.New("get task by id err"))
	require.ErrorContains(t, scheduler.refreshTaskIfNeeded(), "get task by id err")
	require.Equal(t, *scheduler.GetTask(), task)
	require.True(t, ctrl.Satisfied())
	// state changed
	tmpTask = task
	tmpTask.State = proto.TaskStateCancelling
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), task.ID).Return(&tmpTask.TaskBase, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(&tmpTask, nil)
	require.NoError(t, scheduler.refreshTaskIfNeeded())
	require.Equal(t, *scheduler.GetTask(), tmpTask)
	require.True(t, ctrl.Satisfied())
	// step changed
	scheduler.task.Store(&schTask) // revert
	tmpTask = task
	tmpTask.Step = proto.StepTwo
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), task.ID).Return(&tmpTask.TaskBase, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(&tmpTask, nil)
	require.NoError(t, scheduler.refreshTaskIfNeeded())
	require.Equal(t, *scheduler.GetTask(), tmpTask)
	require.True(t, ctrl.Satisfied())
}

func TestSchedulerMaintainTaskFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)
	schExt := schmock.NewMockExtension(ctrl)
	schExt.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(func(base *proto.TaskBase) proto.Step {
		switch base.Step {
		case proto.StepInit:
			return proto.StepOne
		default:
			return proto.StepDone
		}
	}).AnyTimes()
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return([]string{":4000"}, nil).AnyTimes()

	ctx := context.Background()
	task := proto.Task{
		TaskBase: proto.TaskBase{
			ID:    1,
			State: proto.TaskStatePending,
			Step:  proto.StepInit,
		},
		Meta: []byte("aaa"),
	}
	schTask := task
	scheduler := NewBaseScheduler(ctx, &schTask, Param{
		taskMgr: taskMgr,
		nodeMgr: newNodeManager(":4000"),
		slotMgr: newSlotManager(),
	})
	scheduler.Extension = schExt

	t.Run("test onPausing", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().PausedTask(gomock.Any(), task.ID).Return(fmt.Errorf("pause err"))
		require.ErrorContains(t, scheduler.onPausing(), "pause err")
		require.Equal(t, *scheduler.GetTask(), task)
		require.True(t, ctrl.Satisfied())

		// pause task successfully
		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().PausedTask(gomock.Any(), task.ID).Return(nil)
		require.NoError(t, scheduler.onPausing())
		tmpTask := task
		tmpTask.State = proto.TaskStatePaused
		require.Equal(t, *scheduler.GetTask(), tmpTask)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test onResuming", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().ResumedTask(gomock.Any(), task.ID).Return(fmt.Errorf("resume err"))
		require.ErrorContains(t, scheduler.onResuming(), "resume err")
		require.Equal(t, *scheduler.GetTask(), task)
		require.True(t, ctrl.Satisfied())

		// resume task successfully
		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().ResumedTask(gomock.Any(), task.ID).Return(nil)
		require.NoError(t, scheduler.onResuming())
		tmpTask := task
		tmpTask.State = proto.TaskStateRunning
		require.Equal(t, *scheduler.GetTask(), tmpTask)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test onReverting", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		taskMgr.EXPECT().RevertedTask(gomock.Any(), task.ID).Return(fmt.Errorf("reverted err"))
		require.ErrorContains(t, scheduler.onReverting(), "reverted err")
		require.Equal(t, *scheduler.GetTask(), task)
		require.True(t, ctrl.Satisfied())

		// revert task successfully
		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		taskMgr.EXPECT().RevertedTask(gomock.Any(), task.ID).Return(nil)
		require.NoError(t, scheduler.onReverting())
		tmpTask := task
		tmpTask.State = proto.TaskStateReverted
		require.Equal(t, *scheduler.GetTask(), tmpTask)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test switch2NextStep", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		// retryable plan error, nothing changes
		schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("plan err"))
		schExt.EXPECT().IsRetryableErr(gomock.Any()).Return(true)
		require.ErrorContains(t, scheduler.switch2NextStep(), "plan err")
		require.Equal(t, *scheduler.GetTask(), task)
		require.True(t, ctrl.Satisfied())
		// non-retryable plan error, but failed to revert, task state unchanged
		schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("plan err"))
		schExt.EXPECT().IsRetryableErr(gomock.Any()).Return(false)
		taskMgr.EXPECT().RevertTask(gomock.Any(), task.ID, gomock.Any(), gomock.Any()).Return(fmt.Errorf("revert err"))
		require.ErrorContains(t, scheduler.switch2NextStep(), "revert err")
		require.Equal(t, *scheduler.GetTask(), task)
		require.True(t, ctrl.Satisfied())
		// non-retryable plan error, task state changed to reverting
		schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("revert err"))
		schExt.EXPECT().IsRetryableErr(gomock.Any()).Return(false)
		taskMgr.EXPECT().RevertTask(gomock.Any(), task.ID, gomock.Any(), gomock.Any()).Return(nil)
		require.NoError(t, scheduler.switch2NextStep())
		tmpTask := task
		tmpTask.State = proto.TaskStateReverting
		tmpTask.Error = fmt.Errorf("revert err")
		require.Equal(t, *scheduler.GetTask(), tmpTask)
		require.True(t, ctrl.Satisfied())

		// revert task back
		scheduler.task.Store(&schTask)

		// switch to next step, but update failed
		schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, nil)
		taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, fmt.Errorf("update err"))
		require.ErrorContains(t, scheduler.switch2NextStep(), "update err")
		require.Equal(t, *scheduler.GetTask(), task)
		require.True(t, ctrl.Satisfied())
		// switch to next step successfully
		schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, nil)
		taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().SwitchTaskStep(gomock.Any(), gomock.Any(), proto.TaskStateRunning, proto.StepOne, gomock.Any()).Return(nil)
		require.NoError(t, scheduler.switch2NextStep())
		tmpTask = task
		tmpTask.State = proto.TaskStateRunning
		tmpTask.Step = proto.StepOne
		require.Equal(t, *scheduler.GetTask(), tmpTask)
		require.True(t, ctrl.Satisfied())

		// task done, but update failed, task state unchanged
		schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		taskMgr.EXPECT().SucceedTask(gomock.Any(), task.ID).Return(fmt.Errorf("update err"))
		require.ErrorContains(t, scheduler.switch2NextStep(), "update err")
		require.Equal(t, *scheduler.GetTask(), tmpTask)
		// task done successfully, task state changed
		schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		taskMgr.EXPECT().SucceedTask(gomock.Any(), task.ID).Return(nil)
		require.NoError(t, scheduler.switch2NextStep())
		tmpTask.State = proto.TaskStateSucceed
		tmpTask.Step = proto.StepDone
		require.Equal(t, *scheduler.GetTask(), tmpTask)
	})

	t.Run("test revertTask", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		taskMgr.EXPECT().RevertTask(gomock.Any(), task.ID, gomock.Any(), gomock.Any()).Return(fmt.Errorf("revert err"))
		require.ErrorContains(t, scheduler.revertTask(fmt.Errorf("task err")), "revert err")
		require.Equal(t, *scheduler.GetTask(), task)
		require.True(t, ctrl.Satisfied())

		taskMgr.EXPECT().RevertTask(gomock.Any(), task.ID, gomock.Any(), gomock.Any()).Return(nil)
		require.NoError(t, scheduler.revertTask(fmt.Errorf("task err")))
		tmpTask := task
		tmpTask.State = proto.TaskStateReverting
		tmpTask.Error = fmt.Errorf("task err")
		require.Equal(t, *scheduler.GetTask(), tmpTask)
		require.True(t, ctrl.Satisfied())
	})
}
