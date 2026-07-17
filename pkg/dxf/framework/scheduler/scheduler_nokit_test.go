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
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfmetric"
	"github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	schmock "github.com/pingcap/tidb/pkg/dxf/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

func TestSchedulerAutoPauseOnKVDiskFull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)
	taskErr := errdef.ErrKVDiskFull.GenWithStack("store 1 disk full")
	taskErr2 := errdef.ErrKVDiskFull.GenWithStack("store 2 disk full")
	task := proto.Task{
		TaskBase: proto.TaskBase{
			ID:    1,
			State: proto.TaskStateRunning,
			Step:  proto.StepOne,
			ExtraParams: proto.ExtraParams{
				PauseOnKVDiskFull: true,
			},
		},
	}
	sch := createScheduler(&task, true, taskMgr, ctrl)

	schExt := schmock.NewMockExtension(ctrl)
	sch.Extension = schExt
	taskMgr.EXPECT().GetSubtaskStateCntAndErrorsByStep(gomock.Any(), task.ID, task.Step).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStatePending: 1,
		proto.SubtaskStateRunning: 1,
	}, nil, nil)
	schExt.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return()

	require.NoError(t, sch.onRunning())
	require.Equal(t, proto.TaskStateRunning, sch.GetTask().State)
	require.True(t, ctrl.Satisfied())

	taskMgr.EXPECT().GetSubtaskStateCntAndErrorsByStep(gomock.Any(), task.ID, task.Step).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStateFailed: 2,
	}, []error{taskErr, taskErr2}, nil)
	taskMgr.EXPECT().PauseTaskOnError(gomock.Any(), task.ID, task.State, task.Step, taskErr).Return(nil)

	require.NoError(t, sch.onRunning())
	require.Equal(t, proto.TaskStatePausing, sch.GetTask().State)
	require.ErrorIs(t, sch.GetTask().Error, errdef.ErrKVDiskFull)
	require.True(t, ctrl.Satisfied())

	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, task.Step).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStateFailed: 1,
	}, nil)
	taskMgr.EXPECT().GetSubtaskErrors(gomock.Any(), task.ID).Return([]error{taskErr}, nil)
	taskMgr.EXPECT().PauseTaskOnError(gomock.Any(), task.ID, proto.TaskStatePausing, task.Step, taskErr).Return(nil)

	require.NoError(t, sch.onPausing())
	require.Equal(t, proto.TaskStatePausing, sch.GetTask().State)
	require.ErrorIs(t, sch.GetTask().Error, errdef.ErrKVDiskFull)
	require.True(t, ctrl.Satisfied())

	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, task.Step).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStatePaused: 1,
	}, nil)
	taskMgr.EXPECT().PausedTask(gomock.Any(), task.ID).Return(nil)

	require.NoError(t, sch.onPausing())
	require.Equal(t, proto.TaskStatePaused, sch.GetTask().State)
	require.ErrorIs(t, sch.GetTask().Error, errdef.ErrKVDiskFull)
	require.True(t, ctrl.Satisfied())

	tests := []struct {
		name        string
		cntByState  map[proto.SubtaskState]int64
		subTaskErrs []error
	}{
		{
			name: "canceled subtasks present",
			cntByState: map[proto.SubtaskState]int64{
				proto.SubtaskStateFailed:   1,
				proto.SubtaskStateCanceled: 1,
			},
			subTaskErrs: []error{taskErr},
		},
		{
			name: "mixed error types",
			cntByState: map[proto.SubtaskState]int64{
				proto.SubtaskStateFailed: 2,
			},
			subTaskErrs: []error{taskErr, errors.New("network error")},
		},
		{
			name: "failed count and error count mismatch",
			cntByState: map[proto.SubtaskState]int64{
				proto.SubtaskStateFailed: 2,
			},
			subTaskErrs: []error{taskErr},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.False(t, shouldPauseOnKVDiskFull(&task, test.cntByState, test.subTaskErrs))
		})
	}

	missingErrTask := proto.Task{
		TaskBase: proto.TaskBase{
			ID:    2,
			State: proto.TaskStateRunning,
			Step:  proto.StepOne,
			ExtraParams: proto.ExtraParams{
				PauseOnKVDiskFull: true,
			},
		},
	}
	missingErrSch := createScheduler(&missingErrTask, true, taskMgr, ctrl)
	taskMgr.EXPECT().GetSubtaskStateCntAndErrorsByStep(gomock.Any(), missingErrTask.ID, missingErrTask.Step).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStateFailed: 1,
	}, nil, nil)
	taskMgr.EXPECT().RevertTask(gomock.Any(), missingErrTask.ID, proto.TaskStateRunning, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ int64, _ proto.TaskState, err error) error {
			require.ErrorContains(t, err, "without error")
			return nil
		})
	require.NoError(t, missingErrSch.onRunning())
	require.Equal(t, proto.TaskStateReverting, missingErrSch.GetTask().State)
	require.ErrorContains(t, missingErrSch.GetTask().Error, "without error")
	require.True(t, ctrl.Satisfied())
}

func TestSchedulerNotAllocateSlots(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)

	// scheduler not allocated slots, task from paused to resuming. Should exit the scheduler.
	task := proto.Task{
		TaskBase: proto.TaskBase{
			ID:            int64(1),
			RequiredSlots: 1,
			Type:          proto.TaskTypeExample,
			State:         proto.TaskStatePaused,
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
	require.Equal(t, *scheduler.getTaskClone(), task)
	require.True(t, ctrl.Satisfied())
	// get task by id failed
	tmpTask := task
	tmpTask.State = proto.TaskStateCancelling
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), task.ID).Return(&tmpTask.TaskBase, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(nil, errors.New("get task by id err"))
	require.ErrorContains(t, scheduler.refreshTaskIfNeeded(), "get task by id err")
	require.Equal(t, *scheduler.getTaskClone(), task)
	require.True(t, ctrl.Satisfied())
	// state changed
	tmpTask = task
	tmpTask.State = proto.TaskStateCancelling
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), task.ID).Return(&tmpTask.TaskBase, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(&tmpTask, nil)
	require.NoError(t, scheduler.refreshTaskIfNeeded())
	require.Equal(t, *scheduler.getTaskClone(), tmpTask)
	require.True(t, ctrl.Satisfied())
	// step changed
	scheduler.task.Store(&schTask) // revert
	tmpTask = task
	tmpTask.Step = proto.StepTwo
	taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), task.ID).Return(&tmpTask.TaskBase, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(&tmpTask, nil)
	require.NoError(t, scheduler.refreshTaskIfNeeded())
	require.Equal(t, *scheduler.getTaskClone(), tmpTask)
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

	runningTask := task
	runningTask.State = proto.TaskStateRunning

	t.Run("test onPausing", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().PausedTask(gomock.Any(), task.ID).Return(fmt.Errorf("pause err"))
		require.ErrorContains(t, scheduler.onPausing(), "pause err")
		require.Equal(t, *scheduler.getTaskClone(), task)
		require.True(t, ctrl.Satisfied())

		// pause task successfully
		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().PausedTask(gomock.Any(), task.ID).Return(nil)
		require.NoError(t, scheduler.onPausing())
		tmpTask := task
		tmpTask.State = proto.TaskStatePaused
		require.Equal(t, *scheduler.getTaskClone(), tmpTask)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test onResuming", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().ResumedTask(gomock.Any(), task.ID).Return(fmt.Errorf("resume err"))
		require.ErrorContains(t, scheduler.onResuming(), "resume err")
		require.Equal(t, *scheduler.getTaskClone(), task)
		require.True(t, ctrl.Satisfied())

		// resume task successfully
		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		taskMgr.EXPECT().ResumedTask(gomock.Any(), task.ID).Return(nil)
		require.NoError(t, scheduler.onResuming())
		tmpTask := task
		tmpTask.State = proto.TaskStateRunning
		require.Equal(t, *scheduler.getTaskClone(), tmpTask)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test onReverting", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		taskMgr.EXPECT().RevertedTask(gomock.Any(), task.ID).Return(fmt.Errorf("reverted err"))
		require.ErrorContains(t, scheduler.onReverting(), "reverted err")
		require.Equal(t, *scheduler.getTaskClone(), task)
		require.True(t, ctrl.Satisfied())

		// revert task successfully
		taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), task.ID, gomock.Any()).Return(nil, nil)
		schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		taskMgr.EXPECT().RevertedTask(gomock.Any(), task.ID).Return(nil)
		require.NoError(t, scheduler.onReverting())
		tmpTask := task
		tmpTask.State = proto.TaskStateReverted
		require.Equal(t, *scheduler.getTaskClone(), tmpTask)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test switch2NextStep", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		// retryable plan error, nothing changes
		schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("plan err"))
		schExt.EXPECT().IsRetryableErr(gomock.Any()).Return(true)
		require.ErrorContains(t, scheduler.switch2NextStep(), "plan err")
		require.Equal(t, *scheduler.getTaskClone(), task)
		require.True(t, ctrl.Satisfied())
		// non-retryable plan error, but failed to revert, task state unchanged
		schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("plan err"))
		schExt.EXPECT().IsRetryableErr(gomock.Any()).Return(false)
		taskMgr.EXPECT().RevertTask(gomock.Any(), task.ID, gomock.Any(), gomock.Any()).Return(fmt.Errorf("revert err"))
		require.ErrorContains(t, scheduler.switch2NextStep(), "revert err")
		require.Equal(t, *scheduler.getTaskClone(), task)
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
		require.Equal(t, *scheduler.getTaskClone(), tmpTask)
		require.True(t, ctrl.Satisfied())

		// revert task back
		scheduler.task.Store(&schTask)

		// switch to next step, but update failed
		schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, nil)
		taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, fmt.Errorf("update err"))
		require.ErrorContains(t, scheduler.switch2NextStep(), "update err")
		require.Equal(t, *scheduler.getTaskClone(), task)
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
		require.Equal(t, *scheduler.getTaskClone(), tmpTask)
		require.True(t, ctrl.Satisfied())

		// task done, but update failed, task state unchanged
		schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		taskMgr.EXPECT().SucceedTask(gomock.Any(), task.ID).Return(fmt.Errorf("update err"))
		require.ErrorContains(t, scheduler.switch2NextStep(), "update err")
		require.Equal(t, *scheduler.getTaskClone(), tmpTask)
		// task done successfully, task state changed
		schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		taskMgr.EXPECT().SucceedTask(gomock.Any(), task.ID).Return(nil)
		require.NoError(t, scheduler.switch2NextStep())
		tmpTask.State = proto.TaskStateSucceed
		tmpTask.Step = proto.StepDone
		require.Equal(t, *scheduler.getTaskClone(), tmpTask)
	})

	t.Run("test revertTask", func(t *testing.T) {
		scheduler.task.Store(&schTask)

		taskMgr.EXPECT().RevertTask(gomock.Any(), task.ID, gomock.Any(), gomock.Any()).Return(fmt.Errorf("revert err"))
		require.ErrorContains(t, scheduler.revertTask(fmt.Errorf("task err")), "revert err")
		require.Equal(t, *scheduler.getTaskClone(), task)
		require.True(t, ctrl.Satisfied())

		taskMgr.EXPECT().RevertTask(gomock.Any(), task.ID, gomock.Any(), gomock.Any()).Return(nil)
		require.NoError(t, scheduler.revertTask(fmt.Errorf("task err")))
		tmpTask := task
		tmpTask.State = proto.TaskStateReverting
		tmpTask.Error = fmt.Errorf("task err")
		require.Equal(t, *scheduler.getTaskClone(), tmpTask)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test on modifying, failed to update system table", func(t *testing.T) {
		taskBefore := runningTask
		taskBefore.State = proto.TaskStateModifying
		taskBefore.ModifyParam = proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyRequiredSlots, To: 123},
			},
		}
		scheduler.task.Store(&taskBefore)
		taskMgr.EXPECT().ModifiedTask(gomock.Any(), gomock.Any()).Return(fmt.Errorf("modify err"))
		recreateScheduler, err := scheduler.onModifying()
		require.ErrorContains(t, err, "modify err")
		require.False(t, recreateScheduler)
		require.Equal(t, taskBefore, *scheduler.GetTask())
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test on modifying required slots, success", func(t *testing.T) {
		taskBefore := runningTask
		taskBefore.State = proto.TaskStateModifying
		taskBefore.ModifyParam = proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyRequiredSlots, To: 123},
			},
		}
		scheduler.task.Store(&taskBefore)
		taskMgr.EXPECT().ModifiedTask(gomock.Any(), gomock.Any()).Return(nil)
		recreateScheduler, err := scheduler.onModifying()
		require.NoError(t, err)
		require.True(t, recreateScheduler)
		expectedTask := runningTask
		expectedTask.RequiredSlots = 123
		require.Equal(t, expectedTask, *scheduler.GetTask())
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test on modifying task meta, failed to get new meta", func(t *testing.T) {
		taskBefore := runningTask
		taskBefore.State = proto.TaskStateModifying
		taskBefore.ModifyParam = proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyMaxWriteSpeed, To: 11111},
			},
		}
		scheduler.task.Store(&taskBefore)
		schExt.EXPECT().ModifyMeta(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("modify meta err"))
		recreateScheduler, err := scheduler.onModifying()
		require.ErrorContains(t, err, "modify meta err")
		require.False(t, recreateScheduler)
		require.Equal(t, taskBefore, *scheduler.GetTask())
		require.True(t, ctrl.Satisfied())
	})

	t.Run("test on modifying required slots and task meta, success", func(t *testing.T) {
		taskBefore := runningTask
		taskBefore.State = proto.TaskStateModifying
		taskBefore.ModifyParam = proto.ModifyParam{
			PrevState: proto.TaskStateRunning,
			Modifications: []proto.Modification{
				{Type: proto.ModifyRequiredSlots, To: 123},
				{Type: proto.ModifyMaxWriteSpeed, To: 11111},
			},
		}
		scheduler.task.Store(&taskBefore)
		schExt.EXPECT().ModifyMeta(gomock.Any(), gomock.Any()).Return([]byte("max-11111"), nil)
		taskMgr.EXPECT().ModifiedTask(gomock.Any(), gomock.Any()).Return(nil)
		recreateScheduler, err := scheduler.onModifying()
		require.NoError(t, err)
		require.True(t, recreateScheduler)
		expectedTask := runningTask
		expectedTask.RequiredSlots = 123
		expectedTask.Meta = []byte("max-11111")
		require.Equal(t, expectedTask, *scheduler.GetTask())
		require.True(t, ctrl.Satisfied())
	})
}

func TestOnTaskFinished(t *testing.T) {
	bak := dxfmetric.FinishedTaskCounter
	t.Cleanup(func() {
		dxfmetric.FinishedTaskCounter = bak
	})
	dxfmetric.FinishedTaskCounter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test"}, []string{"state"})
	collectMetricsFn := func() map[string]int {
		var ch = make(chan prometheus.Metric)
		items := make([]*dto.Metric, 0)
		var wg tidbutil.WaitGroupWrapper
		wg.Run(func() {
			for m := range ch {
				dm := &dto.Metric{}
				require.NoError(t, m.Write(dm))
				items = append(items, dm)
			}
		})
		dxfmetric.FinishedTaskCounter.Collect(ch)
		close(ch)
		wg.Wait()
		values := make(map[string]int)
		for _, it := range items {
			values[*it.GetLabel()[0].Value] = int(it.GetCounter().GetValue())
		}
		return values
	}
	onTaskFinished(proto.TaskStateSucceed, nil)
	require.EqualValues(t, map[string]int{metricStateAll: 1, "succeed": 1}, collectMetricsFn())
	onTaskFinished(proto.TaskStateReverted, nil)
	require.EqualValues(t, map[string]int{metricStateAll: 2, "succeed": 1, "failed": 1}, collectMetricsFn())
	onTaskFinished(proto.TaskStateReverted, errors.New("some err"))
	require.EqualValues(t, map[string]int{metricStateAll: 3, "succeed": 1, "failed": 2}, collectMetricsFn())
	onTaskFinished(proto.TaskStateReverted, errors.New(storage.TaskCancelMessage))
	require.EqualValues(t, map[string]int{metricStateAll: 4, "succeed": 1, "failed": 2, "cancelled": 1}, collectMetricsFn())
	onTaskFinished(proto.TaskStateFailed, errors.New("some err"))
	require.EqualValues(t, map[string]int{metricStateAll: 5, "succeed": 1, "failed": 3, "cancelled": 1}, collectMetricsFn())
	valueConversionErr := "[Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk: " +
		"encode kv error in file orderlab/orderlab.shipment_events.000000000.csv.gz:0 at offset 0: " +
		"Value conversion failed for column 'event_id'. Expected type: bigint, received value: ?. " +
		"Reason: [types:1292]Truncated incorrect DOUBLE value: '?'."
	onTaskFinished(proto.TaskStateReverted, errors.New(valueConversionErr))
	require.EqualValues(t, map[string]int{
		metricStateAll: 6, "succeed": 1, "failed": 3, "cancelled": 1, "data-error": 1,
	}, collectMetricsFn())
	datetimeConversionErr := "[Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk: " +
		"encode kv error in file orderlab/orderlab.shipment_events.000000000.csv.gz:0 at offset 0: " +
		"Value conversion failed for column 'created_at'. Expected type: datetime, received value: invalid. " +
		"Reason: [types:1292]Incorrect datetime value: 'invalid' for column 'created_at' at row 1."
	onTaskFinished(proto.TaskStateReverted, errors.New(datetimeConversionErr))
	require.EqualValues(t, map[string]int{
		metricStateAll: 7, "succeed": 1, "failed": 3, "cancelled": 1, "data-error": 2,
	}, collectMetricsFn())
	roundTripImportCastErr := func(column, columnType, value, reason string) error {
		castErr := common.ErrCastValue.FastGenByArgs(column, columnType, value, reason)
		encodeErr := common.ErrEncodeKV.Wrap(castErr).FastGenByArgs("data.csv", 0)
		serializedErr := errors.Normalize(errors.GetErrStackMsg(encodeErr),
			errors.RFCCodeText(string(common.ErrEncodeKV.RFCCode())),
			errors.MySQLErrorCode(int(common.ErrEncodeKV.Code())))
		errBytes, err := serializedErr.MarshalJSON()
		require.NoError(t, err)
		restoredErr := errors.Normalize("")
		require.NoError(t, restoredErr.UnmarshalJSON(errBytes))
		return restoredErr
	}
	dataTooLongErr := roundTripImportCastErr("name", "varchar(3)", "abcd",
		"[types:1406]Data Too Long, field len 3, data len 4")
	require.NotContains(t, dataTooLongErr.Error(), "ErrCastValue")
	require.Equal(t, "data-error", getMetricState(proto.TaskStateReverted, dataTooLongErr))
	notNullErr := roundTripImportCastErr("name", "varchar(10)", "NULL",
		"[table:1048]Column 'name' cannot be null")
	require.Equal(t, "data-error", getMetricState(proto.TaskStateReverted, notNullErr))
	checkConstraintErr := "[Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk: " +
		"encode kv error in file data.csv:0 at offset 0: " +
		"Check constraint 'positive_id' is violated."
	require.Equal(t, "data-error", getMetricState(proto.TaskStateReverted, errors.New(checkConstraintErr)))
	noPartitionErr := "[Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk: " +
		"encode kv error in file data.csv:0 at offset 0: " +
		"Table has no partition for value 42"
	require.Equal(t, "data-error", getMetricState(proto.TaskStateReverted, errors.New(noPartitionErr)))
	require.Equal(t, "data-error", getMetricState(proto.TaskStateReverted,
		errors.New("[executor:8167]Duplicate key conflict found. Please resolve conflicts in the input dataset")))
	require.Equal(t, "data-error", getMetricState(proto.TaskStateReverted,
		errors.New("[Lightning:Restore:ErrFoundDataConflictRecords]found data conflict records in table t")))
	require.Equal(t, "data-error", getMetricState(proto.TaskStateReverted,
		errors.New("[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table t")))
	parseErr := "[Lightning:Restore:ErrEncodeKV]encode kv error in file data.csv:0 at offset 0: " +
		"column count mismatch, expected 3, got 2"
	require.Equal(t, proto.TaskStateFailed.String(), getMetricState(proto.TaskStateReverted, errors.New(parseErr)))
	require.Equal(t, proto.TaskStateFailed.String(), getMetricState(proto.TaskStateReverted,
		errors.New("Value conversion failed for column 'name'")))
	onTaskFinished(proto.TaskStateReverted, errors.New("[kv:1062]Duplicate entry '1' for key 't.idx'"))
	require.EqualValues(t, map[string]int{
		metricStateAll: 8, "succeed": 1, "failed": 3, "cancelled": 1, "data-error": 3,
	}, collectMetricsFn())
	// noop for non-finished state.
	onTaskFinished(proto.TaskStateRunning, nil)
	require.EqualValues(t, map[string]int{
		metricStateAll: 8, "succeed": 1, "failed": 3, "cancelled": 1, "data-error": 3,
	}, collectMetricsFn())
}
