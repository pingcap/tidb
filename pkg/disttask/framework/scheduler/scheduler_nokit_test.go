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
	mockScheduler "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func createScheduler(task *proto.Task, taskMgr TaskManager) *BaseScheduler {
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "scheduler")
	nodeMgr := NewNodeManager()
	sch := NewBaseScheduler(ctx, task, Param{
		taskMgr: taskMgr,
		nodeMgr: nodeMgr,
		slotMgr: newSlotManager(),
	})
	return sch
}

func TestSchedulerOnNextStage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)
	task := proto.Task{
		TaskBase: proto.TaskBase{
			ID:    1,
			State: proto.TaskStatePending,
			Step:  proto.StepInit,
		},
	}
	cloneTask := task
	sch := createScheduler(&cloneTask, taskMgr)
	schExt := mockScheduler.NewMockExtension(ctrl)
	sch.Extension = schExt

	// test next step is done
	schExt.EXPECT().GetNextStep(gomock.Any()).Return(proto.StepDone)
	schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("done err"))
	require.ErrorContains(t, sch.Switch2NextStep(), "done err")
	require.True(t, ctrl.Satisfied())
	// we update task step before OnDone
	require.Equal(t, proto.StepDone, sch.GetTask().Step)
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

func TestManagerSchedulersOrdered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mgr := NewManager(context.Background(), nil, "1")
	for i := 1; i <= 5; i++ {
		task := &proto.Task{TaskBase: proto.TaskBase{
			ID: int64(i * 10),
		}}
		mockScheduler := mock.NewMockScheduler(ctrl)
		mockScheduler.EXPECT().GetTask().Return(task).AnyTimes()
		mgr.addScheduler(task.ID, mockScheduler)
	}
	ordered := func(schedulers []Scheduler) bool {
		for i := 1; i < len(schedulers); i++ {
			if schedulers[i-1].GetTask().CompareTask(schedulers[i].GetTask()) >= 0 {
				return false
			}
		}
		return true
	}
	require.Len(t, mgr.getSchedulers(), 5)
	require.True(t, ordered(mgr.getSchedulers()))

	task35 := &proto.Task{TaskBase: proto.TaskBase{
		ID: int64(35),
	}}
	mockScheduler35 := mock.NewMockScheduler(ctrl)
	mockScheduler35.EXPECT().GetTask().Return(task35).AnyTimes()

	mgr.delScheduler(30)
	require.False(t, mgr.hasScheduler(30))
	mgr.addScheduler(task35.ID, mockScheduler35)
	require.True(t, mgr.hasScheduler(35))
	require.Len(t, mgr.getSchedulers(), 5)
	require.True(t, ordered(mgr.getSchedulers()))
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

func TestTaskStateTransition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)

	// pending->running->succeed.
	task := proto.Task{
		TaskBase: proto.TaskBase{
			ID:          int64(1),
			Concurrency: 1,
			Type:        proto.TaskTypeExample,
			State:       proto.TaskStatePending,
			Step:        proto.StepInit,
		},
	}
	cloneTask := task
	sch := createScheduler(&cloneTask, taskMgr)
	schExt := mockScheduler.NewMockExtension(ctrl)
	schExt.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	schExt.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return([]string{"node1"}, nil).AnyTimes()
	schExt.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	schExt.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			if task.Step == proto.StepInit {
				return proto.StepOne
			}
			return proto.StepDone
		},
	).AnyTimes()
	schExt.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ storage.TaskHandle, _ *proto.Task, _ []string, nextStep proto.Step) (metas [][]byte, err error) {
			if nextStep == proto.StepOne {
				res := make([][]byte, 3)
				for i := 0; i < 3; i++ {
					res[i] = []byte(fmt.Sprintf("subtask-%d", i))
				}
				return res, nil
			}
			return nil, nil
		},
	).AnyTimes()
	schExt.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	sch.Extension = schExt
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		return &cloneTask, nil
	})
	taskMgr.EXPECT().SwitchTaskStep(gomock.Any(), sch.task.Load(), proto.TaskStateRunning, proto.StepOne, gomock.Any()).Return(nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{"node1": 0}, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateRunning
		cloneTask.Step = proto.StepOne
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepOne).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStateSucceed: 3}, nil)
	taskMgr.EXPECT().SucceedTask(gomock.Any(), cloneTask.ID).Return(nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateSucceed
		return &cloneTask, nil
	})
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())

	// pending->running->reverting->reverted.
	cloneTask = task
	sch = createScheduler(&cloneTask, taskMgr)
	sch.Extension = schExt
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		return &cloneTask, nil
	})
	taskMgr.EXPECT().SwitchTaskStep(gomock.Any(), sch.task.Load(), proto.TaskStateRunning, proto.StepOne, gomock.Any()).Return(nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{"node1": 0}, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateRunning
		cloneTask.Step = proto.StepOne
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepOne).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStateSucceed: 2,
		proto.SubtaskStatePending: 1}, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateReverting
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepOne).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStatePending: 0,
		proto.SubtaskStateRunning: 0}, nil)
	taskMgr.EXPECT().RevertedTask(gomock.Any(), cloneTask.ID).Return(nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateReverted
		return &cloneTask, nil
	})
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())
	// pending->running->pausing->paused.
	cloneTask = task
	sch = createScheduler(&cloneTask, taskMgr)
	sch.Extension = schExt
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		return &cloneTask, nil
	})
	taskMgr.EXPECT().SwitchTaskStep(gomock.Any(), sch.task.Load(), proto.TaskStateRunning, proto.StepOne, gomock.Any()).Return(nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{"node1": 0}, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStatePausing
		cloneTask.Step = proto.StepOne
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepOne).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStateRunning: 0,
		proto.SubtaskStatePending: 0}, nil)
	taskMgr.EXPECT().PausedTask(gomock.Any(), cloneTask.ID).Return(nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStatePaused
		return &cloneTask, nil
	})
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())

	// pending->running->cancelling->reverting->reverted.
	cloneTask = task
	sch = createScheduler(&cloneTask, taskMgr)
	sch.Extension = schExt
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		return &cloneTask, nil
	})
	taskMgr.EXPECT().SwitchTaskStep(gomock.Any(), sch.task.Load(), proto.TaskStateRunning, proto.StepOne, gomock.Any()).Return(nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{"node1": 0}, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateCancelling
		cloneTask.Step = proto.StepOne
		return &cloneTask, nil
	})
	taskMgr.EXPECT().RevertTask(gomock.Any(), cloneTask.ID, proto.TaskStateCancelling, gomock.Any()).Return(nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateReverting
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepOne).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStatePending: 0,
		proto.SubtaskStateRunning: 0}, nil)
	taskMgr.EXPECT().RevertedTask(gomock.Any(), cloneTask.ID).Return(nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateReverted
		return &cloneTask, nil
	})
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())

	// pending->pausing->paused.
	cloneTask = task
	sch = createScheduler(&cloneTask, taskMgr)
	sch.Extension = schExt
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStatePausing
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepInit).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStateRunning: 0,
		proto.SubtaskStatePending: 0}, nil)
	taskMgr.EXPECT().PausedTask(gomock.Any(), cloneTask.ID).Return(nil)

	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStatePaused
		return &cloneTask, nil
	})
	sch.scheduleTask()

	// resuming->running.
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateResuming
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepInit).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStatePaused: 0}, nil)
	taskMgr.EXPECT().ResumedTask(gomock.Any(), cloneTask.ID).Return(nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateRunning
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepInit).Return(map[proto.SubtaskState]int64{}, nil)
	taskMgr.EXPECT().SwitchTaskStep(gomock.Any(), sch.task.Load(), proto.TaskStateRunning, proto.StepOne, gomock.Any()).Return(nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{"node1": 0}, nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateRunning
		cloneTask.Step = proto.StepOne
		return &cloneTask, nil
	})
	taskMgr.EXPECT().GetSubtaskCntGroupByStates(gomock.Any(), cloneTask.ID, proto.StepOne).Return(map[proto.SubtaskState]int64{
		proto.SubtaskStateSucceed: 3}, nil)
	taskMgr.EXPECT().SucceedTask(gomock.Any(), cloneTask.ID).Return(nil)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), cloneTask.ID).DoAndReturn(func(_ context.Context, _ int64) (*proto.Task, error) {
		cloneTask.State = proto.TaskStateSucceed
		return &cloneTask, nil
	})
	sch.scheduleTask()
	require.True(t, ctrl.Satisfied())
}
