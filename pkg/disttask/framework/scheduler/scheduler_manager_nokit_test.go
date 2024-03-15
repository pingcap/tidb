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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

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

func TestSchedulerCleanupTask(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
	}()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)
	ctx := context.Background()
	mgr := NewManager(ctx, taskMgr, "1")

	// normal
	tasks := []*proto.Task{
		{TaskBase: proto.TaskBase{ID: 1}},
	}
	taskMgr.EXPECT().GetTasksInStates(
		mgr.ctx,
		proto.TaskStateFailed,
		proto.TaskStateReverted,
		proto.TaskStateSucceed).Return(tasks, nil)

	taskMgr.EXPECT().TransferTasks2History(mgr.ctx, tasks).Return(nil)
	mgr.doCleanupTask()
	require.True(t, ctrl.Satisfied())

	// fail in transfer
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", "1*return()"))
	mockErr := errors.New("transfer err")
	taskMgr.EXPECT().GetTasksInStates(
		mgr.ctx,
		proto.TaskStateFailed,
		proto.TaskStateReverted,
		proto.TaskStateSucceed).Return(tasks, nil)
	taskMgr.EXPECT().TransferTasks2History(mgr.ctx, tasks).Return(mockErr)
	mgr.doCleanupTask()
	require.True(t, ctrl.Satisfied())

	taskMgr.EXPECT().GetTasksInStates(
		mgr.ctx,
		proto.TaskStateFailed,
		proto.TaskStateReverted,
		proto.TaskStateSucceed).Return(tasks, nil)
	taskMgr.EXPECT().TransferTasks2History(mgr.ctx, tasks).Return(nil)
	mgr.doCleanupTask()
	require.True(t, ctrl.Satisfied())

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished"))
}

func TestManagerSchedulerNotAllocateSlots(t *testing.T) {
	// the tests make sure allocatedSlots correct.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/exitScheduler", "return()"))
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskMgr := mock.NewMockTaskManager(ctrl)
	mgr := NewManager(context.Background(), taskMgr, "1")
	RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, task *proto.Task, param Param) Scheduler {
			mockScheduler := NewBaseScheduler(ctx, task, param)
			mockScheduler.Extension = GetTestSchedulerExt(ctrl)
			return mockScheduler
		})
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, nil).AnyTimes()
	tasks := []*proto.TaskBase{
		{
			ID:          int64(1),
			Concurrency: 1,
			Type:        proto.TaskTypeExample,
			State:       proto.TaskStateCancelling,
		},
		{
			ID:          int64(2),
			Concurrency: 1,
			Type:        proto.TaskTypeExample,
			State:       proto.TaskStateReverting,
		},
		{
			ID:          int64(3),
			Concurrency: 1,
			Type:        proto.TaskTypeExample,
			State:       proto.TaskStatePausing,
		},
	}
	for i := 1; i <= 3; i++ {
		taskMgr.EXPECT().GetTaskByID(gomock.Any(), int64(i)).Return(&proto.Task{TaskBase: *tasks[i-1]}, nil)
		taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), int64(i)).Return(tasks[i-1], nil)
	}

	require.NoError(t, mgr.startSchedulers(tasks))
	schs := mgr.getSchedulers()
	require.Equal(t, 3, len(schs))
	for _, sch := range schs {
		require.Equal(t, false, sch.(*BaseScheduler).allocatedSlots)
		<-mgr.finishCh
	}
	mgr.schedulerWG.Wait()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/exitScheduler"))
}
