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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	sqlsvrapimock "github.com/pingcap/tidb/pkg/domain/sqlsvrapi/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	mockScheduler "github.com/pingcap/tidb/pkg/dxf/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type storeWithKS struct {
	kv.Storage
	ks string
}

func (s *storeWithKS) GetKeyspace() string {
	return s.ks
}

type sessionWithSQLServer struct {
	*utilmock.Context
	server sqlsvrapi.Server
}

func (s *sessionWithSQLServer) GetSQLServer() sqlsvrapi.Server {
	return s.server
}

func newRuntimeWithStore(t *testing.T, ctrl *gomock.Controller, store kv.Storage) *sqlsvrapimock.MockRuntime {
	t.Helper()

	return newMockRuntime(ctrl, store, newSessionPoolForStore(t, store))
}

func newRuntimeHandle(ctrl *gomock.Controller, store kv.Storage) *sqlsvrapimock.MockKSRuntimeHandle {
	runtimeHandle := sqlsvrapimock.NewMockKSRuntimeHandle(ctrl)
	runtimeHandle.EXPECT().Store().Return(store).AnyTimes()
	runtimeHandle.EXPECT().SysSessionPool().Return(nil).AnyTimes()
	return runtimeHandle
}

func expectRuntimeFromNewSession(ctrl *gomock.Controller, taskMgr *mock.MockTaskManager, runtime sqlsvrapi.Runtime) {
	server := sqlsvrapimock.NewMockServer(ctrl)
	server.EXPECT().GetRuntime().Return(runtime).AnyTimes()
	taskMgr.EXPECT().WithNewSession(gomock.Any()).DoAndReturn(func(fn func(sessionctx.Context) error) error {
		se := utilmock.NewContext()
		// Match the mock session store to the runtime so AcquireTaskRuntime takes the local-runtime path.
		se.Store = runtime.Store()
		return fn(&sessionWithSQLServer{
			Context: se,
			server:  server,
		})
	}).AnyTimes()
}

// GetTestSchedulerExt return scheduler.Extension for testing.
func GetTestSchedulerExt(ctrl *gomock.Controller) Extension {
	mockScheduler := mockScheduler.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]string, error) {
			return nil, nil
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(_ *proto.Task) proto.Step {
			return proto.StepDone
		},
	).AnyTimes()
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ storage.TaskHandle, _ *proto.Task, _ []string, _ proto.Step) (metas [][]byte, err error) {
			return nil, nil
		},
	).AnyTimes()

	mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockScheduler
}

func TestManagerSchedulersOrdered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mgr := NewManager(context.Background(), nil, nil, "1", proto.NodeResourceForTest)
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
	mgr := NewManager(ctx, nil, taskMgr, "1", proto.NodeResourceForTest)

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
}

func TestManagerSchedulerNotAllocateSlots(t *testing.T) {
	// the tests make sure allocatedSlots correct.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/dxf/framework/scheduler/exitScheduler", "return()"))
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskMgr := mock.NewMockTaskManager(ctrl)
	mgr := NewManager(context.Background(), &storeWithKS{}, taskMgr, "1", proto.NodeResourceForTest)
	expectRuntimeFromNewSession(ctrl, taskMgr, newRuntimeWithStore(t, ctrl, mgr.store))
	RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, task *proto.Task, param Param) Scheduler {
			mockScheduler := NewBaseScheduler(ctx, task, param)
			mockScheduler.Extension = GetTestSchedulerExt(ctrl)
			return mockScheduler
		})
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, nil).AnyTimes()
	tasks := []*proto.TaskBase{
		{
			ID:            int64(1),
			RequiredSlots: 1,
			Type:          proto.TaskTypeExample,
			State:         proto.TaskStateCancelling,
		},
		{
			ID:            int64(2),
			RequiredSlots: 1,
			Type:          proto.TaskTypeExample,
			State:         proto.TaskStateReverting,
		},
		{
			ID:            int64(3),
			RequiredSlots: 1,
			Type:          proto.TaskTypeExample,
			State:         proto.TaskStatePausing,
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
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/dxf/framework/scheduler/exitScheduler"))
}

type crossKeyspaceStartCase struct {
	ctrl      *gomock.Controller
	taskMgr   *mock.MockTaskManager
	manager   *Manager
	task      *proto.Task
	taskStore *storeWithKS
	server    *sqlsvrapimock.MockServer
}

func newCrossKeyspaceStartCase(t *testing.T, taskID int64, taskKey string) *crossKeyspaceStartCase {
	t.Helper()
	ClearSchedulerFactory()
	t.Cleanup(ClearSchedulerFactory)

	ctrl := gomock.NewController(t)
	taskMgr := mock.NewMockTaskManager(ctrl)
	manager := NewManager(context.Background(), &storeWithKS{ks: "SYSTEM"}, taskMgr, "1", proto.NodeResourceForTest)
	task := &proto.Task{TaskBase: proto.TaskBase{
		ID:       taskID,
		Key:      taskKey,
		Type:     proto.TaskTypeExample,
		Keyspace: "user_ks",
	}}
	server := sqlsvrapimock.NewMockServer(ctrl)
	taskMgr.EXPECT().GetTaskByID(gomock.Any(), task.ID).Return(task, nil)
	taskMgr.EXPECT().WithNewSession(gomock.Any()).DoAndReturn(func(fn func(sessionctx.Context) error) error {
		se := utilmock.NewContext()
		se.Store = manager.store
		return fn(&sessionWithSQLServer{Context: se, server: server})
	})

	return &crossKeyspaceStartCase{
		ctrl:      ctrl,
		taskMgr:   taskMgr,
		manager:   manager,
		task:      task,
		taskStore: &storeWithKS{ks: task.Keyspace},
		server:    server,
	}
}

func (tc *crossKeyspaceStartCase) expectRuntimeAcquiredAndReleased() *sqlsvrapimock.MockKSRuntimeHandle {
	runtimeHandle := newRuntimeHandle(tc.ctrl, tc.taskStore)
	runtimeHandle.EXPECT().Release()
	tc.server.EXPECT().AcquireKSRuntime(tc.task.Keyspace, tc.holderID()).Return(runtimeHandle, nil)
	return runtimeHandle
}

func (tc *crossKeyspaceStartCase) holderID() string {
	return dxfutil.GenHolderID("scheduler", tc.task.ID)
}

func TestStartSchedulerCrossKeyspaceRuntime(t *testing.T) {
	t.Run("acquires cross-keyspace runtime and releases it on exit", func(t *testing.T) {
		tc := newCrossKeyspaceStartCase(t, 101, "cross-ks-scheduler")
		runtimeHandle := tc.expectRuntimeAcquiredAndReleased()
		runCh := make(chan struct{})
		RegisterSchedulerFactory(proto.TaskTypeExample,
			func(ctx context.Context, gotTask *proto.Task, param Param) Scheduler {
				require.Same(t, tc.task, gotTask)
				require.Same(t, runtimeHandle, param.TaskRuntime)
				require.Same(t, tc.taskStore, param.TaskRuntime.Store())
				scheduler := mock.NewMockScheduler(tc.ctrl)
				scheduler.EXPECT().Init().Return(nil)
				scheduler.EXPECT().ScheduleTask().Do(func() {
					close(runCh)
				})
				scheduler.EXPECT().Close()
				scheduler.EXPECT().GetTask().Return(gotTask).AnyTimes()
				return scheduler
			})

		tc.manager.startScheduler(&tc.task.TaskBase, false, "")
		require.Eventually(t, func() bool {
			select {
			case <-runCh:
				return true
			default:
				return false
			}
		}, 5*time.Second, 100*time.Millisecond)
		tc.manager.schedulerWG.Wait()
	})

	t.Run("releases cross-keyspace runtime when scheduler init fails", func(t *testing.T) {
		tc := newCrossKeyspaceStartCase(t, 102, "cross-ks-scheduler-init-fail")
		tc.task.State = proto.TaskStatePending
		runtimeHandle := tc.expectRuntimeAcquiredAndReleased()
		tc.taskMgr.EXPECT().FailTask(gomock.Any(), tc.task.ID, tc.task.State, gomock.Any()).Return(nil)
		RegisterSchedulerFactory(proto.TaskTypeExample,
			func(ctx context.Context, gotTask *proto.Task, param Param) Scheduler {
				require.Same(t, runtimeHandle, param.TaskRuntime)
				require.Same(t, tc.taskStore, param.TaskRuntime.Store())
				scheduler := mock.NewMockScheduler(tc.ctrl)
				scheduler.EXPECT().Init().Return(errors.New("init failed"))
				return scheduler
			})

		tc.manager.startScheduler(&tc.task.TaskBase, false, "")
	})

	t.Run("does not start scheduler when cross-keyspace runtime acquisition fails", func(t *testing.T) {
		tc := newCrossKeyspaceStartCase(t, 104, "cross-ks-scheduler-acquire-fail")
		tc.task.State = proto.TaskStatePending
		acquireErr := errors.New("acquire failed")
		tc.server.EXPECT().AcquireKSRuntime(tc.task.Keyspace, tc.holderID()).Return(nil, acquireErr)
		var factoryCalled atomic.Bool
		RegisterSchedulerFactory(proto.TaskTypeExample,
			func(ctx context.Context, gotTask *proto.Task, param Param) Scheduler {
				factoryCalled.Store(true)
				return mock.NewMockScheduler(tc.ctrl)
			})

		tc.manager.startScheduler(&tc.task.TaskBase, false, "")

		require.False(t, factoryCalled.Load())
		require.False(t, tc.manager.hasScheduler(tc.task.ID))
	})
}

func TestFastRespondNoNeedResourceTaskWhenSchedulersReachLimit(t *testing.T) {
	bak := proto.MaxConcurrentTask
	t.Cleanup(func() {
		proto.MaxConcurrentTask = bak
	})
	proto.MaxConcurrentTask = 1

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskMgr := mock.NewMockTaskManager(ctrl)
	mgr := NewManager(context.Background(), &storeWithKS{}, taskMgr, "1", proto.NodeResourceForTest)
	expectRuntimeFromNewSession(ctrl, taskMgr, newRuntimeWithStore(t, ctrl, mgr.store))
	taskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]proto.ManagedNode{{CPUCount: 8}}, nil)
	mgr.nodeMgr.refreshNodes(mgr.ctx, mgr.taskMgr, mgr.slotMgr)
	RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, task *proto.Task, param Param) Scheduler {
			mockScheduler := NewBaseScheduler(ctx, task, param)
			mockScheduler.Extension = GetTestSchedulerExt(ctrl)
			return mockScheduler
		})
	for _, state := range []proto.TaskState{
		proto.TaskStateCancelling,
		proto.TaskStateReverting,
		proto.TaskStateModifying,
		proto.TaskStatePausing,
	} {
		t.Run(state.String(), func(t *testing.T) {
			ch := make(chan struct{})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/framework/scheduler/beforeRefreshTask", func(task *proto.Task) {
				if task.ID == 1 {
					<-ch
				}
			})
			taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, nil).AnyTimes()
			task1 := &proto.TaskBase{
				ID:            int64(1),
				RequiredSlots: 1,
				Type:          proto.TaskTypeExample,
				State:         proto.TaskStatePending,
			}
			task1Success := *task1
			task1Success.State = proto.TaskStateSucceed
			taskMgr.EXPECT().GetTaskByID(gomock.Any(), int64(1)).Return(&proto.Task{TaskBase: *task1}, nil)
			taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), int64(1)).Return(&task1Success, nil)
			taskMgr.EXPECT().GetTaskByID(gomock.Any(), int64(1)).Return(&proto.Task{TaskBase: task1Success}, nil)

			task2 := &proto.TaskBase{
				ID:            int64(2),
				RequiredSlots: 1,
				Type:          proto.TaskTypeExample,
				State:         state,
			}
			// we use 'reverted' to finish the task, no matter what state it is.
			task2Reverted := *task2
			task2Reverted.State = proto.TaskStateReverted
			var cancelCalled atomic.Bool
			taskMgr.EXPECT().GetTaskByID(gomock.Any(), int64(2)).Return(&proto.Task{TaskBase: *task2}, nil)
			taskMgr.EXPECT().GetTaskBaseByID(gomock.Any(), int64(2)).DoAndReturn(func(context.Context, int64) (*proto.TaskBase, error) {
				cancelCalled.Store(true)
				return &task2Reverted, nil
			})
			taskMgr.EXPECT().GetTaskByID(gomock.Any(), int64(2)).Return(&proto.Task{TaskBase: task2Reverted}, nil)

			require.NoError(t, mgr.startSchedulers([]*proto.TaskBase{task1, task2}))
			require.Eventually(t, func() bool {
				return cancelCalled.Load()
			}, 15*time.Second, 100*time.Millisecond)
			close(ch)
			mgr.schedulerWG.Wait()
			require.True(t, ctrl.Satisfied())
		})
	}
}
