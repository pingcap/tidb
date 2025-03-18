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

package dispatcher_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

var (
	_ dispatcher.Extension = (*testDispatcherExt)(nil)
	_ dispatcher.Extension = (*numberExampleDispatcherExt)(nil)
)

const (
	subtaskCnt = 3
)

type testDispatcherExt struct{}

func (*testDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

func (*testDispatcherExt) OnNextSubtasksBatch(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
	return nil, nil
}

func (*testDispatcherExt) OnDone(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task) error {
	return nil
}

var mockedAllServerInfos = []*infosync.ServerInfo{}

func (*testDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
	return mockedAllServerInfos, true, nil
}

func (*testDispatcherExt) IsRetryableErr(error) bool {
	return true
}

func (*testDispatcherExt) GetNextStep(*proto.Task) proto.Step {
	return proto.StepDone
}

type numberExampleDispatcherExt struct{}

func (*numberExampleDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

func (n *numberExampleDispatcherExt) OnNextSubtasksBatch(_ context.Context, _ dispatcher.TaskHandle, task *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
	switch task.Step {
	case proto.StepInit:
		for i := 0; i < subtaskCnt; i++ {
			metas = append(metas, []byte{'1'})
		}
		logutil.BgLogger().Info("progress step init")
	case proto.StepOne:
		logutil.BgLogger().Info("progress step one")
		return nil, nil
	default:
		return nil, errors.New("unknown step")
	}
	return metas, nil
}

func (n *numberExampleDispatcherExt) OnDone(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task) error {
	// Don't handle not.
	return nil
}

func (*numberExampleDispatcherExt) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
	serverInfo, err := dispatcher.GenerateSchedulerNodes(ctx)
	return serverInfo, true, err
}

func (*numberExampleDispatcherExt) IsRetryableErr(error) bool {
	return true
}

func (*numberExampleDispatcherExt) GetNextStep(task *proto.Task) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	default:
		return proto.StepDone
	}
}

func MockDispatcherManager(t *testing.T, pool *pools.ResourcePool) (*dispatcher.Manager, *storage.TaskManager) {
	ctx := context.WithValue(context.Background(), "etcd", true)
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	dsp, err := dispatcher.NewManager(util.WithInternalSourceType(ctx, "dispatcher"), mgr, "host:port")
	require.NoError(t, err)
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			mockDispatcher := dsp.MockDispatcher(task)
			mockDispatcher.Extension = &testDispatcherExt{}
			return mockDispatcher
		})
	return dsp, mgr
}

func MockDispatcherManagerWithMockTaskMgr(t *testing.T, pool *pools.ResourcePool, taskMgr *mock.MockTaskManager) *dispatcher.Manager {
	ctx := context.WithValue(context.Background(), "etcd", true)
	dsp, err := dispatcher.NewManager(util.WithInternalSourceType(ctx, "dispatcher"), taskMgr, "host:port")
	require.NoError(t, err)
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			mockDispatcher := dsp.MockDispatcher(task)
			mockDispatcher.Extension = &testDispatcherExt{}
			return mockDispatcher
		})
	return dsp
}

func deleteTasks(t *testing.T, store kv.Storage, taskID int64) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_global_task where id = %d", taskID))
}

func TestGetInstance(t *testing.T) {
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockSchedulerNodes", "return()"))
	dspManager, mgr := MockDispatcherManager(t, pool)
	// test no server
	task := &proto.Task{ID: 1, Type: proto.TaskTypeExample}
	dsp := dspManager.MockDispatcher(task)
	dsp.Extension = &testDispatcherExt{}
	instanceIDs, err := dsp.GetAllSchedulerIDs(ctx, task)
	require.Lenf(t, instanceIDs, 0, "GetAllSchedulerIDs when there's no subtask")
	require.NoError(t, err)

	// test 2 servers
	// server ids: uuid0, uuid1
	// subtask instance ids: nil
	uuids := []string{"ddl_id_1", "ddl_id_2"}
	serverIDs := []string{"10.123.124.10:32457", "[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:65535"}

	dispatcher.MockServerInfo = []*infosync.ServerInfo{
		{
			StaticServerInfo: infosync.StaticServerInfo{
				ID:   uuids[0],
				IP:   "10.123.124.10",
				Port: 32457,
			},
		},
		{
			StaticServerInfo: infosync.StaticServerInfo{
				ID:   uuids[1],
				IP:   "ABCD:EF01:2345:6789:ABCD:EF01:2345:6789",
				Port: 65535,
			},
		},
	}
	instanceIDs, err = dsp.GetAllSchedulerIDs(ctx, task)
	require.Lenf(t, instanceIDs, 0, "GetAllSchedulerIDs")
	require.NoError(t, err)

	// server ids: uuid0, uuid1
	// subtask instance ids: uuid1
	subtask := &proto.Subtask{
		Type:        proto.TaskTypeExample,
		TaskID:      task.ID,
		SchedulerID: serverIDs[1],
	}
	err = mgr.AddNewSubTask(ctx, task.ID, proto.StepInit, subtask.SchedulerID, nil, subtask.Type, true)
	require.NoError(t, err)
	instanceIDs, err = dsp.GetAllSchedulerIDs(ctx, task)
	require.NoError(t, err)
	require.Equal(t, []string{serverIDs[1]}, instanceIDs)
	// server ids: uuid0, uuid1
	// subtask instance ids: uuid0, uuid1
	subtask = &proto.Subtask{
		Type:        proto.TaskTypeExample,
		TaskID:      task.ID,
		SchedulerID: serverIDs[0],
	}
	err = mgr.AddNewSubTask(ctx, task.ID, proto.StepInit, subtask.SchedulerID, nil, subtask.Type, true)
	require.NoError(t, err)
	instanceIDs, err = dsp.GetAllSchedulerIDs(ctx, task)
	require.NoError(t, err)
	require.Len(t, instanceIDs, len(serverIDs))
	require.ElementsMatch(t, instanceIDs, serverIDs)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockSchedulerNodes"))
}

func TestTaskFailInManager(t *testing.T) {
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "handle_test")

	mockDispatcher := mock.NewMockDispatcher(ctrl)
	mockDispatcher.EXPECT().Init().Return(errors.New("mock dispatcher init error"))

	dspManager, mgr := MockDispatcherManager(t, pool)
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			return mockDispatcher
		})
	dspManager.Start()
	defer dspManager.Stop()

	// unknown task type
	taskID, err := mgr.AddNewGlobalTask(ctx, "test", "test-type", 1, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		task, err := mgr.GetGlobalTaskByID(ctx, taskID)
		require.NoError(t, err)
		return task.State == proto.TaskStateFailed &&
			strings.Contains(task.Error.Error(), "unknown task type")
	}, time.Second*10, time.Millisecond*300)

	// dispatcher init error
	taskID, err = mgr.AddNewGlobalTask(ctx, "test2", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		task, err := mgr.GetGlobalTaskByID(ctx, taskID)
		require.NoError(t, err)
		return task.State == proto.TaskStateFailed &&
			strings.Contains(task.Error.Error(), "mock dispatcher init error")
	}, time.Second*10, time.Millisecond*300)
}

func checkDispatch(t *testing.T, taskCnt int, isSucc, isCancel, isSubtaskCancel, isPauseAndResume bool) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/MockDisableDistTask"))
	}()
	// test DispatchTaskLoop
	// test parallelism control
	var originalConcurrency int
	if taskCnt == 1 {
		originalConcurrency = dispatcher.DefaultDispatchConcurrency
		dispatcher.DefaultDispatchConcurrency = 1
	}

	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	dsp, mgr := MockDispatcherManager(t, pool)
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			mockDispatcher := dsp.MockDispatcher(task)
			mockDispatcher.Extension = &numberExampleDispatcherExt{}
			return mockDispatcher
		})
	dsp.Start()
	defer func() {
		dsp.Stop()
		// make data race happy
		if taskCnt == 1 {
			dispatcher.DefaultDispatchConcurrency = originalConcurrency
		}
	}()

	require.NoError(t, mgr.StartManager(ctx, ":4000", "background"))

	// 3s
	cnt := 60
	checkGetRunningTaskCnt := func(expected int) {
		require.Eventually(t, func() bool {
			return dsp.GetRunningTaskCnt() == expected
		}, time.Second, 50*time.Millisecond)
	}

	checkTaskRunningCnt := func() []*proto.Task {
		var tasks []*proto.Task
		require.Eventually(t, func() bool {
			var err error
			tasks, err = mgr.GetGlobalTasksInStates(ctx, proto.TaskStateRunning)
			require.NoError(t, err)
			return len(tasks) == taskCnt
		}, time.Second, 50*time.Millisecond)
		return tasks
	}

	checkSubtaskCnt := func(tasks []*proto.Task, taskIDs []int64) {
		for i, taskID := range taskIDs {
			require.Equal(t, int64(i+1), tasks[i].ID)
			require.Eventually(t, func() bool {
				cnt, err := mgr.GetSubtaskInStatesCnt(ctx, taskID, proto.TaskStatePending)
				require.NoError(t, err)
				return int64(subtaskCnt) == cnt
			}, time.Second, 50*time.Millisecond)
		}
	}

	// Mock add tasks.
	taskIDs := make([]int64, 0, taskCnt)
	for i := 0; i < taskCnt; i++ {
		taskID, err := mgr.AddNewGlobalTask(ctx, fmt.Sprintf("%d", i), proto.TaskTypeExample, 0, nil)
		require.NoError(t, err)
		taskIDs = append(taskIDs, taskID)
	}
	// test OnNextSubtasksBatch.
	checkGetRunningTaskCnt(taskCnt)
	tasks := checkTaskRunningCnt()
	checkSubtaskCnt(tasks, taskIDs)
	// test parallelism control
	if taskCnt == 1 {
		taskID, err := mgr.AddNewGlobalTask(ctx, fmt.Sprintf("%d", taskCnt), proto.TaskTypeExample, 0, nil)
		require.NoError(t, err)
		checkGetRunningTaskCnt(taskCnt)
		// Clean the task.
		deleteTasks(t, store, taskID)
		dsp.DelRunningTask(taskID)
	}

	// test DetectTaskLoop
	checkGetTaskState := func(expectedState proto.TaskState) {
		i := 0
		for ; i < cnt; i++ {
			tasks, err := mgr.GetGlobalTasksInStates(ctx, expectedState)
			require.NoError(t, err)
			if len(tasks) == taskCnt {
				break
			}
			historyTasks, err := mgr.GetGlobalTasksFromHistoryInStates(ctx, expectedState)
			require.NoError(t, err)
			if len(tasks)+len(historyTasks) == taskCnt {
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
		require.Less(t, i, cnt)
	}
	// Test all subtasks are successful.
	var err error
	if isSucc {
		// Mock subtasks succeed.
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.TaskStateSucceed, nil)
			require.NoError(t, err)
		}
		checkGetTaskState(proto.TaskStateSucceed)
		require.Len(t, tasks, taskCnt)

		checkGetRunningTaskCnt(0)
		return
	}

	if isCancel {
		for i := 1; i <= taskCnt; i++ {
			err = mgr.CancelGlobalTask(ctx, int64(i))
			require.NoError(t, err)
		}
	} else if isPauseAndResume {
		for i := 0; i < taskCnt; i++ {
			found, err := mgr.PauseTask(ctx, fmt.Sprintf("%d", i))
			require.Equal(t, true, found)
			require.NoError(t, err)
		}
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.TaskStatePaused, nil)
			require.NoError(t, err)
		}
		checkGetTaskState(proto.TaskStatePaused)
		for i := 0; i < taskCnt; i++ {
			found, err := mgr.ResumeTask(ctx, fmt.Sprintf("%d", i))
			require.Equal(t, true, found)
			require.NoError(t, err)
		}

		// Mock subtasks succeed.
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.TaskStateSucceed, nil)
			require.NoError(t, err)
		}
		checkGetTaskState(proto.TaskStateSucceed)
		return
	} else {
		// Test each task has a subtask failed.
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/MockUpdateTaskErr", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/MockUpdateTaskErr"))
		}()

		if isSubtaskCancel {
			// Mock a subtask canceled
			for i := 1; i <= subtaskCnt*taskCnt; i += subtaskCnt {
				err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.TaskStateCanceled, nil)
				require.NoError(t, err)
			}
		} else {
			// Mock a subtask fails.
			for i := 1; i <= subtaskCnt*taskCnt; i += subtaskCnt {
				err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.TaskStateFailed, nil)
				require.NoError(t, err)
			}
		}
	}

	checkGetTaskState(proto.TaskStateReverting)
	require.Len(t, tasks, taskCnt)
	// Mock all subtask reverted.
	start := subtaskCnt * taskCnt
	for i := start; i <= start+subtaskCnt*taskCnt; i++ {
		err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.TaskStateReverted, nil)
		require.NoError(t, err)
	}
	checkGetTaskState(proto.TaskStateReverted)
	require.Len(t, tasks, taskCnt)
}

func TestSimple(t *testing.T) {
	checkDispatch(t, 1, true, false, false, false)
}

func TestSimpleErrStage(t *testing.T) {
	checkDispatch(t, 1, false, false, false, false)
}

func TestSimpleCancel(t *testing.T) {
	checkDispatch(t, 1, false, true, false, false)
}

func TestSimpleSubtaskCancel(t *testing.T) {
	checkDispatch(t, 1, false, false, true, false)
}

func TestParallel(t *testing.T) {
	checkDispatch(t, 3, true, false, false, false)
}

func TestParallelErrStage(t *testing.T) {
	checkDispatch(t, 3, false, false, false, false)
}

func TestParallelCancel(t *testing.T) {
	checkDispatch(t, 3, false, true, false, false)
}

func TestParallelSubtaskCancel(t *testing.T) {
	checkDispatch(t, 3, false, false, true, false)
}

func TestPause(t *testing.T) {
	checkDispatch(t, 1, false, false, false, true)
}

func TestParallelPause(t *testing.T) {
	checkDispatch(t, 3, false, false, false, true)
}

func TestVerifyTaskStateTransform(t *testing.T) {
	testCases := []struct {
		oldState proto.TaskState
		newState proto.TaskState
		expect   bool
	}{
		{proto.TaskStateRunning, proto.TaskStateRunning, true},
		{proto.TaskStatePending, proto.TaskStateRunning, true},
		{proto.TaskStatePending, proto.TaskStateReverting, false},
		{proto.TaskStateRunning, proto.TaskStateReverting, true},
		{proto.TaskStateReverting, proto.TaskStateReverted, true},
		{proto.TaskStateReverting, proto.TaskStateSucceed, false},
		{proto.TaskStateRunning, proto.TaskStatePausing, true},
		{proto.TaskStateRunning, proto.TaskStateResuming, false},
		{proto.TaskStateCancelling, proto.TaskStateRunning, false},
		{proto.TaskStateCanceled, proto.TaskStateRunning, false},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.expect, dispatcher.VerifyTaskStateTransform(tc.oldState, tc.newState))
	}
}

func TestIsCancelledErr(t *testing.T) {
	require.False(t, dispatcher.IsCancelledErr(errors.New("some err")))
	require.False(t, dispatcher.IsCancelledErr(context.Canceled))
	require.True(t, dispatcher.IsCancelledErr(errors.New("cancelled by user")))
}
