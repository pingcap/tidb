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
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	mockDispatch "github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

const (
	subtaskCnt = 3
)

var mockedAllServerInfos = []*infosync.ServerInfo{}

func getTestDispatcherExt(ctrl *gomock.Controller) dispatcher.Extension {
	mockDispatcher := mockDispatch.NewMockExtension(ctrl)
	mockDispatcher.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockDispatcher.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return mockedAllServerInfos, true, nil
		},
	).AnyTimes()
	mockDispatcher.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockDispatcher.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			return proto.StepDone
		},
	).AnyTimes()
	mockDispatcher.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			return nil, nil
		},
	).AnyTimes()

	mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockDispatcher
}

func getNumberExampleDispatcherExt(ctrl *gomock.Controller) dispatcher.Extension {
	mockDispatcher := mockDispatch.NewMockExtension(ctrl)
	mockDispatcher.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockDispatcher.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			serverInfo, err := dispatcher.GenerateTaskExecutorNodes(ctx)
			return serverInfo, true, err
		},
	).AnyTimes()
	mockDispatcher.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockDispatcher.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			switch task.Step {
			case proto.StepInit:
				return proto.StepOne
			default:
				return proto.StepDone
			}
		},
	).AnyTimes()
	mockDispatcher.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ dispatcher.TaskHandle, task *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
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
		},
	).AnyTimes()

	mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockDispatcher
}

func MockDispatcherManager(t *testing.T, ctrl *gomock.Controller, pool *pools.ResourcePool, ext dispatcher.Extension, cleanUp dispatcher.CleanUpRoutine) (*dispatcher.Manager, *storage.TaskManager) {
	ctx := context.WithValue(context.Background(), "etcd", true)
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	dsp, err := dispatcher.NewManager(util.WithInternalSourceType(ctx, "dispatcher"), mgr, "host:port")
	require.NoError(t, err)
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			mockDispatcher := dsp.MockDispatcher(task)
			mockDispatcher.Extension = ext
			return mockDispatcher
		})
	return dsp, mgr
}

func MockDispatcherManagerWithMockTaskMgr(t *testing.T, ctrl *gomock.Controller, pool *pools.ResourcePool, taskMgr *mock.MockTaskManager, ext dispatcher.Extension, cleanUp dispatcher.CleanUpRoutine) *dispatcher.Manager {
	ctx := context.WithValue(context.Background(), "etcd", true)
	dsp, err := dispatcher.NewManager(util.WithInternalSourceType(ctx, "dispatcher"), taskMgr, "host:port")
	require.NoError(t, err)
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			mockDispatcher := dsp.MockDispatcher(task)
			mockDispatcher.Extension = ext
			return mockDispatcher
		})
	dispatcher.RegisterDispatcherCleanUpFactory(proto.TaskTypeExample,
		func() dispatcher.CleanUpRoutine {
			return cleanUp
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockTaskExecutorNodes", "return()"))
	dspManager, mgr := MockDispatcherManager(t, ctrl, pool, getTestDispatcherExt(ctrl), nil)
	// test no server
	task := &proto.Task{ID: 1, Type: proto.TaskTypeExample}
	dsp := dspManager.MockDispatcher(task)
	dsp.Extension = getTestDispatcherExt(ctrl)
	instanceIDs, err := dsp.GetAllTaskExecutorIDs(ctx, task)
	require.Lenf(t, instanceIDs, 0, "GetAllTaskExecutorIDs when there's no subtask")
	require.NoError(t, err)

	// test 2 servers
	// server ids: uuid0, uuid1
	// subtask instance ids: nil
	uuids := []string{"ddl_id_1", "ddl_id_2"}
	serverIDs := []string{"10.123.124.10:32457", "[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:65535"}

	dispatcher.MockServerInfo = []*infosync.ServerInfo{
		{
			ID:   uuids[0],
			IP:   "10.123.124.10",
			Port: 32457,
		},
		{
			ID:   uuids[1],
			IP:   "ABCD:EF01:2345:6789:ABCD:EF01:2345:6789",
			Port: 65535,
		},
	}
	instanceIDs, err = dsp.GetAllTaskExecutorIDs(ctx, task)
	require.Lenf(t, instanceIDs, 0, "GetAllTaskExecutorIDs")
	require.NoError(t, err)

	// server ids: uuid0, uuid1
	// subtask instance ids: uuid1
	subtask := &proto.Subtask{
		Type:   proto.TaskTypeExample,
		TaskID: task.ID,
		ExecID: serverIDs[1],
	}
	testutil.CreateSubTask(t, mgr, task.ID, proto.StepInit, subtask.ExecID, nil, subtask.Type, 11, true)
	instanceIDs, err = dsp.GetAllTaskExecutorIDs(ctx, task)
	require.NoError(t, err)
	require.Equal(t, []string{serverIDs[1]}, instanceIDs)
	// server ids: uuid0, uuid1
	// subtask instance ids: uuid0, uuid1
	subtask = &proto.Subtask{
		Type:   proto.TaskTypeExample,
		TaskID: task.ID,
		ExecID: serverIDs[0],
	}
	testutil.CreateSubTask(t, mgr, task.ID, proto.StepInit, subtask.ExecID, nil, subtask.Type, 11, true)
	instanceIDs, err = dsp.GetAllTaskExecutorIDs(ctx, task)
	require.NoError(t, err)
	require.Len(t, instanceIDs, len(serverIDs))
	require.ElementsMatch(t, instanceIDs, serverIDs)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockTaskExecutorNodes"))
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
	dspManager, mgr := MockDispatcherManager(t, ctrl, pool, getTestDispatcherExt(ctrl), nil)
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			return mockDispatcher
		})
	dspManager.Start()
	defer dspManager.Stop()

	// unknown task type
	taskID, err := mgr.CreateTask(ctx, "test", "test-type", 1, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		task, err := mgr.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		return task.State == proto.TaskStateFailed &&
			strings.Contains(task.Error.Error(), "unknown task type")
	}, time.Second*10, time.Millisecond*300)

	// dispatcher init error
	taskID, err = mgr.CreateTask(ctx, "test2", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		task, err := mgr.GetTaskByID(ctx, taskID)
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
		originalConcurrency = proto.MaxConcurrentTask
		proto.MaxConcurrentTask = 1
	}

	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	dsp, mgr := MockDispatcherManager(t, ctrl, pool, getNumberExampleDispatcherExt(ctrl), nil)
	dsp.Start()
	defer func() {
		dsp.Stop()
		// make data race happy
		if taskCnt == 1 {
			proto.MaxConcurrentTask = originalConcurrency
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
			tasks, err = mgr.GetTasksInStates(ctx, proto.TaskStateRunning)
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
		taskID, err := mgr.CreateTask(ctx, fmt.Sprintf("%d", i), proto.TaskTypeExample, 0, nil)
		require.NoError(t, err)
		taskIDs = append(taskIDs, taskID)
	}
	// test OnNextSubtasksBatch.
	checkGetRunningTaskCnt(taskCnt)
	tasks := checkTaskRunningCnt()
	checkSubtaskCnt(tasks, taskIDs)
	// test parallelism control
	if taskCnt == 1 {
		taskID, err := mgr.CreateTask(ctx, fmt.Sprintf("%d", taskCnt), proto.TaskTypeExample, 0, nil)
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
			tasks, err := mgr.GetTasksInStates(ctx, expectedState)
			require.NoError(t, err)
			if len(tasks) == taskCnt {
				break
			}
			historyTasks, err := mgr.GetTasksFromHistoryInStates(ctx, expectedState)
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
			err = mgr.CancelTask(ctx, int64(i))
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

func TestManagerDispatchLoop(t *testing.T) {
	// Mock 16 cpu node.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
	})
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDispatcher := mock.NewMockDispatcher(ctrl)

	_ = testkit.CreateMockStore(t)
	require.Eventually(t, func() bool {
		taskMgr, err := storage.GetTaskManager()
		return err == nil && taskMgr != nil
	}, 10*time.Second, 100*time.Millisecond)

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")
	taskMgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	require.NotNil(t, taskMgr)

	// in this test, we only test dispatcher manager, so we add a subtask takes 16
	// slots to avoid reserve by slots, and make sure below test cases works.
	serverInfos, err := infosync.GetAllServerInfo(ctx)
	require.NoError(t, err)
	for _, s := range serverInfos {
		execID := disttaskutil.GenerateExecID(s.IP, s.Port)
		testutil.InsertSubtask(t, taskMgr, 1000000, proto.StepOne, execID, []byte(""), proto.TaskStatePending, proto.TaskTypeExample, 16)
	}
	concurrencies := []int{4, 6, 16, 2, 4, 4}
	waitChannels := make([]chan struct{}, len(concurrencies))
	for i := range waitChannels {
		waitChannels[i] = make(chan struct{})
	}
	var counter atomic.Int32
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			idx := counter.Load()
			mockDispatcher = mock.NewMockDispatcher(ctrl)
			mockDispatcher.EXPECT().Init().Return(nil)
			mockDispatcher.EXPECT().ExecuteTask().Do(func() {
				require.NoError(t, taskMgr.WithNewSession(func(se sessionctx.Context) error {
					_, err := storage.ExecSQL(ctx, se, "update mysql.tidb_global_task set state=%?, step=%? where id=%?",
						proto.TaskStateRunning, proto.StepOne, task.ID)
					return err
				}))
				<-waitChannels[idx]
				require.NoError(t, taskMgr.WithNewSession(func(se sessionctx.Context) error {
					_, err := storage.ExecSQL(ctx, se, "update mysql.tidb_global_task set state=%?, step=%? where id=%?",
						proto.TaskStateSucceed, proto.StepDone, task.ID)
					return err
				}))
			})
			mockDispatcher.EXPECT().Close()
			counter.Add(1)
			return mockDispatcher
		},
	)
	for i := 0; i < len(concurrencies); i++ {
		_, err := taskMgr.CreateTask(ctx, fmt.Sprintf("key/%d", i), proto.TaskTypeExample, concurrencies[i], []byte("{}"))
		require.NoError(t, err)
	}
	getRunningTaskKeys := func() []string {
		tasks, err := taskMgr.GetTasksInStates(ctx, proto.TaskStateRunning)
		require.NoError(t, err)
		taskKeys := make([]string, len(tasks))
		for i, task := range tasks {
			taskKeys[i] = task.Key
		}
		slices.Sort(taskKeys)
		return taskKeys
	}
	require.Eventually(t, func() bool {
		taskKeys := getRunningTaskKeys()
		return err == nil && len(taskKeys) == 4 &&
			taskKeys[0] == "key/0" && taskKeys[1] == "key/1" &&
			taskKeys[2] == "key/3" && taskKeys[3] == "key/4"
	}, time.Second*10, time.Millisecond*100)
	// finish the first task
	close(waitChannels[0])
	require.Eventually(t, func() bool {
		taskKeys := getRunningTaskKeys()
		return err == nil && len(taskKeys) == 4 &&
			taskKeys[0] == "key/1" && taskKeys[1] == "key/3" &&
			taskKeys[2] == "key/4" && taskKeys[3] == "key/5"
	}, time.Second*10, time.Millisecond*100)
	// finish the second task
	close(waitChannels[1])
	require.Eventually(t, func() bool {
		taskKeys := getRunningTaskKeys()
		return err == nil && len(taskKeys) == 4 &&
			taskKeys[0] == "key/2" && taskKeys[1] == "key/3" &&
			taskKeys[2] == "key/4" && taskKeys[3] == "key/5"
	}, time.Second*10, time.Millisecond*100)
	// close others
	for i := 2; i < len(concurrencies); i++ {
		close(waitChannels[i])
	}
	require.Eventually(t, func() bool {
		taskKeys := getRunningTaskKeys()
		return err == nil && len(taskKeys) == 0
	}, time.Second*10, time.Millisecond*100)
}
