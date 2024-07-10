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

package scheduler_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	mockscheduler "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

const (
	subtaskCnt = 3
)

func getNumberExampleSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	mockScheduler := mockscheduler.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *proto.Task) ([]string, error) {
			return nil, nil
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.TaskBase) proto.Step {
			switch task.Step {
			case proto.StepInit:
				return proto.StepOne
			default:
				return proto.StepDone
			}
		},
	).AnyTimes()
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ storage.TaskHandle, task *proto.Task, _ []string, _ proto.Step) (metas [][]byte, err error) {
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

	mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockScheduler
}

func MockSchedulerManager(t *testing.T, ctrl *gomock.Controller, pool *pools.ResourcePool, ext scheduler.Extension, cleanup scheduler.CleanUpRoutine) (*scheduler.Manager, *storage.TaskManager) {
	ctx := context.WithValue(context.Background(), "etcd", true)
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	sch := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), mgr, "host:port")
	scheduler.RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			mockScheduler := scheduler.NewBaseScheduler(ctx, task, param)
			mockScheduler.Extension = ext
			return mockScheduler
		})
	return sch, mgr
}

func deleteTasks(t *testing.T, store kv.Storage, taskID int64) {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_global_task where id = %d", taskID))
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

	schManager, mgr := MockSchedulerManager(t, ctrl, pool, scheduler.GetTestSchedulerExt(ctrl), nil)
	scheduler.RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			mockScheduler := mock.NewMockScheduler(ctrl)
			mockScheduler.EXPECT().Init().Return(errors.New("mock scheduler init error"))
			return mockScheduler
		})
	schManager.Start()
	defer schManager.Stop()

	// unknown task type
	taskID, err := mgr.CreateTask(ctx, "test", "test-type", 1, "", nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		task, err := mgr.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		return task.State == proto.TaskStateFailed &&
			strings.Contains(task.Error.Error(), "unknown task type")
	}, time.Second*10, time.Millisecond*300)

	// scheduler init error
	taskID, err = mgr.CreateTask(ctx, "test2", proto.TaskTypeExample, 1, "", nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		task, err := mgr.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		return task.State == proto.TaskStateFailed &&
			strings.Contains(task.Error.Error(), "mock scheduler init error")
	}, time.Second*10, time.Millisecond*300)
}

func checkSchedule(t *testing.T, taskCnt int, isSucc, isCancel, isSubtaskCancel, isPauseAndResume bool) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	// test scheduleTaskLoop
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
	ctx = util.WithInternalSourceType(ctx, "scheduler")

	sch, mgr := MockSchedulerManager(t, ctrl, pool, getNumberExampleSchedulerExt(ctrl), nil)
	require.NoError(t, mgr.InitMeta(ctx, ":4000", "background"))
	sch.Start()
	defer func() {
		sch.Stop()
		// make data race happy
		if taskCnt == 1 {
			proto.MaxConcurrentTask = originalConcurrency
		}
	}()

	// 3s
	cnt := 60
	checkGetRunningTaskCnt := func(expected int) {
		require.Eventually(t, func() bool {
			return sch.GetRunningTaskCnt() == expected
		}, 5*time.Second, 50*time.Millisecond)
	}

	checkTaskRunningCnt := func() []*proto.Task {
		var tasks []*proto.Task
		require.Eventually(t, func() bool {
			var err error
			tasks, err = mgr.GetTasksInStates(ctx, proto.TaskStateRunning)
			require.NoError(t, err)
			return len(tasks) == taskCnt
		}, 5*time.Second, 50*time.Millisecond)
		return tasks
	}

	checkSubtaskCnt := func(tasks []*proto.Task, taskIDs []int64) {
		for i, taskID := range taskIDs {
			require.Equal(t, int64(i+1), tasks[i].ID)
			require.Eventually(t, func() bool {
				cntByStates, err := mgr.GetSubtaskCntGroupByStates(ctx, taskID, proto.StepOne)
				require.NoError(t, err)
				return int64(subtaskCnt) == cntByStates[proto.SubtaskStatePending]
			}, 5*time.Second, 50*time.Millisecond)
		}
	}

	// Mock add tasks.
	taskIDs := make([]int64, 0, taskCnt)
	for i := 0; i < taskCnt; i++ {
		taskID, err := mgr.CreateTask(ctx, fmt.Sprintf("%d", i), proto.TaskTypeExample, 0, "background", nil)
		require.NoError(t, err)
		taskIDs = append(taskIDs, taskID)
	}
	// test OnNextSubtasksBatch.
	checkGetRunningTaskCnt(taskCnt)
	tasks := checkTaskRunningCnt()
	checkSubtaskCnt(tasks, taskIDs)
	// test parallelism control
	if taskCnt == 1 {
		taskID, err := mgr.CreateTask(ctx, fmt.Sprintf("%d", taskCnt), proto.TaskTypeExample, 0, "background", nil)
		require.NoError(t, err)
		checkGetRunningTaskCnt(taskCnt)
		// Clean the task.
		deleteTasks(t, store, taskID)
		sch.DelRunningTask(taskID)
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
			historyTasks, err := testutil.GetTasksFromHistoryInStates(ctx, mgr, expectedState)
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
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateSucceed, nil)
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
			require.True(t, found)
			require.NoError(t, err)
		}
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStatePaused, nil)
			require.NoError(t, err)
		}
		checkGetTaskState(proto.TaskStatePaused)
		for i := 0; i < taskCnt; i++ {
			found, err := mgr.ResumeTask(ctx, fmt.Sprintf("%d", i))
			require.True(t, found)
			require.NoError(t, err)
		}

		// Mock subtasks succeed.
		for i := 1; i <= subtaskCnt*taskCnt; i++ {
			err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateSucceed, nil)
			require.NoError(t, err)
		}
		checkGetTaskState(proto.TaskStateSucceed)
		return
	} else {
		if isSubtaskCancel {
			// Mock a subtask canceled
			for i := 1; i <= subtaskCnt*taskCnt; i += subtaskCnt {
				err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateCanceled, nil)
				require.NoError(t, err)
			}
		} else {
			// Mock a subtask fails.
			for i := 1; i <= subtaskCnt*taskCnt; i += subtaskCnt {
				err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateFailed, nil)
				require.NoError(t, err)
			}
		}
	}

	checkGetTaskState(proto.TaskStateReverting)
	require.Len(t, tasks, taskCnt)
	for _, task := range tasks {
		subtasks, err := mgr.GetSubtasksByExecIDAndStepAndStates(
			ctx, ":4000", task.ID, task.Step,
			proto.SubtaskStatePending, proto.SubtaskStateRunning)
		require.NoError(t, err)
		for _, subtask := range subtasks {
			require.NoError(t, mgr.UpdateSubtaskStateAndError(ctx, ":4000", subtask.ID, proto.SubtaskStateCanceled, nil))
		}
	}
	checkGetTaskState(proto.TaskStateReverted)
	require.Len(t, tasks, taskCnt)
}

func TestSimple(t *testing.T) {
	checkSchedule(t, 1, true, false, false, false)
}

func TestSimpleErrStage(t *testing.T) {
	checkSchedule(t, 1, false, false, false, false)
}

func TestSimpleCancel(t *testing.T) {
	checkSchedule(t, 1, false, true, false, false)
}

func TestSimpleSubtaskCancel(t *testing.T) {
	checkSchedule(t, 1, false, false, true, false)
}

func TestParallel(t *testing.T) {
	checkSchedule(t, 3, true, false, false, false)
}

func TestParallelErrStage(t *testing.T) {
	checkSchedule(t, 3, false, false, false, false)
}

func TestParallelCancel(t *testing.T) {
	checkSchedule(t, 3, false, true, false, false)
}

func TestParallelSubtaskCancel(t *testing.T) {
	checkSchedule(t, 3, false, false, true, false)
}

func TestPause(t *testing.T) {
	checkSchedule(t, 1, false, false, false, true)
}

func TestParallelPause(t *testing.T) {
	checkSchedule(t, 3, false, false, false, true)
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
	}
	for _, tc := range testCases {
		require.Equal(t, tc.expect, scheduler.VerifyTaskStateTransform(tc.oldState, tc.newState))
	}
}

func TestIsCancelledErr(t *testing.T) {
	require.False(t, scheduler.IsCancelledErr(errors.New("some err")))
	require.False(t, scheduler.IsCancelledErr(context.Canceled))
	require.True(t, scheduler.IsCancelledErr(errors.New("cancelled by user")))
}

func TestManagerScheduleLoop(t *testing.T) {
	// Mock 16 cpu node.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockScheduler := mock.NewMockScheduler(ctrl)

	_ = testkit.CreateMockStore(t)
	require.Eventually(t, func() bool {
		taskMgr, err := storage.GetTaskManager()
		return err == nil && taskMgr != nil
	}, 10*time.Second, 100*time.Millisecond)

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "scheduler")
	taskMgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	require.NotNil(t, taskMgr)

	// in this test, we only test scheduler manager, so we add a subtask takes 16
	// slots to avoid reserve by slots, and make sure below test cases works.
	serverInfos, err := infosync.GetAllServerInfo(ctx)
	require.NoError(t, err)
	for _, s := range serverInfos {
		execID := disttaskutil.GenerateExecID(s)
		testutil.InsertSubtask(t, taskMgr, 1000000, proto.StepOne, execID, []byte(""), proto.SubtaskStatePending, proto.TaskTypeExample, 16)
	}
	concurrencies := []int{4, 6, 16, 2, 4, 4}
	waitChannels := make(map[string](chan struct{}))
	for i := 0; i < len(concurrencies); i++ {
		waitChannels[fmt.Sprintf("key/%d", i)] = make(chan struct{})
	}
	scheduler.RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			mockScheduler = mock.NewMockScheduler(ctrl)
			// below 2 are for balancer loop, it's async, cannot determine how
			// many times it will be called.
			mockScheduler.EXPECT().GetTask().Return(task).AnyTimes()
			mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
			mockScheduler.EXPECT().Init().Return(nil)
			mockScheduler.EXPECT().ScheduleTask().Do(func() {
				if task.IsDone() {
					return
				}
				require.NoError(t, taskMgr.WithNewSession(func(se sessionctx.Context) error {
					_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), "update mysql.tidb_global_task set state=%?, step=%? where id=%?",
						proto.TaskStateRunning, proto.StepOne, task.ID)
					return err
				}))
				<-waitChannels[task.Key]
				require.NoError(t, taskMgr.WithNewSession(func(se sessionctx.Context) error {
					_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), "update mysql.tidb_global_task set state=%?, step=%? where id=%?",
						proto.TaskStateSucceed, proto.StepDone, task.ID)
					return err
				}))
			})
			mockScheduler.EXPECT().Close()
			return mockScheduler
		},
	)
	for i := 0; i < len(concurrencies); i++ {
		_, err := taskMgr.CreateTask(ctx, fmt.Sprintf("key/%d", i), proto.TaskTypeExample, concurrencies[i], "", []byte("{}"))
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
	close(waitChannels["key/0"])
	require.Eventually(t, func() bool {
		taskKeys := getRunningTaskKeys()
		return err == nil && len(taskKeys) == 4 &&
			taskKeys[0] == "key/1" && taskKeys[1] == "key/3" &&
			taskKeys[2] == "key/4" && taskKeys[3] == "key/5"
	}, time.Second*10, time.Millisecond*100)
	// finish the second task
	close(waitChannels["key/1"])
	require.Eventually(t, func() bool {
		taskKeys := getRunningTaskKeys()
		return err == nil && len(taskKeys) == 4 &&
			taskKeys[0] == "key/2" && taskKeys[1] == "key/3" &&
			taskKeys[2] == "key/4" && taskKeys[3] == "key/5"
	}, time.Second*10, time.Millisecond*100)
	// close others
	for i := 2; i < len(concurrencies); i++ {
		close(waitChannels[fmt.Sprintf("key/%d", i)])
	}
	require.Eventually(t, func() bool {
		taskKeys := getRunningTaskKeys()
		return err == nil && len(taskKeys) == 0
	}, time.Second*10, time.Millisecond*100)
}
