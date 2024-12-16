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

package integrationtests

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func registerExampleTaskWithDXFCtx(c *testutil.TestDXFContext, schedulerExt scheduler.Extension, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) {
	registerExampleTask(c.T, c.MockCtrl, schedulerExt, c.TestContext, runSubtaskFn)
}

func registerExampleTask(t testing.TB, ctrl *gomock.Controller, schedulerExt scheduler.Extension, testContext *testutil.TestContext, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) {
	if runSubtaskFn == nil {
		runSubtaskFn = getCommonSubtaskRunFn(testContext)
	}
	executorExt := testutil.GetCommonTaskExecutorExt(ctrl, func(task *proto.Task) (execute.StepExecutor, error) {
		return testutil.GetCommonStepExecutor(ctrl, task.Step, runSubtaskFn), nil
	})
	testutil.RegisterExampleTask(t, schedulerExt, executorExt, testutil.GetCommonCleanUpRoutine(ctrl))
}

func getCommonSubtaskRunFn(testCtx *testutil.TestContext) func(_ context.Context, subtask *proto.Subtask) error {
	return func(_ context.Context, subtask *proto.Subtask) error {
		switch subtask.Step {
		case proto.StepOne, proto.StepTwo:
			testCtx.CollectSubtask(subtask)
		default:
			panic("invalid step")
		}
		return nil
	}
}

func submitTaskAndCheckSuccessForBasic(ctx context.Context, t *testing.T, taskKey string, testContext *testutil.TestContext) int64 {
	return submitTaskAndCheckSuccess(ctx, t, taskKey, "", testContext, map[proto.Step]int{
		proto.StepOne: 3,
		proto.StepTwo: 1,
	})
}

func submitTaskAndCheckSuccess(ctx context.Context, t *testing.T, taskKey string, targetScope string,
	testContext *testutil.TestContext, subtaskCnts map[proto.Step]int) int64 {
	task := testutil.SubmitAndWaitTask(ctx, t, taskKey, targetScope, 1)
	require.Equal(t, proto.TaskStateSucceed, task.State)
	for step, cnt := range subtaskCnts {
		require.Equal(t, cnt, testContext.CollectedSubtaskCnt(task.ID, step))
	}
	return task.ID
}

func TestRandomOwnerChangeWithMultipleTasks(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 5, 16, true)

	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	var wg util.WaitGroupWrapper
	for i := 0; i < 10; i++ {
		taskKey := fmt.Sprintf("key%d", i)
		wg.Run(func() {
			submitTaskAndCheckSuccessForBasic(c.Ctx, t, taskKey, c.TestContext)
		})
	}
	wg.Run(func() {
		seed := time.Now().UnixNano()
		t.Logf("seed in change owner loop: %d", seed)
		random := rand.New(rand.NewSource(seed))
		for i := 0; i < 3; i++ {
			c.ChangeOwner()
			time.Sleep(time.Duration(random.Int63n(int64(3 * time.Second))))
		}
	})
	wg.Wait()
}

func TestFrameworkScaleInAndOut(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 5, 16, true)
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	random := rand.New(rand.NewSource(seed))

	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	var wg util.WaitGroupWrapper
	for i := 0; i < 12; i++ {
		taskKey := fmt.Sprintf("key%d", i)
		wg.Run(func() {
			submitTaskAndCheckSuccessForBasic(c.Ctx, t, taskKey, c.TestContext)
		})
	}
	wg.Run(func() {
		for i := 0; i < 3; i++ {
			if random.Intn(2) == 0 {
				c.ScaleOut(1)
			} else {
				c.ScaleIn(1)
			}
			time.Sleep(time.Duration(random.Int63n(int64(3 * time.Second))))
		}
	})
	wg.Wait()
}

func TestFrameworkWithQuery(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 2, 16, true)

	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		submitTaskAndCheckSuccessForBasic(c.Ctx, t, "key1", c.TestContext)
	})

	tk := testkit.NewTestKit(t, c.Store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null)")
	rs, err := tk.Exec("select ifnull(a,b) from t")
	require.NoError(t, err)
	fields := rs.Fields()
	require.Greater(t, len(fields), 0)
	require.Equal(t, "ifnull(a,b)", rs.Fields()[0].Column.Name.L)
	require.NoError(t, rs.Close())

	wg.Wait()
}

func TestFrameworkCancelTask(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 2, 16, true)

	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	var counter atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterRunSubtask",
		func(e taskexecutor.TaskExecutor, _ *error) {
			if counter.Add(1) == 1 {
				require.NoError(t, c.TaskMgr.CancelTask(c.Ctx, e.GetTaskBase().ID))
			}
		},
	)
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}

func TestFrameworkSubTaskInitEnvFailed(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 1, 16, true)
	schedulerExt := testutil.GetMockBasicSchedulerExt(c.MockCtrl)
	stepExec := mockexecute.NewMockStepExecutor(c.MockCtrl)
	stepExec.EXPECT().Init(gomock.Any()).Return(errors.New("mockExecSubtaskInitEnvErr")).AnyTimes()
	executorExt := testutil.GetCommonTaskExecutorExt(c.MockCtrl, func(task *proto.Task) (execute.StepExecutor, error) {
		return stepExec, nil
	})
	testutil.RegisterExampleTask(t, schedulerExt, executorExt, testutil.GetCommonCleanUpRoutine(c.MockCtrl))
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}

func TestOwnerChangeWhenSchedule(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 3, 16, true)
	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	var counter atomic.Int32
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockOwnerChange", func() {
		if counter.Add(1) == 1 {
			c.AsyncChangeOwner()
			time.Sleep(time.Second)
		}
	}))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockOwnerChange"))
	})
	submitTaskAndCheckSuccessForBasic(c.Ctx, t, "😊", c.TestContext)
}

func TestGC(t *testing.T) {
	ch := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds", func(interval *int) {
		*interval = 1
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/historySubtaskTableGcInterval", func(interval *time.Duration) {
		*interval = 1 * time.Second
		<-ch
	})
	c := testutil.NewTestDXFContext(t, 3, 16, true)

	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)

	submitTaskAndCheckSuccessForBasic(c.Ctx, t, "😊", c.TestContext)

	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)

	var historySubTasksCnt int
	require.Eventually(t, func() bool {
		historySubTasksCnt, err = testutil.GetSubtasksFromHistory(c.Ctx, mgr)
		if err != nil {
			return false
		}
		return historySubTasksCnt == 4
	}, 10*time.Second, 500*time.Millisecond)

	ch <- struct{}{}

	require.Eventually(t, func() bool {
		historySubTasksCnt, err := testutil.GetSubtasksFromHistory(c.Ctx, mgr)
		if err != nil {
			return false
		}
		return historySubTasksCnt == 0
	}, 10*time.Second, 500*time.Millisecond)
}

func TestFrameworkRunSubtaskCancelOrFailed(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 3, 16, true)

	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	t.Run("meet cancel on run subtask", func(t *testing.T) {
		var counter atomic.Int32
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterRunSubtask",
			func(e taskexecutor.TaskExecutor, errP *error) {
				if counter.Add(1) == 1 {
					e.CancelRunningSubtask()
					*errP = taskexecutor.ErrCancelSubtask
				}
			},
		)
		task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
		require.Equal(t, proto.TaskStateReverted, task.State)
	})

	t.Run("meet some error on run subtask", func(t *testing.T) {
		var counter atomic.Int32
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterRunSubtask",
			func(_ taskexecutor.TaskExecutor, errP *error) {
				if counter.Add(1) == 1 {
					*errP = errors.New("MockExecutorRunErr")
				}
			},
		)
		task := testutil.SubmitAndWaitTask(c.Ctx, t, "key2", "", 1)
		require.Equal(t, proto.TaskStateReverted, task.State)
	})
}

func TestFrameworkCleanUpRoutine(t *testing.T) {
	bak := scheduler.DefaultCleanUpInterval
	defer func() {
		scheduler.DefaultCleanUpInterval = bak
	}()
	scheduler.DefaultCleanUpInterval = 500 * time.Millisecond
	c := testutil.NewTestDXFContext(t, 3, 16, true)
	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	ch := make(chan struct{}, 1)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", func() {
		ch <- struct{}{}
	})

	// normal
	submitTaskAndCheckSuccessForBasic(c.Ctx, t, "key1", c.TestContext)
	<-ch
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	tasks, err := mgr.GetTaskByKeyWithHistory(c.Ctx, "key1")
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	subtasks, err := testutil.GetSubtasksFromHistory(c.Ctx, mgr)
	require.NoError(t, err)
	require.NotEmpty(t, subtasks)

	// transfer err
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTransferErr", "1*return()")
	submitTaskAndCheckSuccessForBasic(c.Ctx, t, "key2", c.TestContext)
	<-ch
	mgr, err = storage.GetTaskManager()
	require.NoError(t, err)
	tasks, err = mgr.GetTaskByKeyWithHistory(c.Ctx, "key1")
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	subtasks, err = testutil.GetSubtasksFromHistory(c.Ctx, mgr)
	require.NoError(t, err)
	require.NotEmpty(t, subtasks)
}

func TestTaskCancelledBeforeUpdateTask(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 1, 16, true)

	registerExampleTask(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	var counter atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/cancelBeforeUpdateTask", func(taskID int64) {
		if counter.Add(1) == 1 {
			require.NoError(t, c.TaskMgr.CancelTask(c.Ctx, taskID))
		}
	})
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}
