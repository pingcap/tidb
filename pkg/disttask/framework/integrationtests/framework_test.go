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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

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

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
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

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
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

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
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

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunCancel", "1*return(1)")
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}

func TestFrameworkSubTaskFailed(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 1, 16, true)

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunErr", "1*return(true)")
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}

func TestFrameworkSubTaskInitEnvFailed(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 1, 16, true)
	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockExecSubtaskInitEnvErr", "return()")
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}

func TestOwnerChangeWhenSchedule(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 3, 16, true)
	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	var once sync.Once
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockOwnerChange", func() {
		once.Do(func() {
			c.AsyncChangeOwner()
			time.Sleep(time.Second)
		})
	}))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockOwnerChange"))
	})
	submitTaskAndCheckSuccessForBasic(c.Ctx, t, "😊", c.TestContext)
}

func TestGC(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds", "return(1)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/historySubtaskTableGcInterval", "return(1)")
	c := testutil.NewTestDXFContext(t, 3, 16, true)

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)

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

	scheduler.WaitTaskFinished <- struct{}{}

	require.Eventually(t, func() bool {
		historySubTasksCnt, err := testutil.GetSubtasksFromHistory(c.Ctx, mgr)
		if err != nil {
			return false
		}
		return historySubTasksCnt == 0
	}, 10*time.Second, 500*time.Millisecond)
}

func TestFrameworkSubtaskFinishedCancel(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 3, 16, true)

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockSubtaskFinishedCancel", "1*return(true)")
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}

func TestFrameworkRunSubtaskCancel(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 3, 16, true)

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockRunSubtaskCancel", "1*return(true)")
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}

func TestFrameworkCleanUpRoutine(t *testing.T) {
	bak := scheduler.DefaultCleanUpInterval
	defer func() {
		scheduler.DefaultCleanUpInterval = bak
	}()
	scheduler.DefaultCleanUpInterval = 500 * time.Millisecond
	c := testutil.NewTestDXFContext(t, 3, 16, true)
	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", "return()")

	// normal
	submitTaskAndCheckSuccessForBasic(c.Ctx, t, "key1", c.TestContext)
	<-scheduler.WaitCleanUpFinished
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
	<-scheduler.WaitCleanUpFinished
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

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/cancelBeforeUpdateTask", "1*return(true)")
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}
