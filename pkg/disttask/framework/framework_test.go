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

package framework_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestFrameworkRandomOwnerChange(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 5)
	defer ctrl.Finish()
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	random := rand.New(rand.NewSource(seed))

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	var wg util.WaitGroupWrapper
	for i := 0; i < 10; i++ {
		taskKey := fmt.Sprintf("key%d", i)
		wg.Run(func() {
			testutil.DispatchTaskAndCheckSuccess(ctx, t, taskKey, testContext, nil)
		})
	}
	wg.Run(func() {
		for i := 0; i < 3; i++ {
			time.Sleep(time.Duration(random.Intn(500)) * time.Millisecond)
			distContext.SetOwner(int(random.Int31n(5)))
		}
	})
	wg.Wait()
	distContext.Close()
}

func TestFrameworkAddDomain(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key1", testContext, nil)
	distContext.AddDomain()
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key2", testContext, nil)
	distContext.SetOwner(1)
	time.Sleep(2 * time.Second) // make sure owner changed
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key3", testContext, nil)
	distContext.Close()
	distContext.AddDomain()
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key4", testContext, nil)
}

func TestFrameworkDeleteDomain(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key1", testContext, nil)
	distContext.DeleteDomain(1)
	time.Sleep(2 * time.Second) // make sure the owner changed
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key2", testContext, nil)
	distContext.Close()
}

func TestFrameworkWithQuery(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key1", testContext, nil)

	tk := testkit.NewTestKit(t, distContext.Store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null)")
	rs, err := tk.Exec("select ifnull(a,b) from t")
	require.NoError(t, err)
	fields := rs.Fields()
	require.Greater(t, len(fields), 0)
	require.Equal(t, "ifnull(a,b)", rs.Fields()[0].Column.Name.L)
	require.NoError(t, rs.Close())
	distContext.Close()
}

func TestFrameworkCancelGTask(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunCancel", "1*return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunCancel"))
	}()
	task := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStateReverted, task.State)
	testContext.M.Range(func(key, value interface{}) bool {
		testContext.M.Delete(key)
		return true
	})
	distContext.Close()
}

func TestFrameworkSubTaskFailed(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 1)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunErr", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunErr"))
	}()
	testutil.DispatchTaskAndCheckState(ctx, t, "key1", testContext, proto.TaskStateReverted)

	distContext.Close()
}

func TestFrameworkSubTaskInitEnvFailed(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 1)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockExecSubtaskInitEnvErr", "return()"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockExecSubtaskInitEnvErr"))
	}()
	testutil.DispatchTaskAndCheckState(ctx, t, "key1", testContext, proto.TaskStateReverted)
	distContext.Close()
}

func TestOwnerChange(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	scheduler.MockOwnerChange = func() {
		distContext.SetOwner(0)
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockOwnerChange", "1*return(true)"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockOwnerChange"))
	distContext.Close()
}

func TestTaskExecutorDownBasic(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 4)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "4*return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown", "return(\":4000\")"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	distContext.Close()
}

func TestTaskExecutorDownManyNodes(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 30)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager", "30*return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown", "return(\":4000\")"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
	distContext.Close()
}

func TestFrameworkSetLabel(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	tk := testkit.NewTestKit(t, distContext.Store)

	// 1. all "" role.
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😁", testContext, nil)

	// 2. one "background" role.
	tk.MustExec("set global tidb_service_scope=background")
	tk.MustQuery("select @@global.tidb_service_scope").Check(testkit.Rows("background"))
	tk.MustQuery("select @@tidb_service_scope").Check(testkit.Rows("background"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)

	// 3. 2 "background" role.
	tk.MustExec("update mysql.dist_framework_meta set role = \"background\" where host = \":4001\"")
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😆", testContext, nil)

	// 4. set wrong sys var.
	tk.MustMatchErrMsg("set global tidb_service_scope=wrong", `incorrect value: .*. tidb_service_scope options: "", background`)

	// 5. set keyspace id.
	tk.MustExec("update mysql.dist_framework_meta set keyspace_id = 16777216 where host = \":4001\"")
	tk.MustQuery("select keyspace_id from mysql.dist_framework_meta where host = \":4001\"").Check(testkit.Rows("16777216"))

	distContext.Close()
}

func TestMultiTasks(t *testing.T) {
	ctx, ctrl, _, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testContext := &testutil.TestContext{}
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)

	taskCnt := 3
	tasks := make([]*proto.Task, taskCnt)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunErr", "1*return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunErr"))
	})

	for i := 0; i < taskCnt; i++ {
		task, err := handle.SubmitTask(ctx, fmt.Sprintf("key%d", i), proto.TaskTypeExample, 1, nil)
		require.NoError(t, err)
		tasks[i] = task
	}
	failCount := 0
	for i := 0; i < taskCnt; i++ {
		gotTask := testutil.WaitTaskDoneOrPaused(ctx, t, tasks[i].Key)
		if gotTask.State == proto.TaskStateReverted {
			failCount++
		}
	}
	require.Equal(t, 1, failCount)

	testContext.M.Range(func(key, value interface{}) bool {
		testContext.M.Delete(key)
		return true
	})

	distContext.Close()
}

func TestGC(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds", "return(1)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/historySubtaskTableGcInterval", "return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/historySubtaskTableGcInterval"))
	}()

	testutil.DispatchTaskAndCheckSuccess(ctx, t, "😊", testContext, nil)

	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)

	var historySubTasksCnt int
	require.Eventually(t, func() bool {
		historySubTasksCnt, err = storage.GetSubtasksFromHistoryForTest(ctx, mgr)
		if err != nil {
			return false
		}
		return historySubTasksCnt == 4
	}, 10*time.Second, 500*time.Millisecond)

	scheduler.WaitTaskFinished <- struct{}{}

	require.Eventually(t, func() bool {
		historySubTasksCnt, err := storage.GetSubtasksFromHistoryForTest(ctx, mgr)
		if err != nil {
			return false
		}
		return historySubTasksCnt == 0
	}, 10*time.Second, 500*time.Millisecond)

	distContext.Close()
}

func TestFrameworkSubtaskFinishedCancel(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockSubtaskFinishedCancel", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockSubtaskFinishedCancel"))
	}()
	testutil.DispatchTaskAndCheckState(ctx, t, "key1", testContext, proto.TaskStateReverted)
	distContext.Close()
}

func TestFrameworkRunSubtaskCancel(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockRunSubtaskCancel", "1*return(true)"))
	testutil.DispatchTaskAndCheckState(ctx, t, "key1", testContext, proto.TaskStateReverted)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockRunSubtaskCancel"))
	distContext.Close()
}

func TestFrameworkCleanUpRoutine(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", "return()"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key1", testContext, nil)
	<-scheduler.WaitCleanUpFinished
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	tasks, err := mgr.GetTaskByKeyWithHistory(ctx, "key1")
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	distContext.Close()
}

func TestTaskCancelledBeforeUpdateTask(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 1)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/cancelBeforeUpdateTask", "1*return(true)"))
	testutil.DispatchTaskAndCheckState(ctx, t, "key1", testContext, proto.TaskStateReverted)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/cancelBeforeUpdateTask"))
	distContext.Close()
}
