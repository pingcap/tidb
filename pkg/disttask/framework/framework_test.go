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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestFrameworkBasic(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key1", test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key2", test_context)
	distContext.SetOwner(0)
	time.Sleep(2 * time.Second) // make sure owner changed
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key3", test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key4", test_context)
	distContext.SetOwner(1)
	time.Sleep(2 * time.Second) // make sure owner changed
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key5", test_context)
	distContext.Close()
}

func TestFramework3Server(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key1", test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key2", test_context)
	distContext.SetOwner(0)
	time.Sleep(2 * time.Second) // make sure owner changed
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key3", test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key4", test_context)
	distContext.Close()
}

func TestFrameworkAddDomain(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key1", test_context)
	distContext.AddDomain()
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key2", test_context)
	distContext.SetOwner(1)
	time.Sleep(2 * time.Second) // make sure owner changed
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key3", test_context)
	distContext.Close()
	distContext.AddDomain()
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key4", test_context)
}

func TestFrameworkDeleteDomain(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key1", test_context)
	distContext.DeleteDomain(1)
	time.Sleep(2 * time.Second) // make sure the owner changed
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key2", test_context)
	distContext.Close()
}

func TestFrameworkWithQuery(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key1", test_context)

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
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	testutil.DispatchAndCancelTask(t, ctx, "key1", test_context)
	distContext.Close()
}

func TestFrameworkSubTaskFailed(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 1)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockExecutorRunErr", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockExecutorRunErr"))
	}()
	testutil.DispatchTaskAndCheckState(t, ctx, "key1", test_context, proto.TaskStateReverted)

	distContext.Close()
}

func TestFrameworkSubTaskInitEnvFailed(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 1)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockExecSubtaskInitEnvErr", "return()"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockExecSubtaskInitEnvErr"))
	}()
	testutil.DispatchTaskAndCheckState(t, ctx, "key1", test_context, proto.TaskStateReverted)
	distContext.Close()
}

func TestOwnerChange(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	dispatcher.MockOwnerChange = func() {
		distContext.SetOwner(0)
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockOwnerChange", "1*return(true)"))
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "😊", test_context)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mockOwnerChange"))
	distContext.Close()
}

func TestFrameworkCancelThenSubmitSubTask(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/cancelBeforeUpdate", "return()"))
	testutil.DispatchTaskAndCheckState(t, ctx, "😊", test_context, proto.TaskStateReverted)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/cancelBeforeUpdate"))
	distContext.Close()
}

func TestSchedulerDownBasic(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 4)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "4*return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "😊", test_context)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager"))
	distContext.Close()
}

func TestSchedulerDownManyNodes(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 30)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "30*return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "😊", test_context)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager"))
	distContext.Close()
}

func TestFrameworkSetLabel(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	tk := testkit.NewTestKit(t, distContext.Store)

	// 1. all "" role.
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "😁", test_context)

	// 2. one "background" role.
	tk.MustExec("set global tidb_service_scope=background")
	tk.MustQuery("select @@global.tidb_service_scope").Check(testkit.Rows("background"))
	tk.MustQuery("select @@tidb_service_scope").Check(testkit.Rows("background"))
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "😊", test_context)

	// 3. 2 "background" role.
	tk.MustExec("update mysql.dist_framework_meta set role = \"background\" where host = \":4001\"")
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "😆", test_context)

	// 4. set wrong sys var.
	tk.MustMatchErrMsg("set global tidb_service_scope=wrong", `incorrect value: .*. tidb_service_scope options: "", background`)

	// 5. set keyspace id.
	tk.MustExec("update mysql.dist_framework_meta set keyspace_id = 16777216 where host = \":4001\"")
	tk.MustQuery("select keyspace_id from mysql.dist_framework_meta where host = \":4001\"").Check(testkit.Rows("16777216"))

	distContext.Close()
}

func TestMultiTasks(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	num := 3

	ctx, ctrl, _, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	test_contexts := make([]*testutil.TestContext, 3)

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_contexts[0])
	testutil.RegisterTaskMetaForExample2(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_contexts[1])
	testutil.RegisterTaskMetaForExample3(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_contexts[2])

	testutil.DispatchMultiTasksAndOneFail(t, ctx, num, test_contexts)
	distContext.Close()
}

func TestGC(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds", "return(1)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/historySubtaskTableGcInterval", "return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/subtaskHistoryKeepSeconds"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/historySubtaskTableGcInterval"))
	}()

	testutil.DispatchTaskAndCheckSuccess(t, ctx, "😊", test_context)

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

	dispatcher.WaitTaskFinished <- struct{}{}

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
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockSubtaskFinishedCancel", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockSubtaskFinishedCancel"))
	}()
	testutil.DispatchTaskAndCheckState(t, ctx, "key1", test_context, proto.TaskStateReverted)
	distContext.Close()
}

func TestFrameworkRunSubtaskCancel(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockRunSubtaskCancel", "1*return(true)"))
	testutil.DispatchTaskAndCheckState(t, ctx, "key1", test_context, proto.TaskStateReverted)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/MockRunSubtaskCancel"))
	distContext.Close()
}

func TestFrameworkCleanUpRoutine(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/WaitCleanUpFinished", "return()"))
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key1", test_context)
	<-dispatcher.WaitCleanUpFinished
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	tasks, err := mgr.GetGlobalTaskByKeyWithHistory(ctx, "key1")
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	distContext.Close()
}

func TestTaskCancelledBeforeUpdateTask(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 1)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicDispatcherExt(ctrl), test_context)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/cancelBeforeUpdateTask", "1*return(true)"))
	testutil.DispatchTaskAndCheckState(t, ctx, "key1", test_context, proto.TaskStateReverted)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/cancelBeforeUpdateTask"))
	distContext.Close()
}
