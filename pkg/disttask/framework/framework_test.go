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
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func submitTaskAndCheckSuccessForBasic(ctx context.Context, t *testing.T, taskKey string, testContext *testutil.TestContext) {
	submitTaskAndCheckSuccess(ctx, t, taskKey, testContext, map[proto.Step]int{
		proto.StepOne: 3,
		proto.StepTwo: 1,
	})
}

func submitTaskAndCheckSuccess(ctx context.Context, t *testing.T, taskKey string,
	testContext *testutil.TestContext, subtaskCnts map[proto.Step]int) {
	task := testutil.SubmitAndWaitTask(ctx, t, taskKey)
	require.Equal(t, proto.TaskStateSucceed, task.State)
	for step, cnt := range subtaskCnts {
		require.Equal(t, cnt, testContext.CollectedSubtaskCnt(task.ID, step))
	}
}

func TestRandomOwnerChangeWithMultipleTasks(t *testing.T) {
	nodeCnt := 5
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, nodeCnt)
	defer ctrl.Finish()
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	random := rand.New(rand.NewSource(seed))

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	var wg util.WaitGroupWrapper
	for i := 0; i < 10; i++ {
		taskKey := fmt.Sprintf("key%d", i)
		wg.Run(func() {
			submitTaskAndCheckSuccessForBasic(ctx, t, taskKey, testContext)
		})
	}
	wg.Run(func() {
		for i := 0; i < 3; i++ {
			time.Sleep(time.Duration(random.Intn(2000)) * time.Millisecond)
			idx := int(random.Int31n(int32(nodeCnt)))
			distContext.SetOwner(idx)
			require.Eventually(t, func() bool {
				return distContext.GetDomain(idx).DDL().OwnerManager().IsOwner()
			}, 2*time.Second, 100*time.Millisecond)
		}
	})
	wg.Wait()
	distContext.Close()
}

func TestFrameworkScaleInAndOut(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 5)
	defer ctrl.Finish()
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	random := rand.New(rand.NewSource(seed))

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	var wg util.WaitGroupWrapper
	for i := 0; i < 12; i++ {
		taskKey := fmt.Sprintf("key%d", i)
		wg.Run(func() {
			submitTaskAndCheckSuccessForBasic(ctx, t, taskKey, testContext)
		})
	}
	wg.Run(func() {
		for i := 0; i < 3; i++ {
			if random.Intn(2) == 0 {
				distContext.AddDomain()
			} else {
				// TODO: it's not real scale-in, delete domain doesn't stop task executor
				// closing domain will reset storage.TaskManager which will cause
				// test fail.
				distContext.DeleteDomain(int(random.Int31n(int32(distContext.GetDomainCnt()-1))) + 1)
			}
			time.Sleep(time.Duration(random.Intn(2000)) * time.Millisecond)
			idx := int(random.Int31n(int32(distContext.GetDomainCnt())))
			distContext.SetOwner(idx)
			require.Eventually(t, func() bool {
				return distContext.GetDomain(idx).DDL().OwnerManager().IsOwner()
			}, 2*time.Second, 100*time.Millisecond)
		}
	})
	wg.Wait()
	distContext.Close()
}

func TestFrameworkWithQuery(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		submitTaskAndCheckSuccessForBasic(ctx, t, "key1", testContext)
	})

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

	wg.Wait()
	distContext.Close()
}

func TestFrameworkCancelTask(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunCancel", "1*return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockExecutorRunCancel"))
	}()
	task := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStateReverted, task.State)
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
	task := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStateReverted, task.State)

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
	task := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStateReverted, task.State)
	distContext.Close()
}

func TestOwnerChangeWhenSchedule(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	scheduler.MockOwnerChange = func() {
		distContext.SetOwner(0)
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockOwnerChange", "1*return(true)"))
	submitTaskAndCheckSuccessForBasic(ctx, t, "😊", testContext)
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
	submitTaskAndCheckSuccess(ctx, t, "😊", testContext, map[proto.Step]int{
		proto.StepOne: 3,
		proto.StepTwo: 1,
	})
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
	submitTaskAndCheckSuccess(ctx, t, "😊", testContext, map[proto.Step]int{
		proto.StepOne: 3,
		proto.StepTwo: 1,
	})
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockCleanExecutor"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/mockStopManager"))
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

	submitTaskAndCheckSuccessForBasic(ctx, t, "😊", testContext)

	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)

	var historySubTasksCnt int
	require.Eventually(t, func() bool {
		historySubTasksCnt, err = testutil.GetSubtasksFromHistory(ctx, mgr)
		if err != nil {
			return false
		}
		return historySubTasksCnt == 4
	}, 10*time.Second, 500*time.Millisecond)

	scheduler.WaitTaskFinished <- struct{}{}

	require.Eventually(t, func() bool {
		historySubTasksCnt, err := testutil.GetSubtasksFromHistory(ctx, mgr)
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
	task := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStateReverted, task.State)
	distContext.Close()
}

func TestFrameworkRunSubtaskCancel(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockRunSubtaskCancel", "1*return(true)"))
	task := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStateReverted, task.State)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockRunSubtaskCancel"))
	distContext.Close()
}

func TestFrameworkCleanUpRoutine(t *testing.T) {
	bak := scheduler.DefaultCleanUpInterval
	defer func() {
		scheduler.DefaultCleanUpInterval = bak
	}()
	scheduler.DefaultCleanUpInterval = 500 * time.Millisecond
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished", "return()"))

	// normal
	submitTaskAndCheckSuccessForBasic(ctx, t, "key1", testContext)
	<-scheduler.WaitCleanUpFinished
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	tasks, err := mgr.GetTaskByKeyWithHistory(ctx, "key1")
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	subtasks, err := testutil.GetSubtasksFromHistory(ctx, mgr)
	require.NoError(t, err)
	require.NotEmpty(t, subtasks)

	// transfer err
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTransferErr", "1*return()"))
	submitTaskAndCheckSuccessForBasic(ctx, t, "key2", testContext)
	<-scheduler.WaitCleanUpFinished
	mgr, err = storage.GetTaskManager()
	require.NoError(t, err)
	tasks, err = mgr.GetTaskByKeyWithHistory(ctx, "key1")
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	subtasks, err = testutil.GetSubtasksFromHistory(ctx, mgr)
	require.NoError(t, err)
	require.NotEmpty(t, subtasks)

	distContext.Close()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTransferErr"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/WaitCleanUpFinished"))
}

func TestTaskCancelledBeforeUpdateTask(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 1)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/cancelBeforeUpdateTask", "1*return(true)"))
	task := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStateReverted, task.State)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/cancelBeforeUpdateTask"))
	distContext.Close()
}
