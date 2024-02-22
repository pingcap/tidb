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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func CheckSubtasksState(ctx context.Context, t *testing.T, taskID int64, state proto.SubtaskState, expectedCnt int64) {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	testutil.PrintSubtaskInfo(ctx, mgr, taskID)
	cntByStatesStepOne, err := mgr.GetSubtaskCntGroupByStates(ctx, taskID, proto.StepOne)
	require.NoError(t, err)
	cntByStatesStepTwo, err := mgr.GetSubtaskCntGroupByStates(ctx, taskID, proto.StepTwo)
	require.NoError(t, err)
	historySubTasksCnt, err := testutil.GetSubtasksFromHistoryByTaskID(ctx, mgr, taskID)
	require.NoError(t, err)
	require.Equal(t, expectedCnt, cntByStatesStepOne[state]+cntByStatesStepTwo[state]+int64(historySubTasksCnt))
}

func TestFrameworkPauseAndResume(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 3, 16, true)

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetMockBasicSchedulerExt(c.MockCtrl), c.TestContext, nil)
	// 1. schedule and pause one running task.
	testkit.EnableFailPoint(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/pauseTaskAfterRefreshTask", "2*return(true)")
	testkit.EnableFailPoint(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncAfterResume", "return()")
	task1 := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", 1)
	require.Equal(t, proto.TaskStatePaused, task1.State)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/pauseTaskAfterRefreshTask"))
	// 4 subtask scheduled.
	require.NoError(t, handle.ResumeTask(c.Ctx, "key1"))
	<-scheduler.TestSyncChan
	testutil.WaitTaskDoneOrPaused(c.Ctx, t, task1.Key)
	CheckSubtasksState(c.Ctx, t, 1, proto.SubtaskStateSucceed, 4)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncAfterResume"))

	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	errs, err := mgr.GetSubtaskErrors(c.Ctx, 1)
	require.NoError(t, err)
	require.Empty(t, errs)

	// 2. pause pending task.
	testkit.EnableFailPoint(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/pausePendingTask", "2*return(true)")
	testkit.EnableFailPoint(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncAfterResume", "1*return()")
	task2 := testutil.SubmitAndWaitTask(c.Ctx, t, "key2", 1)
	require.Equal(t, proto.TaskStatePaused, task2.State)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/pausePendingTask"))
	// 4 subtask scheduled.
	require.NoError(t, handle.ResumeTask(c.Ctx, "key2"))
	<-scheduler.TestSyncChan
	testutil.WaitTaskDoneOrPaused(c.Ctx, t, task2.Key)
	CheckSubtasksState(c.Ctx, t, 1, proto.SubtaskStateSucceed, 4)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncAfterResume"))

	errs, err = mgr.GetSubtaskErrors(c.Ctx, 1)
	require.NoError(t, err)
	require.Empty(t, errs)
}
