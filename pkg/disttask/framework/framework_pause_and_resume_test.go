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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/stretchr/testify/require"
)

func CheckSubtasksState(ctx context.Context, t *testing.T, taskID int64, state proto.SubtaskState, expectedCnt int64) {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	mgr.PrintSubtaskInfo(ctx, taskID)
	cntByStates, err := mgr.GetSubtaskCntGroupByStates(ctx, taskID, proto.StepTwo)
	require.NoError(t, err)
	historySubTasksCnt, err := storage.GetSubtasksFromHistoryByTaskIDForTest(ctx, mgr, taskID)
	require.NoError(t, err)
	require.Equal(t, expectedCnt, cntByStates[state]+int64(historySubTasksCnt))
}

func TestFrameworkPauseAndResume(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockBasicSchedulerExt(ctrl), testContext, nil)
	// 1. schedule and pause one running task.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/pauseTaskAfterRefreshTask", "2*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncAfterResume", "return()"))
	task1 := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStatePaused, task1.State)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/pauseTaskAfterRefreshTask"))
	// 4 subtask scheduled.
	require.NoError(t, handle.ResumeTask(ctx, "key1"))
	<-scheduler.TestSyncChan
	testutil.WaitTaskDoneOrPaused(ctx, t, task1.Key)
	CheckSubtasksState(ctx, t, 1, proto.SubtaskStateSucceed, 4)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncAfterResume"))

	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	errs, err := mgr.CollectSubTaskError(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, errs)

	// 2. pause pending task.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/pausePendingTask", "2*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncAfterResume", "1*return()"))
	task2 := testutil.SubmitAndWaitTask(ctx, t, "key2")
	require.Equal(t, proto.TaskStatePaused, task2.State)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/pausePendingTask"))
	// 4 subtask scheduled.
	require.NoError(t, handle.ResumeTask(ctx, "key2"))
	<-scheduler.TestSyncChan
	testutil.WaitTaskDoneOrPaused(ctx, t, task2.Key)
	CheckSubtasksState(ctx, t, 1, proto.SubtaskStateSucceed, 4)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/syncAfterResume"))

	errs, err = mgr.CollectSubTaskError(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, errs)
	distContext.Close()
}
