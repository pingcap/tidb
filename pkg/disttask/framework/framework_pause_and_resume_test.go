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
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func CheckSubtasksState(t *testing.T, taskID int64, state proto.TaskState, expectedCnt int64) {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	mgr.PrintSubtaskInfo(taskID)
	cnt, err := mgr.GetSubtaskInStatesCnt(taskID, state)
	require.NoError(t, err)
	historySubTasksCnt, err := storage.GetSubtasksFromHistoryByTaskIDForTest(mgr, taskID)
	require.NoError(t, err)
	require.Equal(t, expectedCnt, cnt+int64(historySubTasksCnt))
}

func TestFrameworkPauseAndResume(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &testDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 3)
	// 1. dispatch and pause one running task.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/pauseTaskAfterRefreshTask", "2*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/syncAfterResume", "return()"))
	DispatchTaskAndCheckState("key1", t, &m, proto.TaskStatePaused)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/pauseTaskAfterRefreshTask"))
	// 4 subtask dispatched.
	require.NoError(t, handle.ResumeTask("key1"))
	<-dispatcher.TestSyncChan
	WaitTaskExit(t, "key1")
	CheckSubtasksState(t, 1, proto.TaskStateSucceed, 4)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/syncAfterResume"))

	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	errs, err := mgr.CollectSubTaskError(1)
	require.NoError(t, err)
	require.Empty(t, errs)

	// 2. pause pending task.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/pausePendingTask", "2*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/syncAfterResume", "1*return()"))
	DispatchTaskAndCheckState("key2", t, &m, proto.TaskStatePaused)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/pausePendingTask"))
	// 4 subtask dispatched.
	require.NoError(t, handle.ResumeTask("key2"))
	<-dispatcher.TestSyncChan
	WaitTaskExit(t, "key2")
	CheckSubtasksState(t, 1, proto.TaskStateSucceed, 4)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/syncAfterResume"))

	errs, err = mgr.CollectSubTaskError(1)
	require.NoError(t, err)
	require.Empty(t, errs)
	distContext.Close()
}
