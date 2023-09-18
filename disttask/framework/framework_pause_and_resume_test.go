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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/handle"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func CheckSubtasksState(t *testing.T, taskID int64, state string, expectedCnt int64) {
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	mgr.PrintSubtaskInfo(taskID)
	cnt, err := mgr.GetSubtaskInStatesCnt(taskID, state)
	require.NoError(t, err)
	require.Equal(t, expectedCnt, cnt)
}

func TestFrameworkPauseAndResumeBasic(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &testDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 2)

	// dispatch and pause one task.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/pauseTaskAfterRefreshTask", "2*return(true)"))
	DispatchTaskAndCheckState("key1", t, &m, proto.TaskStatePaused)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/pauseTaskAfterRefreshTask"))
	CheckSubtasksState(t, 1, proto.TaskStatePaused, 3)

	// resume one task.
	handle.ResumeTask("key1")
	time.Sleep(3 * time.Second)
	CheckSubtasksState(t, 1, proto.TaskStateSucceed, 4)
	distContext.Close()
}
