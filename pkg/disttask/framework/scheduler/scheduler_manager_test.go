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
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func TestCleanUpRoutine(t *testing.T) {
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "scheduler_manager")
	mockCleanupRoutine := mock.NewMockCleanUpRoutine(ctrl)

	sch, mgr := MockSchedulerManager(t, ctrl, pool, getNumberExampleSchedulerExt(ctrl), mockCleanupRoutine)
	mockCleanupRoutine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	sch.Start()
	defer sch.Stop()
	taskID, err := mgr.CreateTask(ctx, "test", proto.TaskTypeExample, 1, "", nil)
	require.NoError(t, err)

	checkTaskRunningCnt := func() []*proto.Task {
		var tasks []*proto.Task
		require.Eventually(t, func() bool {
			var err error
			tasks, err = mgr.GetTasksInStates(ctx, proto.TaskStateRunning)
			require.NoError(t, err)
			return len(tasks) == 1
		}, 5*time.Second, 50*time.Millisecond)
		return tasks
	}

	checkSubtaskCnt := func(tasks []*proto.Task, taskID int64) {
		require.Eventually(t, func() bool {
			cntByStates, err := mgr.GetSubtaskCntGroupByStates(ctx, taskID, proto.StepOne)
			require.NoError(t, err)
			return int64(subtaskCnt) == cntByStates[proto.SubtaskStatePending]
		}, 5*time.Second, 50*time.Millisecond)
	}

	tasks := checkTaskRunningCnt()
	checkSubtaskCnt(tasks, taskID)
	for i := 1; i <= subtaskCnt; i++ {
		err = mgr.UpdateSubtaskStateAndError(ctx, ":4000", int64(i), proto.SubtaskStateSucceed, nil)
		require.NoError(t, err)
	}
	sch.DoCleanupRoutine()
	require.Eventually(t, func() bool {
		tasks, err := testutil.GetTasksFromHistoryInStates(ctx, mgr, proto.TaskStateSucceed)
		require.NoError(t, err)
		return len(tasks) != 0
	}, 5*time.Second*10, time.Millisecond*300)
}
