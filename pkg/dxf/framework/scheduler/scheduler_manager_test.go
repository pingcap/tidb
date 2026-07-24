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
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func TestCleanUpRoutine(t *testing.T) {
	t.Run("drains bounded batches", func(t *testing.T) {
		t.Cleanup(proto.SetTaskCleanupBatchSizeForTest(2))
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
		store := testkit.CreateMockStore(t)
		gtk := testkit.NewTestKit(t, store)
		pool := pools.NewResourcePool(func() (pools.Resource, error) {
			return gtk.Session(), nil
		}, 1, 1, time.Second)
		defer pool.Close()
		ctrl := gomock.NewController(t)
		ctx := util.WithInternalSourceType(context.Background(), "scheduler_manager")
		mockCleanupRoutine := mock.NewMockCleanUpRoutine(ctrl)

		sch, mgr := MockSchedulerManager(store, pool, nil, mockCleanupRoutine)
		defer sch.Stop()
		require.NoError(t, mgr.InitMeta(ctx, ":4000", handle.GetTargetScope()))
		mockCleanupRoutine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).Times(3)
		for _, taskKey := range []string{"cleanup-batch-0", "cleanup-batch-1", "cleanup-batch-2"} {
			taskID, err := mgr.CreateTask(ctx, taskKey, proto.TaskTypeExample,
				store.GetKeyspace(), 1, handle.GetTargetScope(), 1, proto.ExtraParams{}, nil)
			require.NoError(t, err)
			task, err := mgr.GetTaskByID(ctx, taskID)
			require.NoError(t, err)
			require.NoError(t, mgr.SwitchTaskStep(ctx, task, proto.TaskStateRunning, proto.StepOne, nil))
			require.NoError(t, mgr.SucceedTask(ctx, taskID))
		}

		sch.DoCleanupRoutine()
		historyTasks, err := testutil.GetTasksFromHistoryInStates(ctx, mgr, proto.TaskStateSucceed)
		require.NoError(t, err)
		require.Len(t, historyTasks, 3)
		activeTasks, err := mgr.GetTaskBasesInStates(ctx, proto.TaskStateSucceed)
		require.NoError(t, err)
		require.Empty(t, activeTasks)
	})

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
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

	targetScope := handle.GetTargetScope()
	sch, mgr := MockSchedulerManager(store, pool, getNumberExampleSchedulerExt(ctrl), mockCleanupRoutine)
	require.NoError(t, mgr.InitMeta(ctx, ":4000", targetScope))
	mockCleanupRoutine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	sch.Start()
	defer sch.Stop()
	taskID, err := mgr.CreateTask(ctx, "test", proto.TaskTypeExample, store.GetKeyspace(), 1, targetScope, 1, proto.ExtraParams{}, nil)
	require.NoError(t, err)

	checkTaskRunning := func() {
		require.Eventually(t, func() bool {
			task, err := mgr.GetTaskBaseByID(ctx, taskID)
			require.NoError(t, err)
			return task != nil && task.State == proto.TaskStateRunning
		}, 5*time.Second, 50*time.Millisecond)
	}

	checkSubtaskCnt := func(taskID int64) {
		require.Eventually(t, func() bool {
			cntByStates, err := mgr.GetSubtaskCntGroupByStates(ctx, taskID, proto.StepOne)
			require.NoError(t, err)
			return int64(subtaskCnt) == cntByStates[proto.SubtaskStatePending]
		}, 5*time.Second, 50*time.Millisecond)
	}

	checkTaskRunning()
	checkSubtaskCnt(taskID)
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
