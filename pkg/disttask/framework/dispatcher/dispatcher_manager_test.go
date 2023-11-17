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

package dispatcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
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
	ctx = util.WithInternalSourceType(ctx, "dispatcher_manager")

	dsp, mgr := MockDispatcherManager(t, pool)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			mockDispatcher := dsp.MockDispatcher(task)
			mockDispatcher.Extension = &numberExampleDispatcherExt{}
			return mockDispatcher
		})
	dispatcher.RegisterDispatcherCleanUpFactory(proto.TaskTypeExample,
		func() dispatcher.CleanUpRoutine {
			return mockCleanupRountine
		})
	dsp.Start()
	defer dsp.Stop()
	require.NoError(t, mgr.StartManager(ctx, ":4000", "background"))

	taskID, err := mgr.AddNewGlobalTask(ctx, "test", proto.TaskTypeExample, 1, nil)
	require.NoError(t, err)

	checkTaskRunningCnt := func() []*proto.Task {
		var tasks []*proto.Task
		require.Eventually(t, func() bool {
			var err error
			tasks, err = mgr.GetGlobalTasksInStates(ctx, proto.TaskStateRunning)
			require.NoError(t, err)
			return len(tasks) == 1
		}, time.Second, 50*time.Millisecond)
		return tasks
	}

	checkSubtaskCnt := func(tasks []*proto.Task, taskID int64) {
		require.Eventually(t, func() bool {
			cnt, err := mgr.GetSubtaskInStatesCnt(ctx, taskID, proto.TaskStatePending)
			require.NoError(t, err)
			return int64(subtaskCnt) == cnt
		}, time.Second, 50*time.Millisecond)
	}

	tasks := checkTaskRunningCnt()
	checkSubtaskCnt(tasks, taskID)
	for i := 1; i <= subtaskCnt; i++ {
		err = mgr.UpdateSubtaskStateAndError(ctx, "test", int64(i), proto.TaskStateSucceed, nil)
		require.NoError(t, err)
	}
	dsp.DoCleanUpRoutine()
	require.Eventually(t, func() bool {
		tasks, err := mgr.GetGlobalTasksFromHistoryInStates(ctx, proto.TaskStateSucceed)
		require.NoError(t, err)
		return len(tasks) != 0
	}, time.Second*10, time.Millisecond*300)
}

func TestCleanUpMeta(t *testing.T) {
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)
	mockCleanupRountine := mock.NewMockCleanUpRoutine(ctrl)
	dspMgr := MockDispatcherManagerWithMockTaskMgr(t, pool, mockTaskMgr)
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr dispatcher.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			mockDispatcher := dspMgr.MockDispatcher(task)
			mockDispatcher.Extension = &numberExampleDispatcherExt{}
			return mockDispatcher
		})
	dispatcher.RegisterDispatcherCleanUpFactory(proto.TaskTypeExample,
		func() dispatcher.CleanUpRoutine {
			return mockCleanupRountine
		})

	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]string{":4000", ":4001"}, nil)
	mockTaskMgr.EXPECT().CleanUpMeta(gomock.Any(), gomock.Any()).Return(nil)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	require.Equal(t, 1, dspMgr.CleanUpMeta())

	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]string{":4000"}, nil)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	require.Equal(t, 0, dspMgr.CleanUpMeta())

	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]string{":4000", ":4001", ":4003"}, nil)
	mockTaskMgr.EXPECT().CleanUpMeta(gomock.Any(), gomock.Any()).Return(nil)
	mockCleanupRountine.EXPECT().CleanUp(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	require.Equal(t, 2, dspMgr.CleanUpMeta())
}
