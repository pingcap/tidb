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

package scheduler

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestMaintainLiveNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTaskExecutorNodes", "return()"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTaskExecutorNodes"))
	})

	MockServerInfo.Store(&[]string{":4000"})

	nodeMgr := newNodeManager("")
	ctx := context.Background()
	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return(nil, errors.New("mock error"))
	nodeMgr.maintainLiveNodes(ctx, mockTaskMgr)
	require.Empty(t, nodeMgr.prevLiveNodes)
	require.True(t, ctrl.Satisfied())
	// no change
	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]proto.ManagedNode{{ID: ":4000"}}, nil)
	nodeMgr.maintainLiveNodes(ctx, mockTaskMgr)
	require.Equal(t, map[string]struct{}{":4000": {}}, nodeMgr.prevLiveNodes)
	require.True(t, ctrl.Satisfied())
	// run again, return fast
	nodeMgr.maintainLiveNodes(ctx, mockTaskMgr)
	require.Equal(t, map[string]struct{}{":4000": {}}, nodeMgr.prevLiveNodes)
	require.True(t, ctrl.Satisfied())

	// scale out 1 node
	MockServerInfo.Store(&[]string{":4000", ":4001"})

	// fail on clean
	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]proto.ManagedNode{{ID: ":4000"}, {ID: ":4001"}, {ID: ":4002"}}, nil)
	mockTaskMgr.EXPECT().DeleteDeadNodes(gomock.Any(), gomock.Any()).Return(errors.New("mock error"))
	nodeMgr.maintainLiveNodes(ctx, mockTaskMgr)
	require.Equal(t, map[string]struct{}{":4000": {}}, nodeMgr.prevLiveNodes)
	require.True(t, ctrl.Satisfied())
	// remove 1 node
	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]proto.ManagedNode{{ID: ":4000"}, {ID: ":4001"}, {ID: ":4002"}}, nil)
	mockTaskMgr.EXPECT().DeleteDeadNodes(gomock.Any(), gomock.Any()).Return(nil)
	nodeMgr.maintainLiveNodes(ctx, mockTaskMgr)
	require.Equal(t, map[string]struct{}{":4000": {}, ":4001": {}}, nodeMgr.prevLiveNodes)
	require.True(t, ctrl.Satisfied())
	// run again, return fast
	nodeMgr.maintainLiveNodes(ctx, mockTaskMgr)
	require.Equal(t, map[string]struct{}{":4000": {}, ":4001": {}}, nodeMgr.prevLiveNodes)
	require.True(t, ctrl.Satisfied())

	// scale in 1 node
	MockServerInfo.Store(&[]string{":4000"})

	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]proto.ManagedNode{{ID: ":4000"}, {ID: ":4001"}, {ID: ":4002"}}, nil)
	mockTaskMgr.EXPECT().DeleteDeadNodes(gomock.Any(), gomock.Any()).Return(nil)
	nodeMgr.maintainLiveNodes(ctx, mockTaskMgr)
	require.Equal(t, map[string]struct{}{":4000": {}}, nodeMgr.prevLiveNodes)
	require.True(t, ctrl.Satisfied())
	// run again, return fast
	nodeMgr.maintainLiveNodes(ctx, mockTaskMgr)
	require.Equal(t, map[string]struct{}{":4000": {}}, nodeMgr.prevLiveNodes)
	require.True(t, ctrl.Satisfied())
}

func TestMaintainManagedNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	mockTaskMgr := mock.NewMockTaskManager(ctrl)
	nodeMgr := newNodeManager("")

	slotMgr := newSlotManager()
	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return(nil, errors.New("mock error"))
	nodeMgr.refreshNodes(ctx, mockTaskMgr, slotMgr)
	require.Equal(t, cpu.GetCPUCount(), int(slotMgr.capacity.Load()))
	require.Empty(t, nodeMgr.getNodes())
	require.True(t, ctrl.Satisfied())

	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return([]proto.ManagedNode{
		{ID: ":4000", CPUCount: 100},
		{ID: ":4001", CPUCount: 100},
	}, nil)
	nodeMgr.refreshNodes(ctx, mockTaskMgr, slotMgr)
	require.Equal(t, []proto.ManagedNode{{ID: ":4000", Role: "", CPUCount: 100}, {ID: ":4001", Role: "", CPUCount: 100}}, nodeMgr.getNodes())
	require.Equal(t, 100, int(slotMgr.capacity.Load()))
	require.True(t, ctrl.Satisfied())
	mockTaskMgr.EXPECT().GetAllNodes(gomock.Any()).Return(nil, nil)
	nodeMgr.refreshNodes(ctx, mockTaskMgr, slotMgr)
	require.NotNil(t, nodeMgr.getNodes())
	require.Empty(t, nodeMgr.getNodes())
	require.Equal(t, 100, int(slotMgr.capacity.Load()))
	require.True(t, ctrl.Satisfied())
}
