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

package dispatcher

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSlotManagerCanReserve(t *testing.T) {
	sm := newSlotManager()
	sm.capacity = 16
	cases := []struct {
		concurrency    int
		expectedExecID string
		canReserve     bool
	}{
		{1, "", true},
		{8, "", true},
		{16, "", true},
		{17, "", false},
	}
	for _, c := range cases {
		execID, ok := sm.canReserve(c.concurrency)
		require.Equal(t, c.expectedExecID, execID)
		require.Equal(t, c.canReserve, ok)
	}
	sm.reservedStripes.Store(12)
	sm.usedSlots = map[string]int{
		"tidb-1": 12,
		"tidb-2": 8,
	}
	execID, ok := sm.canReserve(8)
	require.Equal(t, "tidb-2", execID)
	require.True(t, ok)
	execID, ok = sm.canReserve(16)
	require.Equal(t, "", execID)
	require.False(t, ok)
}

func TestSlotManagerUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskMgr := mock.NewMockTaskManager(ctrl)
	taskMgr.EXPECT().GetManagedNodes(gomock.Any()).Return([]string{"tidb-1", "tidb-2", "tidb-3"}, nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{
		"tidb-1": 12,
		"tidb-2": 8,
	}, nil)
	sm := newSlotManager()
	sm.capacity = 16
	require.Empty(t, sm.usedSlots)
	require.Empty(t, sm.reservedSlots)
	require.NoError(t, sm.update(context.Background(), taskMgr))
	require.Empty(t, sm.reservedSlots)
	require.Equal(t, map[string]int{
		"tidb-1": 12,
		"tidb-2": 8,
		"tidb-3": 0,
	}, sm.usedSlots)
	// some node scaled in, should be reflected
	taskMgr.EXPECT().GetManagedNodes(gomock.Any()).Return([]string{"tidb-1"}, nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(map[string]int{
		"tidb-1": 12,
		"tidb-2": 8,
	}, nil)
	require.NoError(t, sm.update(context.Background(), taskMgr))
	require.Empty(t, sm.reservedSlots)
	require.Equal(t, map[string]int{
		"tidb-1": 12,
	}, sm.usedSlots)
	// on error, the usedSlots should not be changed
	taskMgr.EXPECT().GetManagedNodes(gomock.Any()).Return(nil, errors.New("mock err"))
	require.ErrorContains(t, sm.update(context.Background(), taskMgr), "mock err")
	require.Empty(t, sm.reservedSlots)
	require.Equal(t, map[string]int{
		"tidb-1": 12,
	}, sm.usedSlots)
	taskMgr.EXPECT().GetManagedNodes(gomock.Any()).Return([]string{"tidb-1"}, nil)
	taskMgr.EXPECT().GetUsedSlotsOnNodes(gomock.Any()).Return(nil, errors.New("mock err"))
	require.ErrorContains(t, sm.update(context.Background(), taskMgr), "mock err")
	require.Empty(t, sm.reservedSlots)
	require.Equal(t, map[string]int{
		"tidb-1": 12,
	}, sm.usedSlots)
}

func TestSlotManagerReserveUnReserve(t *testing.T) {
	sm := newSlotManager()
	sm.capacity = 16
	// reserve 2 tasks of 4/8 concurrency
	execID, ok := sm.canReserve(4)
	require.True(t, ok)
	sm.reserve(4, execID)
	require.Equal(t, int32(4), sm.reservedStripes.Load())
	require.Empty(t, sm.reservedSlots)
	execID, ok = sm.canReserve(4)
	require.True(t, ok)
	sm.reserve(8, execID)
	require.Equal(t, int32(12), sm.reservedStripes.Load())
	require.Empty(t, sm.reservedSlots)
	// cannot reserve another 8
	execID, ok = sm.canReserve(8)
	require.False(t, ok)

	sm.usedSlots = map[string]int{
		"tidb-1": 12,
		"tidb-2": 8,
	}
	// can reserve 8 on tidb-2
	execID, ok = sm.canReserve(8)
	require.True(t, ok)
	sm.reserve(8, execID)
	require.Equal(t, int32(20), sm.reservedStripes.Load())
	require.Equal(t, map[string]int{
		"tidb-2": 8,
	}, sm.reservedSlots)
	// cannot reserve another 8
	execID, ok = sm.canReserve(8)
	require.False(t, ok)

	// un-reverse second task of 8, we still cannot reserve another 8
	sm.unReserve(8, "")
	require.Equal(t, int32(12), sm.reservedStripes.Load())
	require.Equal(t, map[string]int{
		"tidb-2": 8,
	}, sm.reservedSlots)
	execID, ok = sm.canReserve(8)
	require.False(t, ok)
	// un-reverse third task, now we can reserve another 8
	sm.unReserve(8, "tidb-2")
	require.Equal(t, int32(4), sm.reservedStripes.Load())
	require.Empty(t, sm.reservedSlots)
	_, ok = sm.canReserve(8)
	require.True(t, ok)
}
