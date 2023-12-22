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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSlotManagerReserve(t *testing.T) {
	sm := newSlotManager()
	sm.capacity = 16
	// no node
	_, ok := sm.canReserve(&proto.Task{Concurrency: 1})
	require.False(t, ok)

	// reserve by stripes
	sm.usedSlots = map[string]int{
		"tidb-1": 16,
	}
	task := proto.Task{
		Priority:    proto.NormalPriority,
		Concurrency: 16,
		CreateTime:  time.Now(),
	}
	task10 := task
	task10.ID = 10
	task10.Concurrency = 4
	execID10, ok := sm.canReserve(&task10)
	require.Equal(t, "", execID10)
	require.True(t, ok)
	sm.reserve(&task10, execID10)

	task20 := task
	task20.ID = 20
	task20.Concurrency = 8
	execID20, ok := sm.canReserve(&task20)
	require.Equal(t, "", execID20)
	require.True(t, ok)
	sm.reserve(&task20, execID20)

	task30 := task
	task30.ID = 30
	task30.Concurrency = 8
	execID30, ok := sm.canReserve(&task30)
	require.Equal(t, "", execID30)
	require.False(t, ok)
	require.Len(t, sm.reservedStripes, 2)
	require.Equal(t, 4, sm.reservedStripes[0].stripes)
	require.Equal(t, 8, sm.reservedStripes[1].stripes)
	require.Equal(t, map[int64]int{10: 0, 20: 1}, sm.task2Index)
	require.Empty(t, sm.reservedSlots)
	// higher priority task can preempt lower priority task
	task9 := task
	task9.ID = 9
	task9.Concurrency = 16
	_, ok = sm.canReserve(&task9)
	require.True(t, ok)
	// 4 slots are reserved for high priority tasks, so cannot reserve.
	task11 := task
	task11.ID = 11
	_, ok = sm.canReserve(&task11)
	require.False(t, ok)
	// lower concurrency
	task11.Concurrency = 12
	_, ok = sm.canReserve(&task11)
	require.True(t, ok)

	// reserve by slots
	sm.usedSlots = map[string]int{
		"tidb-1": 12,
		"tidb-2": 8,
	}
	task40 := task
	task40.ID = 40
	task40.Concurrency = 16
	execID40, ok := sm.canReserve(&task40)
	require.Equal(t, "", execID40)
	require.False(t, ok)
	task40.Concurrency = 8
	execID40, ok = sm.canReserve(&task40)
	require.Equal(t, "tidb-2", execID40)
	require.True(t, ok)
	sm.reserve(&task40, execID40)
	require.Len(t, sm.reservedStripes, 3)
	require.Equal(t, 4, sm.reservedStripes[0].stripes)
	require.Equal(t, 8, sm.reservedStripes[1].stripes)
	require.Equal(t, 8, sm.reservedStripes[2].stripes)
	require.Equal(t, map[int64]int{10: 0, 20: 1, 40: 2}, sm.task2Index)
	require.Equal(t, map[string]int{"tidb-2": 8}, sm.reservedSlots)
	// higher priority task stop task 15 to run
	task15 := task
	task15.ID = 15
	task15.Concurrency = 16
	execID15, ok := sm.canReserve(&task15)
	require.Equal(t, "", execID15)
	require.False(t, ok)
	// finish task of id 10
	sm.unReserve(&task10, execID10)
	require.Len(t, sm.reservedStripes, 2)
	require.Equal(t, 8, sm.reservedStripes[0].stripes)
	require.Equal(t, 8, sm.reservedStripes[1].stripes)
	require.Equal(t, map[int64]int{20: 0, 40: 1}, sm.task2Index)
	require.Equal(t, map[string]int{"tidb-2": 8}, sm.reservedSlots)
	// now task 15 can run
	execID15, ok = sm.canReserve(&task15)
	require.Equal(t, "", execID15)
	require.True(t, ok)
	sm.reserve(&task15, execID15)
	require.Len(t, sm.reservedStripes, 3)
	require.Equal(t, 16, sm.reservedStripes[0].stripes)
	require.Equal(t, 8, sm.reservedStripes[1].stripes)
	require.Equal(t, 8, sm.reservedStripes[2].stripes)
	require.Equal(t, map[int64]int{15: 0, 20: 1, 40: 2}, sm.task2Index)
	require.Equal(t, map[string]int{"tidb-2": 8}, sm.reservedSlots)
	// task 50 cannot run
	task50 := task
	task50.ID = 50
	task50.Concurrency = 8
	_, ok = sm.canReserve(&task50)
	require.False(t, ok)
	// finish task 40
	sm.unReserve(&task40, execID40)
	require.Len(t, sm.reservedStripes, 2)
	require.Equal(t, 16, sm.reservedStripes[0].stripes)
	require.Equal(t, 8, sm.reservedStripes[1].stripes)
	require.Equal(t, map[int64]int{15: 0, 20: 1}, sm.task2Index)
	require.Empty(t, sm.reservedSlots)
	// now task 50 can run
	execID50, ok := sm.canReserve(&task50)
	require.Equal(t, "tidb-2", execID50)
	require.True(t, ok)
	sm.reserve(&task50, execID50)
	// task 60 can run too
	task60 := task
	task60.ID = 60
	task60.Concurrency = 4
	execID60, ok := sm.canReserve(&task60)
	require.Equal(t, "tidb-1", execID60)
	require.True(t, ok)
	sm.reserve(&task60, execID60)
	require.Len(t, sm.reservedStripes, 4)
	require.Equal(t, 16, sm.reservedStripes[0].stripes)
	require.Equal(t, 8, sm.reservedStripes[1].stripes)
	require.Equal(t, 8, sm.reservedStripes[2].stripes)
	require.Equal(t, 4, sm.reservedStripes[3].stripes)
	require.Equal(t, map[int64]int{15: 0, 20: 1, 50: 2, 60: 3}, sm.task2Index)
	require.Equal(t, map[string]int{"tidb-1": 4, "tidb-2": 8}, sm.reservedSlots)

	// un-reserve all tasks
	sm.unReserve(&task15, execID15)
	sm.unReserve(&task20, execID20)
	sm.unReserve(&task50, execID50)
	sm.unReserve(&task60, execID60)
	require.Empty(t, sm.reservedStripes)
	require.Empty(t, sm.task2Index)
	require.Empty(t, sm.reservedSlots)
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
