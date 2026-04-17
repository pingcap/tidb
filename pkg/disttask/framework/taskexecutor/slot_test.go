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

package taskexecutor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func TestSlotManager(t *testing.T) {
	sm := newSlotManager(10)

	// rank(concurrency in parenthesis): task1(1), task2(10)
	task1 := &proto.TaskBase{ID: 1, Priority: 1, Concurrency: 1}
	task2 := &proto.TaskBase{ID: 2, Priority: 2, Concurrency: 10}

	canAlloc, tasksNeedFree := sm.canAlloc(task1)
	require.True(t, canAlloc)
	require.Empty(t, tasksNeedFree)
	require.True(t, sm.alloc(task1))
	require.Len(t, sm.executorTasks, 1)
	require.Equal(t, task1, sm.executorTasks[sm.taskID2Index[task1.ID]])
	require.Equal(t, 9, sm.availableSlots())

	// the available slots is not enough for task2
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.False(t, canAlloc)
	require.Empty(t, tasksNeedFree)
	// call alloc will fail
	require.False(t, sm.alloc(task2))

	// rank: task2(10), task1(1)
	// increase the priority of task2, task2 is waiting for allocation
	task2.Priority = 0
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.True(t, canAlloc)
	require.Equal(t, []*proto.TaskBase{task1}, tasksNeedFree)
	// call alloc still fail as task is still holding the resources
	require.False(t, sm.alloc(task2))

	// rank: task3(1), task2(10), task1(1)
	// task with higher priority can alloc
	task3 := &proto.TaskBase{ID: 3, Priority: -1, Concurrency: 1}
	canAlloc, tasksNeedFree = sm.canAlloc(task3)
	require.True(t, canAlloc)
	require.Empty(t, tasksNeedFree)
	require.True(t, sm.alloc(task3))
	require.Len(t, sm.executorTasks, 2)
	require.Equal(t, task3, sm.executorTasks[sm.taskID2Index[task3.ID]])
	require.Equal(t, 8, sm.availableSlots())

	// rank: task3(1), task2(10), task1(1), task4(1)
	task4 := &proto.TaskBase{ID: 4, Priority: 1, Concurrency: 1}
	canAlloc, tasksNeedFree = sm.canAlloc(task4)
	require.True(t, canAlloc)
	require.Empty(t, tasksNeedFree)
	require.True(t, sm.alloc(task4))
	require.Equal(t, 7, sm.availableSlots())
	// rank: task3(1), task2(10), task5(1), task1(1), task4(1)
	task5 := &proto.TaskBase{ID: 5, Priority: 0, Concurrency: 1}
	canAlloc, tasksNeedFree = sm.canAlloc(task5)
	require.True(t, canAlloc)
	require.Empty(t, tasksNeedFree)
	require.True(t, sm.alloc(task5))
	require.Len(t, sm.executorTasks, 4)
	// test the order of executorTasks
	require.Equal(t, []*proto.TaskBase{task4, task1, task5, task3}, sm.executorTasks)
	require.Equal(t, 6, sm.availableSlots())

	// rank: task3(1), task2(10), task5(1), task6(8), task1(1), task4(1)
	task6 := &proto.TaskBase{ID: 6, Priority: 0, Concurrency: 8}
	canAlloc, tasksNeedFree = sm.canAlloc(task6)
	require.True(t, canAlloc)
	require.Equal(t, []*proto.TaskBase{task4, task1}, tasksNeedFree)
	// rank: task3(1), task2(10), task5(1), task6(9), task1(1), task4(1)
	task6.Concurrency = 9
	canAlloc, tasksNeedFree = sm.canAlloc(task6)
	require.False(t, canAlloc)
	require.Empty(t, tasksNeedFree)

	sm.free(task4.ID)
	sm.free(task5.ID)
	sm.free(task3.ID)
	require.Len(t, sm.executorTasks, 1)
	require.Equal(t, task1, sm.executorTasks[sm.taskID2Index[task1.ID]])
	require.Equal(t, 9, sm.availableSlots())

	// task2 is waiting for allocation again
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.True(t, canAlloc)
	require.Equal(t, []*proto.TaskBase{task1}, tasksNeedFree)

	sm.free(task1.ID)
	require.Len(t, sm.executorTasks, 0)
	require.Len(t, sm.taskID2Index, 0)
	require.Equal(t, 10, sm.availableSlots())

	require.True(t, sm.alloc(task2))
	require.Len(t, sm.executorTasks, 1)
	require.Equal(t, task2, sm.executorTasks[sm.taskID2Index[task2.ID]])
	require.Equal(t, 0, sm.availableSlots())
	sm.free(task2.ID)
	require.Len(t, sm.executorTasks, 0)
	require.Len(t, sm.taskID2Index, 0)
	require.Equal(t, 10, sm.availableSlots())
}

func TestSlotManagerExchangeSlots(t *testing.T) {
	task1 := &proto.TaskBase{ID: 1, Concurrency: 4}
	task2 := &proto.TaskBase{ID: 2, Concurrency: 8}

	t.Run("not exist", func(t *testing.T) {
		sm := newSlotManager(16)
		require.False(t, sm.exchange(&proto.TaskBase{ID: 1, Concurrency: 8}))
	})

	t.Run("exchange for more slots but not enough", func(t *testing.T) {
		sm := newSlotManager(16)
		require.True(t, sm.alloc(task1))
		require.Equal(t, 4, sm.executorTasks[sm.taskID2Index[task1.ID]].Concurrency)
		require.Equal(t, 12, sm.availableSlots())
		require.False(t, sm.exchange(&proto.TaskBase{ID: 1, Concurrency: 32}))
		require.Equal(t, 4, sm.executorTasks[sm.taskID2Index[task1.ID]].Concurrency)
		require.Equal(t, 12, sm.availableSlots())
	})

	t.Run("exchange for more slots", func(t *testing.T) {
		sm := newSlotManager(16)
		require.True(t, sm.alloc(task1))
		require.Equal(t, 12, sm.availableSlots())
		require.True(t, sm.exchange(&proto.TaskBase{ID: 1, Concurrency: 8}))
		require.Len(t, sm.executorTasks, 1)
		require.Equal(t, 8, sm.executorTasks[sm.taskID2Index[task1.ID]].Concurrency)
		require.Equal(t, 8, sm.availableSlots())
	})

	t.Run("exchange for less slots, and exchange twice with same concurrency", func(t *testing.T) {
		sm := newSlotManager(16)
		require.True(t, sm.alloc(task1))
		require.Equal(t, 12, sm.availableSlots())
		require.True(t, sm.exchange(&proto.TaskBase{ID: 1, Concurrency: 2}))
		require.Len(t, sm.executorTasks, 1)
		require.Equal(t, 2, sm.executorTasks[sm.taskID2Index[task1.ID]].Concurrency)
		require.Equal(t, 14, sm.availableSlots())
		// exchange again, no change
		require.True(t, sm.exchange(&proto.TaskBase{ID: 1, Concurrency: 2}))
		require.Len(t, sm.executorTasks, 1)
		require.Equal(t, 2, sm.executorTasks[sm.taskID2Index[task1.ID]].Concurrency)
		require.Equal(t, 14, sm.availableSlots())
	})

	t.Run("exchange for more slots makes concurrent alloc fail", func(t *testing.T) {
		sm := newSlotManager(16)
		require.True(t, sm.alloc(task1))
		require.Equal(t, 12, sm.availableSlots())
		canAlloc, tasksNeedFree := sm.canAlloc(task2)
		require.True(t, canAlloc)
		require.Empty(t, tasksNeedFree)
		require.True(t, sm.exchange(&proto.TaskBase{ID: 1, Concurrency: 10}))
		require.Len(t, sm.executorTasks, 1)
		require.Equal(t, 10, sm.executorTasks[sm.taskID2Index[task1.ID]].Concurrency)
		require.Equal(t, 6, sm.availableSlots())
		require.False(t, sm.alloc(task2))
		require.Equal(t, 6, sm.availableSlots())
	})
}
