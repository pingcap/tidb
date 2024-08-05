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

	var (
		task = &proto.TaskBase{
			ID:          1,
			Priority:    1,
			Concurrency: 1,
		}
		task2 = &proto.TaskBase{
			ID:          2,
			Priority:    2,
			Concurrency: 10,
		}
	)

	canAlloc, tasksNeedFree := sm.canAlloc(task)
	require.True(t, canAlloc)
	require.Nil(t, tasksNeedFree)
	sm.alloc(task)
	require.Len(t, sm.executorTasks, 1)
	require.Equal(t, task, sm.executorTasks[sm.taskID2Index[task.ID]])
	require.Equal(t, 9, sm.availableSlots())

	// the available slots is not enough for task2
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.False(t, canAlloc)
	require.Nil(t, tasksNeedFree)

	// increase the priority of task2, task2 is waiting for allocation
	task2.Priority = 0
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.True(t, canAlloc)
	require.Equal(t, []*proto.TaskBase{task}, tasksNeedFree)

	// task with higher priority
	task3 := &proto.TaskBase{
		ID:          3,
		Priority:    -1,
		Concurrency: 1,
	}

	canAlloc, tasksNeedFree = sm.canAlloc(task3)
	require.True(t, canAlloc)
	require.Nil(t, tasksNeedFree)
	// task2 is occupied by task3
	sm.alloc(task3)
	require.Len(t, sm.executorTasks, 2)
	require.Equal(t, task3, sm.executorTasks[sm.taskID2Index[task3.ID]])
	require.Equal(t, 8, sm.availableSlots())

	task4 := &proto.TaskBase{
		ID:          4,
		Priority:    1,
		Concurrency: 1,
	}
	canAlloc, tasksNeedFree = sm.canAlloc(task4)
	require.True(t, canAlloc)
	require.Nil(t, tasksNeedFree)
	sm.alloc(task4)
	task5 := &proto.TaskBase{
		ID:          5,
		Priority:    0,
		Concurrency: 1,
	}
	canAlloc, tasksNeedFree = sm.canAlloc(task5)
	require.True(t, canAlloc)
	require.Nil(t, tasksNeedFree)
	sm.alloc(task5)
	require.Len(t, sm.executorTasks, 4)
	// test the order of executorTasks
	require.Equal(t, []*proto.TaskBase{task4, task, task5, task3}, sm.executorTasks)
	require.Equal(t, 6, sm.availableSlots())

	task6 := &proto.TaskBase{
		ID:          6,
		Priority:    0,
		Concurrency: 8,
	}
	canAlloc, tasksNeedFree = sm.canAlloc(task6)
	require.True(t, canAlloc)
	require.Equal(t, []*proto.TaskBase{task4, task}, tasksNeedFree)
	task6.Concurrency++
	canAlloc, tasksNeedFree = sm.canAlloc(task6)
	require.False(t, canAlloc)
	require.Nil(t, tasksNeedFree)
	sm.free(task4.ID)
	sm.free(task5.ID)

	sm.free(task3.ID)
	require.Len(t, sm.executorTasks, 1)
	require.Equal(t, task, sm.executorTasks[sm.taskID2Index[task.ID]])

	// task2 is waiting for allocation again
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.True(t, canAlloc)
	require.Equal(t, []*proto.TaskBase{task}, tasksNeedFree)

	sm.free(task.ID)
	require.Len(t, sm.executorTasks, 0)
	require.Len(t, sm.taskID2Index, 0)

	sm.alloc(task2)
	require.Len(t, sm.executorTasks, 1)
	require.Equal(t, task2, sm.executorTasks[sm.taskID2Index[task2.ID]])
	require.Equal(t, 0, sm.availableSlots())
	sm.free(task2.ID)
	require.Len(t, sm.executorTasks, 0)
	require.Len(t, sm.taskID2Index, 0)
}
