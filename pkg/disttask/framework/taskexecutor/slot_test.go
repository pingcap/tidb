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
	sm := slotManager{
		executorSlotInfos: make(map[int64]*proto.Task),
		available:         10,
	}

	var (
		taskID  = int64(1)
		taskID2 = int64(2)
		taskID3 = int64(3)
		taskID4 = int64(4)
		task    = &proto.Task{
			ID:          taskID,
			Priority:    1,
			Concurrency: 1,
		}
		task2 = &proto.Task{
			ID:          taskID2,
			Priority:    2,
			Concurrency: 10,
		}
	)

	canAlloc, tasksNeedFree := sm.canAlloc(task)
	require.True(t, canAlloc)
	require.Nil(t, tasksNeedFree)
	sm.alloc(task)
	require.Len(t, sm.executorSlotInfos, 1)
	require.Equal(t, task, sm.executorSlotInfos[taskID])
	require.Equal(t, 9, sm.available)

	// the available slots is not enough for task2
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.False(t, canAlloc)
	require.Nil(t, tasksNeedFree)

	// increase the priority of task2, task2 is waiting for allocation
	task2.Priority = 0
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.False(t, canAlloc)
	require.Equal(t, []*proto.Task{task}, tasksNeedFree)
	require.Equal(t, task2, sm.taskWaitAlloc)

	// task with lower priority is restricted
	task3 := &proto.Task{
		ID:          taskID3,
		Priority:    3,
		Concurrency: 1,
	}
	canAlloc, tasksNeedFree = sm.canAlloc(task3)
	require.False(t, canAlloc)
	require.Nil(t, tasksNeedFree)

	// increase the priority of task3, it can be allocated now
	task3.Priority = -1
	canAlloc, tasksNeedFree = sm.canAlloc(task3)
	require.True(t, canAlloc)
	require.Nil(t, tasksNeedFree)
	// task2 is occupied by task3
	require.Nil(t, sm.taskWaitAlloc)
	sm.alloc(task3)
	require.Len(t, sm.executorSlotInfos, 2)
	require.Equal(t, task3, sm.executorSlotInfos[taskID3])
	require.Equal(t, 8, sm.available)
	sm.free(taskID3)
	require.Len(t, sm.executorSlotInfos, 1)
	require.Nil(t, sm.executorSlotInfos[taskID3])

	// task2 is waiting for allocation again
	canAlloc, tasksNeedFree = sm.canAlloc(task2)
	require.False(t, canAlloc)
	require.Equal(t, []*proto.Task{task}, tasksNeedFree)
	require.Equal(t, task2, sm.taskWaitAlloc)

	// task2 is occupied by task4
	task4 := &proto.Task{
		ID:          taskID4,
		Priority:    -1,
		Concurrency: 10,
	}
	canAlloc, tasksNeedFree = sm.canAlloc(task4)
	require.False(t, canAlloc)
	require.Equal(t, []*proto.Task{task}, tasksNeedFree)
	// task 4 is waiting for allocation
	require.Equal(t, task4, sm.taskWaitAlloc)

	sm.free(taskID)
	require.Len(t, sm.executorSlotInfos, 0)
	require.Nil(t, sm.executorSlotInfos[taskID])

	sm.alloc(task4)
	require.Len(t, sm.executorSlotInfos, 1)
	require.Equal(t, task4, sm.executorSlotInfos[taskID4])
	require.Equal(t, 0, sm.available)
	sm.free(taskID4)
	require.Len(t, sm.executorSlotInfos, 0)
	require.Nil(t, sm.executorSlotInfos[taskID4])
}
