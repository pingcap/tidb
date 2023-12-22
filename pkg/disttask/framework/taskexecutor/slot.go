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
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
)

// slotManager is used to manage the slots of the executor.
type slotManager struct {
	sync.RWMutex
	executorSlotInfos map[int64]*proto.Task

	// The number of slots that can be used by the executor.
	// It is always equal to CPU cores of the instance.
	available int

	taskWaitAlloc *proto.Task
}

func (sm *slotManager) alloc(task *proto.Task) {
	sm.Lock()
	defer sm.Unlock()
	if sm.taskWaitAlloc != nil && sm.taskWaitAlloc.Compare(task) == 0 {
		sm.taskWaitAlloc = nil
	}
	sm.executorSlotInfos[task.ID] = task

	sm.available -= task.Concurrency
}

func (sm *slotManager) free(taskID int64) {
	sm.Lock()
	defer sm.Unlock()

	slotInfo, ok := sm.executorSlotInfos[taskID]
	if ok {
		delete(sm.executorSlotInfos, taskID)
		sm.available += slotInfo.Concurrency
	}
}

// canAlloc is used to check whether the instance has enough slots to run the task.
func (sm *slotManager) canAlloc(task *proto.Task) (canAlloc bool, tasksNeedFree []*proto.Task) {
	sm.RLock()
	defer sm.RUnlock()

	// If a task is waiting for allocation, we can't allocate the lower priority task.
	if sm.taskWaitAlloc != nil && sm.taskWaitAlloc.Compare(task) < 0 {
		return false, nil
	}

	if sm.available >= task.Concurrency {
		// If the upcoming task's priority is higher than the task waiting for allocation,
		// we need free the task waiting for allocation.
		if sm.taskWaitAlloc != nil && sm.taskWaitAlloc.Compare(task) > 0 {
			sm.taskWaitAlloc = nil
		}
		return true, nil
	}

	// If the task is waiting for allocation, we do not need to free any task again.
	if sm.taskWaitAlloc != nil && sm.taskWaitAlloc.Compare(task) == 0 {
		return false, nil
	}

	allSlotInfos := make([]*proto.Task, 0, len(sm.executorSlotInfos))
	for _, slotInfo := range sm.executorSlotInfos {
		allSlotInfos = append(allSlotInfos, slotInfo)
	}
	slices.SortFunc(allSlotInfos, func(a, b *proto.Task) int {
		return a.Compare(b)
	})

	usedSlots := 0
	for _, slotInfo := range allSlotInfos {
		if slotInfo.Compare(task) < 0 {
			continue
		}
		tasksNeedFree = append(tasksNeedFree, slotInfo)
		usedSlots += slotInfo.Concurrency
		if sm.available+usedSlots >= task.Concurrency {
			sm.taskWaitAlloc = task
			return false, tasksNeedFree
		}
	}

	return false, nil
}
