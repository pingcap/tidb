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
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
)

// slotManager is used to manage the slots of the executor.
type slotManager struct {
	sync.RWMutex
	// taskID2Index is the index of the task
	taskID2Index map[int64]int
	// executorTasks is used to record the tasks that is running on the executor,
	// the slice is sorted in reverse task order.
	executorTasks []*proto.TaskBase

	capacity int
	// The number of slots that can be used by the executor.
	// Its initial value is always equal to CPU cores of the instance.
	available atomic.Int32
}

func newSlotManager(capacity int) *slotManager {
	sm := &slotManager{
		taskID2Index:  make(map[int64]int),
		executorTasks: make([]*proto.TaskBase, 0),
		capacity:      capacity,
	}
	sm.available.Store(int32(capacity))
	return sm
}

// subtasks inside a task will be run in serial, so they takes task.Concurrency slots.
func (sm *slotManager) alloc(task *proto.TaskBase) {
	sm.Lock()
	defer sm.Unlock()

	sm.executorTasks = append(sm.executorTasks, task)
	slices.SortFunc(sm.executorTasks, func(a, b *proto.TaskBase) int {
		return b.Compare(a)
	})
	for index, slotInfo := range sm.executorTasks {
		sm.taskID2Index[slotInfo.ID] = index
	}
	sm.available.Add(int32(-task.Concurrency))
}

func (sm *slotManager) free(taskID int64) {
	sm.Lock()
	defer sm.Unlock()

	index, ok := sm.taskID2Index[taskID]
	if !ok {
		return
	}
	sm.available.Add(int32(sm.executorTasks[index].Concurrency))
	sm.executorTasks = append(sm.executorTasks[:index], sm.executorTasks[index+1:]...)

	delete(sm.taskID2Index, taskID)
	for index, slotInfo := range sm.executorTasks {
		sm.taskID2Index[slotInfo.ID] = index
	}
}

// canAlloc is used to check whether the instance has enough slots to run the task.
func (sm *slotManager) canAlloc(task *proto.TaskBase) (canAlloc bool, tasksNeedFree []*proto.TaskBase) {
	sm.RLock()
	defer sm.RUnlock()

	if int(sm.available.Load()) >= task.Concurrency {
		return true, nil
	}

	usedSlots := 0
	for _, slotInfo := range sm.executorTasks {
		if slotInfo.Compare(task) < 0 {
			break
		}
		tasksNeedFree = append(tasksNeedFree, slotInfo)
		usedSlots += slotInfo.Concurrency
		if int(sm.available.Load())+usedSlots >= task.Concurrency {
			return true, tasksNeedFree
		}
	}

	return false, nil
}

func (sm *slotManager) availableSlots() int {
	return int(sm.available.Load())
}

func (sm *slotManager) usedSlots() int {
	return sm.capacity - int(sm.available.Load())
}
