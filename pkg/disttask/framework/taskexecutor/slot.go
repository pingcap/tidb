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
	"sync"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
)

// slotManager is used to manage the slots of the executor.
type slotManager struct {
	sync.RWMutex
	// taskID -> slotInfo
	executorSlotInfos map[int64]*slotInfo

	// The number of slots that can be used by the executor.
	// It is always equal to CPU cores of the instance.
	available int
}

type slotInfo struct {
	taskID int
	// priority will be used in future
	priority  int
	slotCount int
}

func (sm *slotManager) alloc(task *proto.Task) {
	sm.Lock()
	defer sm.Unlock()
	sm.executorSlotInfos[task.ID] = &slotInfo{
		taskID:    int(task.ID),
		priority:  task.Priority,
		slotCount: task.Concurrency,
	}

	sm.available -= task.Concurrency
}

func (sm *slotManager) free(taskID int64) {
	sm.Lock()
	defer sm.Unlock()

	slotInfo, ok := sm.executorSlotInfos[taskID]
	if ok {
		delete(sm.executorSlotInfos, taskID)
		sm.available += slotInfo.slotCount
	}
}

// canReserve is used to check whether the instance has enough slots to run the task.
func (sm *slotManager) canAlloc(task *proto.Task) bool {
	sm.RLock()
	defer sm.RUnlock()

	return sm.available >= task.Concurrency
}
