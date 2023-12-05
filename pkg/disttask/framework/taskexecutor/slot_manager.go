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

type slotManager struct {
	sync.RWMutex
	// taskID -> slotInfo
	schedulerSlotInfos map[int64]*slotInfo

	// The number of slots that can be used by the scheduler.
	available int
}

type slotInfo struct {
	taskID int
	// priority will be used in the feature
	priority  int
	slotCount int
}

func (sm *slotManager) addTask(task *proto.Task) {
	sm.Lock()
	defer sm.Unlock()
	sm.schedulerSlotInfos[task.ID] = &slotInfo{
		taskID:    int(task.ID),
		priority:  task.Priority,
		slotCount: int(task.Concurrency),
	}

	sm.available -= int(task.Concurrency)
}

func (sm *slotManager) removeTask(taskID int64) {
	sm.Lock()
	defer sm.Unlock()

	slotInfo, ok := sm.schedulerSlotInfos[taskID]
	if ok {
		delete(sm.schedulerSlotInfos, taskID)
		sm.available += int(slotInfo.slotCount)
	}
}

func (sm *slotManager) checkSlotAvailabilityForTask(task *proto.Task) bool {
	return sm.available >= int(task.Concurrency)
}
