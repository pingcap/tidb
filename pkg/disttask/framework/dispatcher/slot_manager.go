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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/cpu"
)

// SlotManager is used to manage the resource slots and strips.
//
// Slot is the resource unit of dist framework on each node, each slot represents
// 1 cpu core, 1/total-core of memory, 1/total-core of disk, etc.
//
// Stripe is the resource unit of dist framework, regardless of the node, each
// stripe means 1 slot on all nodes managed by dist framework.
// Number of stripes is equal to number of slots on each node.As we assume that
// all nodes managed by dist framework are isomorphic,
// Stripes reserved for a task defines the maximum resource that a task can use
// but the task might not use all the resources, to maximize the resource utilization,
// we will try to dispatch as many tasks as possible depends on the used slots
// on each node.
//
// Dist framework will try to allocate resource by slots and stripes, and give
// quota to subtask, but subtask can determine what to conform.
type SlotManager struct {
	// Capacity is the total number of slots and stripes.
	capacity int
	// this value might be larger than capacity
	reservedStripes atomic.Int32

	mu            sync.RWMutex
	reservedSlots map[string]int
	// represents the number of slots taken by task on each node
	// on some cases it might be larger than capacity:
	// 	current step of higher priority task A has little subtasks, so we start
	// 	to dispatch lower priority task, but next step of A has many subtasks.
	usedSlots map[string]int
}

// NewSlotManager creates a new SlotManager.
func NewSlotManager() *SlotManager {
	return &SlotManager{
		capacity:      cpu.GetCPUCount(),
		reservedSlots: make(map[string]int),
		usedSlots:     make(map[string]int),
	}
}

// GetCapacity returns the capacity of the slots and stripes.
func (sm *SlotManager) GetCapacity() int {
	return sm.capacity
}

// TryReserve tries to reserve resources for a task.
// If the resource is reserved by slots, it returns the execID of the task.
// else if the resource is reserved by stripes, it returns "".
// as usedSlots is updated asynchronously, it might return false even if there
// are enough resources, or return true when some task dispatched subtasks.
func (sm *SlotManager) TryReserve(taskConcurrency int) (execID string, ok bool) {
	if taskConcurrency+int(sm.reservedStripes.Load()) <= sm.capacity {
		return "", true
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for id, count := range sm.usedSlots {
		if count+sm.reservedSlots[id] <= sm.capacity {
			return id, true
		}
	}
	return "", false
}

// Reserve reserves resources for a task.
// Reserve and UnReserve should be called in pair with same parameters.
func (sm *SlotManager) Reserve(taskConcurrency int, execID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.reservedStripes.Add(int32(taskConcurrency))
	if execID != "" {
		sm.reservedSlots[execID] += taskConcurrency
	}
}

// UnReserve un-reserve resources for a task.
func (sm *SlotManager) UnReserve(taskConcurrency int, execID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.reservedStripes.Add(-int32(taskConcurrency))
	if execID != "" {
		sm.reservedSlots[execID] -= taskConcurrency
	}
}
