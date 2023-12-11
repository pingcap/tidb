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
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/util/cpu"
)

type taskStripes struct {
	task    *proto.Task
	stripes int
}

// slotManager is used to manage the resource slots and stripes.
//
// Slot is the resource unit of dist framework on each node, each slot represents
// 1 cpu core, 1/total-core of memory, 1/total-core of disk, etc.
//
// Stripe is the resource unit of dist framework, regardless of the node, each
// stripe means 1 slot on all nodes managed by dist framework.
// Number of stripes is equal to number of slots on each node, as we assume that
// all nodes managed by dist framework are isomorphic.
// Stripes reserved for a task defines the maximum resource that a task can use
// but the task might not use all the resources. To maximize the resource utilization,
// we will try to dispatch as many tasks as possible depends on the used slots
// on each node and the minimum resource required by the tasks, and in this case,
// we don't consider task order.
//
// Dist framework will try to allocate resource by slots and stripes, and give
// quota to subtask, but subtask can determine what to conform.
type slotManager struct {
	// Capacity is the total number of slots and stripes.
	// TODO: we assume that all nodes managed by dist framework are isomorphic,
	// but dist owner might run on normal node where the capacity might not be
	// able to run any task.
	capacity int

	mu sync.RWMutex
	// represents the number of stripes reserved by task, when we reserve by the
	// minimum resource required by the task, we still append into it, so it summed
	// value might be larger than capacity
	// this slice is in task order.
	reservedStripes []taskStripes
	// map of reservedStripes for fast delete
	task2Index map[int64]int
	// represents the number of slots reserved by task on each node, the execID
	// is only used for reserve minimum resource when starting dispatcher, the
	// subtasks may or may not be scheduled on this node.
	reservedSlots map[string]int
	// represents the number of slots taken by task on each node
	// on some cases it might be larger than capacity:
	// 	current step of higher priority task A has little subtasks, so we start
	// 	to dispatch lower priority task, but next step of A has many subtasks.
	// once initialized, the length of usedSlots should be equal to number of nodes
	// managed by dist framework.
	usedSlots map[string]int
}

// newSlotManager creates a new slotManager.
func newSlotManager() *slotManager {
	return &slotManager{
		capacity:      cpu.GetCPUCount(),
		task2Index:    make(map[int64]int),
		reservedSlots: make(map[string]int),
		usedSlots:     make(map[string]int),
	}
}

// Update updates the used slots on each node.
// TODO: on concurrent call, update once.
func (sm *slotManager) update(ctx context.Context, taskMgr TaskManager) error {
	nodes, err := taskMgr.GetManagedNodes(ctx)
	if err != nil {
		return err
	}
	slotsOnNodes, err := taskMgr.GetUsedSlotsOnNodes(ctx)
	if err != nil {
		return err
	}
	newUsedSlots := make(map[string]int, len(nodes))
	for _, node := range nodes {
		newUsedSlots[node] = slotsOnNodes[node]
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.usedSlots = newUsedSlots
	return nil
}

// CanReserve checks whether there are enough resources for a task.
// If the resource is reserved by slots, it returns the execID of the task.
// else if the resource is reserved by stripes, it returns "".
// as usedSlots is updated asynchronously, it might return false even if there
// are enough resources, or return true on resource shortage when some task
// dispatched subtasks.
func (sm *slotManager) canReserve(task *proto.Task) (execID string, ok bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if len(sm.usedSlots) == 0 {
		// no node managed by dist framework
		return "", false
	}

	reservedForHigherPriority := 0
	for _, s := range sm.reservedStripes {
		if s.task.Compare(task) >= 0 {
			break
		}
		reservedForHigherPriority += s.stripes
	}
	if task.Concurrency+reservedForHigherPriority <= sm.capacity {
		return "", true
	}

	for id, count := range sm.usedSlots {
		if count+sm.reservedSlots[id]+task.Concurrency <= sm.capacity {
			return id, true
		}
	}
	return "", false
}

// Reserve reserves resources for a task.
// Reserve and UnReserve should be called in pair with same parameters.
func (sm *slotManager) reserve(task *proto.Task, execID string) {
	taskClone := *task

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.reservedStripes = append(sm.reservedStripes, taskStripes{&taskClone, taskClone.Concurrency})
	slices.SortFunc(sm.reservedStripes, func(a, b taskStripes) int {
		return a.task.Compare(b.task)
	})
	for i, s := range sm.reservedStripes {
		sm.task2Index[s.task.ID] = i
	}

	if execID != "" {
		sm.reservedSlots[execID] += taskClone.Concurrency
	}
}

// UnReserve un-reserve resources for a task.
func (sm *slotManager) unReserve(task *proto.Task, execID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	idx, ok := sm.task2Index[task.ID]
	if !ok {
		return
	}
	sm.reservedStripes = append(sm.reservedStripes[:idx], sm.reservedStripes[idx+1:]...)
	delete(sm.task2Index, task.ID)
	for i, s := range sm.reservedStripes {
		sm.task2Index[s.task.ID] = i
	}

	if execID != "" {
		sm.reservedSlots[execID] -= task.Concurrency
		if sm.reservedSlots[execID] == 0 {
			delete(sm.reservedSlots, execID)
		}
	}
}
