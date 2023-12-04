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

package proto

type TaskWrapper struct {
	*Task

	IndexInPriorityQueue int // Index of the task in the priority queue
}

// TaskPriorityQueue represents a priority queue of tasks.
type TaskPriorityQueue []*TaskWrapper

// TaskPriorityQueue implementation for heap.Interface
func (pq TaskPriorityQueue) Len() int { return len(pq) }

// Less returns true if the task at index i has higher priority than the task at index j.
func (pq TaskPriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest priority, so we use greater than here.
	return pq[i].Priority > pq[j].Priority ||
		(pq[i].Priority == pq[j].Priority && pq[i].CreateTime.Before(pq[j].CreateTime)) ||
		(pq[i].Priority == pq[j].Priority && pq[i].CreateTime.Equal(pq[j].CreateTime) && pq[i].ID < pq[j].ID)
}

// Swap swaps the tasks at the given indices.
func (pq TaskPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].IndexInPriorityQueue = i
	pq[j].IndexInPriorityQueue = j
}

// Push adds x as element Len().
func (pq *TaskPriorityQueue) Push(x interface{}) {
	task := x.(*TaskWrapper)
	task.IndexInPriorityQueue = len(*pq)
	*pq = append(*pq, task)
}

// Pop removes and returns element Len() - 1.
func (pq *TaskPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	task := old[n-1]
	task.IndexInPriorityQueue = -1 // for safety
	*pq = old[0 : n-1]
	return task
}

func WrapPriorityQueue(task *Task) *TaskWrapper {
	return &TaskWrapper{
		Task: task,
	}
}
