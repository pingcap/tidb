// Copyright 2024 PingCAP, Inc.
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

package priorityqueue

import "container/heap"

// AnalysisPriorityQueue is a priority queue for TableAnalysisJobs.
type AnalysisPriorityQueue struct {
	inner *AnalysisInnerQueue
}

// NewAnalysisPriorityQueue creates a new AnalysisPriorityQueue.
func NewAnalysisPriorityQueue() *AnalysisPriorityQueue {
	q := &AnalysisPriorityQueue{
		inner: &AnalysisInnerQueue{},
	}
	heap.Init(q.inner)
	return q
}

// Push adds a job to the priority queue with the given weight.
func (apq *AnalysisPriorityQueue) Push(job AnalysisJob) {
	heap.Push(apq.inner, job)
}

// Pop removes the highest priority job from the queue.
func (apq *AnalysisPriorityQueue) Pop() AnalysisJob {
	return heap.Pop(apq.inner).(AnalysisJob)
}

// Len returns the number of jobs in the queue.
func (apq *AnalysisPriorityQueue) Len() int {
	return apq.inner.Len()
}

// An AnalysisInnerQueue implements heap.Interface and holds TableAnalysisJobs.
// Exported for testing purposes. You should not use this directly.
type AnalysisInnerQueue []AnalysisJob

// Implement the sort.Interface methods for the priority queue.

func (aq AnalysisInnerQueue) Len() int { return len(aq) }
func (aq AnalysisInnerQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority, so we use greater than here.
	return aq[i].GetWeight() > aq[j].GetWeight()
}
func (aq AnalysisInnerQueue) Swap(i, j int) {
	aq[i], aq[j] = aq[j], aq[i]
}

// Push adds an item to the priority queue.
func (aq *AnalysisInnerQueue) Push(x any) {
	item := x.(AnalysisJob)
	*aq = append(*aq, item)
}

// Pop removes the highest priority item from the queue.
func (aq *AnalysisInnerQueue) Pop() any {
	old := *aq
	n := len(old)
	item := old[n-1]
	*aq = old[0 : n-1]
	return item
}
