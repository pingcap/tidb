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

package autoanalyze

import (
	"container/heap"

	"golang.org/x/exp/maps"
)

type analyzeScheduler struct {
	// priorityQueue is a priority queue for tables to be analyzed.
	priorityQueue *analyzePriorityQueue
	taskset       map[int64]struct{}
}

func newAnalyzeScheduler() *analyzeScheduler {
	result := &analyzeScheduler{
		priorityQueue: &analyzePriorityQueue{},
		taskset:       make(map[int64]struct{}),
	}
	heap.Init(result.priorityQueue)
	return result
}

func (s *analyzeScheduler) addTask(item analyzeItem) {
	if _, ok := s.taskset[item.tid]; ok {
		return
	}
	s.taskset[item.tid] = struct{}{}
	heap.Push(s.priorityQueue, item)
}

func (s *analyzeScheduler) popTask() analyzeItem {
	item := heap.Pop(s.priorityQueue).(analyzeItem)
	delete(s.taskset, item.tid)
	return item
}

func (s *analyzeScheduler) Clear() {
	s.priorityQueue = &analyzePriorityQueue{}
	maps.Clear(s.taskset)
	heap.Init(s.priorityQueue)
}
