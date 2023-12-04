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

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTaskPriorityQueue(t *testing.T) {
	priorityQueue := make(TaskPriorityQueue, 0)
	heap.Init(&priorityQueue)

	task1 := &TaskWrapper{Task: &Task{ID: 1, Priority: 1, CreateTime: time.Now()}}
	task2 := &TaskWrapper{Task: &Task{ID: 2, Priority: 1, CreateTime: time.Now()}}
	task3 := &TaskWrapper{Task: &Task{ID: 3, Priority: 1, CreateTime: time.Now().Add(-2 * time.Second)}}
	task4 := &TaskWrapper{Task: &Task{ID: 4, Priority: 2, CreateTime: time.Now().Add(2 * time.Second)}}

	heap.Push(&priorityQueue, task1)
	heap.Push(&priorityQueue, task2)
	heap.Push(&priorityQueue, task3)
	heap.Push(&priorityQueue, task4)

	expected := []int64{4, 3, 1, 2}

	i := 0
	// Pop tasks in priority order
	for priorityQueue.Len() > 0 {
		task := heap.Pop(&priorityQueue).(*TaskWrapper)
		require.Equal(t, expected[i], task.ID)
		i++
	}
}
