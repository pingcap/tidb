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

package priorityqueue_test

import (
	"container/heap"
	"testing"

	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/stretchr/testify/require"
)

func TestAnalysisInnerQueue(t *testing.T) {
	// Test data
	job1 := &priorityqueue.NonPartitionedTableAnalysisJob{
		Weight: 10,
	}
	job2 := &priorityqueue.NonPartitionedTableAnalysisJob{
		Weight: 5,
	}
	job3 := &priorityqueue.NonPartitionedTableAnalysisJob{
		Weight: 15,
	}

	// Create an empty priority queue
	queue := priorityqueue.AnalysisInnerQueue{}

	// Push items into the queue
	heap.Push(&queue, job1)
	heap.Push(&queue, job2)
	heap.Push(&queue, job3)

	// Test Len()
	require.Equal(t, 3, queue.Len(), "Length of the queue should be 3")

	// Test Less()
	require.True(t, queue.Less(0, 1), "Item at index 0 should have higher priority than item at index 1")

	// Test Swap()
	queue.Swap(0, 2)
	require.NotEqual(t, float64(15), queue[0].GetWeight(), "Item at index 0 should not have weight 15 after swap")
}

func TestPushPopAnalysisInnerQueue(t *testing.T) {
	// Test Push and Pop operations together
	queue := priorityqueue.AnalysisInnerQueue{}
	heap.Push(&queue, &priorityqueue.NonPartitionedTableAnalysisJob{
		Weight: 10,
	})
	heap.Push(&queue, &priorityqueue.NonPartitionedTableAnalysisJob{
		Weight: 5,
	})

	poppedItem := heap.Pop(&queue).(priorityqueue.AnalysisJob)
	require.Equal(t, float64(10), poppedItem.GetWeight(), "Popped item should have weight 10")
	require.Equal(t, 1, queue.Len(), "After Pop, length of the queue should be 1")
}

func TestAnalysisPriorityQueue(t *testing.T) {
	// Test data
	job1 := &priorityqueue.NonPartitionedTableAnalysisJob{
		Weight: 10,
	}
	job2 := &priorityqueue.NonPartitionedTableAnalysisJob{
		Weight: 5,
	}
	job3 := &priorityqueue.NonPartitionedTableAnalysisJob{
		Weight: 15,
	}

	// Create a priority queue
	queue := priorityqueue.NewAnalysisPriorityQueue()

	// Push items into the queue
	queue.Push(job1)
	queue.Push(job2)
	queue.Push(job3)

	// Test Len()
	require.Equal(t, 3, queue.Len(), "Length of the queue should be 3")

	// Test Pop()
	poppedItem := queue.Pop()
	require.Equal(t, float64(15), poppedItem.GetWeight(), "Popped item should have weight 15")
	require.Equal(t, 2, queue.Len(), "After Pop, length of the queue should be 2")
}
