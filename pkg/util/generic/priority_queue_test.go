// Copyright 2025 PingCAP, Inc.
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

package generic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// testItem represents a simple test item with a value for comparison
type testItem struct {
	value int
	name  string
}

// intComparator compares integers (for max-heap behavior, return negative for smaller values)
func intComparator(a, b int) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// testItemComparator compares testItems by value
func testItemComparator(a, b testItem) int {
	return intComparator(a.value, b.value)
}

func TestPriorityQueueBasic(t *testing.T) {
	pq := NewPriorityQueue(3, intComparator)

	// test empty state
	require.Equal(t, 0, pq.Len())
	require.Nil(t, pq.ToSortedSlice())

	// test basic adding within capacity
	pq.Add(5)
	pq.Add(3)
	pq.Add(8)
	require.Equal(t, 3, pq.Len())
	require.Equal(t, []int{8, 5, 3}, pq.ToSortedSlice())

	// test over capacity - should keep only top 3
	items := []int{1, 9, 2, 7, 4}
	for _, item := range items {
		pq.Add(item)
	}
	require.Equal(t, 3, pq.Len())
	require.Equal(t, []int{9, 8, 7}, pq.ToSortedSlice())

	// test duplicate values
	pq2 := NewPriorityQueue(3, intComparator)
	duplicates := []int{5, 5, 3, 8, 5}
	for _, item := range duplicates {
		pq2.Add(item)
	}
	require.Equal(t, 3, pq2.Len())
	require.Equal(t, []int{8, 5, 5}, pq2.ToSortedSlice())
}

func TestPriorityQueueEdgeCases(t *testing.T) {
	// test single item capacity
	pq1 := NewPriorityQueue(1, intComparator)
	pq1.Add(3)
	pq1.Add(1)
	pq1.Add(7)
	pq1.Add(2)
	require.Equal(t, 1, pq1.Len())
	require.Equal(t, []int{7}, pq1.ToSortedSlice())

	// test zero capacity
	pq0 := NewPriorityQueue(0, intComparator)
	pq0.Add(5)
	pq0.Add(10)
	require.Equal(t, 0, pq0.Len())
	require.Nil(t, pq0.ToSortedSlice())
}

func TestPriorityQueueCustomStruct(t *testing.T) {
	pq := NewPriorityQueue(3, testItemComparator)

	// add custom struct items
	pq.Add(testItem{value: 10, name: "ten"})
	pq.Add(testItem{value: 5, name: "five"})
	pq.Add(testItem{value: 15, name: "fifteen"})
	pq.Add(testItem{value: 8, name: "eight"})
	pq.Add(testItem{value: 12, name: "twelve"})

	require.Equal(t, 3, pq.Len())
	result := pq.ToSortedSlice()

	// should have top 3 by value: 15, 12, 10
	require.Equal(t, 15, result[0].value)
	require.Equal(t, "fifteen", result[0].name)
	require.Equal(t, 12, result[1].value)
	require.Equal(t, "twelve", result[1].name)
	require.Equal(t, 10, result[2].value)
	require.Equal(t, "ten", result[2].name)
}

func TestPriorityQueueReverseComparator(t *testing.T) {
	// reverse comparator for min-heap behavior (keeping smallest values)
	reverseComparator := func(a, b int) int {
		return -intComparator(a, b) // reverse the comparison
	}

	pq := NewPriorityQueue(3, reverseComparator)

	items := []int{9, 2, 7, 1, 8, 3}
	for _, item := range items {
		pq.Add(item)
	}

	require.Equal(t, 3, pq.Len())
	result := pq.ToSortedSlice()
	// with reverse comparator, should keep smallest 3: 1, 2, 3
	require.Equal(t, []int{1, 2, 3}, result)
}

func TestPriorityQueueItemReplacement(t *testing.T) {
	pq := NewPriorityQueue(2, intComparator)

	pq.Add(5)
	pq.Add(3)

	// add better items - should replace worse ones
	pq.Add(10)
	pq.Add(8)
	require.Equal(t, 2, pq.Len())
	require.Equal(t, []int{10, 8}, pq.ToSortedSlice())

	// try to add worse items - should be ignored
	pq.Add(2)
	pq.Add(1)
	pq.Add(4)
	require.Equal(t, 2, pq.Len())
	require.Equal(t, []int{10, 8}, pq.ToSortedSlice())

	// test equal values behavior
	pq2 := NewPriorityQueue(3, intComparator)
	pq2.Add(5)
	pq2.Add(5)
	pq2.Add(5)
	pq2.Add(5) // should not be added since queue is full and item is not better
	require.Equal(t, 3, pq2.Len())
	require.Equal(t, []int{5, 5, 5}, pq2.ToSortedSlice())
}

func TestPriorityQueue_LargeDataset(t *testing.T) {
	const capacity = 10
	const dataSize = 1000

	pq := NewPriorityQueue(capacity, intComparator)

	// add many items
	for i := 0; i < dataSize; i++ {
		pq.Add(i)
	}

	require.Equal(t, capacity, pq.Len())
	result := pq.ToSortedSlice()

	// should have the top 10 values: 999, 998, ..., 990
	require.Equal(t, capacity, len(result))
	for i := 0; i < capacity; i++ {
		expected := dataSize - 1 - i // 999, 998, 997, ...
		require.Equal(t, expected, result[i])
	}
}
