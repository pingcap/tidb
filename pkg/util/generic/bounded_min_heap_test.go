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

func TestBoundedMinHeapBasic(t *testing.T) {
	bmh := NewBoundedMinHeap(3, intComparator)

	// test empty state
	require.Equal(t, 0, bmh.Len())
	require.Nil(t, bmh.ToSortedSlice())

	// test basic adding within capacity
	bmh.Add(5)
	bmh.Add(3)
	bmh.Add(8)
	require.Equal(t, 3, bmh.Len())
	require.Equal(t, []int{8, 5, 3}, bmh.ToSortedSlice())

	// test over capacity - should keep only top 3
	items := []int{1, 9, 2, 7, 4}
	for _, item := range items {
		bmh.Add(item)
	}
	require.Equal(t, 3, bmh.Len())
	require.Equal(t, []int{9, 8, 7}, bmh.ToSortedSlice())

	// test duplicate values
	bmh2 := NewBoundedMinHeap(3, intComparator)
	duplicates := []int{5, 5, 3, 8, 5}
	for _, item := range duplicates {
		bmh2.Add(item)
	}
	require.Equal(t, 3, bmh2.Len())
	require.Equal(t, []int{8, 5, 5}, bmh2.ToSortedSlice())
}

func TestBoundedMinHeapEdgeCases(t *testing.T) {
	// test single item capacity
	bmh1 := NewBoundedMinHeap(1, intComparator)
	bmh1.Add(3)
	bmh1.Add(1)
	bmh1.Add(7)
	bmh1.Add(2)
	require.Equal(t, 1, bmh1.Len())
	require.Equal(t, []int{7}, bmh1.ToSortedSlice())

	// test zero capacity
	bmh0 := NewBoundedMinHeap(0, intComparator)
	bmh0.Add(5)
	bmh0.Add(10)
	require.Equal(t, 0, bmh0.Len())
	require.Nil(t, bmh0.ToSortedSlice())
}

func TestBoundedMinHeapCustomStruct(t *testing.T) {
	bmh := NewBoundedMinHeap(3, testItemComparator)

	// add custom struct items
	bmh.Add(testItem{value: 10, name: "ten"})
	bmh.Add(testItem{value: 5, name: "five"})
	bmh.Add(testItem{value: 15, name: "fifteen"})
	bmh.Add(testItem{value: 8, name: "eight"})
	bmh.Add(testItem{value: 12, name: "twelve"})

	require.Equal(t, 3, bmh.Len())
	result := bmh.ToSortedSlice()

	// should have top 3 by value: 15, 12, 10
	require.Equal(t, 15, result[0].value)
	require.Equal(t, "fifteen", result[0].name)
	require.Equal(t, 12, result[1].value)
	require.Equal(t, "twelve", result[1].name)
	require.Equal(t, 10, result[2].value)
	require.Equal(t, "ten", result[2].name)
}

func TestBoundedMinHeapReverseComparator(t *testing.T) {
	// reverse comparator for min-heap behavior (keeping smallest values)
	reverseComparator := func(a, b int) int {
		return -intComparator(a, b) // reverse the comparison
	}

	bmh := NewBoundedMinHeap(3, reverseComparator)

	items := []int{9, 2, 7, 1, 8, 3}
	for _, item := range items {
		bmh.Add(item)
	}

	require.Equal(t, 3, bmh.Len())
	result := bmh.ToSortedSlice()
	// with reverse comparator, should keep smallest 3: 1, 2, 3
	require.Equal(t, []int{1, 2, 3}, result)
}

func TestBoundedMinHeapItemReplacement(t *testing.T) {
	bmh := NewBoundedMinHeap(2, intComparator)

	bmh.Add(5)
	bmh.Add(3)

	// add better items - should replace worse ones
	bmh.Add(10)
	bmh.Add(8)
	require.Equal(t, 2, bmh.Len())
	require.Equal(t, []int{10, 8}, bmh.ToSortedSlice())

	// try to add worse items - should be ignored
	bmh.Add(2)
	bmh.Add(1)
	bmh.Add(4)
	require.Equal(t, 2, bmh.Len())
	require.Equal(t, []int{10, 8}, bmh.ToSortedSlice())

	// test equal values behavior
	bmh2 := NewBoundedMinHeap(3, intComparator)
	bmh2.Add(5)
	bmh2.Add(5)
	bmh2.Add(5)
	bmh2.Add(5) // should not be added since heap is full and item is not better
	require.Equal(t, 3, bmh2.Len())
	require.Equal(t, []int{5, 5, 5}, bmh2.ToSortedSlice())
}

func TestBoundedMinHeap_LargeDataset(t *testing.T) {
	const capacity = 10
	const dataSize = 1000

	bmh := NewBoundedMinHeap(capacity, intComparator)

	// add many items
	for i := 0; i < dataSize; i++ {
		bmh.Add(i)
	}

	require.Equal(t, capacity, bmh.Len())
	result := bmh.ToSortedSlice()

	// should have the top 10 values: 999, 998, ..., 990
	require.Equal(t, capacity, len(result))
	for i := 0; i < capacity; i++ {
		expected := dataSize - 1 - i // 999, 998, 997, ...
		require.Equal(t, expected, result[i])
	}
}

func TestNewBoundedMinHeapSafetyChecks(t *testing.T) {
	// test nil comparison function panic
	require.Panics(t, func() {
		NewBoundedMinHeap[int](10, nil)
	}, "should panic when comparison function is nil")

	// test negative maxSize panic
	require.Panics(t, func() {
		NewBoundedMinHeap(-1, intComparator)
	}, "should panic when maxSize is negative")

	// test valid cases should not panic
	require.NotPanics(t, func() {
		NewBoundedMinHeap(0, intComparator)
	}, "should not panic when maxSize is zero")

	require.NotPanics(t, func() {
		NewBoundedMinHeap(10, intComparator)
	}, "should not panic when maxSize is positive")

	// verify that zero capacity heap works correctly
	bmh := NewBoundedMinHeap(0, intComparator)
	bmh.Add(5)
	require.Equal(t, 0, bmh.Len())
}
