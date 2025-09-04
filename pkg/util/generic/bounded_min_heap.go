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
	"container/heap"
	"sort"
)

// BoundedMinHeap implements a min-heap for maintaining the best N items efficiently.
// It keeps the N items with the highest values according to the comparison function.
// The root of the heap is always the smallest item, making it easy to remove when adding better items.
type BoundedMinHeap[T any] struct {
	cmpFunc func(T, T) int
	items   []T
	maxSize int
}

// NewBoundedMinHeap creates a new bounded min-heap with the specified maximum size and comparison function.
func NewBoundedMinHeap[T any](maxSize int, cmpFunc func(T, T) int) *BoundedMinHeap[T] {
	return &BoundedMinHeap[T]{
		items:   make([]T, 0, maxSize),
		maxSize: maxSize,
		cmpFunc: cmpFunc,
	}
}

// Len returns the number of items in the heap.
func (h *BoundedMinHeap[T]) Len() int { return len(h.items) }

// Less compares two items in the heap. We use a min-heap for efficient bounded operations.
func (h *BoundedMinHeap[T]) Less(i, j int) bool {
	return h.cmpFunc(h.items[i], h.items[j]) < 0
}

// Swap swaps two items in the heap.
func (h *BoundedMinHeap[T]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// Push adds an item to the heap.
func (h *BoundedMinHeap[T]) Push(x any) {
	h.items = append(h.items, x.(T))
}

// Pop removes and returns the smallest item from the heap.
func (h *BoundedMinHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// Add adds an item to the bounded min-heap. If the heap is full and the new item
// is better than the worst item, it replaces the worst item.
func (h *BoundedMinHeap[T]) Add(item T) {
	// handle zero capacity case
	if h.maxSize <= 0 {
		return
	}

	if len(h.items) < h.maxSize {
		// heap not full, just add the item
		heap.Push(h, item)
		return
	}

	// heap is full, check if new item is better than the worst (root of min-heap)
	if h.cmpFunc(item, h.items[0]) > 0 {
		// new item is better, replace the worst
		h.items[0] = item
		heap.Fix(h, 0)
	}
}

// ToSortedSlice returns all items in the heap as a sorted slice (best to worst).
func (h *BoundedMinHeap[T]) ToSortedSlice() []T {
	if len(h.items) == 0 {
		return nil
	}

	// copy items to avoid modifying the original heap
	result := make([]T, len(h.items))
	copy(result, h.items)

	sort.Slice(result, func(i, j int) bool {
		return h.cmpFunc(result[i], result[j]) > 0
	})

	return result
}
