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
	"slices"
)

// internalHeap is an unexported heap implementation backing BoundedMinHeap.
// it keeps the worst item at the root according to cmp.
type internalHeap[T any] struct {
	cmp   func(T, T) int
	items []T
}

// Len implements heap.Interface.
func (h *internalHeap[T]) Len() int { return len(h.items) }

// Less implements heap.Interface; the min-heap keeps the worst item at the root.
func (h *internalHeap[T]) Less(i, j int) bool { return h.cmp(h.items[i], h.items[j]) < 0 }

// Swap implements heap.Interface.
func (h *internalHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

// Push implements heap.Interface.
func (h *internalHeap[T]) Push(x any) { h.items = append(h.items, x.(T)) }

// Pop implements heap.Interface.
func (h *internalHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// BoundedMinHeap maintains the best N items efficiently using an internal min-heap.
// It keeps the N best items according to the comparison function.
// The root of the internal heap is always the worst item, making it easy to remove when a better item arrives.
type BoundedMinHeap[T any] struct {
	data    internalHeap[T]
	maxSize int
}

// NewBoundedMinHeap creates a new bounded min-heap with the specified maximum size and comparison function.
func NewBoundedMinHeap[T any](maxSize int, cmpFunc func(T, T) int) *BoundedMinHeap[T] {
	if cmpFunc == nil {
		panic("comparison function cannot be nil")
	}
	if maxSize < 0 {
		panic("maxSize cannot be negative")
	}

	return &BoundedMinHeap[T]{
		data: internalHeap[T]{
			items: make([]T, 0, maxSize),
			cmp:   cmpFunc,
		},
		maxSize: maxSize,
	}
}

// Len returns the number of items in the heap.
func (h *BoundedMinHeap[T]) Len() int { return h.data.Len() }

// Add adds an item to the bounded min-heap. If the heap is full and the new item
// is better than the worst item, it replaces the worst item.
func (h *BoundedMinHeap[T]) Add(item T) {
	// handle zero capacity case
	if h.maxSize == 0 {
		return
	}

	if len(h.data.items) < h.maxSize {
		// heap not full, just add the item
		heap.Push(&h.data, item)
		return
	}

	// heap is full, check if new item is better than the worst (root of min-heap)
	if h.data.cmp(item, h.data.items[0]) > 0 {
		// new item is better, replace the worst
		h.data.items[0] = item
		heap.Fix(&h.data, 0)
	}
}

// ToSortedSlice returns all items in the heap as a sorted slice (best to worst).
func (h *BoundedMinHeap[T]) ToSortedSlice() []T {
	if len(h.data.items) == 0 {
		return nil
	}

	// copy items to avoid modifying the original heap
	result := make([]T, len(h.data.items))
	copy(result, h.data.items)

	// sort from best to worst using a negated comparator
	slices.SortFunc(result, func(a, b T) int { return -h.data.cmp(a, b) })

	return result
}
