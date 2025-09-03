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

// PriorityQueue implements a min-heap for maintaining the best N items efficiently.
// It keeps the N items with the highest values according to the comparison function.
// The root of the heap is always the smallest item, making it easy to remove when adding better items.
type PriorityQueue[T any] struct {
	cmpFunc func(T, T) int
	items   []T
	maxSize int
}

// NewPriorityQueue creates a new priority queue with the specified maximum size and comparison function.
func NewPriorityQueue[T any](maxSize int, cmpFunc func(T, T) int) *PriorityQueue[T] {
	return &PriorityQueue[T]{
		items:   make([]T, 0, maxSize),
		maxSize: maxSize,
		cmpFunc: cmpFunc,
	}
}

// Len returns the number of items in the heap.
func (p *PriorityQueue[T]) Len() int { return len(p.items) }

// Less compares two items in the heap. We use a min-heap for efficient priority queue operations.
func (p *PriorityQueue[T]) Less(i, j int) bool {
	return p.cmpFunc(p.items[i], p.items[j]) < 0
}

// Swap swaps two items in the heap.
func (p *PriorityQueue[T]) Swap(i, j int) {
	p.items[i], p.items[j] = p.items[j], p.items[i]
}

// Push adds an item to the heap (used by container/heap).
func (p *PriorityQueue[T]) Push(x any) {
	p.items = append(p.items, x.(T))
}

// Pop removes and returns the smallest item from the heap (used by container/heap).
func (p *PriorityQueue[T]) Pop() any {
	old := p.items
	n := len(old)
	item := old[n-1]
	p.items = old[0 : n-1]
	return item
}

// Add adds an item to the priority queue. If the queue is full and the new item
// is better than the worst item, it replaces the worst item.
func (p *PriorityQueue[T]) Add(item T) {
	// handle zero capacity case
	if p.maxSize <= 0 {
		return
	}

	if len(p.items) < p.maxSize {
		// queue not full, just add the item
		heap.Push(p, item)
		return
	}

	// queue is full, check if new item is better than the worst (root of min-heap)
	if p.cmpFunc(item, p.items[0]) > 0 {
		// new item is better, replace the worst
		p.items[0] = item
		heap.Fix(p, 0)
	}
}

// ToSortedSlice returns all items in the heap as a sorted slice (best to worst).
func (p *PriorityQueue[T]) ToSortedSlice() []T {
	if len(p.items) == 0 {
		return nil
	}

	// copy items to avoid modifying the original queue
	result := make([]T, len(p.items))
	copy(result, p.items)

	sort.Slice(result, func(i, j int) bool {
		return p.cmpFunc(result[i], result[j]) > 0
	})

	return result
}
