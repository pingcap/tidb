package utils

import "container/heap"

// Lessable describes values that can be ordered via Less.
type Lessable[T any] interface {
	Less(T) bool
}

// Item is an element in the queue.
type Item[V Lessable[V]] struct {
	Value V
	index int // Index in the heap (optional, but useful for value updates).
}

// PriorityQueue implements heap.Interface.
// It is a min-heap based on Less.
type PriorityQueue[V Lessable[V]] []*Item[V]

func (pq PriorityQueue[V]) Len() int { return len(pq) }

// Less decides max-heap or min-heap ordering.
func (pq PriorityQueue[V]) Less(i, j int) bool {
	return pq[i].Value.Less(pq[j].Value)
}

func (pq PriorityQueue[V]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push/Pop use type any (Go 1.18+).
func (pq *PriorityQueue[V]) Push(x any) {
	item := x.(*Item[V])
	item.index = len(*pq)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue[V]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // Mark removed.
	*pq = old[:n-1]
	return item
}

// Update updates the item value and fixes the heap (optional).
func (pq *PriorityQueue[V]) Update(item *Item[V], value V) {
	item.Value = value
	heap.Fix(pq, item.index)
}
