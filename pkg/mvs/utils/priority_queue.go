package utils

import "container/heap"

// Lessable describes values that can be ordered via Less.
type Lessable[T any] interface {
	Less(T) bool
}

// ItemImpl is an element in the queue.
type ItemImpl[V Lessable[V]] struct {
	Value V
	index int // Index in the heap (optional, but useful for value updates).
}

type Item[V Lessable[V]] = *ItemImpl[V]

func NewItem[V Lessable[V]](value V) Item[V] {
	return &ItemImpl[V]{Value: value}
}

// PriorityQueue implements heap.Interface.
// It is a min-heap based on Less.
type PriorityQueue[V Lessable[V]] struct {
	impl priorityQueueImpl[V]
}

func (p PriorityQueue[V]) Len() int { return p.impl.Len() }

// Front returns the smallest value without removing it.
func (p *PriorityQueue[V]) Front() Item[V] {
	if p.impl.Len() == 0 {
		return nil
	}
	return p.impl[0]
}

// Push adds a value to the priority queue.
func (p *PriorityQueue[V]) Push(value V) Item[V] {
	item := NewItem(value)
	heap.Push(&p.impl, item)
	return item
}

// Pop removes and returns the smallest value in the priority queue.
func (p *PriorityQueue[V]) Pop() Item[V] {
	if p.impl.Len() == 0 {
		return nil
	}
	return heap.Pop(&p.impl).(Item[V])
}

// Update updates the item value and fixes the heap.
func (p *PriorityQueue[V]) Update(item Item[V], value V) {
	if item == nil || item.index < 0 || item.index >= p.impl.Len() {
		return
	}

	item.Value = value
	heap.Fix(&p.impl, item.index)
}

// Remove removes the item from the priority queue and returns it.
func (p *PriorityQueue[V]) Remove(item Item[V]) (value V) {
	if item == nil || item.index < 0 || item.index >= p.impl.Len() {
		return
	}
	return heap.Remove(&p.impl, item.index).(Item[V]).Value
}

type priorityQueueImpl[V Lessable[V]] []Item[V]

func (p priorityQueueImpl[V]) Len() int { return len(p) }

func (p priorityQueueImpl[V]) Less(i, j int) bool {
	return p[i].Value.Less(p[j].Value)
}

func (p priorityQueueImpl[V]) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}

// Push/Pop use type any (Go 1.18+).
func (p *priorityQueueImpl[V]) Push(x any) {
	item := x.(Item[V])
	item.index = len(*p)
	*p = append(*p, item)
}

func (p *priorityQueueImpl[V]) Pop() any {
	old := *p
	n := len(old)
	item := old[n-1]
	item.index = -1 // Mark removed.
	*p = old[:n-1]
	return item
}
