package queue

import "container/heap"

// PriorityQueue is a priority queue implementation based on heap.
type PriorityQueue[T any] struct {
	data   []T
	size   int
	lessFn func(a T, b T) bool
}

// NewPriorityQueue creates a new priority queue.
func NewPriorityQueue[T any](size int, lessFn func(a T, b T) bool) *PriorityQueue[T] {
	data := make([]T, 0, size)
	return &PriorityQueue[T]{
		data:   data,
		size:   size,
		lessFn: lessFn,
	}
}

// Len returns the length of the queue.
func (pq *PriorityQueue[T]) Len() int {
	return len(pq.data)
}

// Less returns true if the element at index i is less than the element at index j.
func (pq *PriorityQueue[T]) Less(i, j int) bool {
	di := pq.data[i]
	dj := pq.data[j]
	return pq.lessFn(di, dj)
}

// Swap swaps the elements at index i and j.
func (pq *PriorityQueue[T]) Swap(i, j int) {
	pq.data[i], pq.data[j] = pq.data[j], pq.data[i]
}

// Push pushes an element into the priority queue.
func (pq *PriorityQueue[T]) Push(x interface{}) {
	pq.data = append(pq.data, x.(T))
}

// Pop pops an element from the priority queue.
func (pq *PriorityQueue[T]) Pop() interface{} {
	n := len(pq.data)
	ret := pq.data[n-1]
	pq.data = pq.data[: n-1 : cap(pq.data)]
	return ret
}

// Enqueue enqueues an element into the priority queue.
func (pq *PriorityQueue[T]) Enqueue(x T) {
	heap.Push(pq, x)
}

// Dequeue dequeues an element from the priority queue.
func (pq *PriorityQueue[T]) Dequeue() T {
	return heap.Pop(pq).(T)
}

// Full is used to check if the priority queue is full.
func (pq *PriorityQueue[T]) Full() bool {
	return len(pq.data) == pq.size
}

// Empty is used to check if the priority queue is empty.
func (pq *PriorityQueue[T]) Empty() bool {
	return len(pq.data) == 0
}
