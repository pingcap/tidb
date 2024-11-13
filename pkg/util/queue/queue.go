// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

// Queue is a circular buffer implementation of queue.
type Queue[T any] struct {
	elements []T
	head     int
	tail     int
	size     int
}

// NewQueue creates a new queue with the given capacity.
func NewQueue[T any](capacity int) *Queue[T] {
	return &Queue[T]{
		elements: make([]T, capacity),
	}
}

// Push pushes an element to the queue.
func (r *Queue[T]) Push(element T) {
	if r.elements == nil {
		r.elements = make([]T, 1)
	}

	if r.size == len(r.elements) {
		// Double capacity when full
		newElements := make([]T, len(r.elements)*2)
		for i := range r.size {
			newElements[i] = r.elements[(r.head+i)%len(r.elements)]
		}
		r.elements = newElements
		r.head = 0
		r.tail = r.size
	}

	r.elements[r.tail] = element
	r.tail = (r.tail + 1) % len(r.elements)
	r.size++
}

// Pop pops an element from the queue.
func (r *Queue[T]) Pop() T {
	if r.size == 0 {
		panic("Queue is empty")
	}
	element := r.elements[r.head]
	r.head = (r.head + 1) % len(r.elements)
	r.size--
	return element
}

// Len returns the number of elements in the queue.
func (r *Queue[T]) Len() int {
	return r.size
}

// IsEmpty returns true if the queue is empty.
func (r *Queue[T]) IsEmpty() bool {
	return r.size == 0
}

// Clear clears the queue.
func (r *Queue[T]) Clear() {
	r.head = 0
	r.tail = 0
	r.size = 0
}

// Cap returns the capacity of the queue.
func (r *Queue[T]) Cap() int {
	return len(r.elements)
}
