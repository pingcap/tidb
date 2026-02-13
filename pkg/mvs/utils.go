// Copyright 2026 PingCAP, Inc.
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

package mvs

import (
	"container/heap"
	"sync/atomic"
)

// Notifier provides a multi-producer, single-consumer wake-up primitive.
type Notifier struct {
	C     chan struct{}
	awake int32
}

// NewNotifier creates a new Notifier instance.
func NewNotifier() Notifier {
	return Notifier{
		C: make(chan struct{}, 1),
	}
}

// clear resets awake flag and returns the previous awake state.
func (n *Notifier) clear() bool {
	return atomic.SwapInt32(&n.awake, 0) != 0
}

// Wait blocks until a wake signal is received.
func (n *Notifier) Wait() {
	<-n.C
	n.clear()
}

// Wake notifies the consumer.
func (n *Notifier) Wake() {
	n.wake()
}

func (n *Notifier) wake() {
	// 1 -> 1: do nothing
	// 0 -> 1: send signal
	if atomic.SwapInt32(&n.awake, 1) == 0 {
		n.C <- struct{}{}
	}
}

func (n *Notifier) isAwake() bool {
	return atomic.LoadInt32(&n.awake) != 0
}

// WeakWake wakes the consumer only when it is currently not awake.
// It may lose signals under concurrent races.
func (n *Notifier) WeakWake() {
	if n.isAwake() {
		return
	}
	n.wake()
}

// Lessable describes values that can be ordered via Less.
type Lessable[T any] interface {
	Less(T) bool
}

// ItemImpl is an element in the queue.
type ItemImpl[V Lessable[V]] struct {
	Value V
	index int // Heap index, used for Update/Remove.
}

// Item is the exported pointer handle of a priority-queue element.
type Item[V Lessable[V]] *ItemImpl[V]

func newItem[V Lessable[V]](value V) Item[V] {
	return &ItemImpl[V]{Value: value, index: -1}
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
	item := newItem(value)
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

// Push/Pop use type any as required by heap.Interface.
func (p *priorityQueueImpl[V]) Push(x any) {
	item := x.(Item[V])
	item.index = len(*p)
	*p = append(*p, item)
}

func (p *priorityQueueImpl[V]) Pop() any {
	old := *p
	n := len(old)
	item := old[n-1]
	item.index = -1 // Mark as removed.
	*p = old[:n-1]
	return item
}
