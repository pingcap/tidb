// Copyright 2017 The Kubernetes Authors.
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
// Modifications:
// 1. Use the `errors` package from PingCAP.
// 2. Use generics to define the `heapData` struct.
// 3. Add a peak API.

package heap

import (
	"container/heap"
	"sync"

	"github.com/pingcap/errors"
)

const (
	closedMsg = "heap is closed"
)

// LessFunc is used to compare two objects in the heap.
type LessFunc[T any] func(T, T) bool

// KeyFunc is used to generate a key for an object.
type KeyFunc[T any, K comparable] func(T) (K, error)

type heapItem[T any] struct {
	obj   T   // The object which is stored in the heap.
	index int // The index of the object's key in the Heap.queue.
}

type itemKeyValue[T any, K comparable] struct {
	obj T
	key K
}

// heapData is an internal struct that implements the standard heap interface
// and keeps the data stored in the heap.
type heapData[T any, K comparable] struct {
	items    map[K]*heapItem[T]
	keyFunc  KeyFunc[T, K]
	lessFunc LessFunc[T]
	queue    []K
}

var (
	_ = heap.Interface(&heapData[any, any]{}) // heapData is a standard heap
)

// Less is a standard heap interface function.
func (h *heapData[T, K]) Less(i, j int) bool {
	if i >= len(h.queue) || j >= len(h.queue) {
		return false
	}
	itemi, ok := h.items[h.queue[i]]
	if !ok {
		return false
	}
	itemj, ok := h.items[h.queue[j]]
	if !ok {
		return false
	}
	return h.lessFunc(itemi.obj, itemj.obj)
}

// Len is a standard heap interface function.
func (h *heapData[T, K]) Len() int { return len(h.queue) }

// Swap is a standard heap interface function.
func (h *heapData[T, K]) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
	item := h.items[h.queue[i]]
	item.index = i
	item = h.items[h.queue[j]]
	item.index = j
}

// Push is a standard heap interface function.
func (h *heapData[T, K]) Push(kv any) {
	keyValue := kv.(*itemKeyValue[T, K])
	n := len(h.queue)
	h.items[keyValue.key] = &heapItem[T]{keyValue.obj, n}
	h.queue = append(h.queue, keyValue.key)
}

// Pop is a standard heap interface function.
func (h *heapData[T, K]) Pop() any {
	key := h.queue[len(h.queue)-1]
	h.queue = h.queue[:len(h.queue)-1]
	item, ok := h.items[key]
	if !ok {
		return nil
	}
	delete(h.items, key)
	return item.obj
}

// Heap is a thread-safe producer/consumer queue that implements a heap data structure.
type Heap[T any, K comparable] struct {
	data   *heapData[T, K]
	cond   sync.Cond
	lock   sync.RWMutex
	closed bool
}

// Close closes the heap.
func (h *Heap[T, K]) Close() {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.closed = true
	h.cond.Broadcast()
}

// Add adds an object or updates it if it already exists.
func (h *Heap[T, K]) Add(obj T) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return errors.Errorf("key error: %v", err)
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return errors.New(closedMsg)
	}
	if _, exists := h.data.items[key]; exists {
		h.data.items[key].obj = obj
		heap.Fix(h.data, h.data.items[key].index)
	} else {
		h.addIfNotPresentLocked(key, obj)
	}
	h.cond.Broadcast()
	return nil
}

// BulkAdd adds a list of objects to the heap.
func (h *Heap[T, K]) BulkAdd(list []T) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return errors.New(closedMsg)
	}
	for _, obj := range list {
		key, err := h.data.keyFunc(obj)
		if err != nil {
			return errors.Errorf("key error: %v", err)
		}
		if _, exists := h.data.items[key]; exists {
			h.data.items[key].obj = obj
			heap.Fix(h.data, h.data.items[key].index)
		} else {
			h.addIfNotPresentLocked(key, obj)
		}
	}
	h.cond.Broadcast()
	return nil
}

// AddIfNotPresent adds an object if it does not already exist.
func (h *Heap[T, K]) AddIfNotPresent(obj T) error {
	id, err := h.data.keyFunc(obj)
	if err != nil {
		return errors.Errorf("key error: %v", err)
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return errors.New(closedMsg)
	}
	h.addIfNotPresentLocked(id, obj)
	h.cond.Broadcast()
	return nil
}

func (h *Heap[T, K]) addIfNotPresentLocked(key K, obj T) {
	if _, exists := h.data.items[key]; exists {
		return
	}
	heap.Push(h.data, &itemKeyValue[T, K]{obj, key})
}

// Update is an alias for Add.
func (h *Heap[T, K]) Update(obj T) error {
	return h.Add(obj)
}

// Delete removes an object from the heap.
func (h *Heap[T, K]) Delete(obj T) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return errors.Errorf("key error: %v", err)
	}
	h.lock.Lock()
	defer h.lock.Unlock()
	if item, ok := h.data.items[key]; ok {
		heap.Remove(h.data, item.index)
		return nil
	}
	return errors.New("object not found")
}

// Peek returns the top object from the heap without removing it.
func (h *Heap[T, K]) Peek() (T, error) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	if len(h.data.queue) == 0 {
		var zero T
		return zero, errors.New("heap is empty")
	}
	return h.data.items[h.data.queue[0]].obj, nil
}

// Pop removes the top object from the heap and returns it.
func (h *Heap[T, K]) Pop() (T, error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	for len(h.data.queue) == 0 {
		if h.closed {
			var zero T
			return zero, errors.New("heap is closed")
		}
		h.cond.Wait()
	}
	obj := heap.Pop(h.data)
	if obj == nil {
		var zero T
		return zero, errors.New("object was removed from heap data")
	}
	return obj.(T), nil
}

// List returns a list of all objects in the heap.
func (h *Heap[T, K]) List() []T {
	h.lock.RLock()
	defer h.lock.RUnlock()
	list := make([]T, 0, len(h.data.items))
	for _, item := range h.data.items {
		list = append(list, item.obj)
	}
	return list
}

// ListKeys returns a list of all keys in the heap.
func (h *Heap[T, K]) ListKeys() []K {
	h.lock.RLock()
	defer h.lock.RUnlock()
	list := make([]K, 0, len(h.data.items))
	for key := range h.data.items {
		list = append(list, key)
	}
	return list
}

// Get returns an object from the heap.
func (h *Heap[T, K]) Get(obj T) (T, bool, error) {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		var zero T
		return zero, false, errors.Errorf("key error: %v", err)
	}
	return h.GetByKey(key)
}

// GetByKey returns an object from the heap by key.
func (h *Heap[T, K]) GetByKey(key K) (T, bool, error) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	item, exists := h.data.items[key]
	if !exists {
		var zero T
		return zero, false, nil
	}
	return item.obj, true, nil
}

// IsClosed returns true if the heap is closed.
func (h *Heap[T, K]) IsClosed() bool {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.closed
}

// NewHeap returns a Heap which can be used to queue up items to process.
func NewHeap[T any, K comparable](keyFn KeyFunc[T, K], lessFn LessFunc[T]) *Heap[T, K] {
	h := &Heap[T, K]{
		data: &heapData[T, K]{
			items:    map[K]*heapItem[T]{},
			queue:    []K{},
			keyFunc:  keyFn,
			lessFunc: lessFn,
		},
	}
	h.cond.L = &h.lock
	return h
}
