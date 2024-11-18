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
// 2. Use AnalysisJob as the object type.
// 3. Add a peak API.
// 4. Add an IsEmpty API.
// 5. Remove the thread-safe and blocking properties.
// 6. Add a Len API.
// 7. Remove the BulkAdd API.

package priorityqueue

import (
	"container/heap"

	"github.com/pingcap/errors"
)

type heapItem struct {
	obj   AnalysisJob
	index int
}

// heapData is an internal struct that implements the standard heap interface
// and keeps the data stored in the heap.
type heapData struct {
	items map[int64]*heapItem
	queue []int64
}

var (
	_ = heap.Interface(&heapData{}) // heapData is a standard heap
)

// Less is a standard heap interface function.
func (h *heapData) Less(i, j int) bool {
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
	return itemi.obj.GetWeight() > itemj.obj.GetWeight()
}

// Len is a standard heap interface function.
func (h *heapData) Len() int { return len(h.queue) }

// Swap is a standard heap interface function.
func (h *heapData) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
	item := h.items[h.queue[i]]
	item.index = i
	item = h.items[h.queue[j]]
	item.index = j
}

// Push is a standard heap interface function.
func (h *heapData) Push(x any) {
	obj := x.(AnalysisJob)
	n := len(h.queue)
	h.items[obj.GetTableID()] = &heapItem{obj, n}
	h.queue = append(h.queue, obj.GetTableID())
}

// Pop is a standard heap interface function.
func (h *heapData) Pop() any {
	key := h.queue[len(h.queue)-1]
	h.queue = h.queue[:len(h.queue)-1]
	item, ok := h.items[key]
	if !ok {
		return nil
	}
	delete(h.items, key)
	return item.obj
}

var (
	// ErrHeapIsEmpty is returned when the heap is empty.
	ErrHeapIsEmpty = errors.New("heap is empty")
)

// pqHeapImpl is a producer/consumer queue that implements a heap data structure.
type pqHeapImpl struct {
	data *heapData
}

// addOrUpdate adds an object or updates it if it already exists.
func (h *pqHeapImpl) addOrUpdate(obj AnalysisJob) error {
	if _, exists := h.data.items[obj.GetTableID()]; exists {
		h.data.items[obj.GetTableID()].obj = obj
		heap.Fix(h.data, h.data.items[obj.GetTableID()].index)
	} else {
		heap.Push(h.data, obj)
	}
	return nil
}

// update is an alias for Add.
func (h *pqHeapImpl) update(obj AnalysisJob) error {
	return h.addOrUpdate(obj)
}

// delete removes an object from the heap.
func (h *pqHeapImpl) delete(obj AnalysisJob) error {
	if item, ok := h.data.items[obj.GetTableID()]; ok {
		heap.Remove(h.data, item.index)
		return nil
	}
	return errors.New("object not found")
}

// peek returns the top object from the heap without removing it.
func (h *pqHeapImpl) peek() (AnalysisJob, error) {
	if len(h.data.queue) == 0 {
		return nil, ErrHeapIsEmpty
	}
	return h.data.items[h.data.queue[0]].obj, nil
}

// pop removes the top object from the heap and returns it.
func (h *pqHeapImpl) pop() (AnalysisJob, error) {
	if len(h.data.queue) == 0 {
		return nil, ErrHeapIsEmpty
	}
	obj := heap.Pop(h.data)
	if obj == nil {
		return nil, errors.New("object was removed from heap data")
	}
	return obj.(AnalysisJob), nil
}

// list returns a list of all objects in the heap.
func (h *pqHeapImpl) list() []AnalysisJob {
	list := make([]AnalysisJob, 0, len(h.data.items))
	for _, item := range h.data.items {
		list = append(list, item.obj)
	}
	return list
}

// len returns the number of objects in the heap.
func (h *pqHeapImpl) len() int {
	return h.data.Len()
}

// ListKeys returns a list of all keys in the heap.
func (h *pqHeapImpl) ListKeys() []int64 {
	list := make([]int64, 0, len(h.data.items))
	for key := range h.data.items {
		list = append(list, key)
	}
	return list
}

// Get returns an object from the heap.
func (h *pqHeapImpl) Get(obj AnalysisJob) (AnalysisJob, bool, error) {
	return h.getByKey(obj.GetTableID())
}

// getByKey returns an object from the heap by key.
func (h *pqHeapImpl) getByKey(key int64) (AnalysisJob, bool, error) {
	item, exists := h.data.items[key]
	if !exists {
		return nil, false, nil
	}
	return item.obj, true, nil
}

// isEmpty returns true if the heap is empty.
func (h *pqHeapImpl) isEmpty() bool {
	return len(h.data.queue) == 0
}

// newHeap returns a Heap which can be used to queue up items to process.
func newHeap() *pqHeapImpl {
	h := &pqHeapImpl{
		data: &heapData{
			items: map[int64]*heapItem{},
			queue: []int64{},
		},
	}
	return h
}
