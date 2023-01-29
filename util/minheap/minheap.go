// Copyright 2023 PingCAP, Inc.
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

package minheap

import (
	"container/heap"
	"sort"

	"golang.org/x/exp/constraints"
)

type Heap[T constraints.Ordered] struct {
	Nodes Nodes[T]
	K     uint32
}

func NewHeap[T constraints.Ordered](k uint32) *Heap[T] {
	h := Nodes[T]{}
	heap.Init(&h)
	return &Heap[T]{Nodes: h, K: k}
}

func (h *Heap[T]) Add(val *Node[T]) *Node[T] {
	if h.K > uint32(len(h.Nodes)) {
		heap.Push(&h.Nodes, val)
	} else if val.Count > h.Nodes[0].Count {
		expelled := heap.Pop(&h.Nodes)
		heap.Push(&h.Nodes, val)
		node := expelled.(*Node[T])
		return node
	}
	return nil
}

func (h *Heap[T]) Pop() *Node[T] {
	expelled := heap.Pop(&h.Nodes)
	return expelled.(*Node[T])
}

func (h *Heap[T]) Fix(idx int, count uint32) {
	h.Nodes[idx].Count = count
	heap.Fix(&h.Nodes, idx)
}

func (h *Heap[T]) Min() uint32 {
	if len(h.Nodes) == 0 {
		return 0
	}
	return h.Nodes[0].Count
}

func (h *Heap[T]) Find(key T) (int, bool) {
	for i := range h.Nodes {
		if h.Nodes[i].Key == key {
			return i, true
		}
	}
	return 0, false
}

func (h *Heap[T]) Sorted() Nodes[T] {
	nodes := append([]*Node[T](nil), h.Nodes...)
	sort.Sort(sort.Reverse(Nodes[T](nodes)))
	return nodes
}

type Nodes[T constraints.Ordered] []*Node[T]

type Node[T constraints.Ordered] struct {
	Key   T
	Count uint32
}

func (n Nodes[T]) Len() int {
	return len(n)
}

func (n Nodes[T]) Less(i, j int) bool {
	return (n[i].Count < n[j].Count) || (n[i].Count == n[j].Count && n[i].Key > n[j].Key)
}

func (n Nodes[T]) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n *Nodes[T]) Push(val any) {
	*n = append(*n, val.(*Node[T]))
}

func (n *Nodes[T]) Pop() any {
	var val *Node[T]
	val, *n = (*n)[len((*n))-1], (*n)[:len((*n))-1]
	return val
}
