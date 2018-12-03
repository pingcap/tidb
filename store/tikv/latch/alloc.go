// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package latch

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

const nodeBlockSize = 1024

type nodeBlock [nodeBlockSize]node

// init makes the nodeBlock into a list.
func (b *nodeBlock) init(start int) {
	for i := 0; i < nodeBlockSize-1; i++ {
		(*b)[i].next = nodePtr(start + i + 1)
	}
}

type nodeAlloc struct {
	// Note that physically, all nodeBlocks are not in a continuous memory,
	// but logically they are conotinuous:
	// data[0] contains node[0-1023]
	// data[1] contains node[1024-2047] ...
	data     []*nodeBlock
	freeList nodePtr
	sync.RWMutex
}

type nodePtr int

func (a *nodeAlloc) New() nodePtr {
	a.Lock()
	defer a.Unlock()

	if a.freeList.IsNil() {
		block := new(nodeBlock)
		start := len(a.data) * nodeBlockSize
		block.init(start)
		a.data = append(a.data, block)
		a.freeList = nodePtr(start)
		if a.freeList == 0 {
			// nodePtr == 0 means IsNil, so don't use the data[0] node.
			a.freeList++
		}
		log.Debug("nodeAlloc len(a.data) = ", len(a.data))
	}
	ret := a.freeList
	n := ret.value(a)
	a.freeList = n.next
	*n = node{}
	return ret
}

func (p nodePtr) IsNil() bool {
	return p == 0
}

func (p nodePtr) Next(alloc *nodeAlloc) nodePtr {
	return p.Value(alloc).next
}

func (p nodePtr) Value(alloc *nodeAlloc) *node {
	alloc.RLock()
	defer alloc.RUnlock()
	return p.value(alloc)
}

func (p nodePtr) value(alloc *nodeAlloc) *node {
	idx := p / nodeBlockSize
	offset := p % nodeBlockSize
	block := alloc.data[idx]
	return &(*block)[offset]
}

func (p nodePtr) Free(alloc *nodeAlloc) {
	alloc.Lock()
	defer alloc.Unlock()

	p.value(alloc).next = alloc.freeList
	alloc.freeList = p
}
