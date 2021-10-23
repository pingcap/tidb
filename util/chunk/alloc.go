// Copyright 2021 PingCAP, Inc.
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

package chunk

import (
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/types"
)

// Allocator is an interface defined to reduce object allocation.
// The typical usage is to call Reset() to recycle objects into a pool,
// and Alloc() allocates from the pool.
type Allocator interface {
	Alloc(fields []*types.FieldType, cap, maxChunkSize int) *Chunk
	Reset()
}

// NewAllocator creates an Allocator.
func NewAllocator() Allocator {
	ret := &allocator{}
	ret.columnAlloc.init()
	return ret
}

// allocator try to reuse objects at the chunk column level.
// It uses `poolColumnAllocator` to alloc chunk column objects.
// The allocated chunks are recorded in the `allocated` array.
// After Reset(), those chunks are decoupled into chunk column objects and get
// into `poolColumnAllocator` again for reuse.
type allocator struct {
	allocated   []Chunk
	columnAlloc poolColumnAllocator
}

// Alloc implements the Allocator interface.
func (a *allocator) Alloc(fields []*types.FieldType, cap, maxChunkSize int) *Chunk {
	a.allocated = append(a.allocated, Chunk{
		columns:      make([]*Column, 0, len(fields)),
		capacity:     mathutil.Min(cap, maxChunkSize),
		requiredRows: maxChunkSize,
	})
	chk := &a.allocated[len(a.allocated)-1]

	for _, f := range fields {
		chk.columns = append(chk.columns, a.columnAlloc.NewColumn(f, chk.capacity))
	}

	return chk
}

// Reset implements the Allocator interface.
func (a *allocator) Reset() {
	for i := 0; i < len(a.allocated); i++ {
		chk := &a.allocated[i]
		// Decouple Chunk into Chunk Column objects for reuse.
		for _, col := range chk.columns {
			a.columnAlloc.put(col)
		}
		// Reset all the fields
		*chk = Chunk{}
	}
	a.allocated = a.allocated[:0]
}

type poolColumnAllocator struct {
	pool map[int]freeList
}

// poolColumnAllocator implements the ColumnAllocator interface.
func (alloc *poolColumnAllocator) NewColumn(ft *types.FieldType, cap int) *Column {
	typeSize := getFixedLen(ft)
	if l, ok := alloc.pool[typeSize]; ok {
		if !l.empty() {
			col := l.pop()
			return col
		}
	}
	return newColumn(typeSize, cap)
}

func (alloc *poolColumnAllocator) init() {
	alloc.pool = make(map[int]freeList)
	return
}

func (alloc *poolColumnAllocator) put(col *Column) {
	typeSize := col.typeSize()
	if typeSize <= 0 {
		return
	}

	l, ok := alloc.pool[typeSize]
	if !ok {
		l = freeList{
			data: make([]*Column, 0, 8),
		}
		l.push(col)
		alloc.pool[typeSize] = l
		return
	}
	l.push(col)
	return
}

type freeList struct {
	data []*Column
}

func (l *freeList) empty() bool {
	return len(l.data) == 0
}

func (l *freeList) pop() *Column {
	ret := l.data[len(l.data)-1]
	l.data = l.data[:len(l.data)-1]
	return ret
}

func (l *freeList) push(c *Column) {
	l.data = append(l.data, c)
}
