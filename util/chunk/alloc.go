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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mathutil"
)

// Allocator is an interface defined to reduce object allocation.
// The typical usage is to call Reset() to recycle objects into a pool,
// and Alloc() allocates from the pool.
type Allocator interface {
	Alloc(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk
	SetLimit(chunknum, columnnum int)
	Reset()
}

// NewAllocator creates an Allocator.
func NewAllocator() *allocator {
	ret := &allocator{freeChunk: maxFreeChunks}
	ret.columnAlloc.init()
	return ret
}

var _ Allocator = &allocator{}

// MaxCachedLen Maximum cacheable length
var MaxCachedLen = 16 * 1024

// allocator try to reuse objects.
// It uses `poolColumnAllocator` to alloc chunk column objects.
// The allocated chunks are recorded in the `allocated` array.
// After Reset(), those chunks are decoupled into chunk column objects and get
// into `poolColumnAllocator` again for reuse.
type allocator struct {
	allocated   []*Chunk
	free        []*Chunk
	columnAlloc poolColumnAllocator
	freeChunk   int
}

// SetLimit limit reuse num
func (a *allocator) SetLimit(chunknum, columnnum int) {
	a.freeChunk = chunknum
	a.columnAlloc.freeColumnsPerType = columnnum
}

// Alloc implements the Allocator interface.
func (a *allocator) Alloc(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk {
	var chk *Chunk
	// Try to alloc from the free list.
	if len(a.free) > 0 {
		chk = a.free[len(a.free)-1]
		a.free = a.free[:len(a.free)-1]
	} else {
		chk = &Chunk{columns: make([]*Column, 0, len(fields))}
	}

	// Init the chunk fields.
	chk.capacity = mathutil.Min(capacity, maxChunkSize)
	chk.requiredRows = maxChunkSize
	// Allocate the chunk columns from the pool column allocator.
	for _, f := range fields {
		chk.columns = append(chk.columns, a.columnAlloc.NewColumn(f, chk.capacity))
	}

	a.allocated = append(a.allocated, chk)
	return chk
}

const (
	maxFreeChunks         = 128
	maxFreeColumnsPerType = 1024
)

// Reset implements the Allocator interface.
func (a *allocator) Reset() {
	//a.free = a.free[:0]
	for i, chk := range a.allocated {
		a.allocated[i] = nil
		// Decouple chunk into chunk column objects and put them back to the column allocator for reuse.
		for _, col := range chk.columns {
			col.reset()
			a.columnAlloc.put(col)
		}
		// Reset the chunk and put it to the free list for reuse.
		chk.resetForReuse()

		if len(a.free) < a.freeChunk { // Don't cache too much data.
			a.free = append(a.free, chk)
		}
	}
	a.allocated = a.allocated[:0]
}

var _ ColumnAllocator = &poolColumnAllocator{}

type poolColumnAllocator struct {
	pool               map[int]freeList
	freeColumnsPerType int
}

// poolColumnAllocator implements the ColumnAllocator interface.
func (alloc *poolColumnAllocator) NewColumn(ft *types.FieldType, count int) *Column {
	typeSize := getFixedLen(ft)
	return alloc.NewSizeColumn(typeSize, count)
}

// poolColumnAllocator implements the ColumnAllocator interface.
func (alloc *poolColumnAllocator) NewSizeColumn(typeSize int, count int) *Column {
	l := alloc.pool[typeSize]
	if l != nil && !l.empty() {
		//col := l.pop()
		col := l[len(l)-1]
		if cap(col.data) < count {
			col = newColumn(typeSize, count)
		}
		alloc.pool[typeSize] = l[:len(l)-1]
		return col
	}
	return newColumn(typeSize, count)
}

func (alloc *poolColumnAllocator) init() {
	alloc.pool = make(map[int]freeList)
	alloc.freeColumnsPerType = maxFreeColumnsPerType
}

func (alloc *poolColumnAllocator) put(col *Column) {
	if col.avoidReusing {
		return
	}
	typeSize := col.typeSize()
	if typeSize <= 0 && typeSize != varElemLen {
		return
	}

	l := alloc.pool[typeSize]
	if l == nil {
		//l = make(map[*Column]struct{}, 8)
		l = make([]*Column, 0, alloc.freeColumnsPerType)
		alloc.pool[typeSize] = l
	}
	//l.push(col)
	alloc.push(col, typeSize)
}

// freeList is defined as a map, rather than a list, because when recycling chunk
// columns, there could be duplicated one: some of the chunk columns are just the
// reference to the others.
// type freeList map[*Column]struct{}
type freeList []*Column

func (l freeList) empty() bool {
	return len(l) == 0
}

func (alloc *poolColumnAllocator) push(col *Column, typeSize int) {
	if len(alloc.pool[typeSize]) >= alloc.freeColumnsPerType {
		return
	}
	if (typeSize == varElemLen) && cap(col.data) > MaxCachedLen {
		return
	}
	alloc.pool[typeSize] = append(alloc.pool[typeSize], col)
}
