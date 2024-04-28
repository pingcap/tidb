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
	"math"
	"sync"

	"github.com/pingcap/tidb/pkg/types"
)

// Allocator is an interface defined to reduce object allocation.
// The typical usage is to call Reset() to recycle objects into a pool,
// and Alloc() allocates from the pool.
type Allocator interface {
	Alloc(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk
	CheckReuseAllocSize() bool
	Reset()
}

var maxFreeChunks = 64
var maxFreeColumnsPerType = 256

// InitChunkAllocSize init the maximum cache size
func InitChunkAllocSize(setMaxFreeChunks, setMaxFreeColumns uint32) {
	if setMaxFreeChunks > math.MaxInt32 {
		setMaxFreeChunks = math.MaxInt32
	}
	if setMaxFreeColumns > math.MaxInt32 {
		setMaxFreeColumns = math.MaxInt32
	}
	maxFreeChunks = int(setMaxFreeChunks)
	maxFreeColumnsPerType = int(setMaxFreeColumns)
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

// columnList keep column
type columnList struct {
	freeColumns  []*Column
	allocColumns []*Column
}

func (cList *columnList) add(col *Column) {
	cList.freeColumns = append(cList.freeColumns, col)
}

// columnList Len Get the number of elements in the list
func (cList *columnList) Len() int {
	return len(cList.freeColumns) + len(cList.allocColumns)
}

// CheckReuseAllocSize return whether the cache can cache objects
func (a *allocator) CheckReuseAllocSize() bool {
	return a.freeChunk > 0 || a.columnAlloc.freeColumnsPerType > 0
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
	chk.capacity = min(capacity, maxChunkSize)
	chk.requiredRows = maxChunkSize
	// Allocate the chunk columns from the pool column allocator.
	for _, f := range fields {
		chk.columns = append(chk.columns, a.columnAlloc.NewColumn(f, chk.capacity))
	}

	//avoid OOM
	if a.freeChunk > len(a.allocated) {
		a.allocated = append(a.allocated, chk)
	}

	return chk
}

// Reset implements the Allocator interface.
func (a *allocator) Reset() {
	for i, chk := range a.allocated {
		a.allocated[i] = nil
		chk.resetForReuse()
		if len(a.free) < a.freeChunk { // Don't cache too much data.
			a.free = append(a.free, chk)
		}
	}
	a.allocated = a.allocated[:0]

	//column objects and put them to the column allocator for reuse.
	for id, pool := range a.columnAlloc.pool {
		for i, col := range pool.allocColumns {
			if (len(pool.freeColumns) < a.columnAlloc.freeColumnsPerType) && checkColumnType(id, col) {
				col.reset()
				pool.freeColumns = append(pool.freeColumns, col)
			}
			pool.allocColumns[i] = nil
		}
		pool.allocColumns = pool.allocColumns[:0]
	}
}

// checkColumnType check whether the conditions for entering the corresponding queue are met
// column Reset may change type
func checkColumnType(id int, col *Column) bool {
	if col.avoidReusing {
		return false
	}

	if id == varElemLen {
		//Take up too much memory,
		if cap(col.data) > MaxCachedLen {
			return false
		}
		return col.elemBuf == nil
	}

	if col.elemBuf == nil {
		return false
	}
	return id == cap(col.elemBuf)
}

var _ ColumnAllocator = &poolColumnAllocator{}

type poolColumnAllocator struct {
	pool               map[int]*columnList
	freeColumnsPerType int
}

// poolColumnAllocator implements the ColumnAllocator interface.
func (alloc *poolColumnAllocator) NewColumn(ft *types.FieldType, count int) *Column {
	typeSize := getFixedLen(ft)
	col := alloc.NewSizeColumn(typeSize, count)

	//column objects and put them back to the allocated column .
	alloc.put(col)
	return col
}

// poolColumnAllocator implements the ColumnAllocator interface.
func (alloc *poolColumnAllocator) NewSizeColumn(typeSize int, count int) *Column {
	l := alloc.pool[typeSize]
	if l != nil && !l.empty() {
		col := l.pop()

		if cap(col.data) < count {
			col = newColumn(typeSize, count)
		}
		return col
	}
	return newColumn(typeSize, count)
}

func (cList *columnList) pop() *Column {
	if len(cList.freeColumns) == 0 {
		return nil
	}
	col := cList.freeColumns[len(cList.freeColumns)-1]
	cList.freeColumns[len(cList.freeColumns)-1] = nil
	cList.freeColumns = cList.freeColumns[:len(cList.freeColumns)-1]
	return col
}

func (alloc *poolColumnAllocator) init() {
	alloc.pool = make(map[int]*columnList)
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
		l = &columnList{freeColumns: nil, allocColumns: nil}
		l.freeColumns = make([]*Column, 0, alloc.freeColumnsPerType)
		l.allocColumns = make([]*Column, 0, alloc.freeColumnsPerType)
		alloc.pool[typeSize] = l
	}
	if len(l.allocColumns) < alloc.freeColumnsPerType {
		l.push(col)
	}
}

// freeList is defined as a map, rather than a list, because when recycling chunk
// columns, there could be duplicated one: some of the chunk columns are just the
// reference to the others.
type freeList map[*Column]struct{}

func (cList *columnList) empty() bool {
	return len(cList.freeColumns) == 0
}

func (cList *columnList) push(col *Column) {
	if cap(col.data) < MaxCachedLen {
		cList.allocColumns = append(cList.allocColumns, col)
	}
}

var _ Allocator = &syncAllocator{}

// syncAllocator uses a mutex to protect the allocator.
type syncAllocator struct {
	mu    sync.Mutex
	alloc Allocator
}

// NewSyncAllocator creates the synchronized version of the `alloc`
func NewSyncAllocator(alloc Allocator) Allocator {
	return &syncAllocator{
		alloc: alloc,
	}
}

// Alloc implements `Allocator` for `*syncAllocator`
func (s *syncAllocator) Alloc(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.alloc.Alloc(fields, capacity, maxChunkSize)
}

// CheckReuseAllocSize implements `Allocator` for `*syncAllocator`
func (s *syncAllocator) CheckReuseAllocSize() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.alloc.CheckReuseAllocSize()
}

// Reset implements `Allocator` for `*syncAllocator`
func (s *syncAllocator) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.alloc.Reset()
}

var _ Allocator = &reuseHookAllocator{}

// reuseHookAllocator will run the function hook when it allocates the first chunk from reused part
type reuseHookAllocator struct {
	once sync.Once
	f    func()

	alloc Allocator
}

// NewReuseHookAllocator creates an allocator, which will call the function `f` when the first reused chunk is allocated.
func NewReuseHookAllocator(alloc Allocator, f func()) Allocator {
	return &reuseHookAllocator{
		f:     f,
		alloc: alloc,
	}
}

// Alloc implements `Allocator` for `*reuseHookAllocator`
func (r *reuseHookAllocator) Alloc(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk {
	if r.alloc.CheckReuseAllocSize() {
		r.once.Do(r.f)
	}

	return r.alloc.Alloc(fields, capacity, maxChunkSize)
}

// CheckReuseAllocSize implements `Allocator` for `*reuseHookAllocator`
func (r *reuseHookAllocator) CheckReuseAllocSize() bool {
	return r.alloc.CheckReuseAllocSize()
}

// Reset implements `Allocator` for `*reuseHookAllocator`
func (r *reuseHookAllocator) Reset() {
	r.alloc.Reset()
}

var _ Allocator = emptyAllocator{}

type emptyAllocator struct{}

var defaultEmptyAllocator Allocator = emptyAllocator{}

// NewEmptyAllocator creates an empty pool, which will always call `chunk.New` to create a new chunk
func NewEmptyAllocator() Allocator {
	return defaultEmptyAllocator
}

// Alloc implements `Allocator` for `*emptyAllocator`
func (emptyAllocator) Alloc(fields []*types.FieldType, capacity, maxChunkSize int) *Chunk {
	return New(fields, capacity, maxChunkSize)
}

// CheckReuseAllocSize implements `Allocator` for `*emptyAllocator`
func (emptyAllocator) CheckReuseAllocSize() bool {
	return false
}

// Reset implements `Allocator` for `*emptyAllocator`
func (emptyAllocator) Reset() {}
