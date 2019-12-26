// Copyright 2017 PingCAP, Inc.
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

package chunk

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// List holds a slice of chunks, use to append rows with max chunk size properly handled.
type List struct {
	fieldTypes    []*types.FieldType
	initChunkSize int
	maxChunkSize  int
	length        int
	chunks        []*Chunk
	freelist      []*Chunk

	memTracker *memory.Tracker // track memory usage.
}

// RowPtr is used to get a row from a list.
// It is only valid for the list that returns it.
type RowPtr struct {
	ChkIdx uint32
	RowIdx uint32
}

var chunkListLabel fmt.Stringer = stringutil.StringerStr("chunk.List")

// NewList creates a new List with field types, init chunk size and max chunk size.
func NewList(fieldTypes []*types.FieldType, initChunkSize, maxChunkSize int) *List {
	l := &List{
		fieldTypes:    fieldTypes,
		initChunkSize: initChunkSize,
		maxChunkSize:  maxChunkSize,
		memTracker:    memory.NewTracker(chunkListLabel, -1),
	}
	return l
}

// GetMemTracker returns the memory tracker of this List.
func (l *List) GetMemTracker() *memory.Tracker {
	return l.memTracker
}

// Len returns the length of the List.
func (l *List) Len() int {
	return l.length
}

// NumChunks returns the number of chunks in the List.
func (l *List) NumChunks() int {
	return len(l.chunks)
}

// FieldTypes returns the fieldTypes of the list
func (l *List) FieldTypes() []*types.FieldType {
	return l.fieldTypes
}

// NumRowsOfChunk returns the number of rows of a chunk in the ListInDisk.
func (l *List) NumRowsOfChunk(chkID int) int {
	return l.chunks[chkID].NumRows()
}

// GetChunk gets the Chunk by ChkIdx.
func (l *List) GetChunk(chkIdx int) *Chunk {
	return l.chunks[chkIdx]
}

// AppendRow appends a row to the List, the row is copied to the List.
func (l *List) AppendRow(row Row) RowPtr {
	chkIdx := len(l.chunks) - 1
	if chkIdx == -1 || l.chunks[chkIdx].NumRows() >= l.chunks[chkIdx].Capacity() {
		newChk := l.AllocChunk()
		l.chunks = append(l.chunks, newChk)
		l.memTracker.Consume(newChk.MemoryUsage())
		chkIdx++
	}
	chk := l.chunks[chkIdx]
	rowIdx := chk.NumRows()
	chk.AppendRow(row)
	l.length++
	return RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)}
}

// Add adds a chunk to the List, the chunk may be modified later by the list.
// Caller must make sure the input chk is not empty and not used any more and has the same field types.
func (l *List) Add(chk *Chunk) {
	// FixMe: we should avoid add a Chunk that chk.NumRows() > list.maxChunkSize.
	if chk.NumRows() == 0 {
		// TODO: return error here.
		panic("chunk appended to List should have at least 1 row")
	}
	l.memTracker.Consume(chk.MemoryUsage())
	l.chunks = append(l.chunks, chk)
	l.length += chk.NumRows()
}

//// BorrowChunk returns a chunk from the list, and it should be returned back by calling List.Add(chk).
//// The returned chunk is still managed by the list, and the memory usage is accounted under the list.
//func (l *List) BorrowChunk() (chk *Chunk) {
//
//}

// AllocChunk allocates a chunk from a list, it returns immediately if there is free chunks in the list,
// otherwise new chunk will be allocated. In either case, the new chunk is no longer managed by the list,
// and it is up to the caller to track the memory usage.
func (l *List) AllocChunk() (chk *Chunk) {
	if len(l.freelist) > 0 {
		chk = l.freelist[0]
		l.freelist = l.freelist[1:]
		l.memTracker.Consume(-chk.MemoryUsage())
		chk.Reset()
		return
	}
	if len(l.chunks) > 0 {
		return Renew(l.chunks[len(l.chunks)-1], l.maxChunkSize)
	}
	return New(l.fieldTypes, l.initChunkSize, l.maxChunkSize)
}

// GetRow gets a Row from the list by RowPtr.
func (l *List) GetRow(ptr RowPtr) Row {
	chk := l.chunks[ptr.ChkIdx]
	return chk.GetRow(int(ptr.RowIdx))
}

// Reset resets the List, the memory is not freed.
func (l *List) Reset() {
	l.freelist = append(l.freelist, l.chunks...)
	l.chunks = l.chunks[:0]
	l.length = 0
}

// Clear triggers GC for all the allocated chunks and reset the list
func (l *List) Clear() {
	l.memTracker.Consume(-l.memTracker.BytesConsumed())
	l.freelist = nil
	l.chunks = nil
	l.length = 0
}

// preAlloc4Row pre-allocates the storage memory for a Row.
// NOTE: only used in test
// 1. The List must be empty or holds no useful data.
// 2. The schema of the Row must be the same with the List.
// 3. This API is paired with the `Insert()` function, which inserts all the
//    rows data into the List after the pre-allocation.
func (l *List) preAlloc4Row(row Row) (ptr RowPtr) {
	chkIdx := len(l.chunks) - 1
	if chkIdx == -1 || l.chunks[chkIdx].NumRows() >= l.chunks[chkIdx].Capacity() {
		newChk := l.AllocChunk()
		l.chunks = append(l.chunks, newChk)
		l.memTracker.Consume(newChk.MemoryUsage())
		chkIdx++
	}
	chk := l.chunks[chkIdx]
	rowIdx := chk.preAlloc(row)
	l.length++
	return RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)}
}

// Insert inserts `row` on the position specified by `ptr`.
// Note: Insert will cover the origin data, it should be called after
// PreAlloc.
func (l *List) Insert(ptr RowPtr, row Row) {
	l.chunks[ptr.ChkIdx].insert(int(ptr.RowIdx), row)
}

// ListWalkFunc is used to walk the list.
// If error is returned, it will stop walking.
type ListWalkFunc = func(row Row) error

// Walk iterate the list and call walkFunc for each row.
func (l *List) Walk(walkFunc ListWalkFunc) error {
	for i := 0; i < len(l.chunks); i++ {
		chk := l.chunks[i]
		for j := 0; j < chk.NumRows(); j++ {
			err := walkFunc(chk.GetRow(j))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}
