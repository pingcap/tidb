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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
)

// List holds a slice of chunks, use to append rows with max chunk size properly handled.
type List struct {
	fieldTypes   []*types.FieldType
	maxChunkSize int
	length       int
	chunks       []*Chunk
	freelist     []*Chunk

	memTracker   *memory.MemoryTracker // track memory usage.
	lastConsumed bool                  // whether the memory usage of last Chunk in "chunks" has consumed.
}

// RowPtr is used to get a row from a list.
// It is only valid for the list that returns it.
type RowPtr struct {
	ChkIdx uint32
	RowIdx uint32
}

// NewList creates a new List with field types and max chunk size.
func NewList(fieldTypes []*types.FieldType, maxChunkSize int) *List {
	l := &List{
		fieldTypes:   fieldTypes,
		maxChunkSize: maxChunkSize,
		memTracker:   memory.NewMemoryTracker("chunk.List", -1),
		lastConsumed: true,
	}
	return l
}

func (l *List) GetMemTracker() *memory.MemoryTracker {
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

// GetChunk gets the Chunk by ChkIdx.
func (l *List) GetChunk(chkIdx int) *Chunk {
	return l.chunks[chkIdx]
}

// AppendRow appends a row to the List, the row is copied to the List.
func (l *List) AppendRow(row Row) RowPtr {
	chkIdx := len(l.chunks) - 1
	if chkIdx == -1 || l.chunks[chkIdx].NumRows() >= l.maxChunkSize {
		newChk := l.allocChunk()
		l.chunks = append(l.chunks, newChk)
		if chkIdx != -1 && !l.lastConsumed {
			l.memTracker.Consume(l.chunks[chkIdx].MemoryUsage())
		}
		l.lastConsumed = false
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
		panic("chunk appended to List should have at least 1 rows")
	}
	l.memTracker.Consume(chk.MemoryUsage())
	l.lastConsumed = true
	l.chunks = append(l.chunks, chk)
	l.length += chk.NumRows()
	return
}

func (l *List) allocChunk() (chk *Chunk) {
	if len(l.freelist) > 0 {
		lastIdx := len(l.freelist) - 1
		chk = l.freelist[lastIdx]
		l.freelist = l.freelist[:lastIdx]
		l.memTracker.Consume(-chk.MemoryUsage())
		chk.Reset()
		return
	}
	return NewChunk(l.fieldTypes)
}

// GetRow gets a Row from the list by RowPtr.
func (l *List) GetRow(ptr RowPtr) Row {
	chk := l.chunks[ptr.ChkIdx]
	return chk.GetRow(int(ptr.RowIdx))
}

// Reset resets the List.
func (l *List) Reset() {
	if lastIdx := len(l.chunks) - 1; !l.lastConsumed && lastIdx >= 0 {
		l.memTracker.Consume(l.chunks[lastIdx].MemoryUsage())
	}
	l.freelist = append(l.freelist, l.chunks...)
	l.chunks = l.chunks[:0]
	l.length = 0
	l.lastConsumed = true
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
