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
)

// List holds a slice of chunks, use to append rows with max chunk size properly handled.
type List struct {
	fieldTypes   []*types.FieldType
	maxChunkSize int
	length       int
	chunks       []*Chunk
	freelist     []*Chunk
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
	}
	return l
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
	if chk.NumRows() == 0 {
		panic(" add empty chunk")
	}
	l.chunks = append(l.chunks, chk)
	l.length += chk.NumRows()
	return
}

func (l *List) allocChunk() (chk *Chunk) {
	if len(l.freelist) > 0 {
		lastIdx := len(l.freelist) - 1
		chk = l.freelist[lastIdx]
		chk.Reset()
		l.freelist = l.freelist[:lastIdx]
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
	l.freelist = append(l.freelist, l.chunks...)
	l.chunks = l.chunks[:0]
	l.length = 0
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

// MemoryUsage calculates the total memory usage of a list.
func (l *List) MemoryUsage() (sum int64) {
	for _, c := range l.chunks {
		sum += c.MemoryUsage()
	}
	for _, f := range l.freelist {
		sum += f.MemoryUsage()
	}
	return sum
}
