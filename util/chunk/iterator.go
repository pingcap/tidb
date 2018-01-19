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

// Iterator is used to iterate a number of rows.
//
// for row := it.Begin(); row != it.End(); row = it.Next() {
//     ...
// }
type Iterator interface {
	// Begin resets the cursor of the iterator and returns the first Row.
	Begin() Row

	// Next returns the next Row.
	Next() Row

	// End returns the invalid end Row.
	End() Row

	// Len returns the length.
	Len() int
}

// NewSliceIterator returns a Iterator for Row slice.
func NewSliceIterator(rows []Row) Iterator {
	return &sliceIterator{rows: rows}
}

type sliceIterator struct {
	rows   []Row
	cursor int
}

func (it *sliceIterator) Begin() Row {
	if len(it.rows) == 0 {
		return it.End()
	}
	it.cursor = 1
	return it.rows[0]
}

func (it *sliceIterator) Next() Row {
	if it.cursor == len(it.rows) {
		return it.End()
	}
	row := it.rows[it.cursor]
	it.cursor++
	return row
}

func (it *sliceIterator) End() Row {
	return Row{}
}

func (it *sliceIterator) Len() int {
	return len(it.rows)
}

// NewChunkIterator returns a iterator for Chunk.
func NewChunkIterator(chk *Chunk) *ChunkIterator {
	return &ChunkIterator{chk: chk}
}

type ChunkIterator struct {
	chk    *Chunk
	cursor int
}

func (it *ChunkIterator) Begin() Row {
	if it.chk.NumRows() == 0 {
		return it.End()
	}
	it.cursor = 1
	return it.chk.GetRow(0)
}

func (it *ChunkIterator) Next() Row {
	if it.cursor == it.chk.NumRows() {
		return it.End()
	}
	row := it.chk.GetRow(it.cursor)
	it.cursor++
	return row
}

func (it *ChunkIterator) End() Row {
	return Row{}
}

func (it *ChunkIterator) Len() int {
	return it.chk.NumRows()
}

// NewListIterator returns a Iterator for List.
func NewListIterator(li *List) Iterator {
	return &listIterator{li: li}
}

type listIterator struct {
	li        *List
	chkCursor int
	rowCursor int
}

func (it *listIterator) Begin() Row {
	if it.chkCursor == it.li.NumChunks() {
		return it.End()
	}
	chk := it.li.GetChunk(0)
	row := chk.GetRow(0)
	if chk.NumRows() == 1 {
		it.chkCursor = 1
		it.rowCursor = 0
	} else {
		it.chkCursor = 0
		it.rowCursor = 1
	}
	return row
}

func (it *listIterator) Next() Row {
	if it.chkCursor == it.li.NumChunks() {
		return it.End()
	}
	chk := it.li.GetChunk(it.chkCursor)
	row := chk.GetRow(it.rowCursor)
	it.rowCursor++
	if it.rowCursor == chk.NumRows() {
		it.rowCursor = 0
		it.chkCursor++
	}
	return row
}

func (it *listIterator) End() Row {
	return Row{}
}

func (it *listIterator) Len() int {
	return it.li.Len()
}

// NewRowPtrIterator returns a Iterator for RowPtrs.
func NewRowPtrIterator(li *List, ptrs []RowPtr) Iterator {
	return &rowPtrIterator{li: li, ptrs: ptrs}
}

type rowPtrIterator struct {
	li     *List
	ptrs   []RowPtr
	cursor int
}

func (it *rowPtrIterator) Begin() Row {
	if len(it.ptrs) == 0 {
		return it.End()
	}
	it.cursor = 1
	return it.li.GetRow(it.ptrs[0])
}

func (it *rowPtrIterator) Next() Row {
	if it.cursor == len(it.ptrs) {
		return it.End()
	}
	row := it.li.GetRow(it.ptrs[it.cursor])
	it.cursor++
	return row
}

func (it *rowPtrIterator) End() Row {
	return Row{}
}

func (it *rowPtrIterator) Len() int {
	return len(it.ptrs)
}
