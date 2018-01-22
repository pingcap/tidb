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

var (
	_ Iterator = (*Iterator4Chunk)(nil)

	_ Iterator = (*iterator4RowPtr)(nil)
	_ Iterator = (*iterator4List)(nil)
	_ Iterator = (*iterator4Slice)(nil)
)

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

// NewIterator4Slice returns a Iterator for Row slice.
func NewIterator4Slice(rows []Row) Iterator {
	return &iterator4Slice{rows: rows}
}

type iterator4Slice struct {
	rows   []Row
	cursor int
}

func (it *iterator4Slice) Begin() Row {
	if len(it.rows) == 0 {
		return it.End()
	}
	it.cursor = 1
	return it.rows[0]
}

func (it *iterator4Slice) Next() Row {
	if it.cursor == len(it.rows) {
		return it.End()
	}
	row := it.rows[it.cursor]
	it.cursor++
	return row
}

func (it *iterator4Slice) End() Row {
	return Row{}
}

func (it *iterator4Slice) Len() int {
	return len(it.rows)
}

// NewIterator4Chunk returns a iterator for Chunk.
func NewIterator4Chunk(chk *Chunk) *Iterator4Chunk {
	return &Iterator4Chunk{chk: chk}
}

// Iterator4Chunk is used to iterate rows inside a chunk.
type Iterator4Chunk struct {
	chk    *Chunk
	cursor int
}

// Begin implements the Iterator interface
func (it *Iterator4Chunk) Begin() Row {
	if it.chk.NumRows() == 0 {
		return it.End()
	}
	it.cursor = 1
	return it.chk.GetRow(0)
}

// Next implements the Iterator interface
func (it *Iterator4Chunk) Next() Row {
	if it.cursor == it.chk.NumRows() {
		return it.End()
	}
	row := it.chk.GetRow(it.cursor)
	it.cursor++
	return row
}

// End implements the Iterator interface
func (it *Iterator4Chunk) End() Row {
	return Row{}
}

// Len implements the Iterator interface
func (it *Iterator4Chunk) Len() int {
	return it.chk.NumRows()
}

// NewIterator4List returns a Iterator for List.
func NewIterator4List(li *List) Iterator {
	return &iterator4List{li: li}
}

type iterator4List struct {
	li        *List
	chkCursor int
	rowCursor int
}

func (it *iterator4List) Begin() Row {
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

func (it *iterator4List) Next() Row {
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

func (it *iterator4List) End() Row {
	return Row{}
}

func (it *iterator4List) Len() int {
	return it.li.Len()
}

// NewIterator4RowPtr returns a Iterator for RowPtrs.
func NewIterator4RowPtr(li *List, ptrs []RowPtr) Iterator {
	return &iterator4RowPtr{li: li, ptrs: ptrs}
}

type iterator4RowPtr struct {
	li     *List
	ptrs   []RowPtr
	cursor int
}

func (it *iterator4RowPtr) Begin() Row {
	if len(it.ptrs) == 0 {
		return it.End()
	}
	it.cursor = 1
	return it.li.GetRow(it.ptrs[0])
}

func (it *iterator4RowPtr) Next() Row {
	if it.cursor == len(it.ptrs) {
		return it.End()
	}
	row := it.li.GetRow(it.ptrs[it.cursor])
	it.cursor++
	return row
}

func (it *iterator4RowPtr) End() Row {
	return Row{}
}

func (it *iterator4RowPtr) Len() int {
	return len(it.ptrs)
}
