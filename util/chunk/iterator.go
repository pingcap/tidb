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
	"context"
	"github.com/pingcap/tidb/types"
)

var (
	_ Iterator = (*Iterator4Chunk)(nil)
	_ Iterator = (*iterator4RowPtr)(nil)
	_ Iterator = (*iterator4List)(nil)
	_ Iterator = (*iterator4Slice)(nil)
)

// NoneRow indicts none row.
var NoneRow Row

// Iterable is used to iterate rows inside the object.
type Iterable interface {
	// Iterator returns an iterator over elements.
	Iterator() Iterator
}

// Iterator is used to iterate a number of rows.
type Iterator interface {

	// Next returns the next Row.
	Next() Row

	// HasNext returns true if the iteration has more elements.
	HasNext() bool

	// Next returns the next Row.
	NextStrict() (Row, error)

	// HasNext returns true if the iteration has more elements.
	HasNextStrict() (bool, error)

	// Len returns the length.
	Len() int

	// ReachEnd reaches the end of iterator.
	ReachEnd()
}

// newIterator4Slice returns a Iterator for Row slice.
func newIterator4Slice(rows []Row) Iterator {
	return &iterator4Slice{rows: rows}
}

type iterator4Slice struct {
	rows   []Row
	cursor int
}

// Next implements the Iterator interface.
func (it *iterator4Slice) Next() Row {
	row, err := it.NextStrict()
	if err != nil {
		panic(err)
	}
	return row
}

// HasNext implements the Iterator interface.
func (it *iterator4Slice) HasNext() bool {
	hasNext, err := it.HasNextStrict()
	if err != nil {
		panic(err)
	}
	return hasNext
}

// NextStrict implements the Iterator interface.
func (it *iterator4Slice) NextStrict() (Row, error) {
	if itLen := it.Len(); it.cursor >= itLen {
		it.cursor = itLen + 1
		return NoneRow, nil
	}
	row := it.rows[it.cursor]
	it.cursor++
	return row, nil
}

// HasNextStrict implements the Iterator interface.
func (it *iterator4Slice) HasNextStrict() (bool, error) {
	return it.cursor < it.Len(), nil
}

// ReachEnd implements the Iterator interface.
func (it *iterator4Slice) ReachEnd() {
	it.cursor = it.Len() + 1
}

// Len implements the Iterator interface.
func (it *iterator4Slice) Len() int {
	return len(it.rows)
}

// newIterator4Chunk returns a iterator for Chunk.
func newIterator4Chunk(chk *Chunk) *Iterator4Chunk {
	return &Iterator4Chunk{chk: chk}
}

// Iterator4Chunk is used to iterate rows inside a chunk.
type Iterator4Chunk struct {
	chk    *Chunk
	cursor int
}

// Next implements the Iterator interface.
func (it *Iterator4Chunk) Next() Row {
	row, err := it.NextStrict()
	if err != nil {
		panic(err)
	}
	return row
}

// HasNext implements the Iterator interface.
func (it *Iterator4Chunk) HasNext() bool {
	hasNext, err := it.HasNextStrict()
	if err != nil {
		panic(err)
	}
	return hasNext
}

// NextStrict implements the Iterator interface.
func (it *Iterator4Chunk) NextStrict() (Row, error) {
	if it.cursor >= it.chk.NumRows() {
		it.cursor = it.chk.NumRows() + 1
		return NoneRow, nil
	}
	row := it.chk.GetRow(it.cursor)
	it.cursor++
	return row, nil
}

// HasStrictNext implements the Iterator interface.
func (it *Iterator4Chunk) HasNextStrict() (bool, error) {
	return it.cursor < it.chk.NumRows(), nil
}

// ReachEnd implements the Iterator interface.
func (it *Iterator4Chunk) ReachEnd() {
	it.cursor = it.Len() + 1
}

// Len implements the Iterator interface
func (it *Iterator4Chunk) Len() int {
	return it.chk.NumRows()
}

// newIterator4List returns a Iterator for List.
func newIterator4List(li *List) Iterator {
	return &iterator4List{li: li}
}

type iterator4List struct {
	li        *List
	chkCursor int
	rowCursor int
}

// Next implements the Iterator interface.
func (it *iterator4List) Next() Row {
	row, err := it.NextStrict()
	if err != nil {
		panic(err)
	}
	return row
}

// HasNext implements the Iterator interface.
func (it *iterator4List) HasNext() bool {
	hasNext, err := it.HasNextStrict()
	if err != nil {
		panic(err)
	}
	return hasNext
}

// Next implements the Iterator interface.
func (it *iterator4List) NextStrict() (Row, error) {
	if it.chkCursor >= it.li.NumChunks() {
		it.chkCursor = it.li.NumChunks() + 1
		return NoneRow, nil
	}
	chk := it.li.GetChunk(it.chkCursor)
	row := chk.GetRow(it.rowCursor)
	it.rowCursor++
	if it.rowCursor == chk.NumRows() {
		it.rowCursor = 0
		it.chkCursor++
	}
	return row, nil
}

// HasNextStrict implements the Iterator interface.
func (it *iterator4List) HasNextStrict() (bool, error) {
	return it.chkCursor < it.li.NumChunks() && it.rowCursor < it.li.GetChunk(it.chkCursor).NumRows(), nil
}

// ReachEndStrict implements the Iterator interface.
func (it *iterator4List) ReachEnd() {
	it.chkCursor = it.li.NumChunks() + 1
}

// Len implements the Iterator interface.
func (it *iterator4List) Len() int {
	return it.li.Len()
}

// newIterator4RowPtr returns a Iterator for RowPtrs.
func newIterator4RowPtr(li *List, ptrs []RowPtr) Iterator {
	return &iterator4RowPtr{li: li, ptrs: ptrs}
}

// Iterable4RowPtr is an adaptor of RowPtrs that implements Iterable interface.
type Iterable4RowPtr struct {
	li   *List
	ptrs []RowPtr
}

// Iterator implements the Iterable interface.
func (it Iterable4RowPtr) Iterator() Iterator {
	return newIterator4RowPtr(it.li, it.ptrs)
}

type iterator4RowPtr struct {
	li     *List
	ptrs   []RowPtr
	cursor int
}

// Next implements the Iterator interface.
func (it *iterator4RowPtr) Next() Row {
	row, err := it.NextStrict()
	if err != nil {
		panic(err)
	}
	return row
}

// HasNext implements the Iterator interface.
func (it *iterator4RowPtr) HasNext() bool {
	hasNext, err := it.HasNextStrict()
	if err != nil {
		panic(err)
	}
	return hasNext
}

// Next implements the Iterator interface.
func (it *iterator4RowPtr) NextStrict() (Row, error) {
	if itLen := it.Len(); it.cursor >= itLen {
		it.cursor = itLen + 1
		return NoneRow, nil
	}
	row := it.li.GetRow(it.ptrs[it.cursor])
	it.cursor++
	return row, nil
}

// HasNextStrict implements the Iterator interface.
func (it *iterator4RowPtr) HasNextStrict() (bool, error) {
	return it.cursor < it.Len(), nil
}

// ReachEndStrict implements the Iterator interface.
func (it *iterator4RowPtr) ReachEnd() {
	it.cursor = it.Len() + 1
}

// Len implements the Iterator interface.
func (it *iterator4RowPtr) Len() int {
	return len(it.ptrs)
}

type IterableDatumRow interface {
	Iterator() IteratorDatumRow
}

type IterableDatumRows []types.DatumRow

func (it IterableDatumRows) Iterator() IteratorDatumRow {
	var rows []types.DatumRow = it
	return &iterator4DatumRowSlice{rows: rows}
}

type iterator4DatumRowSlice struct {
	rows   []types.DatumRow
	cursor int
}

// Next implements the Iterator interface.
func (it *iterator4DatumRowSlice) NextStrict() (types.DatumRow, error) {
	if itLen := it.Len(); it.cursor >= itLen {
		it.cursor = itLen + 1
		return nil, nil
	}
	row := it.rows[it.cursor]
	it.cursor++
	return row, nil
}

// Next implements the Iterator interface.
func (it *iterator4DatumRowSlice) Next() types.DatumRow {
	next, err := it.NextStrict()
	if err != nil {
		panic(err)
	}
	return next
}

// HasNextStrict implements the Iterator interface.
func (it *iterator4DatumRowSlice) HasNext() bool {
	hasNext, err := it.HasNextStrict()
	if err != nil {
		panic(err)
	}
	return hasNext
}

// HasNextStrict implements the Iterator interface.
func (it *iterator4DatumRowSlice) HasNextStrict() (bool, error) {
	return it.cursor < it.Len(), nil
}

// Len implements the Iterator interface.
func (it *iterator4DatumRowSlice) Len() int {
	return len(it.rows)
}

type IteratorDatumRow interface {

	// Next returns the next DataRow.
	Next() types.DatumRow

	// HasNext returns true if the iteration has more elements.
	HasNext() bool

	// NextStrict returns the next DataRow.
	NextStrict() (types.DatumRow, error)

	// HasNextStrict returns true if the iteration has more elements.
	HasNextStrict() (bool, error)
}

func MapIterable(ctx context.Context, iter Iterable, mapFunc func(ctx context.Context, from Row) (types.DatumRow, error)) IterableDatumRow {
	return &mapIterable{iter, ctx, mapFunc}
}

type mapIterable struct {
	Iterable
	ctx     context.Context
	mapFunc func(ctx context.Context, from Row) (types.DatumRow, error)
}

func (it *mapIterable) Iterator() IteratorDatumRow {
	return &mapIterator{it.Iterable.Iterator(), it.ctx, it.mapFunc}
}

type mapIterator struct {
	Iterator
	ctx     context.Context
	mapFunc func(ctx context.Context, from Row) (types.DatumRow, error)
}

// NextStrict implements the Iterator interface.
func (it *mapIterator) NextStrict() (types.DatumRow, error) {
	next, err := it.Iterator.NextStrict()
	if err != nil {
		return nil, err
	}
	if next == NoneRow {
		return nil, err
	}
	mapped, err := it.mapFunc(it.ctx, next)
	if err != nil {
		return nil, err
	}
	return mapped, nil
}

// Next implements the Iterator interface.
func (it *mapIterator) Next() types.DatumRow {
	next, err := it.NextStrict()
	if err != nil {
		panic(err)
	}
	return next
}
