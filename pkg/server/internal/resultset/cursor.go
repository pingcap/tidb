// Copyright 2023 PingCAP, Inc.
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

package resultset

import (
	"context"

	"github.com/pingcap/tidb/pkg/util/chunk"
)

// RowIterator has similar method with `chunk.RowContainerReader`. The only difference is that
// it needs a `context.Context` for the `Next` and `Current` method.
type RowIterator interface {
	// Next returns the next Row.
	Next(context.Context) chunk.Row

	// Current returns the current Row.
	Current(context.Context) chunk.Row

	// End returns the invalid end Row.
	End() chunk.Row

	// Error returns none-nil error if anything wrong happens during the iteration.
	Error() error

	// Close closes the dumper
	Close()
}

// CursorResultSet extends the `ResultSet` to provide the ability to store an iterator
type CursorResultSet interface {
	ResultSet

	GetRowIterator() RowIterator
}

// WrapWithRowContainerCursor wraps a ResultSet into a CursorResultSet
func WrapWithRowContainerCursor(rs ResultSet, rowContainer chunk.RowContainerReader) CursorResultSet {
	return &tidbCursorResultSet{
		ResultSet: rs,
		reader: rowContainerReaderIter{
			RowContainerReader: rowContainer,
		},
	}
}

type tidbCursorResultSet struct {
	ResultSet

	reader rowContainerReaderIter
}

func (tcrs *tidbCursorResultSet) GetRowIterator() RowIterator {
	return &tcrs.reader
}

var _ RowIterator = &rowContainerReaderIter{}

type rowContainerReaderIter struct {
	chunk.RowContainerReader
}

func (iter *rowContainerReaderIter) Next(context.Context) chunk.Row {
	return iter.RowContainerReader.Next()
}

func (iter *rowContainerReaderIter) Current(context.Context) chunk.Row {
	return iter.RowContainerReader.Current()
}

// FetchNotifier represents notifier will be called in COM_FETCH.
type FetchNotifier interface {
	// OnFetchReturned be called when COM_FETCH returns.
	// it will be used in server-side cursor.
	OnFetchReturned()
}

var _ CursorResultSet = &tidbLazyCursorResultSet{}

type tidbLazyCursorResultSet struct {
	ResultSet
	iter lazyRowIterator
}

// WrapWithLazyCursor wraps a ResultSet into a CursorResultSet
func WrapWithLazyCursor(rs ResultSet, capacity, maxChunkSize int) CursorResultSet {
	chk := chunk.New(rs.FieldTypes(), capacity, maxChunkSize)

	return &tidbLazyCursorResultSet{
		ResultSet: rs,
		iter: lazyRowIterator{
			rs:  rs,
			chk: chk,
		},
	}
}

func (tcrs *tidbLazyCursorResultSet) GetRowIterator() RowIterator {
	return &tcrs.iter
}

type lazyRowIterator struct {
	rs       ResultSet
	err      error
	chk      *chunk.Chunk
	idxInChk int
	started  bool
}

func (iter *lazyRowIterator) Next(ctx context.Context) chunk.Row {
	if !iter.started {
		iter.started = true
	}

	iter.idxInChk++

	if iter.idxInChk >= iter.chk.NumRows() {
		// Reached the end, update the chunk
		err := iter.rs.Next(ctx, iter.chk)
		if err != nil {
			iter.err = err
			return chunk.Row{}
		}

		if iter.chk.NumRows() == 0 {
			// reached the end
			return chunk.Row{}
		}
		iter.idxInChk = 0
	}

	return iter.chk.GetRow(iter.idxInChk)
}

func (iter *lazyRowIterator) Current(ctx context.Context) chunk.Row {
	if !iter.started {
		return iter.Next(ctx)
	}

	if iter.chk.NumRows() == 0 {
		return chunk.Row{}
	}

	return iter.chk.GetRow(iter.idxInChk)
}

func (*lazyRowIterator) End() chunk.Row {
	return chunk.Row{}
}

func (iter *lazyRowIterator) Error() error {
	return iter.err
}

func (iter *lazyRowIterator) Close() {
	iter.rs.Close()
}
