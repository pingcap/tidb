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

package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// MergeJoinExec implements the merge join algorithm.
// This operator assumes that two iterators of both sides
// will provide required order on join condition:
// 1. For equal-join, one of the join key from each side
// matches the order given.
// 2. For other cases its preferred not to use SMJ and operator
// will throw error.
type MergeJoinExec struct {
	baseExecutor

	stmtCtx         *stmtctx.StatementContext
	compareFuncs    []chunk.CompareFunc
	resultGenerator joinResultGenerator

	prepared bool
	outerIdx int

	innerTable *mergeJoinInnerTable
	outerTable *mergeJoinOuterTable

	innerRows     []chunk.Row
	outerIter4Row chunk.Iterator
	innerIter4Row chunk.Iterator
}

type mergeJoinOuterTable struct {
	reader Executor
	filter []expression.Expression
	keys   []*expression.Column

	chk      *chunk.Chunk
	selected []bool

	iter *chunk.Iterator4Chunk
	row  chunk.Row
}

// mergeJoinInnerTable represents a row block with the same join keys
type mergeJoinInnerTable struct {
	stmtCtx  *stmtctx.StatementContext
	sctx     sessionctx.Context
	reader   Executor
	joinKeys []*expression.Column
	ctx      context.Context

	// for chunk executions
	sameKeyRows    []chunk.Row
	compareFuncs   []chunk.CompareFunc
	firstRow4Key   chunk.Row
	curRow         chunk.Row
	curResult      *chunk.Chunk
	curIter        *chunk.Iterator4Chunk
	curResultInUse bool
	resultQueue    []*chunk.Chunk
	resourceQueue  []*chunk.Chunk
}

func (t *mergeJoinInnerTable) init(chk4Reader *chunk.Chunk) (err error) {
	if t.reader == nil || t.joinKeys == nil || len(t.joinKeys) == 0 || t.ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	t.stmtCtx = t.sctx.GetSessionVars().StmtCtx
	t.curResult = chk4Reader
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curRow = t.curIter.End()
	t.curResultInUse = false
	t.resultQueue = append(t.resultQueue, chk4Reader)
	t.firstRow4Key, err = t.nextSelectedRow()
	t.compareFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		t.compareFuncs = append(t.compareFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	return errors.Trace(err)
}

func (t *mergeJoinInnerTable) rowsWithSameKey() ([]chunk.Row, error) {
	lastResultIdx := len(t.resultQueue) - 1
	t.resourceQueue = append(t.resourceQueue, t.resultQueue[0:lastResultIdx]...)
	t.resultQueue = t.resultQueue[lastResultIdx:]
	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		return nil, nil
	}
	t.sameKeyRows = t.sameKeyRows[:0]
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	for {
		selectedRow, err := t.nextSelectedRow()
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {
			t.firstRow4Key = t.curIter.End()
			return t.sameKeyRows, errors.Trace(err)
		}
		compareResult := compareChunkRow(t.compareFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			t.sameKeyRows = append(t.sameKeyRows, selectedRow)
		} else {
			t.firstRow4Key = selectedRow
			return t.sameKeyRows, nil
		}
	}
}

func (t *mergeJoinInnerTable) nextSelectedRow() (chunk.Row, error) {
	if t.curRow == t.curIter.End() {
		t.reallocReaderResult()
		err := t.reader.NextChunk(t.ctx, t.curResult)
		// error happens or no more data.
		if err != nil || t.curResult.NumRows() == 0 {
			t.curRow = t.curIter.End()
			return t.curRow, errors.Trace(err)
		}
		t.curRow = t.curIter.Begin()
	}
	result := t.curRow
	t.curResultInUse = true
	t.curRow = t.curIter.Next()
	return result, nil
}

// reallocReaderResult resets "t.curResult" to an empty Chunk to buffer the result of "t.reader".
// It pops a Chunk from "t.resourceQueue" and push it into "t.resultQueue" immediately.
func (t *mergeJoinInnerTable) reallocReaderResult() {
	if !t.curResultInUse {
		// If "t.curResult" is not in use, we can just reuse it.
		t.curResult.Reset()
		return
	}

	// Create a new Chunk and append it to "resourceQueue" if there is no more
	// available chunk in "resourceQueue".
	if len(t.resourceQueue) == 0 {
		t.resourceQueue = append(t.resourceQueue, t.reader.newChunk())
	}

	// NOTE: "t.curResult" is always the last element of "resultQueue".
	t.curResult = t.resourceQueue[0]
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.resourceQueue = t.resourceQueue[1:]
	t.resultQueue = append(t.resultQueue, t.curResult)
	t.curResult.Reset()
	t.curResultInUse = false
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	if err := e.baseExecutor.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.prepared = false
	return nil
}

func compareChunkRow(cmpFuncs []chunk.CompareFunc, lhsRow, rhsRow chunk.Row, lhsKey, rhsKey []*expression.Column) int {
	for i := range lhsKey {
		cmp := cmpFuncs[i](lhsRow, lhsKey[i].Index, rhsRow, rhsKey[i].Index)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// type mergeJoinOuterTable struct {
// 	reader Executor
// 	filter []expression.Expression
// 	keys   []*expression.Column
//
// 	chk      *chunk.Chunk
// 	selected []bool
//
// 	iter *chunk.Iterator4Chunk
// 	row  chunk.Row
// }

func (e *MergeJoinExec) prepare(ctx context.Context, chk *chunk.Chunk) error {
	e.innerTable.ctx = ctx
	err := e.innerTable.init(e.childrenResults[e.outerIdx^1])
	if err != nil {
		return errors.Trace(err)
	}

	err = e.fetchNextInnerRows()
	if err != nil {
		return errors.Trace(err)
	}

	e.outerTable.chk = e.childrenResults[e.outerIdx]
	e.outerTable.selected = make([]bool, 0, e.maxChunkSize)
	e.outerTable.iter = chunk.NewIterator4Chunk(e.outerTable.chk)

	err = e.fetchNextOuterRows(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	e.compareFuncs = e.innerTable.compareFuncs
	e.prepared = true
	return nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *MergeJoinExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !e.prepared {
		if err := e.prepare(ctx, chk); err != nil {
			return errors.Trace(err)
		}
	}

	for chk.NumRows() < e.maxChunkSize {
		hasMore, err := e.joinToChunk(ctx, chk)
		if err != nil || !hasMore {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *MergeJoinExec) joinToChunk(ctx context.Context, chk *chunk.Chunk) (hasMore bool, err error) {
	for {
		if e.outerTable.row == e.outerTable.iter.End() {
			err := e.fetchNextOuterRows(ctx)
			if err != nil || e.outerTable.chk.NumRows() == 0 {
				return false, errors.Trace(err)
			}
		}

		cmpResult := -1
		if e.outerTable.selected[e.outerTable.row.Idx()] && len(e.innerRows) > 0 {
			cmpResult = compareChunkRow(e.compareFuncs, e.outerTable.row, e.innerRows[0], e.outerTable.keys, e.innerTable.joinKeys)
		}

		if cmpResult > 0 {
			if err = e.fetchNextInnerRows(); err != nil {
				return false, errors.Trace(err)
			}
			continue
		}

		if cmpResult < 0 {
			err = e.resultGenerator.emitToChunk(e.outerTable.row, nil, chk)
			if err != nil {
				return false, errors.Trace(err)
			}

			e.outerTable.row = e.outerTable.iter.Next()

			if chk.NumRows() >= e.maxChunkSize {
				return true, nil
			}
			continue
		}

		err = e.resultGenerator.emitToChunk(e.outerTable.row, e.innerIter4Row, chk)
		if err != nil {
			return false, errors.Trace(err)
		}
		if e.innerIter4Row.Current() == e.innerIter4Row.End() {
			e.outerTable.row = e.outerTable.iter.Next()
			e.innerIter4Row.Begin()
			continue
		}

		if chk.NumRows() >= e.maxChunkSize {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (e *MergeJoinExec) fetchNextInnerRows() (err error) {
	e.innerRows, err = e.innerTable.rowsWithSameKey()
	if err != nil {
		return errors.Trace(err)
	}
	e.innerIter4Row = chunk.NewIterator4Slice(e.innerRows)
	e.innerIter4Row.Begin()
	return nil
}

func (e *MergeJoinExec) fetchNextOuterRows(ctx context.Context) (err error) {
	err = e.outerTable.reader.NextChunk(ctx, e.outerTable.chk)
	if err != nil {
		return errors.Trace(err)
	}

	e.outerTable.iter.Begin()
	e.outerTable.selected, err = expression.VectorizedFilter(e.ctx, e.outerTable.filter, e.outerTable.iter, e.outerTable.selected)
	if err != nil {
		return errors.Trace(err)
	}
	e.outerTable.row = e.outerTable.iter.Begin()
	return nil
}
