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
	"context"
	"github.com/pingcap/tidb/sessionctx"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
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

	stmtCtx      *stmtctx.StatementContext
	compareFuncs []chunk.CompareFunc
	joiner       joiner

	prepared bool
	outerIdx int

	innerTable *mergeJoinInnerTable
	outerTable *mergeJoinOuterTable

	innerRows     []chunk.Row
	innerIter4Row chunk.Iterator

	childrenResults []*chunk.Chunk

	memTracker *memory.Tracker

	joinResultCh chan *mergejoinWorkerResult

	closeCh chan struct{}

	innerMergeJoinFetchWorker *innerMergeJoinFetchWorker
	outerMergeJoinFetchWorker *outerMergeJoinFetchWorker
	mergeJoinMergeWorker      *mergeJoinMergeWorker
}

type mergeJoinOuterTable struct {
	reader Executor
	filter []expression.Expression
	keys   []*expression.Column

	chk      *chunk.Chunk
	selected []bool

	iter     *chunk.Iterator4Chunk
	row      chunk.Row
	hasMatch bool

	mergeJoinMergeWorker *mergeJoinMergeWorker
}

// mergeJoinInnerTable represents the inner table of merge join.
// All the inner rows which have the same join key are returned when function
// "rowsWithSameKey()" being called.
type mergeJoinInnerTable struct {
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

	memTracker *memory.Tracker

	mergeJoinMergeWorker *mergeJoinMergeWorker

	innerRows     []chunk.Row
	innerIter4Row chunk.Iterator
}

type mergejoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type mergeJoinMergeWorker struct {
	innerFetchResultCh <-chan *innerFetchResult
	outerFetchResultCh <-chan *outerFetchResult

	innerFetchResult *innerFetchResult
	outerFetchResult *outerFetchResult

	joinResultCh      chan<- *mergejoinWorkerResult
	joinChkResourceCh chan *chunk.Chunk

	outerTable *mergeJoinOuterTable
	innerTable *mergeJoinInnerTable

	joinKeys     []*expression.Column
	joiner       joiner
	maxChunkSize int

	closeCh <-chan struct{}

	curJoinResult *mergejoinWorkerResult

	ctx           sessionctx.Context
}

const closed int64 = 1
const opened int64 = 0

type outerFetchResult struct {
	selected    []bool
	iter        *chunk.Iterator4Chunk
	resultChunk *chunk.Chunk
	err         error
}

type innerFetchResult struct {
	resultChunk *chunk.Chunk
//	innerRowChunk []chunk.Row
	err           error
}

type innerMergeJoinFetchWorker struct {
	innerResultCh chan<- *innerFetchResult

	reader Executor

	//	innerTable    *mergeJoinInnerTable
	isClosed      int64
}

type outerMergeJoinFetchWorker struct {
	fetchResultCh chan<- *outerFetchResult

	reader Executor

	isClosed int64
}

func (ow outerMergeJoinFetchWorker) run(ctx context.Context) {
	defer func() {
		close(ow.fetchResultCh)
	}()

	for atomic.LoadInt64(&ow.isClosed) != closed {
		fetchResult := &outerFetchResult{resultChunk: ow.reader.newFirstChunk()}
		err := ow.reader.Next(ctx, chunk.NewRecordBatch(fetchResult.resultChunk))
		fetchResult.err = err
		ow.fetchResultCh <- fetchResult
		if err != nil || fetchResult.resultChunk.NumRows() == 0 {
			return
		}
	}
}

func (iw innerMergeJoinFetchWorker) run(ctx context.Context) {
	defer func() {
		close(iw.innerResultCh)
	}()

	for atomic.LoadInt64(&iw.isClosed) != closed {
		fetchResult := &innerFetchResult{resultChunk: iw.reader.newFirstChunk()}
		err := iw.reader.Next(ctx, chunk.NewRecordBatch(fetchResult.resultChunk))
		fetchResult.err = err

		iw.innerResultCh <- fetchResult

		if err != nil || fetchResult.resultChunk.NumRows() == 0 {
			return
		}
	}
}

func (mw *mergeJoinMergeWorker) run(ctx context.Context) {
	defer func() {
		if mw.curJoinResult != nil {
			mw.joinResultCh <- mw.curJoinResult
		}
		close(mw.joinResultCh)
	}()

	mw.innerTable.mergeJoinMergeWorker = mw
	if err := mw.innerTable.init(ctx, mw.innerTable.reader.newFirstChunk()); err != nil {
		return
	}

	if err := mw.innerTable.fetchNextInnerRows() ; err != nil {
		return
	}

	mw.outerTable.mergeJoinMergeWorker = mw
	if err := mw.outerTable.fetchNextOuterRows(ctx) ; err != nil {
		return
	}

	mw.joinToChunk(ctx)
}

func (mw *mergeJoinMergeWorker) joinToChunk(ctx context.Context) {
	var ok bool
	for {
		if mw.curJoinResult == nil {
			ok, mw.curJoinResult = mw.getNewJoinResult()
			if !ok {
				return
			}
		}
		if mw.outerTable.row == mw.outerTable.iter.End() {
			err := mw.outerTable.fetchNextOuterRows(ctx)
			if err != nil || mw.outerTable.chk.NumRows() == 0 {
				return
			}
		}

		cmpResult := -1
		if mw.outerTable.selected[mw.outerTable.row.Idx()] && len(mw.innerTable.innerRows) > 0 {
			cmpResult = compareChunkRow(mw.innerTable.compareFuncs, mw.outerTable.row, mw.innerTable.innerRows[0], mw.outerTable.keys, mw.innerTable.joinKeys)
		}

		if cmpResult > 0 {
			if err := mw.innerTable.fetchNextInnerRows(); err != nil {
				mw.curJoinResult.err = errors.Trace(err)
				mw.joinResultCh <- mw.curJoinResult
				return
			}

			continue
		}

		if cmpResult < 0 {
			mw.joiner.onMissMatch(mw.outerTable.row, mw.curJoinResult.chk)

			mw.outerTable.row = mw.outerTable.iter.Next()
			mw.outerTable.hasMatch = false

			if mw.curJoinResult.chk.NumRows() == mw.maxChunkSize {
				mw.joinResultCh <- mw.curJoinResult
				mw.curJoinResult = nil
				ok, mw.curJoinResult = mw.getNewJoinResult()
				if !ok {
					return
				}
			}
			continue
		}

		matched, err := mw.joiner.tryToMatch(mw.outerTable.row, mw.innerTable.innerIter4Row, mw.curJoinResult.chk)
		if err != nil {
			mw.curJoinResult.err = errors.Trace(err)
			mw.joinResultCh <- mw.curJoinResult
			mw.curJoinResult = nil
			return
		}
		mw.outerTable.hasMatch = mw.outerTable.hasMatch || matched

		if mw.innerTable.innerIter4Row.Current() == mw.innerTable.innerIter4Row.End() {
			if !mw.outerTable.hasMatch {
				mw.joiner.onMissMatch(mw.outerTable.row, mw.curJoinResult.chk)
			}
			mw.outerTable.row = mw.outerTable.iter.Next()
			mw.outerTable.hasMatch = false
			mw.innerTable.innerIter4Row.Begin()
		}

		if mw.curJoinResult.chk.NumRows() >= mw.maxChunkSize {
			mw.joinResultCh <- mw.curJoinResult
			mw.curJoinResult = nil
			ok, mw.curJoinResult = mw.getNewJoinResult()
			if !ok {
				return
			}
		}
	}
}

func (mw *mergeJoinMergeWorker) getNewJoinResult() (bool, *mergejoinWorkerResult) {
	joinResult := &mergejoinWorkerResult{
		src: mw.joinChkResourceCh, //用来给next函数归还chunk的
	}
	ok := true
	select {
	case <-mw.closeCh:
		ok = false
	case joinResult.chk, ok = <-mw.joinChkResourceCh:
	}
	return ok, joinResult
}

func (t *mergeJoinInnerTable) init(ctx context.Context, chk4Reader *chunk.Chunk) (err error) {
	if t.reader == nil || ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	t.ctx = ctx
	t.curResult = chk4Reader
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curRow = t.curIter.End()
	t.curResultInUse = false
	t.resultQueue = append(t.resultQueue, chk4Reader)
	t.memTracker.Consume(chk4Reader.MemoryUsage())
	t.firstRow4Key, err = t.nextRow()
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
	t.sameKeyRows = make([]chunk.Row, 0)
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	for {
		selectedRow, err := t.nextRow()
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

func (t *mergeJoinInnerTable) nextRow() (chunk.Row, error) {
	for {
		if t.curRow == t.curIter.End() {
			innerFetchResult, ok := <- t.mergeJoinMergeWorker.innerFetchResultCh
			if !ok || innerFetchResult.resultChunk.NumRows() == 0 || innerFetchResult.err != nil {
				t.curRow = t.curIter.End()
				return t.curRow, innerFetchResult.err
			}
			t.curResult = innerFetchResult.resultChunk
			t.curIter = chunk.NewIterator4Chunk(t.curResult)

//			oldMemUsage := t.curResult.MemoryUsage()
//			newMemUsage := t.curResult.MemoryUsage()
//			t.memTracker.Consume(newMemUsage - oldMemUsage)
			t.curRow = t.curIter.Begin()
		}

		result := t.curRow
		t.curResultInUse = true
		t.curRow = t.curIter.Next()

		if !t.hasNullInJoinKey(result) {
			return result, nil
		}
	}
}

func (t *mergeJoinInnerTable) hasNullInJoinKey(row chunk.Row) bool {
	for _, col := range t.joinKeys {
		ordinal := col.Index
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
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
		newChunk := t.reader.newFirstChunk()
		t.memTracker.Consume(newChunk.MemoryUsage())
		t.resourceQueue = append(t.resourceQueue, newChunk)
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
	e.memTracker.Detach()
	e.childrenResults = nil
	e.memTracker = nil

	close(e.closeCh)
	for range e.joinResultCh {
	}
	atomic.StoreInt64(&e.innerMergeJoinFetchWorker.isClosed, closed)
	atomic.StoreInt64(&e.outerMergeJoinFetchWorker.isClosed, closed)

	for range e.mergeJoinMergeWorker.outerFetchResultCh {
	}

	for range e.mergeJoinMergeWorker.innerFetchResultCh {
	}
	return errors.Trace(e.baseExecutor.Close())
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaMergeJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.childrenResults = make([]*chunk.Chunk, 0, len(e.children))
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, child.newFirstChunk())
	}

	e.innerTable.memTracker = memory.NewTracker("innerTable", -1)
	e.innerTable.memTracker.AttachTo(e.memTracker)

	innerResultCh := make(chan *innerFetchResult)
	innerMergeJoinFetchWorker := &innerMergeJoinFetchWorker{
		innerResultCh: innerResultCh,
		reader :    e.innerTable.reader,
		isClosed:      opened,
	}
	e.innerMergeJoinFetchWorker = innerMergeJoinFetchWorker
	go innerMergeJoinFetchWorker.run(ctx)

	outerFetchResultCh := make(chan *outerFetchResult)
	outerMergeJoinFetchWorker := &outerMergeJoinFetchWorker{
		fetchResultCh: outerFetchResultCh,
		reader:        e.outerTable.reader,
		isClosed:      opened,
	}
	e.outerMergeJoinFetchWorker = outerMergeJoinFetchWorker
	go outerMergeJoinFetchWorker.run(ctx)

	joinChkResourceCh := make(chan *chunk.Chunk, 1)
	joinChkResourceCh <- e.newFirstChunk()
	joinResultCh := make(chan *mergejoinWorkerResult)
	closeCh := make(chan struct{})
	mergeJoinMergeWorker := &mergeJoinMergeWorker{
		innerFetchResultCh: innerResultCh,
		outerFetchResultCh: outerFetchResultCh,
		outerTable:         e.outerTable,
		innerTable:			e.innerTable,
		joinKeys:           e.innerTable.joinKeys,
		joiner:             e.joiner,
		maxChunkSize:       e.maxChunkSize,
		joinResultCh:       joinResultCh,
		ctx:           		e.ctx,
		closeCh:            closeCh,
		joinChkResourceCh:  joinChkResourceCh,
	}
	e.closeCh = closeCh
	e.joinResultCh = joinResultCh
	e.mergeJoinMergeWorker = mergeJoinMergeWorker

	go mergeJoinMergeWorker.run(ctx)
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

func (e *MergeJoinExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	req.Reset()
	if !e.prepared {
		//todo 进行prepared，应该做懒加载，不过现在先不管啦
	}

	if e.joinResultCh == nil {
		return nil
	}

	result, ok := <-e.joinResultCh //把join结果读到result里面
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// fetchNextInnerRows fetches the next join group, within which all the rows
// have the same join key, from the inner table.
func (innerTable *mergeJoinInnerTable) fetchNextInnerRows() (err error) {
	innerTable.innerRows, err = innerTable.rowsWithSameKey()
	if err != nil {
		return errors.Trace(err)
	}
	innerTable.innerIter4Row = chunk.NewIterator4Slice(innerTable.innerRows)
	innerTable.innerIter4Row.Begin()
	return nil
}

func (outerTable *mergeJoinOuterTable) fetchNextOuterRows(ctx context.Context) (err error) {
	outerFetchResult,ok := <- outerTable.mergeJoinMergeWorker.outerFetchResultCh

	if !ok {
		return errors.Errorf("outer channel closed")
	}

	outerTable.chk = outerFetchResult.resultChunk

	outerTable.iter = chunk.NewIterator4Chunk(outerTable.chk)
	outerTable.iter.Begin()
	outerTable.selected, err = expression.VectorizedFilter(outerTable.mergeJoinMergeWorker.ctx, outerTable.filter, outerTable.iter, outerTable.selected)
	if err != nil {
		return errors.Trace(err)
	}
	outerTable.row = outerTable.iter.Begin()
	return nil
}
