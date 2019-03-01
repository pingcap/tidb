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

	outerReader		Executor
	outerKeys		[]*expression.Column
	outerFilter		[]expression.Expression

	innerReader		Executor
	innerKeys		[]*expression.Column

}

type mergeJoinOuterTable struct {
	mergeJoinTable

//	reader Executor
	filter []expression.Expression
//	keys   []*expression.Column

//	chk      *chunk.Chunk
//	selected []bool

//	iter     *chunk.Iterator4Chunk
//	row      chunk.Row
//	hasMatch bool

}

type mergeJoinTable struct {
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

	usingResultQueue []*chunk.Chunk 	//被发送给mergeWorker的chunk
	unusedResultQueue    []*chunk.Chunk	//未使用的chunk(还没有发送给mergeWorker的chunk)
	resourceQueue  []*chunk.Chunk

	memTracker *memory.Tracker
}
// mergeJoinInnerTable represents the inner table of merge join.
// All the inner rows which have the same join key are returned when function
// "rowsWithSameKey()" being called.
type mergeJoinInnerTable struct {
	mergeJoinTable

/*	reader   Executor
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

	usingResultQueue []*chunk.Chunk 	//被发送给mergeWorker的chunk
	unusedResultQueue    []*chunk.Chunk	//未使用的chunk(还没有发送给mergeWorker的chunk)
	resourceQueue  []*chunk.Chunk

	memTracker *memory.Tracker
*/}

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

	//	outerTable *mergeJoinOuterTable
	//	innerTable *mergeJoinInnerTable

	compareFuncs   []chunk.CompareFunc

	joinKeys     []*expression.Column
	joiner       joiner
	maxChunkSize int

	closeCh <-chan struct{}

	ctx           sessionctx.Context

	innerRow 	  chunk.Row
	innerRows     []chunk.Row
	innerIter4Row chunk.Iterator
	innerJoinKeys []*expression.Column

	outerRow      chunk.Row
	outerRows	  []chunk.Row
	outerIter4Row chunk.Iterator
	outerSelected    []bool
	outerRowIdx	  int
	outerJoinKeys []*expression.Column

	hasMatch bool
}

const closed int64 = 1
const opened int64 = 0

type outerFetchResult struct {
	selected    []bool
	fetchRow 	[]chunk.Row
	err         error
}

type innerFetchResult struct {
	fetchRow []chunk.Row
	err           error
}

type innerMergeJoinFetchWorker struct {
	innerResultCh chan<- *innerFetchResult

	innerTable    *mergeJoinInnerTable
	isClosed      int64
}

type outerMergeJoinFetchWorker struct {
	outerFetchResultCh chan<- *outerFetchResult

	outerTable *mergeJoinOuterTable

	isClosed int64

	ctx           sessionctx.Context

}

func (ow outerMergeJoinFetchWorker) run(ctx context.Context) {//row with the same key
	defer func() {
		close(ow.outerFetchResultCh)
	}()

	for atomic.LoadInt64(&ow.isClosed) != closed {
		outerRows, err := ow.outerTable.rowsWithSameKey()
		fetchResult := &outerFetchResult{fetchRow:outerRows , err : err}
		fetchResult.selected, err = expression.VectorizedFilterByRow(ow.ctx, ow.outerTable.filter, outerRows, fetchResult.selected)
		if err != nil {
			fetchResult.err = err
		}

		ow.outerFetchResultCh <- fetchResult
		if err != nil || len(outerRows) == 0 {
			return
		}
	}
}

func (iw innerMergeJoinFetchWorker) run(ctx context.Context) {//row with the same key
	defer func() {
		close(iw.innerResultCh)
	}()

	for atomic.LoadInt64(&iw.isClosed) != closed {
		innerRows, err := iw.innerTable.rowsWithSameKey()
		fetchResult := &innerFetchResult{fetchRow: innerRows, err : err}
		iw.innerResultCh <- fetchResult
		if err != nil || len(innerRows) == 0 {
			return
		}
	}
}

func (mw *mergeJoinMergeWorker) run(ctx context.Context) {//只compare 双方的first key，然后就开始merge
	defer func() {
		close(mw.joinResultCh)
	}()

	mw.joinToChunk(ctx)
}

func (mw *mergeJoinMergeWorker) fetchNextInnerRows() (bool,error) {
	innerResult, ok := <- mw.innerFetchResultCh

	if !ok {
		return false, nil
	}

	if innerResult.err != nil {
		return false, errors.Trace(innerResult.err)
	}

	mw.innerRows = innerResult.fetchRow
	mw.innerIter4Row = chunk.NewIterator4Slice(mw.innerRows)
	mw.innerIter4Row.Begin()

	return true, nil
}

func (mw *mergeJoinMergeWorker) fetchNextOuterRows() (bool, error) {
	outerResult, ok := <- mw.outerFetchResultCh

	if !ok {
		return false, nil
	}
	if outerResult.err != nil {
		return false, errors.Trace(outerResult.err)
	}

	mw.outerRows = outerResult.fetchRow
	mw.outerIter4Row = chunk.NewIterator4Slice(mw.outerRows)
	mw.outerRow = mw.outerIter4Row.Begin()
	mw.outerSelected = outerResult.selected
	mw.outerRowIdx = 0

	return ok, nil
}

func (mw *mergeJoinMergeWorker) joinToChunk(ctx context.Context) {
	var err error

	ok, joinResult := mw.getNewJoinResult()
	if !ok {
		return
	}

	for {
		if ok, err = mw.fetchNextOuterRows() ;!ok || err != nil {
			joinResult.err = errors.Trace(err)
			mw.joinResultCh <- joinResult
			return
		}

		if ok,err = mw.fetchNextInnerRows(); err != nil {
			joinResult.err = errors.Trace(err)
			mw.joinResultCh <- joinResult
			return
		}

		for {
			if mw.outerRow == mw.outerIter4Row.End() || mw.outerSelected[mw.outerRowIdx] {
				break
			}

			mw.outerRow = mw.outerIter4Row.Next()
		}

		if mw.outerRow == mw.outerIter4Row.End() {
			continue
		}

		cmpResult := -1
		if len(mw.innerRows) > 0 {
			cmpResult = compareChunkRow(mw.compareFuncs, mw.outerRow , mw.innerRows[0], mw.outerJoinKeys, mw.innerJoinKeys)
		}

		if cmpResult > 0 {
			continue
		}

		if cmpResult < 0 {
			mw.joiner.onMissMatch(mw.outerRow, joinResult.chk)

			mw.outerRow = mw.outerIter4Row.Next()
			mw.hasMatch = false

			if joinResult.chk.NumRows() == mw.maxChunkSize {
				mw.joinResultCh <- joinResult
				ok, joinResult = mw.getNewJoinResult()
				if !ok {
					return
				}
			}
			continue
		}

		for ; mw.outerRow != mw.outerIter4Row.End() ; {
			matched, err := mw.joiner.tryToMatch(mw.outerRow, mw.innerIter4Row, joinResult.chk)
			if err != nil {
				joinResult.err = errors.Trace(err)
				mw.joinResultCh <- joinResult
				return
			}
			mw.hasMatch = mw.hasMatch || matched

			if mw.innerIter4Row.Current() == mw.innerIter4Row.End() {
				if !mw.hasMatch {
					mw.joiner.onMissMatch(mw.outerRow, joinResult.chk)
				}
				mw.outerRow = mw.outerIter4Row.Next()
				mw.outerRowIdx = mw.outerRowIdx+1
				mw.hasMatch = false
				mw.innerIter4Row.Begin()
			}

			if joinResult.chk.NumRows() >= mw.maxChunkSize {
				mw.joinResultCh <- joinResult
				ok, joinResult = mw.getNewJoinResult()
				if !ok {
					return
				}
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

func (t *mergeJoinTable) init(ctx context.Context, chk4Reader *chunk.Chunk) (err error) {
	if t.reader == nil || ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	t.ctx = ctx
	t.curResult = chk4Reader
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curRow = t.curIter.End()
	t.curResultInUse = false
	t.unusedResultQueue = append(t.unusedResultQueue, chk4Reader)
	t.memTracker.Consume(chk4Reader.MemoryUsage())
	t.firstRow4Key, err = t.nextRow()
	t.compareFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		t.compareFuncs = append(t.compareFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	return errors.Trace(err)
}

func (t *mergeJoinTable) rowsWithSameKey() ([]chunk.Row, error) {
	defer func() {
		lastResultIdx := len(t.usingResultQueue) - 1
		if lastResultIdx > 0 {
			t.usingResultQueue = append(t.usingResultQueue , t.usingResultQueue[0:lastResultIdx]...)
			t.unusedResultQueue = t.unusedResultQueue[lastResultIdx:]
		}
	}()
	t.resourceQueue = append(t.resourceQueue, t.usingResultQueue...)
	t.usingResultQueue = t.usingResultQueue[:0]

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

func (t *mergeJoinTable) nextRow() (chunk.Row, error) {
	for {
		if t.curRow == t.curIter.End() {
			t.reallocReaderResult()
			oldMemUsage := t.curResult.MemoryUsage()
			err := t.reader.Next(t.ctx, chunk.NewRecordBatch(t.curResult))
			// error happens or no more data.
			if err != nil || t.curResult.NumRows() == 0 {
				t.curRow = t.curIter.End()
				return t.curRow, errors.Trace(err)
			}
			newMemUsage := t.curResult.MemoryUsage()
			t.memTracker.Consume(newMemUsage - oldMemUsage)
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

func (t *mergeJoinTable) hasNullInJoinKey(row chunk.Row) bool {
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
func (t *mergeJoinTable) reallocReaderResult() {
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
	t.unusedResultQueue = append(t.unusedResultQueue, t.curResult)
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

	err := e.innerTable.init(ctx, e.childrenResults[e.outerIdx^1])
	if err != nil {
		return errors.Trace(err)
	}
	e.innerTable.memTracker = memory.NewTracker("innerTable", -1)
	e.innerTable.memTracker.AttachTo(e.memTracker)

	err = e.outerTable.init(ctx, e.childrenResults[e.outerIdx^1])
	if err != nil {
		return errors.Trace(err)
	}
	e.outerTable.memTracker = memory.NewTracker("outerTable", -1)
	e.outerTable.memTracker.AttachTo(e.memTracker)

	innerResultCh := make(chan *innerFetchResult)
	innerMergeJoinFetchWorker := &innerMergeJoinFetchWorker{
		innerResultCh: innerResultCh,
		isClosed:      opened,
		innerTable:    e.innerTable,
	}

	e.innerMergeJoinFetchWorker = innerMergeJoinFetchWorker
	go innerMergeJoinFetchWorker.run(ctx)

	outerFetchResultCh := make(chan *outerFetchResult)
	outerMergeJoinFetchWorker := &outerMergeJoinFetchWorker{
		outerTable:			e.outerTable,
		outerFetchResultCh: outerFetchResultCh,
		isClosed:           opened,
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

/*
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
*/
