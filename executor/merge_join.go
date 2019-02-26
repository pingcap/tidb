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
	"github.com/opentracing/opentracing-go/log"
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

	joinKeys     []*expression.Column
	compareFuncs []chunk.CompareFunc
	joiner       joiner
	maxChunkSize int

	innerIter4Row chunk.Iterator

	closeCh <-chan struct{}

	curJoinResult *mergejoinWorkerResult
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
	innerRowChunk []chunk.Row
	err           error
}

type innerMergeJoinFetchWorker struct {
	innerResultCh chan<- *innerFetchResult
	innerTable    *mergeJoinInnerTable
	isClosed      int64
}

type outerMergeJoinFetchWorker struct {
	fetchResultCh chan<- *outerFetchResult
	ctx           sessionctx.Context

	filter []expression.Expression
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
		if err != nil {
			fetchResult.err = err
			ow.fetchResultCh <- fetchResult
			return
		}

		if fetchResult.resultChunk.NumRows() == 0 {
			return //outer fetch 结束了
		}

		fetchResult.iter = chunk.NewIterator4Chunk(fetchResult.resultChunk)
		fetchResult.iter.Begin()
		fetchResult.selected, err = expression.VectorizedFilter(ow.ctx, ow.filter, fetchResult.iter, fetchResult.selected)
		if err != nil {
			fetchResult.err = err
			ow.fetchResultCh <- fetchResult
			return
		}

		{
			chk := fetchResult.resultChunk
			iter := chunk.NewIterator4Chunk(chk)
			row := iter.Begin()

			for {
				row = iter.Next()

				if row == iter.End() {
					break
				}
			}
		}
		ow.fetchResultCh <- fetchResult
	}
}

func (iw innerMergeJoinFetchWorker) run(ctx context.Context) {
	defer func() {
		close(iw.innerResultCh)
	}()

	for atomic.LoadInt64(&iw.isClosed) != closed {
		innerRows, err := iw.innerTable.rowsWithSameKey()
		if len(innerRows) == 0 {
			return
		}

		innerFetchResult := &innerFetchResult{
			innerRowChunk: innerRows,
			err:           errors.Trace(err),
		}

		iw.innerResultCh <- innerFetchResult

		if err != nil {
			log.Error(errors.Trace(err))
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
	var ok bool
	var err error
	for {
		if mw.outerFetchResult == nil {
			mw.outerFetchResult, ok = <-mw.outerFetchResultCh
			if !ok {
				return
			}

			mw.outerTable.chk = mw.outerFetchResult.resultChunk
			mw.outerTable.iter = chunk.NewIterator4Chunk(mw.outerTable.chk)
			mw.outerTable.row = mw.outerTable.iter.Begin()
			mw.outerTable.selected = mw.outerFetchResult.selected
		}

		if mw.innerFetchResult == nil {
			mw.innerFetchResult, ok = <-mw.innerFetchResultCh
			if ok {
				mw.innerIter4Row = chunk.NewIterator4Slice(mw.innerFetchResult.innerRowChunk)
				mw.innerIter4Row.Begin()
			}

			if !ok || mw.innerFetchResult.err != nil {
				return
			}

		}

		ok, err = mw.joinToChunk(ctx)
		if err != nil || !ok {
			mw.curJoinResult = nil
			return
		}
	}
}

func (mw *mergeJoinMergeWorker) joinToChunk(ctx context.Context) (bool, error) {
	var ok bool
	for {
		if mw.curJoinResult == nil {
			ok, mw.curJoinResult = mw.getNewJoinResult()
			if !ok {
				return false, nil
			}
		}
		if mw.outerTable.row == mw.outerTable.iter.End() {
			mw.outerFetchResult = nil
			return true, nil
		}

		cmpResult := -1
		if mw.outerTable.selected[mw.outerTable.row.Idx()] && mw.innerFetchResult != nil && len(mw.innerFetchResult.innerRowChunk) > 0 {
			cmpResult = compareChunkRow(mw.compareFuncs, mw.outerTable.row, mw.innerFetchResult.innerRowChunk[0], mw.outerTable.keys, mw.joinKeys)
		}

		if cmpResult > 0 {
			mw.innerFetchResult = nil
			return true, nil
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
					return false, nil
				}
			}
			continue
		}

		matched, err := mw.joiner.tryToMatch(mw.outerTable.row, mw.innerIter4Row, mw.curJoinResult.chk)
		if err != nil {
			mw.curJoinResult.err = errors.Trace(err)
			mw.joinResultCh <- mw.curJoinResult
			mw.curJoinResult = nil
			return false, errors.Trace(err)
		}
		mw.outerTable.hasMatch = mw.outerTable.hasMatch || matched

		if mw.innerIter4Row.Current() == mw.innerIter4Row.End() {
			if !mw.outerTable.hasMatch {
				mw.joiner.onMissMatch(mw.outerTable.row, mw.curJoinResult.chk)
			}
			mw.outerTable.row = mw.outerTable.iter.Next()
			mw.outerTable.hasMatch = false
			mw.innerIter4Row.Begin()
		}

		if mw.curJoinResult.chk.NumRows() >= mw.maxChunkSize {
			mw.joinResultCh <- mw.curJoinResult
			mw.curJoinResult = nil
			ok, mw.curJoinResult = mw.getNewJoinResult()
			if !ok {
				return false, nil
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
			//			t.reallocReaderResult()
			t.curResult = t.reader.newFirstChunk()
			t.curIter = chunk.NewIterator4Chunk(t.curResult)
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

	if err := e.innerTable.init(ctx, e.newFirstChunk()); err != nil {
		return errors.Trace(err)
	}

	innerMergeJoinFetchWorker := &innerMergeJoinFetchWorker{
		innerResultCh: innerResultCh,
		innerTable:    e.innerTable,
		isClosed:      opened,
	}
	e.innerMergeJoinFetchWorker = innerMergeJoinFetchWorker
	go innerMergeJoinFetchWorker.run(ctx)

	outerFetchResultCh := make(chan *outerFetchResult)
	outerMergeJoinFetchWorker := &outerMergeJoinFetchWorker{
		fetchResultCh: outerFetchResultCh,
		ctx:           e.ctx,
		reader:        e.outerTable.reader,
		filter:        e.outerTable.filter,
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
		joinKeys:           e.innerTable.joinKeys,
		compareFuncs:       e.innerTable.compareFuncs,
		joiner:             e.joiner,
		maxChunkSize:       e.maxChunkSize,
		joinResultCh:       joinResultCh,
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
