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
	"sync"
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

	outerIdx int

	innerTable *mergeJoinInnerTable
	outerTable *mergeJoinOuterTable

	memTracker *memory.Tracker

	curTask     *mergeTask
	mergeTaskCh <-chan *mergeTask

	closeCh            chan struct{}
	joinChkResourceChs []chan *chunk.Chunk
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

	memTracker *memory.Tracker
}
type mergeJoinOuterTable struct {
	mergeJoinTable
	filter []expression.Expression
}

// mergeJoinInnerTable represents the inner table of merge join.
// All the inner rows which have the same join key are returned when function
// "rowsWithSameKey()" being called.
type mergeJoinInnerTable struct {
	mergeJoinTable
}

type mergejoinWorkerResult struct {
	err error
	chk *chunk.Chunk
	src chan<- *chunk.Chunk
}

type mergeJoinCompareWorker struct {
	ctx          sessionctx.Context
	concurrency  int
	compareFuncs []chunk.CompareFunc

	innerRows          []chunk.Row
	innerIter4Row      chunk.Iterator
	innerJoinKeys      []*expression.Column
	innerFetchResultCh <-chan *innerFetchResult

	outerRowIdx        int
	outerSelected      []bool
	outerRow           chunk.Row
	outerRows          []chunk.Row
	outerIter4Row      chunk.Iterator
	outerJoinKeys      []*expression.Column
	outerFetchResultCh <-chan *outerFetchResult

	taskCh            chan<- *mergeTask
	mergeWorkerTaskCh chan<- *mergeTask
}

type mergeJoinMergeWorker struct {
	joiner            joiner
	maxChunkSize      int
	joinChkResourceCh chan *chunk.Chunk

	closeCh     <-chan struct{}
	mergeTaskCh <-chan *mergeTask
}

type mergeTask struct {
	cmp          int
	buildErr     error
	waitGroup    *sync.WaitGroup
	joinResultCh chan *mergejoinWorkerResult

	innerRows []chunk.Row

	outerRows     []chunk.Row
	outerSelected []bool
	outerFrom     int
	outerEnd      int //not included
}

type outerFetchResult struct {
	err        error
	selected   []bool
	fetchRow   []chunk.Row
	memTracker *memory.Tracker
}

type innerFetchResult struct {
	err        error
	fetchRow   []chunk.Row
	memTracker *memory.Tracker
}

type mergeJoinInnerFetchWorker struct {
	innerTable    *mergeJoinInnerTable
	innerResultCh chan<- *innerFetchResult
}

type mergeJoinOuterFetchWorker struct {
	ctx                sessionctx.Context
	outerTable         *mergeJoinOuterTable
	outerFetchResultCh chan<- *outerFetchResult
}

func (mw *mergeJoinMergeWorker) run(ctx context.Context) {
	ok, joinResult := mw.getNewJoinResult(ctx)
	if !ok {
		return
	}

	var mt *mergeTask
	for {
		select {
		case <-ctx.Done():
			ok = false
		case <-mw.closeCh:
			ok = false
		case mt, ok = <-mw.mergeTaskCh:
		}

		if !ok {
			return
		}

		hasMatch := false

		if mt.cmp < 0 {
			var outerRow chunk.Row
			for idx := mt.outerFrom; idx < mt.outerEnd; idx++ {
				outerRow = mt.outerRows[idx]
				mw.joiner.onMissMatch(outerRow, joinResult.chk)

				if joinResult.chk.NumRows() == mw.maxChunkSize {
					mt.joinResultCh <- joinResult
					ok, joinResult = mw.getNewJoinResult(ctx)
					if !ok {
						return
					}
				}
			}
		} else {
			innerRows := mt.innerRows
			innerIter4Row := chunk.NewIterator4Slice(innerRows)
			innerIter4Row.Begin()

			var outerRow chunk.Row
			for idx := mt.outerFrom; idx < mt.outerEnd; {
				outerRow = mt.outerRows[idx]
				if !mt.outerSelected[idx] {
					mw.joiner.onMissMatch(outerRow, joinResult.chk)
					idx++
				} else {
					matched, err := mw.joiner.tryToMatch(outerRow, innerIter4Row, joinResult.chk)
					if err != nil {
						joinResult.err = errors.Trace(err)
						mt.joinResultCh <- joinResult
						return
					}

					hasMatch = hasMatch || matched

					if innerIter4Row.Current() == innerIter4Row.End() {
						if !hasMatch {
							mw.joiner.onMissMatch(outerRow, joinResult.chk)
						}
						hasMatch = false
						innerIter4Row.Begin()
						idx++
					}
				}

				if joinResult.chk.NumRows() >= mw.maxChunkSize {
					mt.joinResultCh <- joinResult
					ok, joinResult = mw.getNewJoinResult(ctx)
					if !ok {
						return
					}
				}
			}
		}

		if joinResult.chk.NumRows() > 0 {
			mt.joinResultCh <- joinResult
			ok, joinResult = mw.getNewJoinResult(ctx)
			if !ok {
				return
			}
		}

		mt.waitGroup.Done()
	}
}

func (ow mergeJoinOuterFetchWorker) run(ctx context.Context) { //row with the same key
	defer func() {
		ow.outerTable.memTracker.Detach()
		close(ow.outerFetchResultCh)
	}()

	fetchResult := &outerFetchResult{}
	err := ow.outerTable.init(ctx, ow.outerTable.reader.newFirstChunk())
	if err != nil {
		fetchResult.err = err
		ow.outerFetchResultCh <- fetchResult
		return
	}

	for {
		fetchResult.fetchRow, fetchResult.err = ow.outerTable.rowsWithSameKey()
		fetchResult.selected, err = expression.VectorizedFilterByRow(ow.ctx, ow.outerTable.filter, fetchResult.fetchRow, fetchResult.selected)

		if len(fetchResult.fetchRow) > 0 || fetchResult.err != nil {
			ow.outerFetchResultCh <- fetchResult
		}

		if err != nil || len(fetchResult.fetchRow) == 0 {
			return
		}
		fetchResult = &outerFetchResult{}
	}
}

func (iw mergeJoinInnerFetchWorker) run(ctx context.Context) {
	defer func() {
		iw.innerTable.memTracker.Detach()
		close(iw.innerResultCh)
	}()

	fetchResult := &innerFetchResult{}
	err := iw.innerTable.init(ctx, iw.innerTable.reader.newFirstChunk())
	if err != nil {
		fetchResult.err = err
		iw.innerResultCh <- fetchResult
		return
	}

	for {
		fetchResult.fetchRow, fetchResult.err = iw.innerTable.rowsWithSameKey()
		if len(fetchResult.fetchRow) > 0 {
			iw.innerResultCh <- fetchResult
		}
		if err != nil || len(fetchResult.fetchRow) == 0 {
			return
		}
		fetchResult = &innerFetchResult{}
	}
}

func (mw *mergeJoinCompareWorker) run(ctx context.Context) {
	defer func() {
		for range mw.innerFetchResultCh {
		}
		close(mw.taskCh)
		close(mw.mergeWorkerTaskCh)
	}()

	if !mw.fetchNextOuterSameKeyGroup(ctx) {
		return
	}

	if !mw.fetchNextInnerSameKeyGroup(ctx) {
		return
	}

	for {
		for mw.outerRow != mw.outerIter4Row.End() && !mw.outerSelected[mw.outerRowIdx] {
			mw.outerRow = mw.outerIter4Row.Next()
			mw.outerRowIdx = mw.outerRowIdx + 1
		}

		cmpResult := -1
		if mw.outerRow != mw.outerIter4Row.End() {
			if len(mw.innerRows) > 0 {
				cmpResult = compareChunkRow(mw.compareFuncs, mw.outerRow, mw.innerRows[0], mw.outerJoinKeys, mw.innerJoinKeys)
			}
		}

		if cmpResult > 0 {
			if !mw.fetchNextInnerSameKeyGroup(ctx) {
				return
			}
			continue
		}

		joinResultCh := make(chan *mergejoinWorkerResult)
		waitGroup := new(sync.WaitGroup)
		hasLeft := len(mw.outerRows) % mw.concurrency
		outerRowCountPreTask := len(mw.outerRows) / mw.concurrency
		for idx := 0; idx < len(mw.outerRows); {
			mt := &mergeTask{waitGroup: waitGroup}
			mt.cmp = cmpResult
			mt.innerRows = mw.innerRows
			mt.outerRows = mw.outerRows
			mt.outerSelected = mw.outerSelected
			mt.outerFrom = idx
			mt.joinResultCh = joinResultCh
			if len(mw.outerRows) < mw.concurrency {
				mt.outerEnd = idx + 1
				idx = idx + 1
			} else {
				if hasLeft > 0 {
					mt.outerEnd = idx + outerRowCountPreTask + 1
					idx = idx + outerRowCountPreTask + 1
					hasLeft--
				} else {
					mt.outerEnd = idx + outerRowCountPreTask
					idx = idx + outerRowCountPreTask
				}
			}

			waitGroup.Add(1)
			mw.mergeWorkerTaskCh <- mt
			mw.taskCh <- mt
		}

		go func() {
			waitGroup.Wait()
			close(joinResultCh)
		}()

		if !mw.fetchNextOuterSameKeyGroup(ctx) {
			return
		}

		if cmpResult == 0 {
			if !mw.fetchNextInnerSameKeyGroup(ctx) {
				return
			}
		}
	}
}

func (mw *mergeJoinCompareWorker) fetchNextInnerSameKeyGroup(ctx context.Context) bool {
	select {
	case innerResult, ok := <-mw.innerFetchResultCh:

		if !ok {
			mw.innerRows = make([]chunk.Row, 0)
			return true
		}

		if innerResult.err != nil {
			mt := &mergeTask{buildErr: innerResult.err}
			mw.taskCh <- mt
			return false
		}

		mw.innerRows = innerResult.fetchRow
		mw.innerIter4Row = chunk.NewIterator4Slice(mw.innerRows)
		mw.innerIter4Row.Begin()
		return true
	case <-ctx.Done():
		return false
	}
}

func (mw *mergeJoinCompareWorker) fetchNextOuterSameKeyGroup(ctx context.Context) bool {
	select {
	case outerResult, ok := <-mw.outerFetchResultCh:
		if !ok {
			return false
		}
		if outerResult.err != nil {
			mt := &mergeTask{buildErr: outerResult.err}
			mw.taskCh <- mt
			return false
		}

		mw.outerRows = outerResult.fetchRow
		mw.outerIter4Row = chunk.NewIterator4Slice(mw.outerRows)
		mw.outerRow = mw.outerIter4Row.Begin()
		mw.outerSelected = outerResult.selected
		mw.outerRowIdx = 0
		return true
	case <-ctx.Done():
		return false
	}
}

func (mw *mergeJoinMergeWorker) getNewJoinResult(ctx context.Context) (bool, *mergejoinWorkerResult) {
	joinResult := &mergejoinWorkerResult{
		src: mw.joinChkResourceCh,
	}
	ok := true
	select {
	case <-ctx.Done():
		ok = false
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
	t.memTracker.Consume(chk4Reader.MemoryUsage())
	t.firstRow4Key, err = t.nextRow()
	t.compareFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		t.compareFuncs = append(t.compareFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	return errors.Trace(err)
}

func (t *mergeJoinTable) rowsWithSameKey() ([]chunk.Row, error) {
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

	// NOTE: "t.curResult" is always the last element of "resultQueue".
	t.curResult = t.reader.newFirstChunk()
	t.curIter = chunk.NewIterator4Chunk(t.curResult)
	t.curResult.Reset()
	t.curResultInUse = false
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	e.memTracker.Detach()
	e.memTracker = nil

	close(e.closeCh)

	for _, joinChkResourceCh := range e.joinChkResourceChs {
		close(joinChkResourceCh)
		for range joinChkResourceCh {
		}
	}
	return errors.Trace(e.baseExecutor.Close())
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	closeCh := make(chan struct{})
	e.closeCh = closeCh
	taskCh := make(chan *mergeTask, concurrency)
	e.mergeTaskCh = taskCh
	joinChkResourceChs := make([]chan *chunk.Chunk, concurrency)
	for i := 0; i < concurrency; i++ {
		joinChkResourceChs[i] = make(chan *chunk.Chunk, 1)
		joinChkResourceChs[i] <- e.newFirstChunk()
	}
	e.joinChkResourceChs = joinChkResourceChs

	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaMergeJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.innerTable.memTracker = memory.NewTracker("innerTable", -1)
	e.innerTable.memTracker.AttachTo(e.memTracker)
	e.outerTable.memTracker = memory.NewTracker("outerTable", -1)
	e.outerTable.memTracker.AttachTo(e.memTracker)

	innerFetchResultCh := make(chan *innerFetchResult)
	iw := e.newInnerFetchWorker(innerFetchResultCh)
	go iw.run(ctx)

	outerFetchResultCh := make(chan *outerFetchResult)
	ow := e.newOuterFetchWorker(outerFetchResultCh)
	go ow.run(ctx)

	mergeWorkerMergeTaskCh := make(chan *mergeTask, concurrency)
	for i := 0; i < concurrency; i++ {
		mw := e.newMergeWorker(i, mergeWorkerMergeTaskCh, joinChkResourceChs[i])
		go mw.run(ctx)
	}

	cw := e.newCompareWorker(innerFetchResultCh, outerFetchResultCh, mergeWorkerMergeTaskCh, taskCh, concurrency)
	go cw.run(ctx)

	return nil
}

func (e *MergeJoinExec) newOuterFetchWorker(outerFetchResultCh chan<- *outerFetchResult) *mergeJoinOuterFetchWorker {
	return &mergeJoinOuterFetchWorker{
		outerTable:         e.outerTable,
		outerFetchResultCh: outerFetchResultCh,
		ctx:                e.ctx,
	}
}

func (e *MergeJoinExec) newInnerFetchWorker(innerResultCh chan<- *innerFetchResult) *mergeJoinInnerFetchWorker {
	return &mergeJoinInnerFetchWorker{
		innerResultCh: innerResultCh,
		innerTable:    e.innerTable,
	}
}

func (e *MergeJoinExec) newCompareWorker(innerFetchResulCh chan *innerFetchResult, outerFetchResultCh chan *outerFetchResult,
	mergeWorkerMergeTaskCh chan *mergeTask, taskCh chan *mergeTask, concurrency int) *mergeJoinCompareWorker {
	return &mergeJoinCompareWorker{
		innerFetchResultCh: innerFetchResulCh,
		outerFetchResultCh: outerFetchResultCh,
		ctx:                e.ctx,
		mergeWorkerTaskCh:  mergeWorkerMergeTaskCh,
		taskCh:             taskCh,
		concurrency:        concurrency,
		compareFuncs:       e.compareFuncs,
		outerJoinKeys:      e.outerTable.joinKeys,
		innerJoinKeys:      e.innerTable.joinKeys,
	}
}

func (e *MergeJoinExec) newMergeWorker(workerId int, mergeTaskCh chan *mergeTask, joinChkResourceCh chan *chunk.Chunk) *mergeJoinMergeWorker {
	return &mergeJoinMergeWorker{
		closeCh:           e.closeCh,
		mergeTaskCh:       mergeTaskCh,
		joinChkResourceCh: joinChkResourceCh,
		joiner:            e.joiner,
		maxChunkSize:      e.maxChunkSize,
	}
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

	var err error
	for {
		if e.curTask == nil {
			e.curTask = e.getNextTask(ctx)
			if e.curTask == nil { //index merge join all complete
				break
			}

			if e.curTask.buildErr != nil {
				return e.curTask.buildErr
			}
		}

		joinResult, ok := <-e.curTask.joinResultCh
		if !ok { //curTask process complete,we need getNextTask,so set curTask = nil
			e.curTask = nil
			continue
		}

		if joinResult.err != nil {
			err = errors.Trace(joinResult.err)
			break
		}

		req.SwapColumns(joinResult.chk)
		joinResult.src <- joinResult.chk
		break
	}

	return err

}

func (e *MergeJoinExec) getNextTask(ctx context.Context) *mergeTask {
	select {
	case task, ok := <-e.mergeTaskCh:
		if ok {
			return task
		}
	case <-ctx.Done():
		return nil
	}

	return nil
}
