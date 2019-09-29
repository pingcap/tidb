// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

var (
	_ Executor = &HashJoinExec{}
	_ Executor = &NestedLoopApplyExec{}
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	baseExecutor

	outerExec     Executor
	innerExec     Executor
	innerEstCount float64
	outerFilter   expression.CNFExprs
	outerKeys     []*expression.Column
	innerKeys     []*expression.Column

	// concurrency is the number of partition, build and join workers.
	concurrency   uint
	rowContainer  *hashRowContainer
	innerFinished chan error
	// joinWorkerWaitGroup is for sync multiple join workers.
	joinWorkerWaitGroup sync.WaitGroup
	finished            atomic.Value
	// closeCh add a lock for closing executor.
	closeCh      chan struct{}
	joinType     plannercore.JoinType
	requiredRows int64

	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner

	outerChkResourceCh chan *outerChkResource
	outerResultChs     []chan *chunk.Chunk
	joinChkResourceCh  []chan *chunk.Chunk
	joinResultCh       chan *hashjoinWorkerResult

	memTracker  *memory.Tracker // track memory usage.
	prepared    bool
	isOuterJoin bool
}

// outerChkResource stores the result of the join outer fetch worker,
// `dest` is for Chunk reuse: after join workers process the outer chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the outer fetch worker will put new data into `chk` and write `chk` into dest.
type outerChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
	close(e.closeCh)
	e.finished.Store(true)
	if e.prepared {
		if e.innerFinished != nil {
			for range e.innerFinished {
			}
		}
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.outerChkResourceCh != nil {
			close(e.outerChkResourceCh)
			for range e.outerChkResourceCh {
			}
		}
		for i := range e.outerResultChs {
			for range e.outerResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
		}
		e.outerChkResourceCh = nil
		e.joinChkResourceCh = nil
		terror.Call(e.rowContainer.Close)
	}

	err := e.baseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.joinWorkerWaitGroup = sync.WaitGroup{}
	return nil
}

// fetchOuterChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchOuterChunks(ctx context.Context) {
	hasWaitedForInner := false
	for {
		if e.finished.Load().(bool) {
			return
		}

		var outerResource *outerChkResource
		var ok bool
		select {
		case <-e.closeCh:
			return
		case outerResource, ok = <-e.outerChkResourceCh:
			if !ok {
				return
			}
		}
		outerResult := outerResource.chk
		if e.isOuterJoin {
			required := int(atomic.LoadInt64(&e.requiredRows))
			outerResult.SetRequiredRows(required, e.maxChunkSize)
		}
		err := Next(ctx, e.outerExec, outerResult)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{
				err: err,
			}
			return
		}
		if !hasWaitedForInner {
			if outerResult.NumRows() == 0 {
				e.finished.Store(true)
				return
			}
			jobFinished, innerErr := e.wait4Inner()
			if innerErr != nil {
				e.joinResultCh <- &hashjoinWorkerResult{
					err: innerErr,
				}
				return
			} else if jobFinished {
				return
			}
			hasWaitedForInner = true
		}

		if outerResult.NumRows() == 0 {
			return
		}

		outerResource.dest <- outerResult
	}
}

func (e *HashJoinExec) wait4Inner() (finished bool, err error) {
	select {
	case <-e.closeCh:
		return true, nil
	case err := <-e.innerFinished:
		if err != nil {
			return false, err
		}
	}
	if e.rowContainer.Len() == 0 && (e.joinType == plannercore.InnerJoin || e.joinType == plannercore.SemiJoin) {
		return true, nil
	}
	return false, nil
}

var innerResultLabel fmt.Stringer = stringutil.StringerStr("hashJoin.innerResult")

// fetchInnerRows fetches all rows from inner executor,
// and append them to e.innerResult.
func (e *HashJoinExec) fetchInnerRows(ctx context.Context, chkCh chan<- *chunk.Chunk, doneCh <-chan struct{}) {
	defer close(chkCh)
	var err error
	for {
		if e.finished.Load().(bool) {
			return
		}
		chk := chunk.NewChunkWithCapacity(e.innerExec.base().retFieldTypes, e.ctx.GetSessionVars().MaxChunkSize)
		err = Next(ctx, e.innerExec, chk)
		if err != nil {
			e.innerFinished <- errors.Trace(err)
			return
		}
		if chk.NumRows() == 0 {
			return
		}
		select {
		case <-doneCh:
			return
		case <-e.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

func (e *HashJoinExec) initializeForProbe() {
	// e.outerResultChs is for transmitting the chunks which store the data of
	// outerExec, it'll be written by outer worker goroutine, and read by join
	// workers.
	e.outerResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.outerChkResourceCh is for transmitting the used outerExec chunks from
	// join workers to outerExec worker.
	e.outerChkResourceCh = make(chan *outerChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerChkResourceCh <- &outerChkResource{
			chk:  newFirstChunk(e.outerExec),
			dest: e.outerResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)
}

func (e *HashJoinExec) fetchOuterAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.joinWorkerWaitGroup.Add(1)
	go util.WithRecovery(func() { e.fetchOuterChunks(ctx) }, e.handleOuterFetcherPanic)

	outerKeyColIdx := make([]int, len(e.outerKeys))
	for i := range e.outerKeys {
		outerKeyColIdx[i] = e.outerKeys[i].Index
	}

	// Start e.concurrency join workers to probe hash table and join inner and
	// outer rows.
	for i := uint(0); i < e.concurrency; i++ {
		e.joinWorkerWaitGroup.Add(1)
		workID := i
		go util.WithRecovery(func() { e.runJoinWorker(workID, outerKeyColIdx) }, e.handleJoinWorkerPanic)
	}
	go util.WithRecovery(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (e *HashJoinExec) handleOuterFetcherPanic(r interface{}) {
	for i := range e.outerResultChs {
		close(e.outerResultChs[i])
	}
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) handleJoinWorkerPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.joinWorkerWaitGroup.Wait()
	close(e.joinResultCh)
}

func (e *HashJoinExec) runJoinWorker(workerID uint, outerKeyColIdx []int) {
	var (
		outerResult *chunk.Chunk
		selected    = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}

	// Read and filter outerResult, and join the outerResult with the inner rows.
	emptyOuterResult := &outerChkResource{
		dest: e.outerResultChs[workerID],
	}
	hCtx := &hashContext{
		allTypes:  retTypes(e.outerExec),
		keyColIdx: outerKeyColIdx,
	}
	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case outerResult, ok = <-e.outerResultChs[workerID]:
		}
		if !ok {
			break
		}
		ok, joinResult = e.join2Chunk(workerID, outerResult, hCtx, joinResult, selected)
		if !ok {
			break
		}
		outerResult.Reset()
		emptyOuterResult.chk = outerResult
		e.outerChkResourceCh <- emptyOuterResult
	}
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.joinResultCh <- joinResult
	}
}

func (e *HashJoinExec) joinMatchedOuterRow2Chunk(workerID uint, outerRow chunk.Row, hCtx *hashContext,
	joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	innerRows, err := e.rowContainer.GetMatchedRows(outerRow, hCtx)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(innerRows) == 0 {
		e.joiners[workerID].onMissMatch(false, outerRow, joinResult.chk)
		return true, joinResult
	}
	iter := chunk.NewIterator4Slice(innerRows)
	hasMatch, hasNull := false, false
	for iter.Begin(); iter.Current() != iter.End(); {
		matched, isNull, err := e.joiners[workerID].tryToMatchInners(outerRow, iter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	if !hasMatch {
		e.joiners[workerID].onMissMatch(hasNull, outerRow, joinResult.chk)
	}
	return true, joinResult
}

func (e *HashJoinExec) getNewJoinResult(workerID uint) (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: e.joinChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[workerID]:
	}
	return ok, joinResult
}

func (e *HashJoinExec) join2Chunk(workerID uint, outerChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinWorkerResult,
	selected []bool) (ok bool, _ *hashjoinWorkerResult) {
	var err error
	selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(outerChk), selected)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	for i := range selected {
		if !selected[i] { // process unmatched outer rows
			e.joiners[workerID].onMissMatch(false, outerChk.GetRow(i), joinResult.chk)
		} else { // process matched outer rows
			ok, joinResult = e.joinMatchedOuterRow2Chunk(workerID, outerChk.GetRow(i), hCtx, joinResult)
			if !ok {
				return false, joinResult
			}
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from inner child and build a hash table;
// step 2. fetch data from outer child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.innerFinished = make(chan error, 1)
		go util.WithRecovery(func() { e.fetchInnerAndBuildHashTable(ctx) }, e.handleFetchInnerAndBuildHashTablePanic)
		e.fetchOuterAndProbeHashTable(ctx)
		e.prepared = true
	}
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()

	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		e.finished.Store(true)
		return result.err
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *HashJoinExec) handleFetchInnerAndBuildHashTablePanic(r interface{}) {
	if r != nil {
		e.innerFinished <- errors.Errorf("%v", r)
	}
	close(e.innerFinished)
}

func (e *HashJoinExec) fetchInnerAndBuildHashTable(ctx context.Context) {
	// innerResultCh transfers inner chunk from inner fetch to build hash table.
	innerResultCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	go util.WithRecovery(func() { e.fetchInnerRows(ctx, innerResultCh, doneCh) }, nil)

	// TODO: Parallel build hash table. Currently not support because `rowHashMap` is not thread-safe.
	err := e.buildHashTableForList(innerResultCh)
	if err != nil {
		e.innerFinished <- errors.Trace(err)
		close(doneCh)
	}
	// Wait fetchInnerRows be finished.
	// 1. if buildHashTableForList fails
	// 2. if outerResult.NumRows() == 0, fetchOutChunks will not wait for inner.
	for range innerResultCh {
	}
}

// buildHashTableForList builds hash table from `list`.
func (e *HashJoinExec) buildHashTableForList(innerResultCh <-chan *chunk.Chunk) error {
	innerKeyColIdx := make([]int, len(e.innerKeys))
	for i := range e.innerKeys {
		innerKeyColIdx[i] = e.innerKeys[i].Index
	}
	allTypes := e.innerExec.base().retFieldTypes
	hCtx := &hashContext{
		allTypes:  allTypes,
		keyColIdx: innerKeyColIdx,
	}
	e.rowContainer = newHashRowContainer(e.ctx, int(e.innerEstCount), hCtx)
	e.rowContainer.GetMemTracker().AttachTo(e.memTracker)
	e.rowContainer.GetMemTracker().SetLabel(innerResultLabel)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		actionSpill := e.rowContainer.ActionSpill()
		e.ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}
	for chk := range innerResultCh {
		if e.finished.Load().(bool) {
			return nil
		}
		err := e.rowContainer.PutChunk(chk)
		if err != nil {
			return err
		}
	}
	return nil
}

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	baseExecutor

	innerRows   []chunk.Row
	cursor      int
	innerExec   Executor
	outerExec   Executor
	innerFilter expression.CNFExprs
	outerFilter expression.CNFExprs
	outer       bool

	joiner joiner

	outerSchema []*expression.CorrelatedColumn

	outerChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	innerList        *chunk.List
	innerChunk       *chunk.Chunk
	innerSelected    []bool
	innerIter        chunk.Iterator
	outerRow         *chunk.Row
	hasMatch         bool
	hasNull          bool

	memTracker *memory.Tracker // track memory usage.
}

// Close implements the Executor interface.
func (e *NestedLoopApplyExec) Close() error {
	e.innerRows = nil

	e.memTracker = nil
	return e.outerExec.Close()
}

var innerListLabel fmt.Stringer = stringutil.StringerStr("innerList")

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return err
	}
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.outerChunk = newFirstChunk(e.outerExec)
	e.innerChunk = newFirstChunk(e.innerExec)
	e.innerList = chunk.NewList(retTypes(e.innerExec), e.initCap, e.maxChunkSize)

	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaNestedLoopApply)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.innerList.GetMemTracker().SetLabel(innerListLabel)
	e.innerList.GetMemTracker().AttachTo(e.memTracker)

	return nil
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(ctx context.Context, chk *chunk.Chunk) (*chunk.Row, error) {
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		if e.outerChunkCursor >= e.outerChunk.NumRows() {
			err := Next(ctx, e.outerExec, e.outerChunk)
			if err != nil {
				return nil, err
			}
			if e.outerChunk.NumRows() == 0 {
				return nil, nil
			}
			e.outerSelected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected)
			if err != nil {
				return nil, err
			}
			e.outerChunkCursor = 0
		}
		outerRow := e.outerChunk.GetRow(e.outerChunkCursor)
		selected := e.outerSelected[e.outerChunkCursor]
		e.outerChunkCursor++
		if selected {
			return &outerRow, nil
		} else if e.outer {
			e.joiner.onMissMatch(false, outerRow, chk)
			if chk.IsFull() {
				return nil, nil
			}
		}
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(ctx context.Context) error {
	err := e.innerExec.Open(ctx)
	defer terror.Call(e.innerExec.Close)
	if err != nil {
		return err
	}
	e.innerList.Reset()
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := Next(ctx, e.innerExec, e.innerChunk)
		if err != nil {
			return err
		}
		if e.innerChunk.NumRows() == 0 {
			return nil
		}

		e.innerSelected, err = expression.VectorizedFilter(e.ctx, e.innerFilter, innerIter, e.innerSelected)
		if err != nil {
			return err
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[row.Idx()] {
				e.innerList.AppendRow(row)
			}
		}
	}
}

// Next implements the Executor interface.
func (e *NestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			if e.outerRow != nil && !e.hasMatch {
				e.joiner.onMissMatch(e.hasNull, *e.outerRow, req)
			}
			e.outerRow, err = e.fetchSelectedOuterRow(ctx, req)
			if e.outerRow == nil || err != nil {
				return err
			}
			e.hasMatch = false
			e.hasNull = false

			for _, col := range e.outerSchema {
				*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
			}
			err = e.fetchAllInners(ctx)
			if err != nil {
				return err
			}
			e.innerIter = chunk.NewIterator4List(e.innerList)
			e.innerIter.Begin()
		}

		matched, isNull, err := e.joiner.tryToMatchInners(*e.outerRow, e.innerIter, req)
		e.hasMatch = e.hasMatch || matched
		e.hasNull = e.hasNull || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}
