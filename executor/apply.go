// Copyright 2020 PingCAP, Inc.
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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

var (
	_ Executor = &NestedLoopApplyExec{}
)

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	baseExecutor

	innerWorkers []*applyInnerWorker
	outerExec    Executor
	outerFilter  expression.CNFExprs

	joiner joiner

	innerWorkerChunk []*chunk.Chunk
	outerChunk       []*chunk.Chunk
	outerChunkCursor []int
	outerSelected    [][]bool

	outerRow []*chunk.Row
	hasMatch []bool
	hasNull  []bool

	outer bool

	memTracker *memory.Tracker // track memory usage.

	// applyWorkerWaitGroup is for sync multiple apply workers.
	applyWorkerWaitGroup sync.WaitGroup
	finished             atomic.Value
	prepared             bool
	outerChkResourceCh   chan *outerChkResource
	outerResultChs       []chan *chunk.Chunk
	applyChkResourceCh   []chan *chunk.Chunk
	applyResultCh        chan *applyWorkerResult
	// closeCh add a lock for closing executor.
	closeCh chan struct{}
	// concurrency is the number of partition
	concurrency uint
}

// outerChkResource stores the result of the apply outer side fetch worker,
// `dest` is for Chunk reuse: after apply workers process the outer side chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the outer side fetch worker will put new data into `chk` and write `chk` into dest.
type outerChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// applyWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type applyWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

var innerListLabel fmt.Stringer = stringutil.StringerStr("innerList")

// Close implements the Executor interface.
func (e *NestedLoopApplyExec) Close() error {
	close(e.closeCh)
	e.finished.Store(true)
	if e.prepared {
		if e.applyResultCh != nil {
			for range e.applyResultCh {
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
		for i := range e.applyChkResourceCh {
			close(e.applyChkResourceCh[i])
			for range e.applyChkResourceCh[i] {
			}
		}
		e.outerChkResourceCh = nil
		e.applyChkResourceCh = nil
	}

	for i := uint(0); i < e.concurrency; i++ {
		err := e.innerWorkers[i].Close()
		if err != nil {
			return err
		}
	}
	return e.outerExec.Close()
}

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return err
	}

	e.prepared = false
	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.applyWorkerWaitGroup = sync.WaitGroup{}

	e.outerChunk = make([]*chunk.Chunk, e.concurrency)
	e.outerSelected = make([][]bool, e.concurrency)
	e.outerRow = make([]*chunk.Row, e.concurrency)
	e.hasNull = make([]bool, e.concurrency)
	e.hasMatch = make([]bool, e.concurrency)
	e.outerChunkCursor = make([]int, e.concurrency)
	e.innerWorkerChunk = make([]*chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerChunk[i] = newFirstChunk(e.outerExec)
		e.innerWorkerChunk[i] = newFirstChunk(e.innerWorkers[i])
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	return nil
}

// Next implements the Executor interface.
func (e *NestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.fetchOuterAndMatchInner(ctx)
		e.prepared = true
	}

	req.Reset()
	result, ok := <-e.applyResultCh
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

func (e *NestedLoopApplyExec) fetchOuterAndMatchInner(ctx context.Context) {
	e.initializeForOuter()
	e.applyWorkerWaitGroup.Add(1)
	go e.fetchOuterSideChunks(ctx)

	for i := uint(0); i < e.concurrency; i++ {
		e.applyWorkerWaitGroup.Add(1)
		workerID := i
		go e.runApplyWorker(ctx, workerID)
	}
	go func() {
		e.applyWorkerWaitGroup.Wait()
		close(e.applyResultCh)
	}()
}

func (e *NestedLoopApplyExec) initializeForOuter() {
	// e.outerResultChs is for transmitting the chunks which store the data of
	// outerSideExec, it'll be written by outer side worker goroutine, and read by apply
	// workers.
	e.outerResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.outerChkResourceCh is for transmitting the used outerSideExec chunks from
	// apply workers to outerSideExec worker.
	e.outerChkResourceCh = make(chan *outerChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerChkResourceCh <- &outerChkResource{
			chk:  newFirstChunk(e.outerExec),
			dest: e.outerResultChs[i],
		}
	}

	// e.applyChkResourceCh is for transmitting the reused apply result chunks
	// from the main thread to apply worker goroutines.
	e.applyChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.applyChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.applyChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.applyResultCh is for transmitting the apply result chunks to the main
	// thread.
	e.applyResultCh = make(chan *applyWorkerResult, e.concurrency+1)
}

func (e *NestedLoopApplyExec) fetchOuterSideChunks(ctx context.Context) {
	defer e.applyWorkerWaitGroup.Done()
	for {
		if e.finished.Load().(bool) {
			return
		}

		var outerSideResource *outerChkResource
		var ok bool
		select {
		case <-e.closeCh:
			return
		case outerSideResource, ok = <-e.outerChkResourceCh:
			if !ok {
				return
			}
		}
		outerSideResult := outerSideResource.chk
		err := Next(ctx, e.outerExec, outerSideResult)
		if err != nil {
			e.applyResultCh <- &applyWorkerResult{
				err: err,
			}
			return
		}

		if outerSideResult.NumRows() == 0 {
			return
		}

		outerSideResource.dest <- outerSideResult
	}
}

func (e *NestedLoopApplyExec) runApplyWorker(ctx context.Context, workerID uint) (err error) {
	defer e.applyWorkerWaitGroup.Done()
	var probeSideResult *chunk.Chunk
	ok, applyResult := e.getNewApplyResult(workerID)
	if !ok {
		return
	}

	emptyOuterSideResult := &outerChkResource{
		dest: e.outerResultChs[workerID],
	}
	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case probeSideResult, ok = <-e.outerResultChs[workerID]:
		}
		if !ok {
			break
		}
		applyResult, err = e.apply2Chunk(ctx, workerID, probeSideResult, applyResult)
		if err != nil {
			return err
		}
		probeSideResult.Reset()
		emptyOuterSideResult.chk = probeSideResult
		e.outerChkResourceCh <- emptyOuterSideResult
	}
	// note applyResult.chk may be nil when getNewApplyResult fails in loops
	if applyResult == nil {
		return
	} else if applyResult.err != nil || (applyResult.chk != nil && applyResult.chk.NumRows() > 0) {
		e.applyResultCh <- applyResult
	} else if applyResult.chk != nil && applyResult.chk.NumRows() == 0 {
		e.applyChkResourceCh[workerID] <- applyResult.chk
	}
	return
}

func (e *NestedLoopApplyExec) getNewApplyResult(workerID uint) (bool, *applyWorkerResult) {
	applyResult := &applyWorkerResult{
		src: e.applyChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case applyResult.chk, ok = <-e.applyChkResourceCh[workerID]:
	}
	return ok, applyResult
}

func (e *NestedLoopApplyExec) apply2Chunk(ctx context.Context, workerID uint, probeSideChk *chunk.Chunk, applyResult *applyWorkerResult) (_ *applyWorkerResult, err error) {
	e.outerChunk[workerID] = probeSideChk
	if e.outerChunk[workerID].NumRows() == 0 {
		return nil, nil
	}
	outerIter := chunk.NewIterator4Chunk(e.outerChunk[workerID])
	e.outerSelected[workerID], err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected[workerID])
	if err != nil {
		return nil, err
	}
	e.outerChunkCursor[workerID] = 0

	req := applyResult.chk
	req.Reset()
	for {
		if e.innerWorkers[workerID].innerIter == nil || e.innerWorkers[workerID].innerIter.Current() == e.innerWorkers[workerID].innerIter.End() {
			if e.outerRow[workerID] != nil && !e.hasMatch[workerID] {
				e.joiner.onMissMatch(e.hasNull[workerID], *e.outerRow[workerID], req)
			}
			e.outerRow[workerID], err = e.fetchSelectedOuterRow(workerID, req)
			if e.outerRow[workerID] == nil || err != nil {
				return applyResult, err
			}
			e.hasMatch[workerID] = false
			e.hasNull[workerID] = false

			for _, col := range e.innerWorkers[workerID].outerSchema {
				*col.Data = e.outerRow[workerID].GetDatum(col.Index, col.RetType)
			}
			err := Next(ctx, e.innerWorkers[workerID], e.innerWorkerChunk[workerID])
			if err != nil {
				return applyResult, err
			}
		}

		matched, isNull, err := e.joiner.tryToMatchInners(*e.outerRow[workerID], e.innerWorkers[workerID].innerIter, req)
		e.hasMatch[workerID] = e.hasMatch[workerID] || matched
		e.hasNull[workerID] = e.hasNull[workerID] || isNull

		if err != nil {
			return applyResult, err
		}

		if req.IsFull() {
			e.applyResultCh <- applyResult
			_, applyResult = e.getNewApplyResult(workerID)
			return applyResult, nil
		}
	}
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(workID uint, chk *chunk.Chunk) (*chunk.Row, error) {
	for {
		if e.outerChunkCursor[workID] >= e.outerChunk[workID].NumRows() {
			return nil, nil
		}
		outerRow := e.outerChunk[workID].GetRow(e.outerChunkCursor[workID])
		selected := e.outerSelected[workID][e.outerChunkCursor[workID]]
		e.outerChunkCursor[workID]++
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

type applyInnerWorker struct {
	baseExecutor

	innerFilter   expression.CNFExprs
	innerExec     Executor
	innerRows     []chunk.Row
	cursor        int
	innerList     *chunk.List
	innerChunk    *chunk.Chunk
	innerSelected []bool
	innerIter     chunk.Iterator
	outerSchema   []*expression.CorrelatedColumn
}

var applyInnerWorkerLabel fmt.Stringer = stringutil.StringerStr("applyInnerWork")

// Open implements the Executor Open interface.
func (e *applyInnerWorker) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	if err := e.innerExec.Open(ctx); err != nil {
		return err
	}
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.innerChunk = newFirstChunk(e.innerExec)
	e.innerList = chunk.NewList(retTypes(e.innerExec), e.initCap, e.maxChunkSize)

	return nil
}

// Close implements the Executor Close interface.
func (e *applyInnerWorker) Close() error {
	e.innerRows = nil
	return e.innerExec.Close()
}

// Next implements the Executor Next interface.
func (e *applyInnerWorker) Next(ctx context.Context, req *chunk.Chunk) error {
	e.innerList.Reset()
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := Next(ctx, e.innerExec, e.innerChunk)
		if err != nil {
			return err
		}
		if e.innerChunk.NumRows() == 0 {
			break
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
	e.innerIter = chunk.NewIterator4List(e.innerList)
	e.innerIter.Begin()
	return nil
}
