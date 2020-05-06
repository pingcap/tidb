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

	"github.com/pingcap/parser/terror"
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

	// inner worker
	innerFilter   []expression.CNFExprs
	innerExec     []Executor
	innerRows     [][]chunk.Row
	cursor        []int
	innerList     []*chunk.List
	innerChunk    []*chunk.Chunk
	innerSelected [][]bool
	innerIter     []chunk.Iterator
	outerSchema   [][]*expression.CorrelatedColumn

	// outer worker
	outerExec     Executor
	outerFilter   expression.CNFExprs
	outerSelected []bool

	joiner     joiner
	outerRow   []*chunk.Row
	hasMatch   []bool
	hasNull    []bool
	outer      bool
	memTracker *memory.Tracker // track memory usage.

	// applyWorkerWaitGroup is for sync multiple apply workers.
	applyWorkerWaitGroup sync.WaitGroup
	finished             atomic.Value
	prepared             bool
	applyChkResourceCh   []chan *chunk.Chunk
	applyResultCh        chan *applyWorkerResult
	// closeCh add a lock for closing executor.
	closeCh chan struct{}
	// concurrency is the number of partition
	concurrency uint
	outer2Inner chan outerRow
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

type outerRow struct {
	chk      chunk.Row
	selected bool
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
		for i := range e.applyChkResourceCh {
			close(e.applyChkResourceCh[i])
			for range e.applyChkResourceCh[i] {
			}
		}
		e.applyChkResourceCh = nil
	}
	return e.outerExec.Close()
}

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return err
	}

	e.cursor = make([]int, e.concurrency)
	e.innerRows = make([][]chunk.Row, e.concurrency)
	e.innerIter = make([]chunk.Iterator, e.concurrency)
	e.innerChunk = make([]*chunk.Chunk, e.concurrency)
	e.innerList = make([]*chunk.List, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.cursor[i] = 0
		e.innerRows[i] = e.innerRows[i][:0]
		e.innerChunk[i] = newFirstChunk(e.innerExec[i])
		e.innerList[i] = chunk.NewList(retTypes(e.innerExec[i]), e.initCap, e.maxChunkSize)
	}
	e.outer2Inner = make(chan outerRow, 1024)

	e.prepared = false
	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.applyWorkerWaitGroup = sync.WaitGroup{}

	e.outerRow = make([]*chunk.Row, e.concurrency)
	e.hasNull = make([]bool, e.concurrency)
	e.hasMatch = make([]bool, e.concurrency)

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
		go e.runApplyWorker(ctx, i)
	}
	go func() {
		e.applyWorkerWaitGroup.Wait()
		close(e.applyResultCh)
	}()
}

func (e *NestedLoopApplyExec) initializeForOuter() {
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
		//select {
		//case <-e.closeCh:
		//	return
		//}

		outerSideResult := newFirstChunk(e.outerExec)
		err := Next(ctx, e.outerExec, outerSideResult)
		if err != nil {
			e.applyResultCh <- &applyWorkerResult{
				err: err,
			}
			return
		}

		if outerSideResult.NumRows() == 0 {
			close(e.outer2Inner)
			return
		}

		outerIter := chunk.NewIterator4Chunk(outerSideResult)
		e.outerSelected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected)
		if err != nil {
			e.applyResultCh <- &applyWorkerResult{
				err: err,
			}
			return
		}
		for i := 0; i < outerSideResult.NumRows(); i++ {
			e.outer2Inner <- outerRow{outerSideResult.GetRow(i), e.outerSelected[i]}
		}
	}
}

func (e *NestedLoopApplyExec) runApplyWorker(ctx context.Context, id uint) {
	defer e.applyWorkerWaitGroup.Done()
	var outerRow outerRow
	ok, applyResult := e.getNewApplyResult(id)
	if !ok {
		return
	}

	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case outerRow, ok = <-e.outer2Inner:
		}
		if !ok {
			break
		}

		ok, applyResult = e.innerWorker(ctx, id, applyResult, outerRow.chk, outerRow.selected)
		if !ok {
			break
		}
	}
	// note applyResult.chk may be nil when getNewApplyResult fails in loops
	if applyResult == nil {
		return
	} else if applyResult.err != nil || (applyResult.chk != nil && applyResult.chk.NumRows() > 0) {
		e.applyResultCh <- applyResult
	} else if applyResult.chk != nil && applyResult.chk.NumRows() == 0 {
		e.applyChkResourceCh[id] <- applyResult.chk
	}
	return
}

func (e *NestedLoopApplyExec) getNewApplyResult(id uint) (bool, *applyWorkerResult) {
	applyResult := &applyWorkerResult{
		src: e.applyChkResourceCh[id],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case applyResult.chk, ok = <-e.applyChkResourceCh[id]:
	}
	applyResult.chk.Reset()
	return ok, applyResult
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(ctx context.Context, id uint) error {
	err := e.innerExec[id].Open(ctx)
	defer terror.Call(e.innerExec[id].Close)
	if err != nil {
		return err
	}
	e.innerList[id].Reset()
	innerIter := chunk.NewIterator4Chunk(e.innerChunk[id])
	for {
		err := Next(ctx, e.innerExec[id], e.innerChunk[id])
		if err != nil {
			return err
		}
		if e.innerChunk[id].NumRows() == 0 {
			return nil
		}

		e.innerSelected[id], err = expression.VectorizedFilter(e.ctx, e.innerFilter[id], innerIter, e.innerSelected[id])
		if err != nil {
			return err
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[id][row.Idx()] {
				e.innerList[id].AppendRow(row)
			}
		}
	}
}

func (e *NestedLoopApplyExec) innerWorker(ctx context.Context, id uint, applyResult *applyWorkerResult, row chunk.Row, selected bool) (ok bool, _ *applyWorkerResult) {
	req := applyResult.chk
	if !selected {
		e.joiner.onMissMatch(false, row, req)
		if req.IsFull() {
			e.applyResultCh <- applyResult
			ok, applyResult = e.getNewApplyResult(id)
			req = applyResult.chk
			if !ok {
				return false, applyResult
			}
		}
	}

	e.hasMatch[id] = false
	e.hasNull[id] = false
	for _, col := range e.outerSchema[id] {
		*col.Data = row.GetDatum(col.Index, col.RetType)
	}
	err := e.fetchAllInners(ctx, id)
	if err != nil {
		applyResult.err = err
		return false, applyResult
	}
	e.innerIter[id] = chunk.NewIterator4List(e.innerList[id])
	e.innerIter[id].Begin()

	for e.innerIter[id] != nil && e.innerIter[id].Current() != e.innerIter[id].End() {
		matched, isNull, err := e.joiner.tryToMatchInners(row, e.innerIter[id], req)
		e.hasMatch[id] = e.hasMatch[id] || matched
		e.hasNull[id] = e.hasNull[id] || isNull
		if err != nil {
			applyResult.err = err
			return false, applyResult
		}
		if req.IsFull() {
			e.applyResultCh <- applyResult
			ok, applyResult = e.getNewApplyResult(id)
			req = applyResult.chk
			if !ok {
				return false, applyResult
			}
		}
	}
	if &row != nil && !e.hasMatch[id] {
		e.joiner.onMissMatch(e.hasNull[id], row, req)
	}
	return true, applyResult
}
