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
	"github.com/pingcap/tidb/util/stringutil"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

type result struct {
	chk *chunk.Chunk
	err error
}

type outerRow struct {
	row      *chunk.Row
	selected bool // if this row is selected by the outer side
}

// ParallelNestedLoopApplyExec is the executor for apply.
type ParallelNestedLoopApplyExec struct {
	baseExecutor

	// outer-side fields
	cursor        int
	outerExec     Executor
	outerFilter   expression.CNFExprs
	outerList     *chunk.List
	outerRowMutex sync.Mutex
	outer         bool

	// inner-side fields
	// use slices since the inner side is paralleled
	corCols       [][]*expression.CorrelatedColumn
	innerFilter   []expression.CNFExprs
	innerExecs    []Executor
	innerList     []*chunk.List
	innerChunk    []*chunk.Chunk
	innerSelected [][]bool
	innerIter     []chunk.Iterator
	outerRow      []*chunk.Row
	hasMatch      []bool
	hasNull       []bool

	// fields are used to do concurrency control
	concurrency int
	started     bool
	freeChkCh   chan *chunk.Chunk
	resultChkCh chan result
	outerRowCh  chan outerRow
	numWorkers  int32
	exit        chan struct{}
	wg          sync.WaitGroup

	joiner     joiner          // it's stateless
	memTracker *memory.Tracker // track memory usage.
}

// Open implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.outerList = chunk.NewList(retTypes(e.outerExec), e.initCap, e.maxChunkSize)
	e.outerList.GetMemTracker().SetLabel(stringutil.StringerStr("outerList"))
	e.outerList.GetMemTracker().AttachTo(e.memTracker)

	e.innerList = make([]*chunk.List, e.concurrency)
	e.innerChunk = make([]*chunk.Chunk, e.concurrency)
	e.innerSelected = make([][]bool, e.concurrency)
	e.innerIter = make([]chunk.Iterator, e.concurrency)
	e.outerRow = make([]*chunk.Row, e.concurrency)
	e.hasMatch = make([]bool, e.concurrency)
	e.hasNull = make([]bool, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.innerChunk[i] = newFirstChunk(e.innerExecs[i])
		e.innerList[i] = chunk.NewList(retTypes(e.innerExecs[i]), e.initCap, e.maxChunkSize)
		e.innerList[i].GetMemTracker().SetLabel(innerListLabel)
		e.innerList[i].GetMemTracker().AttachTo(e.memTracker)
	}

	e.freeChkCh = make(chan *chunk.Chunk, e.concurrency)
	e.resultChkCh = make(chan result, e.concurrency+1) // innerWorkers + outerWorker
	e.outerRowCh = make(chan outerRow)
	e.exit = make(chan struct{})
	for i := 0; i < e.concurrency; i++ {
		e.freeChkCh <- newFirstChunk(e)
	}
	return nil
}

// Next implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.started {
		e.wg.Add(1)
		go e.outerWorker(ctx)
		for i := 0; i < e.concurrency; i++ {
			e.wg.Add(1)
			atomic.AddInt32(&e.numWorkers, 1)
			go e.innerWorker(ctx, i)
		}
		e.started = true
	}
	result := <-e.resultChkCh
	if result.err != nil {
		return result.err
	}
	req.SwapColumns(result.chk)
	e.freeChkCh <- result.chk
	return nil
}

// Close implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Close() error {
	e.memTracker = nil
	err := e.outerExec.Close()
	if e.started {
		close(e.exit)
		e.started = false
		e.wg.Wait()
	}
	return err
}

func (e *ParallelNestedLoopApplyExec) outerWorker(ctx context.Context) {
	defer e.handleWorkerPanic(ctx)
	var selected []bool
	var err error
	for {
		chk := newFirstChunk(e.outerExec)
		if err := Next(ctx, e.outerExec, chk); err != nil {
			e.putResult(nil, err)
			return
		}
		if chk.NumRows() == 0 {
			close(e.outerRowCh)
			return
		}
		e.outerList.Add(chk)
		outerIter := chunk.NewIterator4Chunk(chk)
		selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, selected)
		if err != nil {
			e.putResult(nil, err)
			return
		}
		for i := 0; i < chk.NumRows(); i++ {
			row := chk.GetRow(i)
			select {
			case e.outerRowCh <- outerRow{&row, selected[i]}:
			case <-e.exit:
				return
			}
		}
	}
}

func (e *ParallelNestedLoopApplyExec) innerWorker(ctx context.Context, id int) {
	defer e.handleWorkerPanic(ctx)
	for {
		var chk *chunk.Chunk
		select {
		case chk = <-e.freeChkCh:
		case <-e.exit:
			return
		}
		err := e.fillInnerChunk(ctx, id, chk)
		if err == nil && chk.NumRows() == 0 { // no more data, this goroutine can exit
			for {
				numWorkers := atomic.LoadInt32(&e.numWorkers) // decrease the numWorkers counter
				if atomic.CompareAndSwapInt32(&e.numWorkers, numWorkers, numWorkers-1) {
					if numWorkers == 1 { // it's the last one, deliver an empty chunk to notify the upper executor
						if e.putResult(chk, nil) {
							return
						}
					}
					break
				}
			}
			return
		}
		if e.putResult(chk, err) {
			return
		}
	}
}

func (e *ParallelNestedLoopApplyExec) putResult(chk *chunk.Chunk, err error) (exit bool) {
	select {
	case e.resultChkCh <- result{chk, err}:
		return false
	case <-e.exit:
		return true
	}
}

func (e *ParallelNestedLoopApplyExec) handleWorkerPanic(ctx context.Context) {
	if r := recover(); r != nil {
		err := errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("parallel nested loop join worker panicked", zap.Error(err), zap.Stack("stack"))
		e.resultChkCh <- result{nil, err}
	}
	e.wg.Done()
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *ParallelNestedLoopApplyExec) fetchAllInners(ctx context.Context, id int) error {
	err := e.innerExecs[id].Open(ctx)
	defer terror.Call(e.innerExecs[id].Close)
	if err != nil {
		return err
	}
	e.innerList[id].Reset()
	innerIter := chunk.NewIterator4Chunk(e.innerChunk[id])
	for {
		err := Next(ctx, e.innerExecs[id], e.innerChunk[id])
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

func (e *ParallelNestedLoopApplyExec) fetchNextOuterRow(req *chunk.Chunk) (row *chunk.Row, exit bool) {
	for {
		select {
		case outerRow, ok := <-e.outerRowCh:
			if !ok { // no more data
				return nil, false
			}
			if !outerRow.selected {
				if e.outer {
					e.joiner.onMissMatch(false, *outerRow.row, req)
					if req.IsFull() {
						return nil, false
					}
				}
				continue // try the next outer row
			}
			return outerRow.row, false
		case <-e.exit:
			return nil, true
		}
	}
}

func (e *ParallelNestedLoopApplyExec) fillInnerChunk(ctx context.Context, id int, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter[id] == nil || e.innerIter[id].Current() == e.innerIter[id].End() {
			if e.outerRow[id] != nil && !e.hasMatch[id] {
				e.joiner.onMissMatch(e.hasNull[id], *e.outerRow[id], req)
			}
			var exit bool
			e.outerRow[id], exit = e.fetchNextOuterRow(req)
			if exit || req.IsFull() || e.outerRow[id] == nil {
				return nil
			}

			e.hasMatch[id] = false
			e.hasNull[id] = false

			for _, col := range e.corCols[id] {
				*col.Data = e.outerRow[id].GetDatum(col.Index, col.RetType)
			}
			err = e.fetchAllInners(ctx, id)
			if err != nil {
				return err
			}
			e.innerIter[id] = chunk.NewIterator4List(e.innerList[id])
			e.innerIter[id].Begin()
		}

		matched, isNull, err := e.joiner.tryToMatchInners(*e.outerRow[id], e.innerIter[id], req)
		e.hasMatch[id] = e.hasMatch[id] || matched
		e.hasNull[id] = e.hasNull[id] || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}
