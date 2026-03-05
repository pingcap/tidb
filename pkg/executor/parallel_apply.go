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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/applycache"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

type result struct {
	chk *chunk.Chunk
	err error
}

type outerRow struct {
	row      *chunk.Row
	selected bool // if this row is selected by the outer side
	seq      uint64
}

// orderedResult carries the join output for a single outer row, tagged with
// a sequence number so the reorder worker can emit results in outer-row order.
type orderedResult struct {
	seq  uint64
	chks []*chunk.Chunk // result rows for this outer row (may be empty)
	err  error
}

// ParallelNestedLoopApplyExec is the executor for apply.
type ParallelNestedLoopApplyExec struct {
	exec.BaseExecutor

	// outer-side fields
	outerExec   exec.Executor
	outerFilter expression.CNFExprs
	outerList   *chunk.List
	outer       bool

	// inner-side fields
	// use slices since the inner side is paralleled
	corCols       [][]*expression.CorrelatedColumn
	innerFilter   []expression.CNFExprs
	innerExecs    []exec.Executor
	innerList     []*chunk.List
	innerChunk    []*chunk.Chunk
	innerSelected [][]bool
	innerIter     []chunk.Iterator
	outerRow      []*chunk.Row
	hasMatch      []bool
	hasNull       []bool
	joiners       []join.Joiner

	// fields about concurrency control
	concurrency int
	keepOrder   bool // when true, use reorder buffer to preserve outer-side ordering
	started     uint32
	drained     uint32 // drained == true indicates there is no more data
	freeChkCh   chan *chunk.Chunk
	resultChkCh chan result
	outerRowCh  chan outerRow
	exit        chan struct{}
	workerWg    sync.WaitGroup
	notifyWg    sync.WaitGroup

	// ordered-mode channels (keepOrder == true)
	orderedResultCh chan orderedResult

	// fields about cache
	cache              *applycache.ApplyCache
	useCache           bool
	cacheHitCounter    int64
	cacheAccessCounter int64

	memTracker *memory.Tracker // track memory usage.
}

// Open implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Open(ctx context.Context) error {
	err := exec.Open(ctx, e.outerExec)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	e.outerList = chunk.NewList(exec.RetTypes(e.outerExec), e.InitCap(), e.MaxChunkSize())
	e.outerList.GetMemTracker().SetLabel(memory.LabelForOuterList)
	e.outerList.GetMemTracker().AttachTo(e.memTracker)

	e.innerList = make([]*chunk.List, e.concurrency)
	e.innerChunk = make([]*chunk.Chunk, e.concurrency)
	e.innerSelected = make([][]bool, e.concurrency)
	e.innerIter = make([]chunk.Iterator, e.concurrency)
	e.outerRow = make([]*chunk.Row, e.concurrency)
	e.hasMatch = make([]bool, e.concurrency)
	e.hasNull = make([]bool, e.concurrency)
	for i := range e.concurrency {
		e.innerChunk[i] = exec.TryNewCacheChunk(e.innerExecs[i])
		e.innerList[i] = chunk.NewList(exec.RetTypes(e.innerExecs[i]), e.InitCap(), e.MaxChunkSize())
		e.innerList[i].GetMemTracker().SetLabel(memory.LabelForInnerList)
		e.innerList[i].GetMemTracker().AttachTo(e.memTracker)
	}

	e.freeChkCh = make(chan *chunk.Chunk, e.concurrency)
	e.resultChkCh = make(chan result, e.concurrency+1) // innerWorkers + outerWorker
	e.outerRowCh = make(chan outerRow)
	e.exit = make(chan struct{})

	if e.keepOrder {
		// In ordered mode, freeChkCh is consumed by the reorder worker.
		// Inner workers allocate their own temporary chunks.
		e.orderedResultCh = make(chan orderedResult, e.concurrency*2)
	}

	for range e.concurrency {
		e.freeChkCh <- exec.NewFirstChunk(e)
	}

	if e.useCache {
		if e.cache, err = applycache.NewApplyCache(e.Ctx()); err != nil {
			return err
		}
		e.cache.GetMemTracker().AttachTo(e.memTracker)
	}
	return nil
}

// Next implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if atomic.LoadUint32(&e.drained) == 1 {
		req.Reset()
		return nil
	}

	if atomic.CompareAndSwapUint32(&e.started, 0, 1) {
		e.workerWg.Add(1)
		go e.outerWorker(ctx)
		if e.keepOrder {
			for i := range e.concurrency {
				e.workerWg.Add(1)
				go e.innerWorkerOrdered(ctx, i)
			}
			// Bridge goroutine: when all outer+inner workers finish,
			// close orderedResultCh so the reorder worker can drain and exit.
			go func() {
				e.workerWg.Wait()
				close(e.orderedResultCh)
			}()
			e.notifyWg.Add(1)
			go e.reorderWorker(ctx)
		} else {
			for i := range e.concurrency {
				e.workerWg.Add(1)
				workID := i
				go e.innerWorker(ctx, workID)
			}
			e.notifyWg.Add(1)
			go e.notifyWorker(ctx)
		}
	}
	result := <-e.resultChkCh
	if result.err != nil {
		return result.err
	}
	if result.chk == nil { // no more data
		req.Reset()
		atomic.StoreUint32(&e.drained, 1)
		return nil
	}
	req.SwapColumns(result.chk)
	e.freeChkCh <- result.chk
	return nil
}

// Close implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Close() error {
	e.memTracker = nil
	if atomic.LoadUint32(&e.started) == 1 {
		close(e.exit)
		e.notifyWg.Wait()
		e.started = 0
	}
	// Wait all workers to finish before Close() is called.
	// Otherwise we may got data race.
	err := exec.Close(e.outerExec)

	if e.RuntimeStats() != nil {
		runtimeStats := join.NewJoinRuntimeStats()
		if e.useCache {
			var hitRatio float64
			if e.cacheAccessCounter > 0 {
				hitRatio = float64(e.cacheHitCounter) / float64(e.cacheAccessCounter)
			}
			runtimeStats.SetCacheInfo(true, hitRatio)
		} else {
			runtimeStats.SetCacheInfo(false, 0)
		}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", e.concurrency))
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), runtimeStats)
	}
	return err
}

// notifyWorker waits for all inner/outer-workers finishing and then put an empty
// chunk into the resultCh to notify the upper executor there is no more data.
// Used only in unordered mode.
func (e *ParallelNestedLoopApplyExec) notifyWorker(ctx context.Context) {
	defer e.handleWorkerPanic(ctx, &e.notifyWg)
	e.workerWg.Wait()
	e.putResult(nil, nil)
}

func (e *ParallelNestedLoopApplyExec) outerWorker(ctx context.Context) {
	defer trace.StartRegion(ctx, "ParallelApplyOuterWorker").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)
	var selected []bool
	var err error
	var seq uint64
	for {
		failpoint.Inject("parallelApplyOuterWorkerPanic", nil)
		chk := exec.TryNewCacheChunk(e.outerExec)
		if err := exec.Next(ctx, e.outerExec, chk); err != nil {
			e.putResult(nil, err)
			return
		}
		if chk.NumRows() == 0 {
			close(e.outerRowCh)
			return
		}
		e.outerList.Add(chk)
		outerIter := chunk.NewIterator4Chunk(chk)
		selected, err = expression.VectorizedFilter(e.Ctx().GetExprCtx().GetEvalCtx(), e.Ctx().GetSessionVars().EnableVectorizedExpression, e.outerFilter, outerIter, selected)
		if err != nil {
			e.putResult(nil, err)
			return
		}
		for i := range chk.NumRows() {
			row := chk.GetRow(i)
			or := outerRow{row: &row, selected: selected[i], seq: seq}
			seq++
			select {
			case e.outerRowCh <- or:
			case <-e.exit:
				return
			}
		}
	}
}

// innerWorker is used in unordered mode. Workers compete for outer rows from
// a shared channel and emit result chunks in arbitrary order.
func (e *ParallelNestedLoopApplyExec) innerWorker(ctx context.Context, id int) {
	defer trace.StartRegion(ctx, "ParallelApplyInnerWorker").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)
	for {
		var chk *chunk.Chunk
		select {
		case chk = <-e.freeChkCh:
		case <-e.exit:
			return
		}
		failpoint.Inject("parallelApplyInnerWorkerPanic", nil)
		err := e.fillInnerChunk(ctx, id, chk)
		if err == nil && chk.NumRows() == 0 { // no more data, this goroutine can exit
			return
		}
		if e.putResult(chk, err) {
			return
		}
	}
}

// innerWorkerOrdered is used in keepOrder mode. Each worker processes one
// outer row at a time and tags the result with the row's sequence number.
// Results are sent to orderedResultCh for the reorder worker to sort.
func (e *ParallelNestedLoopApplyExec) innerWorkerOrdered(ctx context.Context, id int) {
	defer trace.StartRegion(ctx, "ParallelApplyInnerWorkerOrdered").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)

	for {
		var or outerRow
		var ok bool
		select {
		case or, ok = <-e.outerRowCh:
			if !ok {
				return // outer channel closed – no more work
			}
		case <-e.exit:
			return
		}

		chks, err := e.processOneOuterRow(ctx, id, or)
		if err != nil {
			select {
			case e.orderedResultCh <- orderedResult{seq: or.seq, err: err}:
			case <-e.exit:
			}
			return
		}
		select {
		case e.orderedResultCh <- orderedResult{seq: or.seq, chks: chks}:
		case <-e.exit:
			return
		}
	}
}

// processOneOuterRow executes the inner side for a single outer row and
// returns the joined result chunks. For semi-joins this is typically 0–1 rows.
func (e *ParallelNestedLoopApplyExec) processOneOuterRow(ctx context.Context, id int, or outerRow) ([]*chunk.Chunk, error) {
	chk := exec.NewFirstChunk(e)

	if !or.selected {
		if e.outer {
			e.joiners[id].OnMissMatch(false, *or.row, chk)
		}
		return []*chunk.Chunk{chk}, nil
	}

	e.outerRow[id] = or.row
	e.hasMatch[id] = false
	e.hasNull[id] = false

	if err := e.fetchAllInners(ctx, id); err != nil {
		return nil, err
	}

	e.innerIter[id] = chunk.NewIterator4List(e.innerList[id])
	e.innerIter[id].Begin()

	var chks []*chunk.Chunk
	for e.innerIter[id].Current() != e.innerIter[id].End() {
		matched, isNull, err := e.joiners[id].TryToMatchInners(*e.outerRow[id], e.innerIter[id], chk)
		e.hasMatch[id] = e.hasMatch[id] || matched
		e.hasNull[id] = e.hasNull[id] || isNull
		if err != nil {
			return nil, err
		}
		if chk.IsFull() {
			chks = append(chks, chk)
			chk = exec.NewFirstChunk(e)
		}
	}

	if !e.hasMatch[id] {
		e.joiners[id].OnMissMatch(e.hasNull[id], *or.row, chk)
	}
	chks = append(chks, chk)
	return chks, nil
}

// reorderWorker collects orderedResults from inner workers and emits them to
// resultChkCh in monotonically increasing sequence order, batching small
// per-row results into full output chunks. Used only in keepOrder mode.
func (e *ParallelNestedLoopApplyExec) reorderWorker(ctx context.Context) {
	defer e.handleWorkerPanic(ctx, &e.notifyWg)

	pending := make(map[uint64]orderedResult)
	nextSeq := uint64(0)

	// Get the first output chunk from the free pool.
	var outputChk *chunk.Chunk
	select {
	case outputChk = <-e.freeChkCh:
	case <-e.exit:
		return
	}

	flushOutput := func() bool {
		if e.putResult(outputChk, nil) {
			return true // exit signalled
		}
		select {
		case outputChk = <-e.freeChkCh:
			// Reset is required because the consumer may reuse the
			// same req chunk across Next() calls (e.g. writeChunks).
			// After SwapColumns the recycled chunk can carry leftover
			// column data; Reset clears it before we append new rows.
			outputChk.Reset()
			return false
		case <-e.exit:
			return true
		}
	}

	// appendRow copies a row into the current output chunk, flushing when full.
	appendRow := func(row chunk.Row) bool {
		outputChk.AppendRow(row)
		if outputChk.IsFull() {
			return flushOutput()
		}
		return false
	}

	// emitResult appends all rows from an orderedResult to the output stream.
	emitResult := func(r orderedResult) bool {
		for _, chk := range r.chks {
			for i := range chk.NumRows() {
				if appendRow(chk.GetRow(i)) {
					return true
				}
			}
		}
		return false
	}

	for {
		select {
		case r, ok := <-e.orderedResultCh:
			if !ok {
				// Channel closed – all workers done. Flush remaining rows.
				if outputChk.NumRows() > 0 {
					e.putResult(outputChk, nil)
				}
				e.putResult(nil, nil) // signal EOF
				return
			}
			if r.err != nil {
				e.putResult(nil, r.err)
				return
			}
			pending[r.seq] = r

			// Drain as many in-order results as possible.
			for {
				pr, exists := pending[nextSeq]
				if !exists {
					break
				}
				delete(pending, nextSeq)
				nextSeq++
				if emitResult(pr) {
					return
				}
			}

			// Flush partial output so the consumer (e.g. Limit) can
			// receive rows as soon as they are ready in order, rather
			// than waiting for a full chunk.  This allows early
			// termination: the Limit stops calling Next() and Close()
			// propagates the exit signal to all workers.
			if outputChk.NumRows() > 0 {
				if flushOutput() {
					return
				}
			}

		case <-e.exit:
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

func (e *ParallelNestedLoopApplyExec) handleWorkerPanic(ctx context.Context, wg *sync.WaitGroup) {
	if r := recover(); r != nil {
		err := util.GetRecoverError(r)
		logutil.Logger(ctx).Error("parallel nested loop join worker panicked", zap.Error(err), zap.Stack("stack"))
		e.resultChkCh <- result{nil, err}
	}
	if wg != nil {
		wg.Done()
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *ParallelNestedLoopApplyExec) fetchAllInners(ctx context.Context, id int) (err error) {
	var key []byte
	for _, col := range e.corCols[id] {
		*col.Data = e.outerRow[id].GetDatum(col.Index, col.RetType)
		if e.useCache {
			key, err = codec.EncodeKey(e.Ctx().GetSessionVars().StmtCtx.TimeZone(), key, *col.Data)
			err = e.Ctx().GetSessionVars().StmtCtx.HandleError(err)
			if err != nil {
				return err
			}
		}
	}
	if e.useCache { // look up the cache
		atomic.AddInt64(&e.cacheAccessCounter, 1)
		failpoint.Inject("parallelApplyGetCachePanic", nil)
		value, err := e.cache.Get(key)
		if err != nil {
			return err
		}
		if value != nil {
			e.innerList[id] = value
			atomic.AddInt64(&e.cacheHitCounter, 1)
			return nil
		}
	}

	err = exec.Open(ctx, e.innerExecs[id])
	defer func() { terror.Log(exec.Close(e.innerExecs[id])) }()
	if err != nil {
		return err
	}

	if e.useCache {
		// create a new one in this case since it may be in the cache
		e.innerList[id] = chunk.NewList(exec.RetTypes(e.innerExecs[id]), e.InitCap(), e.MaxChunkSize())
	} else {
		e.innerList[id].Reset()
	}

	innerIter := chunk.NewIterator4Chunk(e.innerChunk[id])
	for {
		err := exec.Next(ctx, e.innerExecs[id], e.innerChunk[id])
		if err != nil {
			return err
		}
		if e.innerChunk[id].NumRows() == 0 {
			break
		}

		e.innerSelected[id], err = expression.VectorizedFilter(e.Ctx().GetExprCtx().GetEvalCtx(), e.Ctx().GetSessionVars().EnableVectorizedExpression, e.innerFilter[id], innerIter, e.innerSelected[id])
		if err != nil {
			return err
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[id][row.Idx()] {
				e.innerList[id].AppendRow(row)
			}
		}
	}

	if e.useCache { // update the cache
		failpoint.Inject("parallelApplySetCachePanic", nil)
		if _, err := e.cache.Set(key, e.innerList[id]); err != nil {
			return err
		}
	}
	return nil
}

func (e *ParallelNestedLoopApplyExec) fetchNextOuterRow(id int, req *chunk.Chunk) (row *chunk.Row, exit bool) {
	for {
		select {
		case outerRow, ok := <-e.outerRowCh:
			if !ok { // no more data
				return nil, false
			}
			if !outerRow.selected {
				if e.outer {
					e.joiners[id].OnMissMatch(false, *outerRow.row, req)
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
				e.joiners[id].OnMissMatch(e.hasNull[id], *e.outerRow[id], req)
			}
			var exit bool
			e.outerRow[id], exit = e.fetchNextOuterRow(id, req)
			if exit || req.IsFull() || e.outerRow[id] == nil {
				return nil
			}

			e.hasMatch[id] = false
			e.hasNull[id] = false

			err = e.fetchAllInners(ctx, id)
			if err != nil {
				return err
			}
			e.innerIter[id] = chunk.NewIterator4List(e.innerList[id])
			e.innerIter[id].Begin()
		}

		matched, isNull, err := e.joiners[id].TryToMatchInners(*e.outerRow[id], e.innerIter[id], req)
		e.hasMatch[id] = e.hasMatch[id] || matched
		e.hasNull[id] = e.hasNull[id] || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}
