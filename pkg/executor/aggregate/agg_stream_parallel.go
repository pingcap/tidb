// Copyright 2026 PingCAP, Inc.
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

package aggregate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/reorder"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

// streamAggGroupTask represents a complete group dispatched to an aggregate worker.
type streamAggGroupTask struct {
	seq  uint64       // monotonic sequence for reordering
	rows []chunk.Row  // copied rows belonging to this group
	chk  *chunk.Chunk // the chunk owning the copied rows (for memory tracking)
}

// streamAggOutput carries a result chunk from the reorder worker to Next().
type streamAggOutput struct {
	chk *chunk.Chunk
	err error
}

// openParallel initializes channels and state for parallel execution.
func (e *StreamAggExec) openParallel() error {
	e.GroupChecker.Reset()
	e.executed = false
	e.IsChildReturnEmpty = true
	e.started.Store(0)
	e.drained.Store(0)

	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	if e.Ctx().GetSessionVars().TrackAggregateMemoryUsage {
		e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	}

	e.groupCh = make(chan streamAggGroupTask, e.Concurrency*2)
	e.orderedResultCh = make(chan reorder.SeqResult[*chunk.Chunk], e.Concurrency*2)
	e.paceCh = make(chan struct{}, e.Concurrency*4)
	e.resultChkCh = make(chan streamAggOutput, e.Concurrency+1)
	e.freeChkCh = make(chan *chunk.Chunk, e.Concurrency)
	e.exit = make(chan struct{})

	for range e.Concurrency {
		e.freeChkCh <- exec.NewFirstChunk(e)
	}

	e.initParallelStats()
	return nil
}

// nextParallel reads the next output chunk in parallel mode.
// On the first call it launches all goroutines.
func (e *StreamAggExec) nextParallel(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.drained.Load() == 1 {
		return nil
	}

	if e.started.CompareAndSwap(0, 1) {
		workerCtx, cancel := context.WithCancel(ctx)
		e.cancelWorkers = cancel

		e.workerWg.Add(1)
		go e.groupSplitter(workerCtx)

		for i := range e.Concurrency {
			e.workerWg.Add(1)
			go e.aggWorker(workerCtx, i)
		}

		// Bridge: wait for splitter + workers, then close orderedResultCh.
		e.notifyWg.Add(1)
		go func() {
			defer e.handleWorkerPanic(workerCtx, &e.notifyWg)
			e.workerWg.Wait()
			close(e.orderedResultCh)
		}()

		e.notifyWg.Add(1)
		go e.reorderWorker(workerCtx)
	}

	r := <-e.resultChkCh
	if r.err != nil {
		return r.err
	}
	if r.chk == nil {
		e.drained.Store(1)
		return nil
	}
	req.SwapColumns(r.chk)
	r.chk.Reset()
	e.freeChkCh <- r.chk
	return nil
}

// closeParallel shuts down all goroutines and cleans up.
func (e *StreamAggExec) closeParallel() error {
	if e.started.Load() == 1 {
		close(e.exit)
		if e.cancelWorkers != nil {
			e.cancelWorkers()
		}
		e.notifyWg.Wait()
		e.started.Store(0)
		e.drainPendingGroupTasks()
	}

	if e.RuntimeStats() != nil {
		stats := e.stats
		if stats != nil {
			e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), stats)
		}
	}

	e.GroupChecker.Reset()
	return e.BaseExecutor.Close()
}

// drainPendingGroupTasks releases memory for any group tasks still queued
// in groupCh after workers have been stopped. This prevents memory leaks
// when closeParallel fires before all dispatched groups are consumed
// (e.g., LIMIT queries).
func (e *StreamAggExec) drainPendingGroupTasks() {
	for {
		select {
		case task, ok := <-e.groupCh:
			if !ok {
				return
			}
			if task.chk != nil {
				e.memTracker.Consume(-task.chk.MemoryUsage())
			}
		default:
			return
		}
	}
}

// groupSplitter reads sorted input from the child executor, identifies
// group boundaries, copies group rows into a persistent accumulator chunk,
// and dispatches complete groups to aggregate workers via groupCh.
func (e *StreamAggExec) groupSplitter(ctx context.Context) {
	defer e.handleWorkerPanic(ctx, &e.workerWg)

	var seq uint64
	childResult := exec.TryNewCacheChunk(e.Children(0))
	e.memTracker.Consume(childResult.MemoryUsage())

	// accChk accumulates rows for the current group. Rows are copied into
	// it immediately so they survive child chunk reuse across fetches.
	childTypes := exec.RetTypes(e.Children(0))
	accChk := chunk.New(childTypes, 0, e.MaxChunkSize())
	e.memTracker.Consume(accChk.MemoryUsage())

	var workerStart time.Time
	if e.stats != nil {
		workerStart = time.Now()
	}

	defer func() {
		e.memTracker.Consume(-childResult.MemoryUsage() - accChk.MemoryUsage())
		if e.stats != nil {
			atomic.AddInt64(&e.stats.WallTime, int64(time.Since(workerStart)))
		}
	}()

	// appendRows copies rows [begin, end) from childResult into accChk.
	appendRows := func(begin, end int) {
		oldMem := accChk.MemoryUsage()
		for i := begin; i < end; i++ {
			accChk.AppendRow(childResult.GetRow(i))
		}
		e.memTracker.Consume(accChk.MemoryUsage() - oldMem)
	}

	// dispatchAcc sends the accumulated group to a worker and resets accChk.
	dispatchAcc := func() bool {
		if accChk.NumRows() == 0 {
			return false
		}
		rows := make([]chunk.Row, accChk.NumRows())
		for i := range rows {
			rows[i] = accChk.GetRow(i)
		}
		select {
		case e.paceCh <- struct{}{}:
		case <-e.exit:
			return true
		}
		select {
		case e.groupCh <- streamAggGroupTask{seq: seq, rows: rows, chk: accChk}:
			seq++
		case <-e.exit:
			return true
		}
		// Allocate a fresh accumulator; the old one is owned by the worker.
		accChk = chunk.New(childTypes, 0, e.MaxChunkSize())
		e.memTracker.Consume(accChk.MemoryUsage())
		return false
	}

	sendErr := func(err error) {
		select {
		case e.resultChkCh <- streamAggOutput{err: err}:
		case <-e.exit:
		}
	}

	for {
		mSize := childResult.MemoryUsage()
		if err := exec.Next(ctx, e.Children(0), childResult); err != nil {
			select {
			case <-e.exit:
				return
			default:
			}
			sendErr(err)
			return
		}
		e.memTracker.Consume(childResult.MemoryUsage() - mSize)

		if childResult.NumRows() == 0 {
			// Flush remaining buffered group.
			if dispatchAcc() {
				return
			}
			close(e.groupCh)
			return
		}
		e.IsChildReturnEmpty = false

		isFirstGroupSameAsPrev, err := e.GroupChecker.SplitIntoGroups(childResult)
		if err != nil {
			sendErr(err)
			return
		}

		if isFirstGroupSameAsPrev {
			// First group in this chunk continues the previous group.
			begin, end := e.GroupChecker.GetNextGroup()
			appendRows(begin, end)
		} else {
			// Previous group is complete — dispatch it.
			if dispatchAcc() {
				return
			}
			begin, end := e.GroupChecker.GetNextGroup()
			appendRows(begin, end)
		}

		// Dispatch any fully contained groups within this chunk.
		for !e.GroupChecker.IsExhausted() {
			if dispatchAcc() {
				return
			}
			begin, end := e.GroupChecker.GetNextGroup()
			appendRows(begin, end)
		}
		// accChk now holds the last (possibly incomplete) group in this chunk.
	}
}

// aggWorker processes groups from groupCh and sends aggregated results
// to orderedResultCh.
func (e *StreamAggExec) aggWorker(ctx context.Context, id int) {
	defer e.handleWorkerPanic(ctx, &e.workerWg)

	var stats *AggWorkerStat
	if e.stats != nil && id < len(e.stats.WorkerStats) {
		stats = e.stats.WorkerStats[id]
	}

	exprCtx := e.Ctx().GetExprCtx()
	for {
		var task streamAggGroupTask
		var ok bool

		waitStart := time.Now()
		select {
		case task, ok = <-e.groupCh:
			if !ok {
				return
			}
		case <-e.exit:
			return
		}
		if stats != nil {
			atomic.AddInt64(&stats.WaitTime, int64(time.Since(waitStart)))
		}

		execStart := time.Now()
		// Allocate fresh partial results for this group.
		partialResults := make([]aggfuncs.PartialResult, len(e.AggFuncs))
		for i, af := range e.AggFuncs {
			partialResults[i], _ = af.AllocPartialResult()
		}

		// Accumulate all rows.
		for i, af := range e.AggFuncs {
			if _, err := af.UpdatePartialResult(exprCtx.GetEvalCtx(), task.rows, partialResults[i]); err != nil {
				e.memTracker.Consume(-task.chk.MemoryUsage())
				select {
				case e.orderedResultCh <- reorder.SeqResult[*chunk.Chunk]{Seq: task.seq, Err: err}:
				case <-e.exit:
				}
				return
			}
		}

		// Produce the single output row.
		resultChk := chunk.New(exec.RetTypes(e), 1, 1)
		for i, af := range e.AggFuncs {
			if err := af.AppendFinalResult2Chunk(exprCtx.GetEvalCtx(), partialResults[i], resultChk); err != nil {
				e.memTracker.Consume(-task.chk.MemoryUsage())
				select {
				case e.orderedResultCh <- reorder.SeqResult[*chunk.Chunk]{Seq: task.seq, Err: err}:
				case <-e.exit:
				}
				return
			}
		}
		if len(e.AggFuncs) == 0 {
			resultChk.SetNumVirtualRows(resultChk.NumRows() + 1)
		}

		// Release the input chunk memory.
		e.memTracker.Consume(-task.chk.MemoryUsage())

		if stats != nil {
			atomic.AddInt64(&stats.ExecTime, int64(time.Since(execStart)))
			atomic.AddInt64(&stats.TaskNum, 1)
		}

		select {
		case e.orderedResultCh <- reorder.SeqResult[*chunk.Chunk]{Seq: task.seq, Val: resultChk}:
		case <-e.exit:
			return
		}
	}
}

// reorderWorker collects results from aggregate workers and emits them to
// resultChkCh in monotonically increasing sequence order using the shared
// reorder.Run infrastructure.
func (e *StreamAggExec) reorderWorker(ctx context.Context) {
	defer e.handleWorkerPanic(ctx, &e.notifyWg)

	// emitRows extracts the single row from each group result chunk and
	// feeds it to the output stream via appendRow.
	emitRows := func(appendRow reorder.AppendRow, resultChk *chunk.Chunk) bool {
		if resultChk != nil && resultChk.NumRows() > 0 {
			return appendRow(resultChk.GetRow(0))
		}
		return false
	}

	// sendResult delivers output chunks (or errors/EOF) to Next().
	sendResult := func(chk *chunk.Chunk, err error) bool {
		select {
		case e.resultChkCh <- streamAggOutput{chk: chk, err: err}:
			return false
		case <-e.exit:
			return true
		}
	}

	reorder.Run(e.orderedResultCh, emitRows, sendResult, e.freeChkCh, e.paceCh, e.exit)
}

// handleWorkerPanic recovers from panics in worker goroutines.
// It calls wg.Done() before attempting error delivery so that
// closeParallel (which waits on the waitgroup) is never blocked
// by a full resultChkCh.
func (e *StreamAggExec) handleWorkerPanic(ctx context.Context, wg *sync.WaitGroup) {
	r := recover()
	if wg != nil {
		wg.Done()
	}
	if r != nil {
		err := util.GetRecoverError(r)
		logutil.Logger(ctx).Error("parallel stream agg worker panicked", zap.Error(err), zap.Stack("stack"))
		select {
		case e.resultChkCh <- streamAggOutput{err: err}:
		default:
			logutil.Logger(ctx).Warn("parallel stream agg: recovered error dropped, resultChkCh full", zap.Error(err))
		}
	}
}

// initParallelStats sets up runtime stats tracking for parallel execution.
// Must be called from openParallel (before goroutines start) so that
// e.stats is a stable pointer readable by all workers without races.
func (e *StreamAggExec) initParallelStats() {
	if e.RuntimeStats() == nil {
		e.stats = nil
		return
	}
	stats := &StreamAggRuntimeStats{
		Concurrency: e.Concurrency,
		WorkerStats: make([]*AggWorkerStat, e.Concurrency),
	}
	for i := range stats.WorkerStats {
		stats.WorkerStats[i] = &AggWorkerStat{}
	}
	e.stats = stats
}
