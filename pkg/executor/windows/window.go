// Copyright 2019 PingCAP, Inc.
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

package windows

import (
	"context"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

const (
	chunkPointerSize = int64(unsafe.Sizeof((*chunk.Chunk)(nil)))
	intSize          = int64(unsafe.Sizeof(int(0)))
)

type windowMemoryTracker struct {
	tracker                      *memory.Tracker
	initialPartialResultMemUsage int64
	partialResultMemUsage        []int64
}

func newWindowMemoryTracker(initialPartialResultMemUsage int64, numWindowFuncs int) *windowMemoryTracker {
	return &windowMemoryTracker{
		initialPartialResultMemUsage: initialPartialResultMemUsage,
		partialResultMemUsage:        make([]int64, numWindowFuncs),
	}
}

func (t *windowMemoryTracker) open(label int, stmtTracker *memory.Tracker) {
	if t.tracker == nil {
		t.tracker = memory.NewTracker(label, -1)
	} else {
		t.tracker.Reset()
	}
	clear(t.partialResultMemUsage)
	t.tracker.AttachTo(stmtTracker)
	t.tracker.Consume(t.initialPartialResultMemUsage)
}

func (t *windowMemoryTracker) consume(bytes int64) {
	if t == nil || t.tracker == nil || bytes == 0 {
		return
	}
	t.tracker.Consume(bytes)
}

func (t *windowMemoryTracker) updatePartialResult(
	idx int,
	windowFunc aggfuncs.AggFunc,
	ctx sessionctx.Context,
	rows []chunk.Row,
	partialResult aggfuncs.PartialResult,
) error {
	memDelta, err := windowFunc.UpdatePartialResult(ctx.GetExprCtx().GetEvalCtx(), rows, partialResult)
	t.partialResultMemUsage[idx] += memDelta
	t.consume(memDelta)
	return err
}

func (t *windowMemoryTracker) resetPartialResult(idx int, windowFunc aggfuncs.AggFunc, partialResult aggfuncs.PartialResult) {
	windowFunc.ResetPartialResult(partialResult)
	t.consume(-t.partialResultMemUsage[idx])
	t.partialResultMemUsage[idx] = 0
}

func (t *windowMemoryTracker) resetAllPartialResults(windowFuncs []aggfuncs.AggFunc, partialResults []aggfuncs.PartialResult) {
	for i, windowFunc := range windowFuncs {
		t.resetPartialResult(i, windowFunc, partialResults[i])
	}
}

func (t *windowMemoryTracker) close() {
	if t == nil || t.tracker == nil {
		return
	}
	t.tracker.ReplaceBytesUsed(0)
}

// WindowExec is the executor for window functions.
type WindowExec struct {
	exec.BaseExecutor

	groupChecker *vecgroupchecker.VecGroupChecker
	// childResult stores the child chunk
	childResult *chunk.Chunk
	// executed indicates the child executor is drained or something unexpected happened.
	executed bool
	// resultChunks stores the chunks to return
	resultChunks []*chunk.Chunk
	// remainingRowsInChunk indicates how many rows the resultChunks[i] is not prepared.
	remainingRowsInChunk  []int
	resultChunksMemUsage  int64
	remainingRowsMemUsage int64

	numWindowFuncs int
	processor      windowProcessor
	memTracker     *windowMemoryTracker
}

// Open implements the Executor Open interface.
func (e *WindowExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	e.executed = false
	e.childResult = nil
	e.resultChunks = nil
	e.remainingRowsInChunk = nil
	e.resultChunksMemUsage = 0
	e.remainingRowsMemUsage = 0
	e.groupChecker.Reset()
	e.memTracker.open(e.ID(), e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	e.processor.resetPartialResult()
	return nil
}

// Close implements the Executor Close interface.
func (e *WindowExec) Close() error {
	e.processor.resetPartialResult()
	e.childResult = nil
	e.resultChunks = nil
	e.remainingRowsInChunk = nil
	e.resultChunksMemUsage = 0
	e.remainingRowsMemUsage = 0
	e.groupChecker.Reset()
	e.memTracker.close()
	return errors.Trace(e.BaseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *WindowExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for !e.executed && !e.preparedChunkAvailable() {
		err := e.consumeOneGroup(ctx)
		if err != nil {
			e.executed = true
			return err
		}
	}
	if len(e.resultChunks) > 0 {
		resultChk := e.resultChunks[0]
		// The output chunk takes ownership of the referenced input columns here.
		// Stop charging them to Window before swapping the column pointers.
		e.memTracker.consume(-resultChk.MemoryUsage())
		chk.SwapColumns(resultChk)
		e.resultChunks[0] = nil // GC it. TODO: Reuse it.
		e.resultChunks = e.resultChunks[1:]
		e.remainingRowsInChunk = e.remainingRowsInChunk[1:]
		if len(e.resultChunks) == 0 {
			e.resultChunks = nil
			e.remainingRowsInChunk = nil
			e.memTracker.consume(-e.resultChunksMemUsage - e.remainingRowsMemUsage)
			e.resultChunksMemUsage = 0
			e.remainingRowsMemUsage = 0
		}
	}
	return nil
}

func (e *WindowExec) preparedChunkAvailable() bool {
	return len(e.resultChunks) > 0 && e.remainingRowsInChunk[0] == 0
}

func (e *WindowExec) consumeOneGroup(ctx context.Context) error {
	var groupRows []chunk.Row
	groupRowsMemUsage := int64(0)
	defer func() {
		e.memTracker.consume(-groupRowsMemUsage)
	}()
	appendGroupRows := func(begin, end int) {
		oldCap := cap(groupRows)
		for i := begin; i < end; i++ {
			groupRows = append(groupRows, e.childResult.GetRow(i))
		}
		if cap(groupRows) != oldCap {
			newMemUsage := int64(cap(groupRows)) * aggfuncs.DefRowSize
			e.memTracker.consume(newMemUsage - groupRowsMemUsage)
			groupRowsMemUsage = newMemUsage
		}
	}
	if e.groupChecker.IsExhausted() {
		eof, err := e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if eof {
			e.executed = true
			return e.consumeGroupRows(groupRows)
		}
		_, err = e.groupChecker.SplitIntoGroups(e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
	}
	begin, end := e.groupChecker.GetNextGroup()
	appendGroupRows(begin, end)

	for meetLastGroup := end == e.childResult.NumRows(); meetLastGroup; {
		meetLastGroup = false
		eof, err := e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if eof {
			e.executed = true
			return e.consumeGroupRows(groupRows)
		}

		isFirstGroupSameAsPrev, err := e.groupChecker.SplitIntoGroups(e.childResult)
		if err != nil {
			return errors.Trace(err)
		}

		if isFirstGroupSameAsPrev {
			begin, end = e.groupChecker.GetNextGroup()
			appendGroupRows(begin, end)
			meetLastGroup = end == e.childResult.NumRows()
		}
	}
	return e.consumeGroupRows(groupRows)
}

func (e *WindowExec) consumeGroupRows(groupRows []chunk.Row) (err error) {
	remainingRowsInGroup := len(groupRows)
	if remainingRowsInGroup == 0 {
		return nil
	}
	for i := range e.resultChunks {
		remained := min(e.remainingRowsInChunk[i], remainingRowsInGroup)
		e.remainingRowsInChunk[i] -= remained
		remainingRowsInGroup -= remained
		resultChk := e.resultChunks[i]
		oldMemUsage := resultChk.MemoryUsage()

		// TODO: Combine these three methods.
		// The old implementation needs the processor has these three methods
		// but now it does not have to.
		groupRows, err = e.processor.consumeGroupRows(e.Ctx(), groupRows)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = e.processor.appendResult2Chunk(e.Ctx(), groupRows, resultChk, remained)
		e.memTracker.consume(resultChk.MemoryUsage() - oldMemUsage)
		if err != nil {
			return errors.Trace(err)
		}
		if remainingRowsInGroup == 0 {
			e.processor.resetPartialResult()
			break
		}
	}
	return nil
}

func (e *WindowExec) fetchChild(ctx context.Context) (eof bool, err error) {
	childResult := exec.TryNewCacheChunk(e.Children(0))
	err = exec.Next(ctx, e.Children(0), childResult)
	if err != nil {
		return false, errors.Trace(err)
	}
	// No more data.
	numRows := childResult.NumRows()
	if numRows == 0 {
		return true, nil
	}

	resultChk := e.AllocPool.Alloc(e.RetFieldTypes(), 0, numRows)
	err = e.copyChk(childResult, resultChk)
	if err != nil {
		return false, err
	}
	oldResultChunksCap := cap(e.resultChunks)
	oldRemainingRowsCap := cap(e.remainingRowsInChunk)
	e.resultChunks = append(e.resultChunks, resultChk)
	e.remainingRowsInChunk = append(e.remainingRowsInChunk, numRows)
	// resultChk references the input columns in childResult. Charge only the
	// retained result chunk so shared column buffers are not counted twice.
	e.memTracker.consume(resultChk.MemoryUsage())
	if cap(e.resultChunks) != oldResultChunksCap {
		newMemUsage := int64(cap(e.resultChunks)) * chunkPointerSize
		e.memTracker.consume(newMemUsage - e.resultChunksMemUsage)
		e.resultChunksMemUsage = newMemUsage
	}
	if cap(e.remainingRowsInChunk) != oldRemainingRowsCap {
		newMemUsage := int64(cap(e.remainingRowsInChunk)) * intSize
		e.memTracker.consume(newMemUsage - e.remainingRowsMemUsage)
		e.remainingRowsMemUsage = newMemUsage
	}

	e.childResult = childResult
	return false, nil
}

func (e *WindowExec) copyChk(src, dst *chunk.Chunk) error {
	columns := e.Schema().Columns[:len(e.Schema().Columns)-e.numWindowFuncs]
	for i, col := range columns {
		if err := dst.MakeRefTo(i, src, col.Index); err != nil {
			return err
		}
	}
	return nil
}

// windowProcessor is the interface for processing different kinds of windows.
type windowProcessor interface {
	// consumeGroupRows updates the result for an window function using the input rows
	// which belong to the same partition.
	consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error)
	// appendResult2Chunk appends the final results to chunk.
	// It is called when there are no more rows in current partition.
	appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error)
	// resetPartialResult resets the partial result to the original state for a specific window function.
	resetPartialResult()
}

type aggWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
	memTracker     *windowMemoryTracker
}

func (p *aggWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	for i, windowFunc := range p.windowFuncs {
		if err := p.memTracker.updatePartialResult(i, windowFunc, ctx, rows, p.partialResults[i]); err != nil {
			return nil, err
		}
	}
	rows = rows[:0]
	return rows, nil
}

func (p *aggWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	for remained > 0 {
		for i, windowFunc := range p.windowFuncs {
			// TODO: We can extend the agg func interface to avoid the `for` loop  here.
			err := windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
		}
		remained--
	}
	return rows, nil
}

func (p *aggWindowProcessor) resetPartialResult() {
	p.memTracker.resetAllPartialResults(p.windowFuncs, p.partialResults)
}

type rowFrameWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
	start          *logicalop.FrameBound
	end            *logicalop.FrameBound
	curRowIdx      uint64
	memTracker     *windowMemoryTracker
}

func (p *rowFrameWindowProcessor) getStartOffset(numRows uint64) uint64 {
	if p.start.UnBounded {
		return 0
	}
	switch p.start.Type {
	case ast.Preceding:
		if p.curRowIdx >= p.start.Num {
			return p.curRowIdx - p.start.Num
		}
		return 0
	case ast.Following:
		offset := p.curRowIdx + p.start.Num
		if offset >= numRows {
			return numRows
		}
		return offset
	case ast.CurrentRow:
		return p.curRowIdx
	}
	// It will never reach here.
	return 0
}

func (p *rowFrameWindowProcessor) getEndOffset(numRows uint64) uint64 {
	if p.end.UnBounded {
		return numRows
	}
	switch p.end.Type {
	case ast.Preceding:
		if p.curRowIdx >= p.end.Num {
			return p.curRowIdx - p.end.Num + 1
		}
		return 0
	case ast.Following:
		offset := p.curRowIdx + p.end.Num
		if offset >= numRows {
			return numRows
		}
		return offset + 1
	case ast.CurrentRow:
		return p.curRowIdx + 1
	}
	// It will never reach here.
	return 0
}

func (*rowFrameWindowProcessor) consumeGroupRows(_ sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	return rows, nil
}

func (p *rowFrameWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	numRows := uint64(len(rows))
	var (
		err                      error
		initializedSlidingWindow bool
		start                    uint64
		end                      uint64
		lastStart                uint64
		lastEnd                  uint64
		shiftStart               uint64
		shiftEnd                 uint64
	)
	slidingWindowAggFuncs := make([]aggfuncs.SlidingWindowAggFunc, len(p.windowFuncs))
	for i, windowFunc := range p.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			slidingWindowAggFuncs[i] = slidingWindowAggFunc
		}
	}
	for ; remained > 0; lastStart, lastEnd = start, end {
		start = p.getStartOffset(numRows)
		end = p.getEndOffset(numRows)
		p.curRowIdx++
		remained--
		shiftStart = start - lastStart
		shiftEnd = end - lastEnd
		if start >= end {
			for i, windowFunc := range p.windowFuncs {
				slidingWindowAggFunc := slidingWindowAggFuncs[i]
				if slidingWindowAggFunc != nil && initializedSlidingWindow {
					err = slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), func(u uint64) chunk.Row {
						return rows[u]
					}, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
					if err != nil {
						return nil, err
					}
				}
				err = windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk)
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		for i, windowFunc := range p.windowFuncs {
			slidingWindowAggFunc := slidingWindowAggFuncs[i]
			if slidingWindowAggFunc != nil && initializedSlidingWindow {
				err = slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), func(u uint64) chunk.Row {
					return rows[u]
				}, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
			} else {
				// For MinMaxSlidingWindowAggFuncs, it needs the absolute value of each start of window, to compare
				// whether elements inside deque are out of current window.
				if minMaxSlidingWindowAggFunc, ok := windowFunc.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
					// Store start inside MaxMinSlidingWindowAggFunc.windowInfo
					minMaxSlidingWindowAggFunc.SetWindowStart(start)
				}
				err = p.memTracker.updatePartialResult(i, windowFunc, ctx, rows[start:end], p.partialResults[i])
			}
			if err != nil {
				return nil, err
			}
			err = windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
			if slidingWindowAggFunc == nil {
				p.memTracker.resetPartialResult(i, windowFunc, p.partialResults[i])
			}
		}
		if !initializedSlidingWindow {
			initializedSlidingWindow = true
		}
	}
	for i, slidingWindowAggFunc := range slidingWindowAggFuncs {
		if slidingWindowAggFunc != nil {
			p.memTracker.resetPartialResult(i, p.windowFuncs[i], p.partialResults[i])
		}
	}
	return rows, nil
}

func (p *rowFrameWindowProcessor) resetPartialResult() {
	p.curRowIdx = 0
	p.memTracker.resetAllPartialResults(p.windowFuncs, p.partialResults)
}

type rangeFrameWindowProcessor struct {
	windowFuncs     []aggfuncs.AggFunc
	partialResults  []aggfuncs.PartialResult
	start           *logicalop.FrameBound
	end             *logicalop.FrameBound
	curRowIdx       uint64
	lastStartOffset uint64
	lastEndOffset   uint64
	orderByCols     []*expression.Column
	// expectedCmpResult is used to decide if one value is included in the frame.
	expectedCmpResult int64
	memTracker        *windowMemoryTracker
}

func (p *rangeFrameWindowProcessor) getStartOffset(ctx sessionctx.Context, rows []chunk.Row) (uint64, error) {
	if p.start.UnBounded {
		return 0, nil
	}
	numRows := uint64(len(rows))
	for ; p.lastStartOffset < numRows; p.lastStartOffset++ {
		var res int64
		var err error
		for i := range p.orderByCols {
			res, _, err = p.start.CmpFuncs[i](ctx.GetExprCtx().GetEvalCtx(), p.start.CompareCols[i], p.start.CalcFuncs[i], rows[p.lastStartOffset], rows[p.curRowIdx])
			if err != nil {
				return 0, err
			}
			if res != 0 {
				break
			}
		}
		// For asc, break when the current value is greater or equal to the calculated result;
		// For desc, break when the current value is less or equal to the calculated result.
		if res != p.expectedCmpResult {
			break
		}
	}
	return p.lastStartOffset, nil
}

func (p *rangeFrameWindowProcessor) getEndOffset(ctx sessionctx.Context, rows []chunk.Row) (uint64, error) {
	numRows := uint64(len(rows))
	if p.end.UnBounded {
		return numRows, nil
	}
	for ; p.lastEndOffset < numRows; p.lastEndOffset++ {
		var res int64
		var err error
		for i := range p.orderByCols {
			res, _, err = p.end.CmpFuncs[i](ctx.GetExprCtx().GetEvalCtx(), p.end.CalcFuncs[i], p.end.CompareCols[i], rows[p.curRowIdx], rows[p.lastEndOffset])
			if err != nil {
				return 0, err
			}
			if res != 0 {
				break
			}
		}
		// For asc, break when the calculated result is greater than the current value.
		// For desc, break when the calculated result is less than the current value.
		if res == p.expectedCmpResult {
			break
		}
	}
	return p.lastEndOffset, nil
}

func (p *rangeFrameWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	var (
		err                      error
		initializedSlidingWindow bool
		start                    uint64
		end                      uint64
		lastStart                uint64
		lastEnd                  uint64
		shiftStart               uint64
		shiftEnd                 uint64
	)
	slidingWindowAggFuncs := make([]aggfuncs.SlidingWindowAggFunc, len(p.windowFuncs))
	for i, windowFunc := range p.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			slidingWindowAggFuncs[i] = slidingWindowAggFunc
		}
	}
	for ; remained > 0; lastStart, lastEnd = start, end {
		start, err = p.getStartOffset(ctx, rows)
		if err != nil {
			return nil, err
		}
		end, err = p.getEndOffset(ctx, rows)
		if err != nil {
			return nil, err
		}
		p.curRowIdx++
		remained--
		shiftStart = start - lastStart
		shiftEnd = end - lastEnd
		if start >= end {
			for i, windowFunc := range p.windowFuncs {
				slidingWindowAggFunc := slidingWindowAggFuncs[i]
				if slidingWindowAggFunc != nil && initializedSlidingWindow {
					err = slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), func(u uint64) chunk.Row {
						return rows[u]
					}, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
					if err != nil {
						return nil, err
					}
				}
				err = windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk)
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		for i, windowFunc := range p.windowFuncs {
			slidingWindowAggFunc := slidingWindowAggFuncs[i]
			if slidingWindowAggFunc != nil && initializedSlidingWindow {
				err = slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), func(u uint64) chunk.Row {
					return rows[u]
				}, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
			} else {
				if minMaxSlidingWindowAggFunc, ok := windowFunc.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
					minMaxSlidingWindowAggFunc.SetWindowStart(start)
				}
				err = p.memTracker.updatePartialResult(i, windowFunc, ctx, rows[start:end], p.partialResults[i])
			}
			if err != nil {
				return nil, err
			}
			err = windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
			if slidingWindowAggFunc == nil {
				p.memTracker.resetPartialResult(i, windowFunc, p.partialResults[i])
			}
		}
		if !initializedSlidingWindow {
			initializedSlidingWindow = true
		}
	}
	for i, slidingWindowAggFunc := range slidingWindowAggFuncs {
		if slidingWindowAggFunc != nil {
			p.memTracker.resetPartialResult(i, p.windowFuncs[i], p.partialResults[i])
		}
	}
	return rows, nil
}

func (*rangeFrameWindowProcessor) consumeGroupRows(_ sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	return rows, nil
}

func (p *rangeFrameWindowProcessor) resetPartialResult() {
	p.curRowIdx = 0
	p.lastStartOffset = 0
	p.lastEndOffset = 0
	p.memTracker.resetAllPartialResults(p.windowFuncs, p.partialResults)
}
