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

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
)

// WindowExec is the executor for window functions.
type WindowExec struct {
	baseExecutor

	groupChecker *vecGroupChecker
	// childResult stores the child chunk
	childResult *chunk.Chunk
	// executed indicates the child executor is drained or something unexpected happened.
	executed bool
	// resultChunks stores the chunks to return
	resultChunks []*chunk.Chunk
	// remainingRowsInChunk indicates how many rows the resultChunks[i] is not prepared.
	remainingRowsInChunk []int

	numWindowFuncs int
	processor      windowProcessor
}

// Close implements the Executor Close interface.
func (e *WindowExec) Close() error {
	return errors.Trace(e.baseExecutor.Close())
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
		chk.SwapColumns(e.resultChunks[0])
		e.resultChunks[0] = nil // GC it. TODO: Reuse it.
		e.resultChunks = e.resultChunks[1:]
		e.remainingRowsInChunk = e.remainingRowsInChunk[1:]
	}
	return nil
}

func (e *WindowExec) preparedChunkAvailable() bool {
	return len(e.resultChunks) > 0 && e.remainingRowsInChunk[0] == 0
}

func (e *WindowExec) consumeOneGroup(ctx context.Context) error {
	var groupRows []chunk.Row
	if e.groupChecker.isExhausted() {
		eof, err := e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if eof {
			e.executed = true
			return e.consumeGroupRows(groupRows)
		}
		_, err = e.groupChecker.splitIntoGroups(e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
	}
	begin, end := e.groupChecker.getNextGroup()
	for i := begin; i < end; i++ {
		groupRows = append(groupRows, e.childResult.GetRow(i))
	}

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

		isFirstGroupSameAsPrev, err := e.groupChecker.splitIntoGroups(e.childResult)
		if err != nil {
			return errors.Trace(err)
		}

		if isFirstGroupSameAsPrev {
			begin, end = e.groupChecker.getNextGroup()
			for i := begin; i < end; i++ {
				groupRows = append(groupRows, e.childResult.GetRow(i))
			}
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
	for i := 0; i < len(e.resultChunks); i++ {
		remained := mathutil.Min(e.remainingRowsInChunk[i], remainingRowsInGroup)
		e.remainingRowsInChunk[i] -= remained
		remainingRowsInGroup -= remained

		// TODO: Combine these three methods.
		// The old implementation needs the processor has these three methods
		// but now it does not have to.
		groupRows, err = e.processor.consumeGroupRows(e.ctx, groupRows)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = e.processor.appendResult2Chunk(e.ctx, groupRows, e.resultChunks[i], remained)
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

func (e *WindowExec) fetchChild(ctx context.Context) (EOF bool, err error) {
	childResult := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], childResult)
	if err != nil {
		return false, errors.Trace(err)
	}
	// No more data.
	numRows := childResult.NumRows()
	if numRows == 0 {
		return true, nil
	}

	resultChk := chunk.New(e.retFieldTypes, 0, numRows)
	err = e.copyChk(childResult, resultChk)
	if err != nil {
		return false, err
	}
	e.resultChunks = append(e.resultChunks, resultChk)
	e.remainingRowsInChunk = append(e.remainingRowsInChunk, numRows)

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
}

func (p *aggWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	for i, windowFunc := range p.windowFuncs {
		// @todo Add memory trace
		_, err := windowFunc.UpdatePartialResult(ctx, rows, p.partialResults[i])
		if err != nil {
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
			err := windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
		}
		remained--
	}
	return rows, nil
}

func (p *aggWindowProcessor) resetPartialResult() {
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
}

type rowFrameWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
	start          *core.FrameBound
	end            *core.FrameBound
	curRowIdx      uint64
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

func (p *rowFrameWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
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
					err = slidingWindowAggFunc.Slide(ctx, func(u uint64) chunk.Row {
						return rows[u]
					}, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
					if err != nil {
						return nil, err
					}
				}
				err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		for i, windowFunc := range p.windowFuncs {
			slidingWindowAggFunc := slidingWindowAggFuncs[i]
			if slidingWindowAggFunc != nil && initializedSlidingWindow {
				err = slidingWindowAggFunc.Slide(ctx, func(u uint64) chunk.Row {
					return rows[u]
				}, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
			} else {
				// For MinMaxSlidingWindowAggFuncs, it needs the absolute value of each start of window, to compare
				// whether elements inside deque are out of current window.
				if minMaxSlidingWindowAggFunc, ok := windowFunc.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
					// Store start inside MaxMinSlidingWindowAggFunc.windowInfo
					minMaxSlidingWindowAggFunc.SetWindowStart(start)
				}
				_, err = windowFunc.UpdatePartialResult(ctx, rows[start:end], p.partialResults[i])
			}
			if err != nil {
				return nil, err
			}
			err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
			if slidingWindowAggFunc == nil {
				windowFunc.ResetPartialResult(p.partialResults[i])
			}
		}
		if !initializedSlidingWindow {
			initializedSlidingWindow = true
		}
	}
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
	return rows, nil
}

func (p *rowFrameWindowProcessor) resetPartialResult() {
	p.curRowIdx = 0
}

type rangeFrameWindowProcessor struct {
	windowFuncs     []aggfuncs.AggFunc
	partialResults  []aggfuncs.PartialResult
	start           *core.FrameBound
	end             *core.FrameBound
	curRowIdx       uint64
	lastStartOffset uint64
	lastEndOffset   uint64
	orderByCols     []*expression.Column
	// expectedCmpResult is used to decide if one value is included in the frame.
	expectedCmpResult int64
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
			res, _, err = p.start.CmpFuncs[i](ctx, p.orderByCols[i], p.start.CalcFuncs[i], rows[p.lastStartOffset], rows[p.curRowIdx])
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
			res, _, err = p.end.CmpFuncs[i](ctx, p.end.CalcFuncs[i], p.orderByCols[i], rows[p.curRowIdx], rows[p.lastEndOffset])
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
					err = slidingWindowAggFunc.Slide(ctx, func(u uint64) chunk.Row {
						return rows[u]
					}, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
					if err != nil {
						return nil, err
					}
				}
				err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		for i, windowFunc := range p.windowFuncs {
			slidingWindowAggFunc := slidingWindowAggFuncs[i]
			if slidingWindowAggFunc != nil && initializedSlidingWindow {
				err = slidingWindowAggFunc.Slide(ctx, func(u uint64) chunk.Row {
					return rows[u]
				}, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
			} else {
				if minMaxSlidingWindowAggFunc, ok := windowFunc.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
					minMaxSlidingWindowAggFunc.SetWindowStart(start)
				}
				_, err = windowFunc.UpdatePartialResult(ctx, rows[start:end], p.partialResults[i])
			}
			if err != nil {
				return nil, err
			}
			err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
			if slidingWindowAggFunc == nil {
				windowFunc.ResetPartialResult(p.partialResults[i])
			}
		}
		if !initializedSlidingWindow {
			initializedSlidingWindow = true
		}
	}
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
	return rows, nil
}

func (p *rangeFrameWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	return rows, nil
}

func (p *rangeFrameWindowProcessor) resetPartialResult() {
	p.curRowIdx = 0
	p.lastStartOffset = 0
	p.lastEndOffset = 0
}
