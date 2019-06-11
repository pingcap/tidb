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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"time"

	"github.com/cznic/mathutil"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// WindowExec is the executor for window functions.
type WindowExec struct {
	baseExecutor

	groupChecker         *groupChecker
	inputIter            *chunk.Iterator4Chunk
	inputRow             chunk.Row
	groupRows            []chunk.Row
	childResults         []*chunk.Chunk
	executed             bool
	meetNewGroup         bool
	remainingRowsInGroup int
	remainingRowsInChunk int
	numWindowFuncs       int
	processor            windowProcessor
}

// Close implements the Executor Close interface.
func (e *WindowExec) Close() error {
	e.childResults = nil
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *WindowExec) Next(ctx context.Context, chk *chunk.RecordBatch) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("windowExec.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.Reset()
	if e.meetNewGroup && e.remainingRowsInGroup > 0 {
		err := e.appendResult2Chunk(chk.Chunk)
		if err != nil {
			return err
		}
	}
	for !e.executed && (chk.NumRows() == 0 || e.remainingRowsInChunk > 0) {
		err := e.consumeOneGroup(ctx, chk.Chunk)
		if err != nil {
			e.executed = true
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *WindowExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) error {
	var err error
	if err = e.fetchChildIfNecessary(ctx, chk); err != nil {
		return errors.Trace(err)
	}
	for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
		e.meetNewGroup, err = e.groupChecker.meetNewGroup(e.inputRow)
		if err != nil {
			return errors.Trace(err)
		}
		if e.meetNewGroup {
			err := e.consumeGroupRows()
			if err != nil {
				return errors.Trace(err)
			}
			err = e.appendResult2Chunk(chk)
			if err != nil {
				return errors.Trace(err)
			}
		}
		e.remainingRowsInGroup++
		e.groupRows = append(e.groupRows, e.inputRow)
		if e.meetNewGroup {
			e.inputRow = e.inputIter.Next()
			return nil
		}
	}
	return nil
}

func (e *WindowExec) consumeGroupRows() (err error) {
	if len(e.groupRows) == 0 {
		return nil
	}
	e.groupRows, err = e.processor.consumeGroupRows(e.ctx, e.groupRows)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *WindowExec) fetchChildIfNecessary(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.inputIter != nil && e.inputRow != e.inputIter.End() {
		return nil
	}

	// Before fetching a new batch of input, we should consume the last group rows.
	err = e.consumeGroupRows()
	if err != nil {
		return errors.Trace(err)
	}

	childResult := e.children[0].newFirstChunk()
	err = e.children[0].Next(ctx, &chunk.RecordBatch{Chunk: childResult})
	if err != nil {
		return errors.Trace(err)
	}
	e.childResults = append(e.childResults, childResult)
	// No more data.
	if childResult.NumRows() == 0 {
		e.executed = true
		err = e.appendResult2Chunk(chk)
		return errors.Trace(err)
	}

	e.inputIter = chunk.NewIterator4Chunk(childResult)
	e.inputRow = e.inputIter.Begin()
	return nil
}

// appendResult2Chunk appends result of the window function to the result chunk.
func (e *WindowExec) appendResult2Chunk(chk *chunk.Chunk) (err error) {
	e.copyChk(chk)
	remained := mathutil.Min(e.remainingRowsInChunk, e.remainingRowsInGroup)
	e.groupRows, err = e.processor.appendResult2Chunk(e.ctx, e.groupRows, chk, remained)
	if err != nil {
		return err
	}
	e.remainingRowsInGroup -= remained
	e.remainingRowsInChunk -= remained
	if e.remainingRowsInGroup == 0 {
		e.processor.resetPartialResult()
		e.groupRows = e.groupRows[:0]
	}
	return nil
}

func (e *WindowExec) copyChk(chk *chunk.Chunk) {
	if len(e.childResults) == 0 || chk.NumRows() > 0 {
		return
	}
	childResult := e.childResults[0]
	e.childResults = e.childResults[1:]
	e.remainingRowsInChunk = childResult.NumRows()
	columns := e.Schema().Columns[:len(e.Schema().Columns)-e.numWindowFuncs]
	for i, col := range columns {
		chk.MakeRefTo(i, childResult, col.Index)
	}
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
		err := windowFunc.UpdatePartialResult(ctx, rows, p.partialResults[i])
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

// TODO: We can optimize it using sliding window algorithm.
func (p *rowFrameWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	numRows := uint64(len(rows))
	for remained > 0 {
		start := p.getStartOffset(numRows)
		end := p.getEndOffset(numRows)
		p.curRowIdx++
		remained--
		if start >= end {
			for i, windowFunc := range p.windowFuncs {
				err := windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		for i, windowFunc := range p.windowFuncs {
			err := windowFunc.UpdatePartialResult(ctx, rows[start:end], p.partialResults[i])
			if err != nil {
				return nil, err
			}
			err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
			windowFunc.ResetPartialResult(p.partialResults[i])
		}
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
	for remained > 0 {
		start, err := p.getStartOffset(ctx, rows)
		if err != nil {
			return nil, err
		}
		end, err := p.getEndOffset(ctx, rows)
		if err != nil {
			return nil, err
		}
		p.curRowIdx++
		remained--
		if start >= end {
			for i, windowFunc := range p.windowFuncs {
				err := windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		for i, windowFunc := range p.windowFuncs {
			err := windowFunc.UpdatePartialResult(ctx, rows[start:end], p.partialResults[i])
			if err != nil {
				return nil, err
			}
			err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
			windowFunc.ResetPartialResult(p.partialResults[i])
		}
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
