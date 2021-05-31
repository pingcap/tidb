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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type dataInfo struct {
	chk         *chunk.Chunk
	remaining   uint64
	accumulated uint64
}

// PipelinedWindowExec is the executor for window functions.
type PipelinedWindowExec struct {
	baseExecutor
	numWindowFuncs     int
	windowFuncs        []aggfuncs.AggFunc
	slidingWindowFuncs []aggfuncs.SlidingWindowAggFunc
	partialResults     []aggfuncs.PartialResult
	start              *core.FrameBound
	end                *core.FrameBound
	groupChecker       *vecGroupChecker

	// childResult stores the child chunk
	childResult *chunk.Chunk
	data        []dataInfo
	dataIdx     int

	// done indicates the child executor is drained or something unexpected happened.
	done         bool
	accumulated  uint64
	dropped      uint64
	consumed     bool
	rowToConsume uint64
	newPartition bool

	curRowIdx uint64
	// curStartRow and curEndRow defines the current frame range
	lastStartRow uint64
	lastEndRow   uint64
	rowStart     uint64
	orderByCols  []*expression.Column
	// expectedCmpResult is used to decide if one value is included in the frame.
	expectedCmpResult int64

	// rows keeps rows starting from curStartRow
	rows                     []chunk.Row
	rowCnt                   uint64
	whole                    bool
	isRangeFrame             bool
	emptyFrame               bool
	initializedSlidingWindow bool
}

// Close implements the Executor Close interface.
func (e *PipelinedWindowExec) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

// Open implements the Executor Open interface
func (e *PipelinedWindowExec) Open(ctx context.Context) (err error) {
	e.rowToConsume = 0
	e.done = false
	e.accumulated = 0
	e.dropped = 0
	e.data = make([]dataInfo, 0)
	e.dataIdx = 0
	e.slidingWindowFuncs = make([]aggfuncs.SlidingWindowAggFunc, len(e.windowFuncs))
	for i, windowFunc := range e.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			e.slidingWindowFuncs[i] = slidingWindowAggFunc
		}
	}
	e.rows = make([]chunk.Row, 0)
	return e.baseExecutor.Open(ctx)
}

func (e *PipelinedWindowExec) firstResultChunkNotReady() bool {
	// chunk can't be ready unless, 1. all of the rows in the chunk is filled, 2. e.rows doesn't contain rows in the chunk
	return len(e.data) > 0 && (e.data[0].remaining != 0 || e.data[0].accumulated > e.dropped)
}

// Next implements the Executor Next interface.
func (e *PipelinedWindowExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	chk.Reset()

	for !e.done || e.firstResultChunkNotReady() {
		// we firstly gathering enough rows and consume them, until we are able to produce.
		// for unbounded frame, it needs consume the whole partition before being able to produce, in this case
		// e.p.moreToProduce will be false until so.
		var more bool
		more, err = e.moreToProduce(e.ctx)
		if err != nil {
			return
		}
		if !more {
			if !e.done && e.rowToConsume == 0 {
				err = e.getRowsInPartition(ctx)
				if err != nil {
					return err
				}
			}
			if e.done || e.newPartition {
				e.finish()
				// if we continued, the rows will not be consumed, so next time we should consume it instead of calling e.getRowsInPartition
				more, err = e.moreToProduce(e.ctx)
				if err != nil {
					return
				}
				if more {
					continue
				}
				e.newPartition = false
				e.reset()
				if e.rowToConsume == 0 {
					// no more data
					break
				}
			}
			e.rowCnt += e.rowToConsume
			e.rowToConsume = 0
		}

		// e.p is ready to produce data
		if len(e.data) > e.dataIdx && e.data[e.dataIdx].remaining != 0 {
			produced, err := e.produce(e.ctx, e.data[e.dataIdx].chk, e.data[e.dataIdx].remaining)
			if err != nil {
				return err
			}
			e.data[e.dataIdx].remaining -= produced
			if e.data[e.dataIdx].remaining == 0 {
				e.dataIdx++
			}
		}
	}
	if len(e.data) > 0 {
		chk.SwapColumns(e.data[0].chk)
		e.data = e.data[1:]
		e.dataIdx--
	}
	return nil
}

func (e *PipelinedWindowExec) getRowsInPartition(ctx context.Context) (err error) {
	e.newPartition = true
	if len(e.rows) == 0 {
		// if getRowsInPartition is called for the first time, we ignore it as a new partition
		e.newPartition = false
	}
	e.consumed = false

	if e.groupChecker.isExhausted() {
		var drained, samePartition bool
		drained, err = e.fetchChild(ctx)
		if err != nil {
			err = errors.Trace(err)
		}
		// we return immediately to use a combination of true newPartition but 0 in e.rowToConsume to indicate the data source is drained,
		if drained {
			e.done = true
			return nil
		}
		samePartition, err = e.groupChecker.splitIntoGroups(e.childResult)
		if samePartition {
			// the only case that when getRowsInPartition gets called, it is not a new partition.
			e.newPartition = false
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	begin, end := e.groupChecker.getNextGroup()
	e.rowToConsume += uint64(end - begin)
	for i := begin; i < end; i++ {
		e.rows = append(e.rows, e.childResult.GetRow(i))
	}
	return
}

func (e *PipelinedWindowExec) fetchChild(ctx context.Context) (EOF bool, err error) {
	// TODO: reuse chunks
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

	// TODO: reuse chunks
	resultChk := chunk.New(e.retFieldTypes, 0, numRows)
	err = e.copyChk(childResult, resultChk)
	if err != nil {
		return false, err
	}
	e.accumulated += uint64(numRows)
	e.data = append(e.data, dataInfo{chk: resultChk, remaining: uint64(numRows), accumulated: e.accumulated})

	e.childResult = childResult
	return false, nil
}

func (e *PipelinedWindowExec) copyChk(src, dst *chunk.Chunk) error {
	columns := e.Schema().Columns[:len(e.Schema().Columns)-e.numWindowFuncs]
	for i, col := range columns {
		if err := dst.MakeRefTo(i, src, col.Index); err != nil {
			return err
		}
	}
	return nil
}

func (p *PipelinedWindowExec) getRow(i uint64) chunk.Row {
	return p.rows[i-p.rowStart]
}

func (p *PipelinedWindowExec) getRows(s, e uint64) []chunk.Row {
	return p.rows[s-p.rowStart : e-p.rowStart]
}

// finish is called upon a whole partition is consumed
func (p *PipelinedWindowExec) finish() {
	p.whole = true
}

func (p *PipelinedWindowExec) getStart(ctx sessionctx.Context) (uint64, error) {
	if p.start.UnBounded {
		return 0, nil
	}
	if p.isRangeFrame {
		var start uint64
		for start = p.lastStartRow; start < p.rowCnt; start++ {
			var res int64
			var err error
			for i := range p.orderByCols {
				res, _, err = p.start.CmpFuncs[i](ctx, p.orderByCols[i], p.start.CalcFuncs[i], p.getRow(start), p.getRow(p.curRowIdx))
				if err != nil {
					return 0, err
				}
				if res != 0 {
					break
				}
			}
			// For asc, break when the calculated result is greater than the current value.
			// For desc, break when the calculated result is less than the current value.
			if res != p.expectedCmpResult {
				break
			}
		}
		return start, nil
	}
	switch p.start.Type {
	case ast.Preceding:
		if p.curRowIdx > p.start.Num {
			return p.curRowIdx - p.start.Num, nil
		}
		return 0, nil
	case ast.Following:
		return p.curRowIdx + p.start.Num, nil
	default: // ast.CurrentRow
		return p.curRowIdx, nil
	}
}

func (p *PipelinedWindowExec) getEnd(ctx sessionctx.Context) (uint64, error) {
	if p.end.UnBounded {
		return p.rowCnt, nil
	}
	if p.isRangeFrame {
		var end uint64
		for end = p.lastEndRow; end < p.rowCnt; end++ {
			var res int64
			var err error
			for i := range p.orderByCols {
				res, _, err = p.end.CmpFuncs[i](ctx, p.end.CalcFuncs[i], p.orderByCols[i], p.getRow(p.curRowIdx), p.getRow(end))
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
		return end, nil
	}
	switch p.end.Type {
	case ast.Preceding:
		if p.curRowIdx >= p.end.Num {
			return p.curRowIdx - p.end.Num + 1, nil
		}
		return 0, nil
	case ast.Following:
		return p.curRowIdx + p.end.Num + 1, nil
	default: // ast.CurrentRow:
		return p.curRowIdx + 1, nil
	}
}

// produce produces rows and append it to chk, return produced means number of rows appended into chunk, available means
// number of rows processed but not fetched
func (p *PipelinedWindowExec) produce(ctx sessionctx.Context, chk *chunk.Chunk, remained uint64) (produced uint64, err error) {
	var (
		start uint64
		end   uint64
		more  bool
	)
	for remained > 0 {
		more, err = p.moreToProduce(ctx)
		if err != nil {
			return
		}
		if !more {
			break
		}
		start, err = p.getStart(ctx)
		if err != nil {
			return
		}
		end, err = p.getEnd(ctx)
		if err != nil {
			return
		}
		if end > p.rowCnt {
			if !p.whole {
				return
			}
			end = p.rowCnt
		}
		if start >= p.rowCnt {
			if !p.whole {
				return
			}
			start = p.rowCnt
		}
		// if start >= end, we should return a default value, and we reset the frame to empty.
		if start >= end {
			for i, wf := range p.windowFuncs {
				if !p.emptyFrame {
					wf.ResetPartialResult(p.partialResults[i])
				}
				err = wf.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					return
				}
			}
			if !p.emptyFrame {
				p.emptyFrame = true
				p.initializedSlidingWindow = false
			}
		} else {
			p.emptyFrame = false
			for i, wf := range p.windowFuncs {
				slidingWindowAggFunc := p.slidingWindowFuncs[i]
				if p.lastStartRow != start || p.lastEndRow != end {
					if slidingWindowAggFunc != nil && p.initializedSlidingWindow {
						err = slidingWindowAggFunc.Slide(ctx, p.getRow, p.lastStartRow, p.lastEndRow, start-p.lastStartRow, end-p.lastEndRow, p.partialResults[i])
					} else {
						// For MinMaxSlidingWindowAggFuncs, it needs the absolute value of each start of window, to compare
						// whether elements inside deque are out of current window.
						if minMaxSlidingWindowAggFunc, ok := wf.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
							// Store start inside MaxMinSlidingWindowAggFunc.windowInfo
							minMaxSlidingWindowAggFunc.SetWindowStart(start)
						}
						// TODO(zhifeng): track memory usage here
						wf.ResetPartialResult(p.partialResults[i])
						_, err = wf.UpdatePartialResult(ctx, p.getRows(start, end), p.partialResults[i])
					}
				}
				if err != nil {
					return
				}
				err = wf.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					return
				}
			}
			p.initializedSlidingWindow = true
		}
		p.curRowIdx++
		p.lastStartRow, p.lastEndRow = start, end

		produced++
		remained--
	}
	extend := p.curRowIdx
	if p.lastEndRow < extend {
		extend = p.lastEndRow
	}
	if p.lastStartRow < extend {
		extend = p.lastStartRow
	}
	if extend > p.rowStart {
		numDrop := extend - p.rowStart
		p.dropped += numDrop
		p.rows = p.rows[numDrop:]
		p.rowStart = extend
	}
	return
}

func (p *PipelinedWindowExec) moreToProduce(ctx sessionctx.Context) (more bool, err error) {
	if p.curRowIdx >= p.rowCnt {
		return false, nil
	}
	if p.whole {
		return true, nil
	}
	start, err := p.getStart(ctx)
	if err != nil {
		return
	}
	end, err := p.getEnd(ctx)
	if err != nil {
		return
	}
	return end < p.rowCnt && start < p.rowCnt, nil
}

// reset resets the processor
func (p *PipelinedWindowExec) reset() {
	p.lastStartRow = 0
	p.lastEndRow = 0
	p.emptyFrame = false
	p.curRowIdx = 0
	p.whole = false
	numDrop := p.rowCnt - p.rowStart
	p.dropped += numDrop
	p.rows = p.rows[numDrop:]
	p.rowStart = 0
	p.rowCnt = 0
	p.initializedSlidingWindow = false
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
}
