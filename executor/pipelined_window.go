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

// PipelinedWindowExec is the executor for window functions.
type PipelinedWindowExec struct {
	baseExecutor

	groupChecker *vecGroupChecker
	// childResult stores the child chunk
	childResult *chunk.Chunk
	// resultChunks stores the chunks to return
	resultChunks []*chunk.Chunk
	// remainingRowsInChunk indicates how many rows the resultChunks[i] is not prepared.
	remainingRowsInChunk []int

	numWindowFuncs int
	// done indicates the child executor is drained or something unexpected happened.
	done         bool
	p            processor // TODO(zhifeng): you don't need a processor, just make it into the executor
	rows         []chunk.Row
	wantMore     int64
	consumed     bool
	newPartition bool
}

// Close implements the Executor Close interface.
func (e *PipelinedWindowExec) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

// Open implements the Executor Open interface
func (e *PipelinedWindowExec) Open(ctx context.Context) (err error) {
	e.consumed = true
	e.p.init()
	e.rows = make([]chunk.Row, 0)
	return e.baseExecutor.Open(ctx)
}

func (e *PipelinedWindowExec) firstResultChunkNotReady() bool {
	return len(e.remainingRowsInChunk) > 0 && e.remainingRowsInChunk[0] != 0
}

// Next implements the Executor Next interface.
func (e *PipelinedWindowExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	chk.Reset()

	for !e.done || e.firstResultChunkNotReady() {
		// we firstly gathering enough rows and consume them, until we are able to produce.
		// for unbounded frame, it needs consume the whole partition before being able to produce, in this case
		// e.p.moreToProduce will be false until so.
		if !e.done && !e.p.moreToProduce(e.ctx) {
			if e.consumed {
				err = e.getRowsInPartition(ctx)
				if err != nil {
					return err
				}
			}
			if e.newPartition {
				e.p.finish()
				e.wantMore = 0
				// if we continued, the rows will not be consumed, so next time we should consume it instead of calling e.getRowsInPartition
				if e.p.moreToProduce(e.ctx) {
					continue
				}
				e.newPartition = false
				e.p.reset()
				if len(e.rows) == 0 {
					// no more data
					break
				}
			}
			e.wantMore, err = e.p.consume(e.ctx, e.rows)
			e.consumed = true // TODO(zhifeng): move this into consume() after absorbing p into e
			if err != nil {
				return err
			}
		}

		// e.p is ready to produce data, we use wantMore here to avoid meaningless produce when the whole frame is required
		if e.wantMore == 0 && len(e.resultChunks) > 0 && e.remainingRowsInChunk[0] != 0 {
			produced, err := e.p.produce(e.ctx, e.resultChunks[0], e.remainingRowsInChunk[0])
			if err != nil {
				return err
			}
			e.remainingRowsInChunk[0] -= int(produced)
			if e.remainingRowsInChunk[0] == 0 {
				break
			}
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

func (e *PipelinedWindowExec) getRowsInPartition(ctx context.Context) (err error) {
	e.newPartition = true
	if len(e.rows) == 0 {
		// if getRowsInPartition is called for the first time, we ignore it as a new partition
		e.newPartition = false
	}
	e.rows = e.rows[:0]
	e.consumed = false

	if e.groupChecker.isExhausted() {
		var drained, samePartition bool
		drained, err = e.fetchChild(ctx)
		if err != nil {
			err = errors.Trace(err)
		}
		// we return immediately to use a combination of true newPartition but empty e.rows to indicate the data source is drained,
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
	e.resultChunks = append(e.resultChunks, resultChk)
	e.remainingRowsInChunk = append(e.remainingRowsInChunk, numRows)

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

type processor struct {
	windowFuncs        []aggfuncs.AggFunc
	slidingWindowFuncs []aggfuncs.SlidingWindowAggFunc
	partialResults     []aggfuncs.PartialResult
	start              *core.FrameBound
	end                *core.FrameBound
	curRowIdx          uint64
	// curStartRow and curEndRow defines the current frame range
	lastStartRow uint64
	lastEndRow   uint64
	rowStart     uint64
	orderByCols  []*expression.Column
	// expectedCmpResult is used to decide if one value is included in the frame.
	expectedCmpResult int64

	// rows keeps rows starting from curStartRow, TODO(zhifeng): make it a queue
	rows                     []chunk.Row
	rowCnt                   uint64
	whole                    bool
	isRangeFrame             bool
	emptyFrame               bool
	initializedSlidingWindow bool
}

func (p *processor) getRow(i uint64) chunk.Row {
	return p.rows[i-p.rowStart]
}

func (p *processor) getRows(s, e uint64) []chunk.Row {
	return p.rows[s-p.rowStart : e-p.rowStart]
}

func (p *processor) init() {
	p.slidingWindowFuncs = make([]aggfuncs.SlidingWindowAggFunc, len(p.windowFuncs))
	for i, windowFunc := range p.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			p.slidingWindowFuncs[i] = slidingWindowAggFunc
		}
	}
}

// slide consumes more rows, and it returns the number of more rows needed in order to proceed, more = -1 means it can only
// process when the whole partition is consumed, more = 0 means it is ready now, more = n (n > 0), means it need at least n
// more rows to process.
func (p *processor) consume(ctx sessionctx.Context, rows []chunk.Row) (more int64, err error) {
	num := uint64(len(rows))
	p.rowCnt += num
	p.rows = append(p.rows, rows...)
	rows = rows[:0]
	if p.end.UnBounded {
		if !p.whole {
			// we can't proceed until the whole partition is consumed
			return -1, nil
		}
	}
	// let's just try produce
	return 0, nil
}

// finish is called upon a whole partition is consumed
func (p *processor) finish() {
	p.whole = true
}

func (p *processor) getStart(ctx sessionctx.Context) (uint64, error) {
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

func (p *processor) getEnd(ctx sessionctx.Context) (uint64, error) {
	if p.end.UnBounded {
		if p.whole {
			return p.rowCnt, nil
		}
		return p.rowCnt + 1, nil
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
func (p *processor) produce(ctx sessionctx.Context, chk *chunk.Chunk, remained int) (produced int64, err error) {
	var (
		start uint64
		end   uint64
	)
	for remained > 0 && p.moreToProduce(ctx) {
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
		p.rows = p.rows[extend-p.rowStart:]
		p.rowStart = extend
	}
	return
}

func (p *processor) moreToProduce(ctx sessionctx.Context) bool {
	if p.curRowIdx >= p.rowCnt {
		return false
	}
	if p.whole {
		return true
	}
	start, err := p.getStart(ctx)
	_ = err
	end, err := p.getEnd(ctx)
	_ = err
	// TODO(Zhifeng): handle the error here
	return end < p.rowCnt && start < p.rowCnt
}

// reset resets the processor
func (p *processor) reset() {
	p.lastStartRow = 0
	p.lastEndRow = 0
	p.rowStart = 0
	p.emptyFrame = false
	p.curRowIdx = 0
	p.rowCnt = 0
	p.whole = false
	p.rows = p.rows[:0]
	p.initializedSlidingWindow = false
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
}
