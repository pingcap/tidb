// Copyright 2021 PingCAP, Inc.
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

	// childResult stores the child chunk. Note that even if remaining is 0, e.rows might still references rows in data[0].chk after returned it to upper executor, since there is no guarantee what the upper executor will do to the returned chunk, it might destroy the data (as in the benchmark test, it reused the chunk to pull data, and it will be chk.Reset(), causing panicking). So dataIdx, accumulated and dropped are added to ensure that chunk will only be returned if there is no row reference.
	childResult *chunk.Chunk
	data        []dataInfo
	dataIdx     int

	// done indicates the child executor is drained or something unexpected happened.
	done         bool
	accumulated  uint64
	dropped      uint64
	rowToConsume uint64
	newPartition bool

	curRowIdx uint64
	// curStartRow and curEndRow defines the current frame range
	lastStartRow   uint64
	lastEndRow     uint64
	stagedStartRow uint64
	stagedEndRow   uint64
	rowStart       uint64
	orderByCols    []*expression.Column
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
	if !e.done && len(e.data) == 0 {
		return true
	}
	// chunk can't be ready unless, 1. all of the rows in the chunk is filled, 2. e.rows doesn't contain rows in the chunk
	return len(e.data) > 0 && (e.data[0].remaining != 0 || e.data[0].accumulated > e.dropped)
}

// Next implements the Executor Next interface.
func (e *PipelinedWindowExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	chk.Reset()

	for e.firstResultChunkNotReady() {
		// we firstly gathering enough rows and consume them, until we are able to produce.
		// for unbounded frame, it needs consume the whole partition before being able to produce, in this case
		// e.p.enoughToProduce will be false until so.
		var enough bool
		enough, err = e.enoughToProduce(e.ctx)
		if err != nil {
			return
		}
		if !enough {
			if !e.done && e.rowToConsume == 0 {
				err = e.getRowsInPartition(ctx)
				if err != nil {
					return err
				}
			}
			if e.done || e.newPartition {
				e.finish()
				// if we continued, the rows will not be consumed, so next time we should consume it instead of calling e.getRowsInPartition
				enough, err = e.enoughToProduce(e.ctx)
				if err != nil {
					return
				}
				if enough {
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

	if e.groupChecker.isExhausted() {
		var drained, samePartition bool
		drained, err = e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
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

func (e *PipelinedWindowExec) getRow(i uint64) chunk.Row {
	return e.rows[i-e.rowStart]
}

func (e *PipelinedWindowExec) getRows(start, end uint64) []chunk.Row {
	return e.rows[start-e.rowStart : end-e.rowStart]
}

// finish is called upon a whole partition is consumed
func (e *PipelinedWindowExec) finish() {
	e.whole = true
}

func (e *PipelinedWindowExec) getStart(ctx sessionctx.Context) (uint64, error) {
	if e.start.UnBounded {
		return 0, nil
	}
	if e.isRangeFrame {
		var start uint64
		for start = mathutil.Max(e.lastStartRow, e.stagedStartRow); start < e.rowCnt; start++ {
			var res int64
			var err error
			for i := range e.orderByCols {
				res, _, err = e.start.CmpFuncs[i](ctx, e.orderByCols[i], e.start.CalcFuncs[i], e.getRow(start), e.getRow(e.curRowIdx))
				if err != nil {
					return 0, err
				}
				if res != 0 {
					break
				}
			}
			// For asc, break when the calculated result is greater than the current value.
			// For desc, break when the calculated result is less than the current value.
			if res != e.expectedCmpResult {
				break
			}
		}
		e.stagedStartRow = start
		return start, nil
	}
	switch e.start.Type {
	case ast.Preceding:
		if e.curRowIdx > e.start.Num {
			return e.curRowIdx - e.start.Num, nil
		}
		return 0, nil
	case ast.Following:
		return e.curRowIdx + e.start.Num, nil
	default: // ast.CurrentRow
		return e.curRowIdx, nil
	}
}

func (e *PipelinedWindowExec) getEnd(ctx sessionctx.Context) (uint64, error) {
	if e.end.UnBounded {
		return e.rowCnt, nil
	}
	if e.isRangeFrame {
		var end uint64
		for end = mathutil.Max(e.lastEndRow, e.stagedEndRow); end < e.rowCnt; end++ {
			var res int64
			var err error
			for i := range e.orderByCols {
				res, _, err = e.end.CmpFuncs[i](ctx, e.end.CalcFuncs[i], e.orderByCols[i], e.getRow(e.curRowIdx), e.getRow(end))
				if err != nil {
					return 0, err
				}
				if res != 0 {
					break
				}
			}
			// For asc, break when the calculated result is greater than the current value.
			// For desc, break when the calculated result is less than the current value.
			if res == e.expectedCmpResult {
				break
			}
		}
		e.stagedEndRow = end
		return end, nil
	}
	switch e.end.Type {
	case ast.Preceding:
		if e.curRowIdx >= e.end.Num {
			return e.curRowIdx - e.end.Num + 1, nil
		}
		return 0, nil
	case ast.Following:
		return e.curRowIdx + e.end.Num + 1, nil
	default: // ast.CurrentRow:
		return e.curRowIdx + 1, nil
	}
}

// produce produces rows and append it to chk, return produced means number of rows appended into chunk, available means
// number of rows processed but not fetched
func (e *PipelinedWindowExec) produce(ctx sessionctx.Context, chk *chunk.Chunk, remained uint64) (produced uint64, err error) {
	var (
		start  uint64
		end    uint64
		enough bool
	)
	for remained > 0 {
		enough, err = e.enoughToProduce(ctx)
		if err != nil {
			return
		}
		if !enough {
			break
		}
		start, err = e.getStart(ctx)
		if err != nil {
			return
		}
		end, err = e.getEnd(ctx)
		if err != nil {
			return
		}
		if end > e.rowCnt {
			end = e.rowCnt
		}
		if start >= e.rowCnt {
			start = e.rowCnt
		}
		// if start >= end, we should return a default value, and we reset the frame to empty.
		if start >= end {
			for i, wf := range e.windowFuncs {
				if !e.emptyFrame {
					wf.ResetPartialResult(e.partialResults[i])
				}
				err = wf.AppendFinalResult2Chunk(ctx, e.partialResults[i], chk)
				if err != nil {
					return
				}
			}
			if !e.emptyFrame {
				e.emptyFrame = true
				e.initializedSlidingWindow = false
			}
		} else {
			e.emptyFrame = false
			for i, wf := range e.windowFuncs {
				slidingWindowAggFunc := e.slidingWindowFuncs[i]
				if e.lastStartRow != start || e.lastEndRow != end {
					if slidingWindowAggFunc != nil && e.initializedSlidingWindow {
						err = slidingWindowAggFunc.Slide(ctx, e.getRow, e.lastStartRow, e.lastEndRow, start-e.lastStartRow, end-e.lastEndRow, e.partialResults[i])
					} else {
						// For MinMaxSlidingWindowAggFuncs, it needs the absolute value of each start of window, to compare
						// whether elements inside deque are out of current window.
						if minMaxSlidingWindowAggFunc, ok := wf.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
							// Store start inside MaxMinSlidingWindowAggFunc.windowInfo
							minMaxSlidingWindowAggFunc.SetWindowStart(start)
						}
						// TODO(zhifeng): track memory usage here
						wf.ResetPartialResult(e.partialResults[i])
						_, err = wf.UpdatePartialResult(ctx, e.getRows(start, end), e.partialResults[i])
					}
				}
				if err != nil {
					return
				}
				err = wf.AppendFinalResult2Chunk(ctx, e.partialResults[i], chk)
				if err != nil {
					return
				}
			}
			e.initializedSlidingWindow = true
		}
		e.curRowIdx++
		e.lastStartRow, e.lastEndRow = start, end

		produced++
		remained--
	}
	extend := mathutil.Min(e.curRowIdx, e.lastEndRow, e.lastStartRow)
	if extend > e.rowStart {
		numDrop := extend - e.rowStart
		e.dropped += numDrop
		e.rows = e.rows[numDrop:]
		e.rowStart = extend
	}
	return
}

func (e *PipelinedWindowExec) enoughToProduce(ctx sessionctx.Context) (enough bool, err error) {
	if e.curRowIdx >= e.rowCnt {
		return false, nil
	}
	if e.whole {
		return true, nil
	}
	start, err := e.getStart(ctx)
	if err != nil {
		return
	}
	end, err := e.getEnd(ctx)
	if err != nil {
		return
	}
	return end < e.rowCnt && start < e.rowCnt, nil
}

// reset resets the processor
func (e *PipelinedWindowExec) reset() {
	e.lastStartRow = 0
	e.lastEndRow = 0
	e.stagedStartRow = 0
	e.stagedEndRow = 0
	e.emptyFrame = false
	e.curRowIdx = 0
	e.whole = false
	numDrop := e.rowCnt - e.rowStart
	e.dropped += numDrop
	e.rows = e.rows[numDrop:]
	e.rowStart = 0
	e.rowCnt = 0
	e.initializedSlidingWindow = false
	for i, windowFunc := range e.windowFuncs {
		windowFunc.ResetPartialResult(e.partialResults[i])
	}
}
