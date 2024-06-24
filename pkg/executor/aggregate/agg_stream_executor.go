// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// StreamAggExec deals with all the aggregate functions.
// It assumes all the input data is sorted by group by key.
// When Next() is called, it will return a result for the same group.
type StreamAggExec struct {
	exec.BaseExecutor

	executed bool
	// IsChildReturnEmpty indicates whether the child executor only returns an empty input.
	IsChildReturnEmpty bool
	DefaultVal         *chunk.Chunk
	GroupChecker       *vecgroupchecker.VecGroupChecker
	inputIter          *chunk.Iterator4Chunk
	inputRow           chunk.Row
	AggFuncs           []aggfuncs.AggFunc
	partialResults     []aggfuncs.PartialResult
	groupRows          []chunk.Row
	childResult        *chunk.Chunk

	memTracker *memory.Tracker // track memory usage.
	// memUsageOfInitialPartialResult indicates the memory usage of all partial results after initialization.
	// All partial results will be reset after processing one group data, and the memory usage should also be reset.
	// We can't get memory delta from ResetPartialResult, so record the memory usage here.
	memUsageOfInitialPartialResult int64
}

// Open implements the Executor Open interface.
func (e *StreamAggExec) Open(ctx context.Context) error {
	failpoint.Inject("mockStreamAggExecBaseExecutorOpenReturnedError", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			failpoint.Return(errors.New("mock StreamAggExec.baseExecutor.Open returned error"))
		}
	})

	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	// If panic in Open, the children executor should be closed because they are open.
	defer closeBaseExecutor(&e.BaseExecutor)
	return e.OpenSelf()
}

// OpenSelf just opens the StreamAggExec.
func (e *StreamAggExec) OpenSelf() error {
	e.childResult = exec.TryNewCacheChunk(e.Children(0))
	e.executed = false
	e.IsChildReturnEmpty = true
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()

	e.partialResults = make([]aggfuncs.PartialResult, 0, len(e.AggFuncs))
	for _, aggFunc := range e.AggFuncs {
		partialResult, memDelta := aggFunc.AllocPartialResult()
		e.partialResults = append(e.partialResults, partialResult)
		e.memUsageOfInitialPartialResult += memDelta
	}

	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		// bytesLimit <= 0 means no limit, for now we just track the memory footprint
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	if e.Ctx().GetSessionVars().TrackAggregateMemoryUsage {
		e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(e.childResult.MemoryUsage() + e.memUsageOfInitialPartialResult)
	return nil
}

// Close implements the Executor Close interface.
func (e *StreamAggExec) Close() error {
	if e.childResult != nil {
		e.memTracker.Consume(-e.childResult.MemoryUsage() - e.memUsageOfInitialPartialResult)
		e.childResult = nil
	}
	e.GroupChecker.Reset()
	return e.BaseExecutor.Close()
}

// Next implements the Executor Next interface.
func (e *StreamAggExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for !e.executed && !req.IsFull() {
		err = e.consumeOneGroup(ctx, req)
		if err != nil {
			e.executed = true
			return err
		}
	}
	return nil
}

func (e *StreamAggExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.GroupChecker.IsExhausted() {
		if err = e.consumeCurGroupRowsAndFetchChild(ctx, chk); err != nil {
			return err
		}
		if e.executed {
			return nil
		}
		_, err := e.GroupChecker.SplitIntoGroups(e.childResult)
		if err != nil {
			return err
		}
	}
	begin, end := e.GroupChecker.GetNextGroup()
	for i := begin; i < end; i++ {
		e.groupRows = append(e.groupRows, e.childResult.GetRow(i))
	}

	for meetLastGroup := end == e.childResult.NumRows(); meetLastGroup; {
		meetLastGroup = false
		if err = e.consumeCurGroupRowsAndFetchChild(ctx, chk); err != nil || e.executed {
			return err
		}

		isFirstGroupSameAsPrev, err := e.GroupChecker.SplitIntoGroups(e.childResult)
		if err != nil {
			return err
		}

		if isFirstGroupSameAsPrev {
			begin, end = e.GroupChecker.GetNextGroup()
			for i := begin; i < end; i++ {
				e.groupRows = append(e.groupRows, e.childResult.GetRow(i))
			}
			meetLastGroup = end == e.childResult.NumRows()
		}
	}

	err = e.consumeGroupRows()
	if err != nil {
		return err
	}

	return e.appendResult2Chunk(chk)
}

func (e *StreamAggExec) consumeGroupRows() error {
	if len(e.groupRows) == 0 {
		return nil
	}

	allMemDelta := int64(0)
	exprCtx := e.Ctx().GetExprCtx()
	for i, aggFunc := range e.AggFuncs {
		memDelta, err := aggFunc.UpdatePartialResult(exprCtx.GetEvalCtx(), e.groupRows, e.partialResults[i])
		if err != nil {
			return err
		}
		allMemDelta += memDelta
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(allMemDelta)
	e.groupRows = e.groupRows[:0]
	return nil
}

func (e *StreamAggExec) consumeCurGroupRowsAndFetchChild(ctx context.Context, chk *chunk.Chunk) (err error) {
	// Before fetching a new batch of input, we should consume the last group.
	err = e.consumeGroupRows()
	if err != nil {
		return err
	}

	mSize := e.childResult.MemoryUsage()
	err = exec.Next(ctx, e.Children(0), e.childResult)
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
	if err != nil {
		return err
	}

	// No more data.
	if e.childResult.NumRows() == 0 {
		if !e.IsChildReturnEmpty {
			err = e.appendResult2Chunk(chk)
		} else if e.DefaultVal != nil {
			chk.Append(e.DefaultVal, 0, 1)
		}
		e.executed = true
		return err
	}
	// Reach here, "e.childrenResults[0].NumRows() > 0" is guaranteed.
	e.IsChildReturnEmpty = false
	e.inputRow = e.inputIter.Begin()
	return nil
}

// appendResult2Chunk appends result of all the aggregation functions to the
// result chunk, and reset the evaluation context for each aggregation.
func (e *StreamAggExec) appendResult2Chunk(chk *chunk.Chunk) error {
	exprCtx := e.Ctx().GetExprCtx()
	for i, aggFunc := range e.AggFuncs {
		err := aggFunc.AppendFinalResult2Chunk(exprCtx.GetEvalCtx(), e.partialResults[i], chk)
		if err != nil {
			return err
		}
		aggFunc.ResetPartialResult(e.partialResults[i])
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	// All partial results have been reset, so reset the memory usage.
	e.memTracker.ReplaceBytesUsed(e.childResult.MemoryUsage() + e.memUsageOfInitialPartialResult)
	if len(e.AggFuncs) == 0 {
		chk.SetNumVirtualRows(chk.NumRows() + 1)
	}
	return nil
}
