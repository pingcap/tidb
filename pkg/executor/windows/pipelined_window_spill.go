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

package windows

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

// isWholePartitionFrame reports whether the frame always covers the whole
// partition. A nil frame is treated as the whole partition, the same as
// PipelinedWindowExec does.
func isWholePartitionFrame(frame *logicalop.WindowFrame) bool {
	if frame == nil {
		return true
	}
	return frame.Start.UnBounded && frame.Start.Type == ast.Preceding &&
		frame.End.UnBounded && frame.End.Type == ast.Following
}

// isStreamableAggFunc reports whether feeding rows chunk by chunk to the
// function's UpdatePartialResult gives the same result as feeding the whole
// partition at once. This holds for these plain aggregate functions, which
// hash aggregation already updates chunk by chunk.
func isStreamableAggFunc(desc *aggregation.AggFuncDesc) bool {
	if desc.HasDistinct || len(desc.OrderByItems) > 0 {
		return false
	}
	switch desc.Name {
	case ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor,
		ast.AggFuncVarPop, ast.AggFuncVarSamp, ast.AggFuncStddevPop, ast.AggFuncStddevSamp:
		return true
	}
	return false
}

// wholePartitionAggState is the state of PipelinedWindowExec when the frame
// covers the whole partition and all window functions are streamable
// aggregate functions, for example `SUM(a) OVER (PARTITION BY b)`.
//
// Such a window has a single result per partition. Rows are consumed into the
// partial results as soon as they arrive, and only the input columns are
// buffered until the partition ends. Buffered chunks are read back strictly in
// arrival order, so they can be moved to disk when the query exceeds its
// memory quota; this keeps a huge partition from holding the whole input in
// memory.
//
// Rows are numbered from the start of the input. The buffered chunks together
// cover rows [emittedRows, bufferedRows), and the oldest one can be emitted
// once all its rows are below finishedRows, that is, once every partition it
// overlaps has ended.
type wholePartitionAggState struct {
	// numInputCols is the number of non-window output columns. They come
	// first in the output, followed by the window function columns listed in
	// windowColIdxs.
	numInputCols  int
	inputTypes    []*types.FieldType
	windowColIdxs []int

	// head is the oldest buffered chunk. It is always in memory.
	head *chunk.Chunk
	// pending holds the in-memory buffered chunks after head. It is empty
	// while inDisk is not nil.
	pending []*chunk.Chunk
	// inDisk holds the spilled chunks after head. While it is not nil, newly
	// buffered chunks are appended to it to keep the arrival order.
	inDisk      *chunk.DataInDiskByChunks
	diskReadIdx int

	bufferedRows uint64
	finishedRows uint64
	emittedRows  uint64

	// results stores one row of window function results for each finished
	// partition that still has rows to emit, and resultEnds[i] is the
	// exclusive end row of the partition of results row i. Rows before
	// resultsStart are already emitted. results has the output layout because
	// window functions append to their output column, but only the window
	// function columns are filled.
	results         *chunk.Chunk
	resultEnds      []uint64
	resultsStart    int
	resultsMemUsage int64

	rowBuf         []chunk.Row
	rowBufMemUsage int64

	childDrained bool
	// pendingMemUsage is the memory usage of pending, which is what a spill
	// can release. needSpill is set by spillAction, and the executor spills
	// pending when it buffers the next chunk. Both are read by spillAction,
	// possibly from another goroutine.
	pendingMemUsage atomic.Int64
	needSpill       atomic.Bool
	spillAction     *windowSpillDiskAction
	diskTracker     *disk.Tracker
}

// windowSpillDiskAction implements memory.ActionOnExceed for
// PipelinedWindowExec in the whole-partition aggregate mode.
type windowSpillDiskAction struct {
	memory.BaseOOMAction
	e *PipelinedWindowExec
}

// Action asks the window executor to move its pending chunks to disk. The
// executor spills when it buffers the next chunk, so while a requested spill
// is not done yet, this action returns without triggering the fallback. The
// fallback is triggered when the pending chunks are too small for a spill to
// help, which includes the time after they are moved to disk.
func (a *windowSpillDiskAction) Action(t *memory.Tracker) {
	s := a.e.wholePartition
	if pending := s.pendingMemUsage.Load(); pending > 0 && pending >= t.GetBytesLimit()/10 {
		if s.needSpill.CompareAndSwap(false, true) {
			logutil.BgLogger().Info("memory exceeds quota, window executor will spill to disk",
				zap.Int64("consumed", t.BytesConsumed()),
				zap.Int64("quota", t.GetBytesLimit()))
			memory.QueryForceDisk.Add(1)
		}
		return
	}
	if fallback := a.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// GetPriority implements memory.ActionOnExceed.
func (*windowSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (e *PipelinedWindowExec) openWholePartition() {
	s := e.wholePartition
	e.closeWholePartition()
	s.results = chunk.NewEmptyChunk(e.RetFieldTypes())
	s.childDrained = false
	s.needSpill.Store(false)
	if !vardef.EnableTmpStorageOnOOM.Load() {
		return
	}
	vars := e.Ctx().GetSessionVars()
	if s.diskTracker == nil {
		s.diskTracker = disk.NewTracker(e.ID(), -1)
	} else {
		s.diskTracker.Reset()
	}
	if vars.StmtCtx.DiskTracker != nil {
		s.diskTracker.AttachTo(vars.StmtCtx.DiskTracker)
	}
	s.spillAction = &windowSpillDiskAction{e: e}
	vars.MemTracker.FallbackOldAndSetNewAction(s.spillAction)
}

// closeWholePartition releases buffered data and the spill file. The memory
// tracker is cleared by the caller.
func (e *PipelinedWindowExec) closeWholePartition() {
	s := e.wholePartition
	if s.spillAction != nil {
		s.spillAction.SetFinished()
		s.spillAction = nil
	}
	if s.inDisk != nil {
		s.inDisk.Close()
		s.inDisk = nil
	}
	s.head, s.pending, s.results, s.resultEnds, s.rowBuf = nil, nil, nil, nil, nil
	s.diskReadIdx, s.resultsStart = 0, 0
	s.bufferedRows, s.finishedRows, s.emittedRows = 0, 0, 0
	s.resultsMemUsage, s.rowBufMemUsage = 0, 0
	s.pendingMemUsage.Store(0)
}

func (e *PipelinedWindowExec) nextWholePartition(ctx context.Context, chk *chunk.Chunk) error {
	s := e.wholePartition
	for {
		if s.head == nil {
			if err := e.loadHead(); err != nil {
				return err
			}
		}
		if s.head != nil && s.emittedRows+uint64(s.head.NumRows()) <= s.finishedRows {
			return e.emitHead(chk)
		}
		if s.childDrained {
			// All partitions are finished after the child is drained, so
			// nothing is buffered here.
			return nil
		}
		if err := e.fetchChildForWholePartition(ctx); err != nil {
			return err
		}
	}
}

// loadHead moves the oldest buffered chunk, if any, into head.
func (e *PipelinedWindowExec) loadHead() error {
	s := e.wholePartition
	if s.inDisk != nil {
		chk, err := s.inDisk.GetChunk(s.diskReadIdx)
		if err != nil {
			return err
		}
		s.diskReadIdx++
		if s.diskReadIdx == s.inDisk.NumChunks() {
			// Everything spilled is read back. Buffer new chunks in memory
			// again.
			s.inDisk.Close()
			s.inDisk = nil
			s.diskReadIdx = 0
		}
		s.head = chk
		e.memTracker.consume(chk.MemoryUsage())
		return nil
	}
	if len(s.pending) > 0 {
		// The memory of pending chunks is already tracked.
		s.head = s.pending[0]
		s.pendingMemUsage.Add(-s.head.MemoryUsage())
		s.pending[0] = nil
		s.pending = s.pending[1:]
		if len(s.pending) == 0 {
			s.pending = nil
		}
	}
	return nil
}

// emitHead outputs head with the window function results of its rows.
func (e *PipelinedWindowExec) emitHead(chk *chunk.Chunk) error {
	s := e.wholePartition
	head := s.head
	numRows := head.NumRows()
	resultChk := e.AllocPool.Alloc(e.RetFieldTypes(), 0, numRows)
	for i := range s.numInputCols {
		if err := resultChk.MakeRefTo(i, head, i); err != nil {
			return err
		}
	}
	for row := s.emittedRows; row < s.emittedRows+uint64(numRows); row++ {
		for s.resultEnds[s.resultsStart] <= row {
			s.resultsStart++
		}
		resultChk.AppendPartialRowByColIdxs(s.numInputCols, s.results.GetRow(s.resultsStart), s.windowColIdxs)
	}
	e.memTracker.consume(-head.MemoryUsage())
	chk.SwapColumns(resultChk)
	s.head = nil
	s.emittedRows += uint64(numRows)
	for s.resultsStart < len(s.resultEnds) && s.resultEnds[s.resultsStart] <= s.emittedRows {
		s.resultsStart++
	}
	e.compactResults()
	return nil
}

// compactResults drops the results of partitions whose rows are all emitted.
func (e *PipelinedWindowExec) compactResults() {
	s := e.wholePartition
	switch {
	case s.resultsStart == len(s.resultEnds):
		s.results.Reset()
		s.resultEnds = s.resultEnds[:0]
	case s.resultsStart >= e.MaxChunkSize() && s.resultsStart*2 >= len(s.resultEnds):
		remaining := chunk.NewEmptyChunk(e.RetFieldTypes())
		for i := s.resultsStart; i < len(s.resultEnds); i++ {
			remaining.AppendPartialRowByColIdxs(s.numInputCols, s.results.GetRow(i), s.windowColIdxs)
		}
		s.results = remaining
		s.resultEnds = append(s.resultEnds[:0], s.resultEnds[s.resultsStart:]...)
	default:
		return
	}
	s.resultsStart = 0
	e.trackResultsMemUsage()
}

func (e *PipelinedWindowExec) trackResultsMemUsage() {
	s := e.wholePartition
	newMemUsage := s.results.MemoryUsage() + int64(cap(s.resultEnds))*8
	e.memTracker.consume(newMemUsage - s.resultsMemUsage)
	s.resultsMemUsage = newMemUsage
}

// fetchChildForWholePartition reads one chunk from the child, consumes its
// rows into the partial results, finishes the partitions that end in it and
// buffers its input columns.
func (e *PipelinedWindowExec) fetchChildForWholePartition(ctx context.Context) error {
	s := e.wholePartition
	childResult := exec.TryNewCacheChunk(e.Children(0))
	if err := exec.Next(ctx, e.Children(0), childResult); err != nil {
		return errors.Trace(err)
	}
	if childResult.NumRows() == 0 {
		s.childDrained = true
		return e.finishWholePartition()
	}
	isFirstGroupSameAsPrev, err := e.groupChecker.SplitIntoGroups(childResult)
	if err != nil {
		return errors.Trace(err)
	}
	for first := true; !e.groupChecker.IsExhausted(); first = false {
		begin, end := e.groupChecker.GetNextGroup()
		if !first || !isFirstGroupSameAsPrev {
			if err := e.finishWholePartition(); err != nil {
				return err
			}
		}
		if err := e.consumeGroupRows(childResult, begin, end); err != nil {
			return err
		}
	}
	return e.bufferInputChunk(childResult)
}

func (e *PipelinedWindowExec) consumeGroupRows(childResult *chunk.Chunk, begin, end int) error {
	s := e.wholePartition
	oldCap := cap(s.rowBuf)
	s.rowBuf = s.rowBuf[:0]
	for i := begin; i < end; i++ {
		s.rowBuf = append(s.rowBuf, childResult.GetRow(i))
	}
	if cap(s.rowBuf) != oldCap {
		newMemUsage := int64(cap(s.rowBuf)) * aggfuncs.DefRowSize
		e.memTracker.consume(newMemUsage - s.rowBufMemUsage)
		s.rowBufMemUsage = newMemUsage
	}
	for i, wf := range e.windowFuncs {
		if err := updatePartialResultAndTrackMemory(e.memTracker, i, wf, e.Ctx(), s.rowBuf, e.partialResults[i]); err != nil {
			return err
		}
	}
	s.bufferedRows += uint64(end - begin)
	return nil
}

// finishWholePartition records the results of the current partition, if it
// has any row, and resets the partial results for the next partition.
func (e *PipelinedWindowExec) finishWholePartition() error {
	s := e.wholePartition
	if s.bufferedRows == s.finishedRows {
		return nil
	}
	evalCtx := e.Ctx().GetExprCtx().GetEvalCtx()
	for i, wf := range e.windowFuncs {
		if err := wf.AppendFinalResult2Chunk(evalCtx, e.partialResults[i], s.results); err != nil {
			return err
		}
	}
	resetPartialResultsAndReleaseMemory(e.memTracker, e.windowFuncs, e.partialResults)
	s.resultEnds = append(s.resultEnds, s.bufferedRows)
	s.finishedRows = s.bufferedRows
	e.trackResultsMemUsage()
	return nil
}

// bufferInputChunk buffers the non-window output columns of childResult, and
// spills the buffered chunks if requested.
func (e *PipelinedWindowExec) bufferInputChunk(childResult *chunk.Chunk) error {
	s := e.wholePartition
	inputChk := chunk.NewEmptyChunk(s.inputTypes)
	if err := e.copyChk(childResult, inputChk); err != nil {
		return err
	}
	// While inDisk is not nil, pending is empty and new chunks go to disk
	// anyway.
	if s.inDisk == nil && s.needSpill.Load() {
		if err := e.spillPendingChunks(); err != nil {
			return err
		}
	}
	if s.inDisk != nil {
		return s.inDisk.Add(inputChk)
	}
	s.pending = append(s.pending, inputChk)
	s.pendingMemUsage.Add(inputChk.MemoryUsage())
	e.memTracker.consume(inputChk.MemoryUsage())
	return nil
}

// spillPendingChunks moves the pending chunks to disk. head stays in memory
// because it is the next chunk to emit.
func (e *PipelinedWindowExec) spillPendingChunks() error {
	s := e.wholePartition
	s.inDisk = chunk.NewDataInDiskByChunks(s.inputTypes, "")
	if s.diskTracker != nil {
		s.inDisk.GetDiskTracker().AttachTo(s.diskTracker)
	}
	for i, chk := range s.pending {
		if err := s.inDisk.Add(chk); err != nil {
			return err
		}
		e.memTracker.consume(-chk.MemoryUsage())
		s.pending[i] = nil
	}
	s.pending = nil
	s.pendingMemUsage.Store(0)
	s.needSpill.Store(false)
	return nil
}
