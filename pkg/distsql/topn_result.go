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

package distsql

import (
	"container/heap"
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

// NewTopnSelectResults creates a new TopnSelectResults.
func NewTopnSelectResults(
	ectx expression.EvalContext,
	selectResult []SelectResult,
	schema *expression.Schema,
	byItems []*util.ByItems,
	memTracker *memory.Tracker,
	offset int,
	count int,
	maxChunkSize int,
	handleIdx []int,
	inputOffsets []uint32,
) SelectResult {
	tsr := &topnSelectResults{
		evalCtx:       exprstatic.NewEvalContext(),
		schema:        schema,
		selectResult:  selectResult,
		byItems:       byItems,
		memTracker:    memTracker,
		offset:        offset,
		count:         count,
		handleIdx:     handleIdx,
		inputOffsets:  inputOffsets,
		heap:          &topNChunkHeap{},
		resultChannel: make(chan rowWithError, maxChunkSize),
	}

	tsr.initCompareFuncs(ectx)
	tsr.initEvaluator()
	tsr.initChunks(ectx, maxChunkSize)
	return tsr
}

func (tsr *topnSelectResults) getOrderByExpressions() []expression.Expression {
	exprs := make([]expression.Expression, 0, len(tsr.byItems))
	for _, byItem := range tsr.byItems {
		exprs = append(exprs, byItem.Expr)
	}
	return exprs
}

// Initializes the evaluator for ORDER BY expressions.
func (tsr *topnSelectResults) initEvaluator() {
	exprs := tsr.getOrderByExpressions()
	tsr.evaluator = expression.NewEvaluatorSuiteTopN(exprs, tsr.inputOffsets, tsr.handleIdx)
}

// Initializes comparison functions for sorting.
func (tsr *topnSelectResults) initCompareFuncs(ectx expression.EvalContext) {
	tsr.compareFuncs = make([]chunk.CompareFunc, len(tsr.byItems))
	for i, item := range tsr.byItems {
		keyType := item.Expr.GetType(ectx)
		tsr.compareFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (*topnSelectResults) NextRaw(context.Context) ([]byte, error) {
	panic("Not support NextRaw for sortedSelectResults")
}

func (tsr *topnSelectResults) compareRow(rowI, rowJ chunk.Row) int {
	// Compare rows using ORDER BY expressions
	for i, byItem := range tsr.byItems {
		cmpFunc := tsr.compareFuncs[i]
		colIdx := i
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if byItem.Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// Implements row comparison logic for sorting.
func (tsr *topnSelectResults) greaterRow(rowI, rowJ chunk.Row) bool {
	// Compare rows using ORDER BY expressions
	for i, byItem := range tsr.byItems {
		cmpFunc := tsr.compareFuncs[i]
		colIdx := i
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if byItem.Desc {
			cmp = -cmp
		}
		if cmp > 0 {
			return true
		} else if cmp < 0 {
			return false
		}
	}
	return false
}

// Implements Close() to clean up resources.
func (tsr *topnSelectResults) Close() error {
	var firstErr error
	for _, sr := range tsr.selectResult {
		if err := sr.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	tsr.heap.clear()
	if tsr.inputChunk != nil {
		tsr.inputChunk.Reset()
	}
	if tsr.outputChunk != nil {
		tsr.outputChunk.Reset()
	}
	return firstErr
}

// Initializes input and output chunks.
func (tsr *topnSelectResults) initChunks(ectx expression.EvalContext, maxChunkSize int) {
	inputFieldTypes := make([]*types.FieldType, len(tsr.inputOffsets))
	for i, colidx := range tsr.inputOffsets {
		inputFieldTypes[i] = tsr.schema.Columns[colidx].GetStaticType()
	}
	outputFieldTypes := tsr.buildFieldTypes(ectx)

	tsr.inputChunk = chunk.NewChunkWithCapacity(inputFieldTypes, maxChunkSize)
	tsr.outputChunk = chunk.NewChunkWithCapacity(outputFieldTypes, maxChunkSize)
}

// Builds field types for ORDER BY expressions.
func (tsr *topnSelectResults) buildFieldTypes(ectx expression.EvalContext) []*types.FieldType {
	// Get field types for ORDER BY expressions
	orderByFieldTypes := make([]*types.FieldType, 0, len(tsr.byItems))
	for _, item := range tsr.byItems {
		orderByFieldTypes = append(orderByFieldTypes, item.Expr.GetType(ectx))
	}
	indexMap := make(map[uint32]int)
	for newIndex, oldIndex := range tsr.inputOffsets {
		indexMap[oldIndex] = newIndex
	}
	// Check which handleIdx are NOT already in tsr.byItems
	handleFieldTypes := make([]*types.FieldType, 0, len(tsr.handleIdx))
handleLoop:
	for _, idx := range tsr.handleIdx {
		//Check if this index column already exists in byItems
		for _, item := range tsr.byItems {
			if col, isCol := item.Expr.(*expression.Column); isCol && col.Index == indexMap[uint32(idx)] {
				continue handleLoop
			}
		}
		//If not found in byItems, add it
		handleFieldTypes = append(handleFieldTypes, tsr.schema.Columns[idx].GetStaticType())
	}

	return append(orderByFieldTypes, handleFieldTypes...)
}

type topnSelectResults struct {
	evaluator       *expression.EvaluatorSuite
	evalCtx         *exprstatic.EvalContext
	schema          *expression.Schema
	selectResult    []SelectResult
	compareFuncs    []chunk.CompareFunc
	byItems         []*util.ByItems
	memTracker      *memory.Tracker
	offset          int
	count           int
	handleIdx       []int
	inputChunk      *chunk.Chunk
	outputChunk     *chunk.Chunk
	heap            *topNChunkHeap
	resultChannel   chan rowWithError
	heapInitialized int32
	inputOffsets    []uint32
}

// Fetches chunks of data.
func (tsr *topnSelectResults) fetchChunks(ctx context.Context) error {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(tsr.resultChannel, r)
			close(tsr.resultChannel)
		}
	}()

	err := tsr.loadChunksUntilTotalLimit(ctx)
	if err != nil {
		close(tsr.resultChannel)
		return err
	}

	go tsr.executeTopN(ctx)
	return nil
}

// Loads chunks until the total limit is reached.
func (tsr *topnSelectResults) loadChunksUntilTotalLimit(ctx context.Context) error {
	outputFieldTypes := tsr.buildFieldTypes(tsr.evalCtx)
	tsr.heap.init(tsr.compareRow, tsr.memTracker, tsr.outputChunk.Capacity(), uint64(tsr.count+tsr.offset), tsr.greaterRow, outputFieldTypes)

	for _, result := range tsr.selectResult {
		for uint64(tsr.heap.rowChunks.Len()) < tsr.heap.totalLimit {
			srcChk := tsr.inputChunk
			srcChk.Reset()
			// Adjust required rows by total limit
			srcChk.SetRequiredRows(int(tsr.heap.totalLimit-uint64(tsr.heap.rowChunks.Len())), tsr.inputChunk.Capacity())
			err := result.Next(ctx, srcChk)
			if err != nil {
				return err
			}
			if srcChk.NumRows() == 0 {
				break
			}
			expectedRows := min(srcChk.NumRows(), tsr.outputChunk.Capacity())
			rowchunk := chunk.NewChunkWithCapacity(outputFieldTypes, expectedRows)
			err = tsr.evaluator.Run(tsr.evalCtx, false, srcChk, rowchunk)
			if err != nil {
				panic(fmt.Sprintf("EvaluatorSuite Run failed: %v", err))
			}
			if rowchunk.NumRows() > 0 {
				tsr.heap.rowChunks.Add(rowchunk)
			}
		}
	}
	err := tsr.heap.initPtrs()
	if err != nil {
		return err
	}
	return nil
}

// Executes the Top-N algorithm when no spill is triggered.
func (tsr *topnSelectResults) executeTopNWhenNoSpillTriggered(ctx context.Context) error {
	childRowChk := tsr.inputChunk
	childRowChk.Reset()
	for _, result := range tsr.selectResult {
		for {
			err := result.Next(ctx, childRowChk)
			if err != nil {
				return err
			}
			if childRowChk.NumRows() == 0 {
				break
			}

			tsr.heap.processChk(childRowChk, tsr)
			if tsr.heap.rowChunks.Len() > len(tsr.heap.rowPtrs)*tsr.heap.compactionFactor {
				err = tsr.heap.doCompaction(tsr)
				if err != nil {
					return err
				}
			}
		}
	}

	slices.SortFunc(tsr.heap.rowPtrs, tsr.heap.keyColumnsCompare)
	return nil
}

// Executes the Top-N algorithm.
func (tsr *topnSelectResults) executeTopN(ctx context.Context) {
	defer func() {
		close(tsr.resultChannel)
		if r := recover(); r != nil {
			tsr.resultChannel <- rowWithError{err: GetRecoverError(r)}
		}
	}()
	heap.Init(tsr.heap)
	for uint64(len(tsr.heap.rowPtrs)) > tsr.heap.totalLimit {
		heap.Pop(tsr.heap)
	}

	if len(tsr.heap.rowPtrs) == 0 {
		return
	}

	err := tsr.executeTopNWhenNoSpillTriggered(ctx)
	if err != nil {
		tsr.resultChannel <- rowWithError{err: err}
		return
	}
	startIndex := tsr.offset
	if startIndex >= len(tsr.heap.rowPtrs) {
		return
	}
	for i := startIndex; i < len(tsr.heap.rowPtrs); i++ {
		select {
		case <-ctx.Done():
			return
		default:
			rowPtr := tsr.heap.rowPtrs[i]
			tsr.resultChannel <- rowWithError{row: tsr.heap.rowChunks.GetRow(rowPtr)}
		}
	}
}

// Processes panic and logs the error.
func processPanicAndLog(errOutputChan chan<- rowWithError, r any) {
	err := GetRecoverError(r)
	errOutputChan <- rowWithError{err: err}
	logutil.BgLogger().Error("executor panicked", zap.Error(err), zap.Stack("stack"))
}

// GetRecoverError returns the error recovered from a panic.
func GetRecoverError(r any) error {
	if err, ok := r.(error); ok {
		return errors.Trace(err)
	}
	return errors.Errorf("%v", r)
}

// Represents a row with an error.
type rowWithError struct {
	row chunk.Row
	err error
}

// Implements the Next method to fetch the next chunk.
func (tsr *topnSelectResults) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()

	if atomic.CompareAndSwapInt32(&tsr.heapInitialized, 0, 1) {
		err := tsr.fetchChunks(ctx)
		if err != nil {
			return err
		}
	}

	numToAppend := req.RequiredRows() - req.NumRows()
	for i := 0; i < numToAppend; i++ {
		row, ok := <-tsr.resultChannel
		if !ok {
			return nil
		}
		if row.err != nil {
			return row.err
		}
		req.AppendRow(row.row)
	}

	return nil
}

// topNChunkHeap implements a min-heap for top-N selection.
// The heap maintains the top N largest elements according to the greaterRow comparison.
type topNChunkHeap struct {
	compareRow       func(chunk.Row, chunk.Row) int
	greaterRow       func(chunk.Row, chunk.Row) bool
	rowChunks        *chunk.List
	rowPtrs          []chunk.RowPtr
	memTracker       *memory.Tracker
	totalLimit       uint64
	compactionFactor int
}

// Initializes the heap.
func (h *topNChunkHeap) init(compareRow func(chunk.Row, chunk.Row) int, memTracker *memory.Tracker, maxChunkSize int, totalLimit uint64, greaterRow func(chunk.Row, chunk.Row) bool, fieldTypes []*types.FieldType) {
	h.memTracker = memTracker
	h.compareRow = compareRow
	initialCapacity := min(int(totalLimit), maxChunkSize)
	h.rowChunks = chunk.NewList(fieldTypes, initialCapacity, maxChunkSize)
	h.rowChunks.GetMemTracker().AttachTo(memTracker)
	h.greaterRow = greaterRow
	h.totalLimit = totalLimit
	h.rowPtrs = make([]chunk.RowPtr, 0, initialCapacity)
	h.compactionFactor = 4
}

// Initializes pointers for the heap.
func (h *topNChunkHeap) initPtrs() error {
	totalRows := h.rowChunks.Len()
	memUsage := int64(chunk.RowPtrSize * totalRows)

	h.memTracker.Consume(memUsage)

	defer func() {
		if r := recover(); r != nil {
			h.memTracker.Consume(-memUsage)
			panic(r)
		}
	}()

	h.initPtrsImpl()
	return nil
}

// Initializes pointers implementation.
func (h *topNChunkHeap) initPtrsImpl() {
	totalRows := h.rowChunks.Len()
	h.rowPtrs = make([]chunk.RowPtr, 0, totalRows)

	for chkIdx := 0; chkIdx < h.rowChunks.NumChunks(); chkIdx++ {
		rowChk := h.rowChunks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			h.rowPtrs = append(h.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
}

func (h *topNChunkHeap) doCompaction(tsr *topnSelectResults) error {
	capacity := min(tsr.outputChunk.Capacity(), len(h.rowPtrs))
	newRowChunks := chunk.NewList(
		tsr.buildFieldTypes(tsr.evalCtx),
		capacity,
		tsr.count+tsr.offset,
	)
	newRowPtrs := make([]chunk.RowPtr, 0, len(h.rowPtrs))

	for _, ptr := range h.rowPtrs {
		row := h.rowChunks.GetRow(ptr)
		newPtr := newRowChunks.AppendRow(row)
		newRowPtrs = append(newRowPtrs, newPtr)
	}
	newRowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)
	h.memTracker.ReplaceChild(h.rowChunks.GetMemTracker(), newRowChunks.GetMemTracker())
	h.rowChunks = newRowChunks

	h.memTracker.Consume(int64(chunk.RowPtrSize * (len(newRowPtrs) - len(h.rowPtrs))))
	h.rowPtrs = newRowPtrs

	return nil
}

// Clears the heap.
func (h *topNChunkHeap) clear() {
	h.rowChunks.Clear()
	h.memTracker.Consume(int64(-chunk.RowPtrSize * len(h.rowPtrs)))
	h.rowPtrs = nil
}

// Updates the heap with a new row.
func (h *topNChunkHeap) update(heapMaxRow chunk.Row, newRow chunk.Row) {
	if h.greaterRow(heapMaxRow, newRow) {
		h.rowPtrs[0] = h.rowChunks.AppendRow(newRow)
		heap.Fix(h, 0)
	}
}

// Processes a chunk and updates the heap.
func (h *topNChunkHeap) processChk(chk *chunk.Chunk, tsr *topnSelectResults) {
	if tsr.outputChunk == nil {
		outputFieldTypes := tsr.buildFieldTypes(tsr.evalCtx)
		tsr.outputChunk = chunk.NewChunkWithCapacity(outputFieldTypes, chk.Capacity())
	} else if tsr.outputChunk.Capacity() < chk.Capacity() {
		outputFieldTypes := tsr.buildFieldTypes(tsr.evalCtx)
		tsr.outputChunk = chunk.NewChunkWithCapacity(outputFieldTypes, chk.Capacity())
	} else {
		tsr.outputChunk.Reset()
	}

	err := tsr.evaluator.Run(tsr.evalCtx, false, chk, tsr.outputChunk)
	if err != nil {
		panic(fmt.Sprintf("EvaluatorSuite Run failed: %v", err))
	}

	for i := 0; i < tsr.outputChunk.NumRows(); i++ {
		heapMaxRow := h.rowChunks.GetRow(h.rowPtrs[0])
		newRow := tsr.outputChunk.GetRow(i)
		h.update(heapMaxRow, newRow)
	}
}

// Compares key columns in the heap.
func (h *topNChunkHeap) keyColumnsCompare(i, j chunk.RowPtr) int {
	rowI := h.rowChunks.GetRow(i)
	rowJ := h.rowChunks.GetRow(j)
	return h.compareRow(rowI, rowJ)
}

// Less implements heap.Interface, returning true if row i is greater than row j.
func (h *topNChunkHeap) Less(i, j int) bool {
	rowI := h.rowChunks.GetRow(h.rowPtrs[i])
	rowJ := h.rowChunks.GetRow(h.rowPtrs[j])
	return h.greaterRow(rowI, rowJ)
}

func (h *topNChunkHeap) Len() int {
	return len(h.rowPtrs)
}

func (*topNChunkHeap) Push(any) {
	// Should never be called.
}

func (h *topNChunkHeap) Pop() any {
	h.rowPtrs = h.rowPtrs[:len(h.rowPtrs)-1]
	return nil // We don't need the popped value, return nil to avoid memory allocation.
}

func (h *topNChunkHeap) Swap(i, j int) {
	h.rowPtrs[i], h.rowPtrs[j] = h.rowPtrs[j], h.rowPtrs[i]
}
