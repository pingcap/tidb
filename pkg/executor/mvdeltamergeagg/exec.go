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

package mvdeltamergeagg

import (
	"bytes"
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"golang.org/x/sync/errgroup"
)

// Mapping is one aggregate merge rule.
type Mapping struct {
	// AggFunc must be non-nil.
	AggFunc *aggregation.AggFuncDesc
	// ColID are output column IDs in child schema.
	ColID []int
	// DependencyColID are dependency column IDs.
	// A dependency is either a delta-agg column (< DeltaAggColCount) or a
	// previously computed output column.
	DependencyColID []int
}

// MinMaxRecomputeStrategy is the recompute mode for MIN/MAX fallback.
type MinMaxRecomputeStrategy uint8

const (
	// MinMaxRecomputeSingleRow recomputes one group key per execution.
	MinMaxRecomputeSingleRow MinMaxRecomputeStrategy = iota + 1
	// MinMaxRecomputeBatch recomputes multiple group keys in one execution.
	MinMaxRecomputeBatch
)

// MinMaxBatchLookupContent is one batch lookup key.
type MinMaxBatchLookupContent struct {
	// Keys are group-key datums in planner-defined key order.
	Keys []types.Datum
}

// MinMaxBatchBuildRequest is the input to build one batch recompute executor.
type MinMaxBatchBuildRequest struct {
	// LookupKeys are deduplicated keys for this batch.
	LookupKeys []*MinMaxBatchLookupContent
}

// MinMaxBatchExecBuilder builds one opened recompute executor for one batch request.
type MinMaxBatchExecBuilder interface {
	Build(context.Context, *MinMaxBatchBuildRequest) (exec.Executor, error)
}

// MinMaxRecomputeSingleRowWorker is one worker-local single-row slot.
type MinMaxRecomputeSingleRowWorker struct {
	// KeyCols are correlated key columns bound to this worker slot.
	KeyCols []*expression.CorrelatedColumn
	// Exec is the worker-local reusable executor.
	Exec exec.Executor
}

// MinMaxRecomputeSingleRowExec stores single-row recompute worker slots.
type MinMaxRecomputeSingleRowExec struct {
	// Workers has one slot per MV merge worker.
	Workers []MinMaxRecomputeSingleRowWorker
}

// MinMaxRecomputeMapping is recompute metadata for one MIN/MAX mapping.
type MinMaxRecomputeMapping struct {
	// OutputColIDs are target output columns to be recomputed.
	// They are usually a subset of Mapping.ColID.
	OutputColIDs []int
	// Strategy is the recompute mode.
	Strategy MinMaxRecomputeStrategy
	// SingleRow is set when Strategy is MinMaxRecomputeSingleRow.
	SingleRow *MinMaxRecomputeSingleRowExec
	// BatchResultColIdxes are result-column indexes in batch recompute output.
	// It is valid only when Strategy is MinMaxRecomputeBatch.
	BatchResultColIdxes []int
}

// MinMaxRecomputeExec stores all MIN/MAX recompute metadata.
type MinMaxRecomputeExec struct {
	// KeyInputColIDs are group-key columns in child schema.
	KeyInputColIDs []int
	// Mappings is aligned with Exec.AggMappings by mapping index.
	// Non MIN/MAX positions should be nil.
	Mappings []*MinMaxRecomputeMapping
	// BatchBuilder creates one batch recompute executor.
	// Build must be safe for concurrent calls across MV merge workers.
	BatchBuilder MinMaxBatchExecBuilder
}

// RowOpType is the row write operation on MV table.
type RowOpType uint8

const (
	// RowOpNoOp means there is no MV row to touch.
	RowOpNoOp RowOpType = iota
	// RowOpInsert means insert a new MV row.
	RowOpInsert
	// RowOpUpdate means update an existing MV row.
	RowOpUpdate
	// RowOpDelete means delete an existing MV row.
	RowOpDelete
)

// RowOp is one row-level write action.
type RowOp struct {
	RowIdx int
	Tp     RowOpType
	// updateOrdinal is the index in update candidates for this chunk.
	// It is valid for rows that were initially update candidates.
	updateOrdinal int32
}

// ChunkResult is one worker result for one input chunk.
type ChunkResult struct {
	// Input is the original joined chunk.
	Input *chunk.Chunk
	// ComputedCols is indexed by input column ID. Nil means not computed.
	ComputedCols []*chunk.Column
	// RowOps are row write operations.
	RowOps []RowOp
	// UpdateTouchedBitmap stores touched-column bitmaps for update candidates.
	UpdateTouchedBitmap []uint8
	// UpdateTouchedStride is bytes per update row in UpdateTouchedBitmap.
	UpdateTouchedStride int
	// UpdateTouchedBitCnt is touched-bit count per update row.
	UpdateTouchedBitCnt int
}

// ResultWriter consumes merged chunk results.
type ResultWriter interface {
	WriteChunk(ctx context.Context, result *ChunkResult) error
}

// Exec is the sink executor for incremental MV merge.
type Exec struct {
	exec.BaseExecutor

	AggMappings      []Mapping
	DeltaAggColCount int
	// MinMaxRecompute stores MIN/MAX recompute metadata.
	MinMaxRecompute *MinMaxRecomputeExec

	WorkerCnt int
	Writer    ResultWriter

	TargetTable table.Table
	TargetInfo  *model.TableInfo
	// TargetHandleCols builds row handles for update/delete.
	TargetHandleCols plannerutil.HandleCols
	// TargetWritableColIDs maps writable target column index to input column index.
	TargetWritableColIDs []int

	compiledMergers      []aggMerger
	compiledOutputColCnt int
	aggOutputColIDs      []int
	prepared             bool
	executed             bool
}

type mvMergeAggWorkerData struct {
	updateRows      []int
	updateOpIndexes []int
	updateChanged   []bool
}

// Open implements the Executor interface.
func (e *Exec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	if e.WorkerCnt <= 0 {
		e.WorkerCnt = e.Ctx().GetSessionVars().ExecutorConcurrency
		if e.WorkerCnt <= 0 {
			e.WorkerCnt = 1
		}
	}
	if err := e.prepareMergers(); err != nil {
		return err
	}
	if e.Writer == nil {
		if e.TargetTable != nil {
			tblWriter, err := e.buildTableResultWriter()
			if err != nil {
				return err
			}
			e.Writer = tblWriter
		} else {
			e.Writer = noopWriter{}
		}
	}
	e.prepared = true
	e.executed = false
	return nil
}

// Next implements the Executor interface.
// Exec is a sink executor and always returns an empty chunk.
func (e *Exec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		return errors.New("Exec is not opened")
	}
	if e.executed {
		return nil
	}
	e.executed = true
	return e.runMergePipeline(ctx)
}

// Close implements the Executor interface.
func (e *Exec) Close() error {
	e.compiledMergers = nil
	e.compiledOutputColCnt = 0
	e.aggOutputColIDs = nil
	e.prepared = false
	e.executed = false
	return e.BaseExecutor.Close()
}

func (e *Exec) runMergePipeline(ctx context.Context) error {
	workerCnt := e.WorkerCnt
	if workerCnt <= 0 {
		workerCnt = 1
	}

	inputBufSize := max(workerCnt*2, 2)

	inputCh := make(chan *chunk.Chunk, inputBufSize)
	freeInputCh := make(chan *chunk.Chunk, inputBufSize)
	resultCh := make(chan *ChunkResult, workerCnt)

	for i := 0; i < inputBufSize; i++ {
		freeInputCh <- exec.NewFirstChunk(e.Children(0))
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return e.runReader(gctx, inputCh, freeInputCh)
	})

	workerWG := sync.WaitGroup{}
	workerWG.Add(workerCnt)
	for i := 0; i < workerCnt; i++ {
		g.Go(func() error {
			defer workerWG.Done()
			return e.runWorker(gctx, inputCh, resultCh)
		})
	}

	go func() {
		workerWG.Wait()
		close(resultCh)
	}()

	g.Go(func() error {
		return e.runWriter(gctx, resultCh, freeInputCh)
	})

	return g.Wait()
}

func (e *Exec) runReader(
	ctx context.Context,
	inputCh chan<- *chunk.Chunk,
	freeInputCh <-chan *chunk.Chunk,
) error {
	defer close(inputCh)
	child := e.Children(0)

	for {
		var chk *chunk.Chunk
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chk = <-freeInputCh:
		}

		chk.Reset()
		if err := exec.Next(ctx, child, chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case inputCh <- chk:
		}
	}
}

func (e *Exec) runWorker(
	ctx context.Context,
	inputCh <-chan *chunk.Chunk,
	resultCh chan<- *ChunkResult,
) error {
	var workerData mvMergeAggWorkerData
	for {
		var (
			chk *chunk.Chunk
			ok  bool
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chk, ok = <-inputCh:
			if !ok {
				return nil
			}
		}

		result, err := e.mergeOneChunk(chk, &workerData)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultCh <- result:
		}
	}
}

func (e *Exec) runWriter(
	ctx context.Context,
	resultCh <-chan *ChunkResult,
	freeInputCh chan<- *chunk.Chunk,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-resultCh:
			if !ok {
				return nil
			}
			if err := e.Writer.WriteChunk(ctx, result); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case freeInputCh <- result.Input:
			}
		}
	}
}

func isFirstAggCountAllRows(aggFunc *aggregation.AggFuncDesc) bool {
	if aggFunc == nil {
		return false
	}
	if aggFunc.Name != ast.AggFuncCount || aggFunc.HasDistinct {
		return false
	}
	if len(aggFunc.Args) != 1 {
		return false
	}
	con, ok := aggFunc.Args[0].(*expression.Constant)
	if !ok {
		return false
	}
	// Require a strict non-NULL compile-time constant.
	if con.ConstLevel() != expression.ConstStrict || con.Value.IsNull() {
		return false
	}
	return true
}

func aggFuncForErr(aggFunc *aggregation.AggFuncDesc) string {
	if aggFunc == nil {
		return "<nil>"
	}
	return aggFunc.StringWithCtx(exprctx.EmptyParamValues, errors.RedactLogDisable)
}

func isMinMaxAgg(aggName string) bool {
	return aggName == ast.AggFuncMin || aggName == ast.AggFuncMax
}

func containsColID(colIDs []int, target int) bool {
	for i := range colIDs {
		if colIDs[i] == target {
			return true
		}
	}
	return false
}

func (e *Exec) validateMinMaxRecompute(childTypes []*types.FieldType) error {
	if e.MinMaxRecompute == nil {
		return nil
	}
	meta := e.MinMaxRecompute
	if len(meta.KeyInputColIDs) == 0 {
		return errors.New("MinMaxRecompute requires non-empty KeyInputColIDs")
	}
	if len(meta.Mappings) != len(e.AggMappings) {
		return errors.Errorf("MinMaxRecompute.Mappings length mismatch: expect %d, got %d", len(e.AggMappings), len(meta.Mappings))
	}
	seenKey := make(map[int]struct{}, len(meta.KeyInputColIDs))
	for _, keyColID := range meta.KeyInputColIDs {
		if keyColID < 0 || keyColID >= len(childTypes) {
			return errors.Errorf("MinMaxRecompute key col %d out of range [0,%d)", keyColID, len(childTypes))
		}
		if _, dup := seenKey[keyColID]; dup {
			return errors.Errorf("duplicate MinMaxRecompute key col %d", keyColID)
		}
		seenKey[keyColID] = struct{}{}
	}
	hasBatch := false
	for mappingIdx := range e.AggMappings {
		agg := e.AggMappings[mappingIdx]
		if agg.AggFunc == nil {
			return errors.Errorf("MinMaxRecompute validation requires AggMappings[%d].AggFunc", mappingIdx)
		}
		aggName := agg.AggFunc.Name
		recompute := meta.Mappings[mappingIdx]
		if !isMinMaxAgg(aggName) {
			if recompute != nil {
				return errors.Errorf("MinMaxRecompute mapping %d is set for non-MIN/MAX agg=%s", mappingIdx, aggName)
			}
			continue
		}
		if recompute == nil {
			return errors.Errorf("missing MinMaxRecompute mapping for agg index %d (%s)", mappingIdx, aggName)
		}
		if len(recompute.OutputColIDs) == 0 {
			return errors.Errorf("MinMaxRecompute mapping %d requires non-empty OutputColIDs", mappingIdx)
		}
		seenOutput := make(map[int]struct{}, len(recompute.OutputColIDs))
		for _, outputColID := range recompute.OutputColIDs {
			if !containsColID(agg.ColID, outputColID) {
				return errors.Errorf("MinMaxRecompute mapping %d output col %d is not in AggMappings[%d].ColID", mappingIdx, outputColID, mappingIdx)
			}
			if _, dup := seenOutput[outputColID]; dup {
				return errors.Errorf("duplicate MinMaxRecompute output col %d in mapping %d", outputColID, mappingIdx)
			}
			seenOutput[outputColID] = struct{}{}
		}
		switch recompute.Strategy {
		case MinMaxRecomputeSingleRow:
			if recompute.SingleRow == nil {
				return errors.Errorf("MinMaxRecompute mapping %d requires SingleRow execution metadata", mappingIdx)
			}
			if len(recompute.BatchResultColIdxes) > 0 {
				return errors.Errorf("MinMaxRecompute mapping %d single-row strategy should not set BatchResultColIdxes", mappingIdx)
			}
			if e.WorkerCnt > 0 && len(recompute.SingleRow.Workers) != e.WorkerCnt {
				return errors.Errorf("MinMaxRecompute mapping %d single-row worker slots mismatch: expect %d, got %d", mappingIdx, e.WorkerCnt, len(recompute.SingleRow.Workers))
			}
			for workerIdx := range recompute.SingleRow.Workers {
				worker := recompute.SingleRow.Workers[workerIdx]
				if worker.Exec == nil {
					return errors.Errorf("MinMaxRecompute mapping %d single-row worker %d requires executor", mappingIdx, workerIdx)
				}
				if len(worker.KeyCols) != len(meta.KeyInputColIDs) {
					return errors.Errorf("MinMaxRecompute mapping %d single-row worker %d key column mismatch: expect %d, got %d", mappingIdx, workerIdx, len(meta.KeyInputColIDs), len(worker.KeyCols))
				}
			}
		case MinMaxRecomputeBatch:
			hasBatch = true
			if recompute.SingleRow != nil {
				return errors.Errorf("MinMaxRecompute mapping %d batch strategy should not set SingleRow metadata", mappingIdx)
			}
			if len(recompute.BatchResultColIdxes) == 0 {
				return errors.Errorf("MinMaxRecompute mapping %d batch strategy requires BatchResultColIdxes", mappingIdx)
			}
			if len(recompute.BatchResultColIdxes) != len(recompute.OutputColIDs) {
				return errors.Errorf("MinMaxRecompute mapping %d batch result column mismatch: OutputColIDs=%d BatchResultColIdxes=%d", mappingIdx, len(recompute.OutputColIDs), len(recompute.BatchResultColIdxes))
			}
			seenBatchResult := make(map[int]struct{}, len(recompute.BatchResultColIdxes))
			for _, resultColIdx := range recompute.BatchResultColIdxes {
				if resultColIdx < 0 {
					return errors.Errorf("MinMaxRecompute mapping %d batch result col idx %d must be non-negative", mappingIdx, resultColIdx)
				}
				if _, dup := seenBatchResult[resultColIdx]; dup {
					return errors.Errorf("duplicate batch result col idx %d in MinMaxRecompute mapping %d", resultColIdx, mappingIdx)
				}
				seenBatchResult[resultColIdx] = struct{}{}
			}
		default:
			return errors.Errorf("MinMaxRecompute mapping %d has unknown strategy %d", mappingIdx, recompute.Strategy)
		}
	}
	if hasBatch && meta.BatchBuilder == nil {
		return errors.New("MinMaxRecompute batch strategy requires BatchBuilder")
	}
	if !hasBatch && meta.BatchBuilder != nil {
		return errors.New("MinMaxRecompute BatchBuilder is set but no batch strategy mapping exists")
	}
	return nil
}

func (e *Exec) prepareMergers() error {
	if e.ChildrenLen() != 1 {
		return errors.Errorf("Exec expects exactly 1 child, got %d", e.ChildrenLen())
	}
	if len(e.AggMappings) == 0 {
		return errors.New("Exec requires non-empty AggMappings")
	}

	childTypes := exec.RetTypes(e.Children(0))
	if e.DeltaAggColCount < 0 || e.DeltaAggColCount > len(childTypes) {
		return errors.Errorf("DeltaAggColCount %d out of range [0,%d]", e.DeltaAggColCount, len(childTypes))
	}
	if err := e.validateMinMaxRecompute(childTypes); err != nil {
		return err
	}
	colID2ComputedIdx := make(map[int]int, len(e.AggMappings))
	e.compiledMergers = make([]aggMerger, 0, len(e.AggMappings))
	e.compiledOutputColCnt = 0
	e.aggOutputColIDs = make([]int, 0, len(e.AggMappings))

	firstAgg := e.AggMappings[0].AggFunc
	if firstAgg == nil {
		return errors.New("MVDeltaMergeAgg mapping requires AggFunc")
	}
	if !isFirstAggCountAllRows(firstAgg) {
		return errors.Errorf("the first MVDeltaMergeAgg mapping must be COUNT(*)/COUNT(non-NULL constant), got %s", aggFuncForErr(firstAgg))
	}
	if len(e.AggMappings[0].ColID) != 1 {
		return errors.Errorf("the first MVDeltaMergeAgg mapping must output exactly 1 column, got %d", len(e.AggMappings[0].ColID))
	}

	for i := range e.AggMappings {
		mapping := e.AggMappings[i]
		if mapping.AggFunc == nil {
			return errors.New("MVDeltaMergeAgg mapping requires AggFunc")
		}
		aggName := mapping.AggFunc.Name
		if len(mapping.ColID) == 0 {
			return errors.Errorf("mapping for agg=%s must output at least 1 column", aggName)
		}
		seenInMapping := make(map[int]struct{}, len(mapping.ColID))
		for _, outputColID := range mapping.ColID {
			if outputColID < 0 || outputColID >= len(childTypes) {
				return errors.Errorf("output col id %d out of range [0,%d)", outputColID, len(childTypes))
			}
			if _, dup := seenInMapping[outputColID]; dup {
				return errors.Errorf("duplicate output col id %d in one mapping", outputColID)
			}
			if _, dup := colID2ComputedIdx[outputColID]; dup {
				return errors.Errorf("duplicate output col id %d in AggMappings", outputColID)
			}
			seenInMapping[outputColID] = struct{}{}
		}

		var merger aggMerger
		var err error
		switch aggName {
		case ast.AggFuncCount:
			merger, err = e.buildCountMerger(mapping, colID2ComputedIdx, childTypes)
		case ast.AggFuncSum:
			merger, err = e.buildSumMerger(mapping, colID2ComputedIdx, childTypes)
		case ast.AggFuncMin, ast.AggFuncMax:
			err = errors.Errorf("%s merge is not implemented yet", aggName)
		default:
			err = errors.Errorf("unsupported agg function in Exec: %s", aggName)
		}
		if err != nil {
			return err
		}

		outputColIDs := merger.outputColIDs()
		if len(outputColIDs) != len(mapping.ColID) {
			return errors.Errorf("agg=%s output col count mismatch: mapping has %d but merger has %d", aggName, len(mapping.ColID), len(outputColIDs))
		}
		for idx, outputColID := range outputColIDs {
			if outputColID != mapping.ColID[idx] {
				return errors.Errorf("agg=%s output col mismatch at position %d: mapping=%d merger=%d", aggName, idx, mapping.ColID[idx], outputColID)
			}
			colID2ComputedIdx[outputColID] = e.compiledOutputColCnt
			e.compiledOutputColCnt++
			e.aggOutputColIDs = append(e.aggOutputColIDs, outputColID)
		}
		e.compiledMergers = append(e.compiledMergers, merger)
	}

	return nil
}

func (e *Exec) mergeOneChunk(chk *chunk.Chunk, workerData *mvMergeAggWorkerData) (*ChunkResult, error) {
	computedByOrder := make([]*chunk.Column, e.compiledOutputColCnt)
	computedBuiltCnt := 0
	computedByColID := make([]*chunk.Column, chk.NumCols())
	for _, merger := range e.compiledMergers {
		outputColIDs := merger.outputColIDs()
		outputCnt := len(outputColIDs)
		if outputCnt == 0 {
			return nil, errors.New("agg merger has no output columns")
		}
		if computedBuiltCnt+outputCnt > len(computedByOrder) {
			return nil, errors.Errorf("computed output overflow: built=%d current=%d total=%d", computedBuiltCnt, outputCnt, len(computedByOrder))
		}
		outputCols := computedByOrder[computedBuiltCnt : computedBuiltCnt+outputCnt]
		if err := merger.mergeChunk(chk, computedByOrder[:computedBuiltCnt], outputCols); err != nil {
			return nil, err
		}
		for idx, col := range outputCols {
			if col == nil {
				return nil, errors.Errorf("agg merger output column is nil at position %d", idx)
			}
			outputColID := outputColIDs[idx]
			computedByColID[outputColID] = col
		}
		computedBuiltCnt += outputCnt
	}
	if computedBuiltCnt != e.compiledOutputColCnt {
		return nil, errors.Errorf("computed output count mismatch, expected=%d got=%d", e.compiledOutputColCnt, computedBuiltCnt)
	}

	rowOps, updateTouchedBitmap, updateTouchedStride, updateTouchedBitCnt, err := e.buildRowOps(chk, computedByColID, workerData)
	if err != nil {
		return nil, err
	}
	return &ChunkResult{
		Input:               chk,
		ComputedCols:        computedByColID,
		RowOps:              rowOps,
		UpdateTouchedBitmap: updateTouchedBitmap,
		UpdateTouchedStride: updateTouchedStride,
		UpdateTouchedBitCnt: updateTouchedBitCnt,
	}, nil
}

func (e *Exec) buildRowOps(input *chunk.Chunk, computedByColID []*chunk.Column, workerData *mvMergeAggWorkerData) ([]RowOp, []uint8, int, int, error) {
	if len(e.aggOutputColIDs) == 0 {
		return nil, nil, 0, 0, errors.New("no aggregate outputs in Exec")
	}
	countStarOutputColID := e.aggOutputColIDs[0]
	if countStarOutputColID < 0 || countStarOutputColID >= input.NumCols() {
		return nil, nil, 0, 0, errors.Errorf("count(*) output col %d out of input chunk range", countStarOutputColID)
	}
	if countStarOutputColID >= len(computedByColID) {
		return nil, nil, 0, 0, errors.Errorf("count(*) output col %d out of computed-by-col-id range [0,%d)", countStarOutputColID, len(computedByColID))
	}
	newCountStarCol := computedByColID[countStarOutputColID]
	if newCountStarCol == nil {
		return nil, nil, 0, 0, errors.Errorf("count(*) output col %d is missing in computed columns", countStarOutputColID)
	}

	oldCountStarCol := input.Column(countStarOutputColID)
	newCountStarVals := newCountStarCol.Int64s()
	rowOps := make([]RowOp, 0, input.NumRows())

	var updateRows []int
	var updateOpIndexes []int
	if workerData != nil {
		updateRows = workerData.updateRows[:0]
		updateOpIndexes = workerData.updateOpIndexes[:0]
	}
	for rowIdx := 0; rowIdx < input.NumRows(); rowIdx++ {
		newCount := newCountStarVals[rowIdx]
		if newCount < 0 {
			return nil, nil, 0, 0, errors.Errorf("count(*) becomes negative (%d) at row %d", newCount, rowIdx)
		}
		oldExists := !oldCountStarCol.IsNull(rowIdx)

		switch {
		case newCount == 0:
			if oldExists {
				rowOps = append(rowOps, RowOp{RowIdx: rowIdx, Tp: RowOpDelete, updateOrdinal: -1})
			}
		case !oldExists:
			rowOps = append(rowOps, RowOp{RowIdx: rowIdx, Tp: RowOpInsert, updateOrdinal: -1})
		default:
			updateOrdinal := len(updateRows)
			rowOps = append(rowOps, RowOp{
				RowIdx:        rowIdx,
				Tp:            RowOpUpdate,
				updateOrdinal: int32(updateOrdinal),
			})
			updateRows = append(updateRows, rowIdx)
			updateOpIndexes = append(updateOpIndexes, len(rowOps)-1)
		}
	}

	if workerData != nil {
		workerData.updateRows = updateRows
		workerData.updateOpIndexes = updateOpIndexes
	}

	updateTouchedBitCnt := len(e.aggOutputColIDs)
	updateTouchedStride := (updateTouchedBitCnt + 7) >> 3
	updateCnt := len(updateRows)
	if updateCnt == 0 {
		return rowOps, nil, updateTouchedStride, updateTouchedBitCnt, nil
	}
	updateTouchedBitmap := make([]uint8, updateCnt*updateTouchedStride)

	var updateChanged []bool
	if workerData != nil {
		updateChanged = workerData.updateChanged
	}
	if cap(updateChanged) < updateCnt {
		updateChanged = make([]bool, updateCnt)
	} else {
		updateChanged = updateChanged[:updateCnt]
		clear(updateChanged)
	}
	if workerData != nil {
		workerData.updateChanged = updateChanged
	}

	fieldTypes := e.Children(0).RetFieldTypes()
	for bitPos, colID := range e.aggOutputColIDs {
		if colID < 0 || colID >= input.NumCols() {
			return nil, nil, 0, 0, errors.Errorf("agg output col %d out of input chunk range", colID)
		}
		if colID >= len(computedByColID) {
			return nil, nil, 0, 0, errors.Errorf("agg output col %d out of computed-by-col-id range [0,%d)", colID, len(computedByColID))
		}
		if colID >= len(fieldTypes) {
			return nil, nil, 0, 0, errors.Errorf("agg output col %d out of field type range [0,%d)", colID, len(fieldTypes))
		}
		oldCol := input.Column(colID)
		newCol := computedByColID[colID]
		if newCol == nil {
			return nil, nil, 0, 0, errors.Errorf("computed agg col %d is nil", colID)
		}
		if err := markUpdateTouchedRowsByColumn(
			updateRows,
			updateChanged,
			updateTouchedBitmap,
			updateTouchedStride,
			bitPos,
			oldCol,
			newCol,
			fieldTypes[colID],
		); err != nil {
			return nil, nil, 0, 0, err
		}
	}

	for updateOrdinal, opIdx := range updateOpIndexes {
		if !updateChanged[updateOrdinal] {
			rowOps[opIdx].Tp = RowOpNoOp
		}
	}
	return rowOps, updateTouchedBitmap, updateTouchedStride, updateTouchedBitCnt, nil
}

func markUpdateTouchedRowsByColumn(
	updateRows []int,
	updateChanged []bool,
	updateTouchedBitmap []uint8,
	updateTouchedStride int,
	updateBitPos int,
	oldCol *chunk.Column,
	newCol *chunk.Column,
	ft *types.FieldType,
) error {
	if ft == nil {
		return errors.New("field type is nil when comparing aggregate outputs")
	}
	if len(updateRows) == 0 {
		return nil
	}
	bitMask := uint8(1 << (updateBitPos & 7))
	bitByteOffset := updateBitPos >> 3
	singleByteStride := updateTouchedStride == 1

	switch ft.EvalType() {
	case types.ETInt:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			oldVals := oldCol.Uint64s()
			newVals := newCol.Uint64s()
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						updateChanged[updateOrdinal] = true
						if singleByteStride {
							updateTouchedBitmap[updateOrdinal] |= bitMask
						} else {
							updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
						}
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
			}
			return nil
		}
		oldVals := oldCol.Int64s()
		newVals := newCol.Int64s()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETReal:
		if ft.GetType() == mysql.TypeFloat {
			oldVals := oldCol.Float32s()
			newVals := newCol.Float32s()
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						updateChanged[updateOrdinal] = true
						if singleByteStride {
							updateTouchedBitmap[updateOrdinal] |= bitMask
						} else {
							updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
						}
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
			}
			return nil
		}
		oldVals := oldCol.Float64s()
		newVals := newCol.Float64s()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETDecimal:
		oldVals := oldCol.Decimals()
		newVals := newCol.Decimals()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx].Compare(&newVals[rowIdx]) != 0 {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETString:
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETDatetime, types.ETTimestamp:
		oldVals := oldCol.Times()
		newVals := newCol.Times()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETDuration:
		oldVals := oldCol.GoDurations()
		newVals := newCol.GoDurations()
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	case types.ETJson, types.ETVectorFloat32:
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					updateChanged[updateOrdinal] = true
					if singleByteStride {
						updateTouchedBitmap[updateOrdinal] |= bitMask
					} else {
						updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
					}
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				updateChanged[updateOrdinal] = true
				if singleByteStride {
					updateTouchedBitmap[updateOrdinal] |= bitMask
				} else {
					updateTouchedBitmap[updateOrdinal*updateTouchedStride+bitByteOffset] |= bitMask
				}
			}
		}
		return nil
	default:
		return errors.Errorf("unsupported eval type %d in aggregate change comparison", ft.EvalType())
	}
}

func markChangedRowsByColumn(changedMask []bool, oldCol, newCol *chunk.Column, ft *types.FieldType) error {
	if ft == nil {
		return errors.New("field type is nil when comparing aggregate outputs")
	}
	rowCnt := len(changedMask)
	switch ft.EvalType() {
	case types.ETInt:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			oldVals := oldCol.Uint64s()
			newVals := newCol.Uint64s()
			for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						changedMask[rowIdx] = true
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					changedMask[rowIdx] = true
				}
			}
			return nil
		}
		oldVals := oldCol.Int64s()
		newVals := newCol.Int64s()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETReal:
		if ft.GetType() == mysql.TypeFloat {
			oldVals := oldCol.Float32s()
			newVals := newCol.Float32s()
			for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						changedMask[rowIdx] = true
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					changedMask[rowIdx] = true
				}
			}
			return nil
		}
		oldVals := oldCol.Float64s()
		newVals := newCol.Float64s()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETDecimal:
		oldVals := oldCol.Decimals()
		newVals := newCol.Decimals()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx].Compare(&newVals[rowIdx]) != 0 {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETString:
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETDatetime, types.ETTimestamp:
		oldVals := oldCol.Times()
		newVals := newCol.Times()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETDuration:
		oldVals := oldCol.GoDurations()
		newVals := newCol.GoDurations()
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				changedMask[rowIdx] = true
			}
		}
		return nil
	case types.ETJson, types.ETVectorFloat32:
		for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					changedMask[rowIdx] = true
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				changedMask[rowIdx] = true
			}
		}
		return nil
	default:
		return errors.Errorf("unsupported eval type %d in aggregate change comparison", ft.EvalType())
	}
}

func chunkRowColDatum(col *chunk.Column, rowIdx int, ft *types.FieldType) types.Datum {
	var d types.Datum
	if col.IsNull(rowIdx) {
		d.SetNull()
		return d
	}
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			d.SetUint64(col.GetUint64(rowIdx))
		} else {
			d.SetInt64(col.GetInt64(rowIdx))
		}
	case mysql.TypeYear:
		d.SetInt64(col.GetInt64(rowIdx))
	case mysql.TypeFloat:
		d.SetFloat32(col.GetFloat32(rowIdx))
	case mysql.TypeDouble:
		d.SetFloat64(col.GetFloat64(rowIdx))
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetString(col.GetString(rowIdx), ft.GetCollate())
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		d.SetMysqlTime(col.GetTime(rowIdx))
	case mysql.TypeDuration:
		d.SetMysqlDuration(col.GetDuration(rowIdx, ft.GetDecimal()))
	case mysql.TypeNewDecimal:
		dec := col.GetDecimal(rowIdx)
		d.SetMysqlDecimal(dec)
		d.SetLength(ft.GetFlen())
		if ft.GetDecimal() == types.UnspecifiedLength {
			d.SetFrac(int(dec.GetDigitsFrac()))
		} else {
			d.SetFrac(ft.GetDecimal())
		}
	case mysql.TypeEnum:
		d.SetMysqlEnum(col.GetEnum(rowIdx), ft.GetCollate())
	case mysql.TypeSet:
		d.SetMysqlSet(col.GetSet(rowIdx), ft.GetCollate())
	case mysql.TypeBit:
		d.SetMysqlBit(col.GetBytes(rowIdx))
	case mysql.TypeJSON:
		d.SetMysqlJSON(col.GetJSON(rowIdx))
	case mysql.TypeTiDBVectorFloat32:
		d.SetVectorFloat32(col.GetVectorFloat32(rowIdx))
	default:
		d.SetBytes(col.GetRaw(rowIdx))
	}
	return d
}
