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
	"time"

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
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
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
	// Build must return an opened executor.
	// The result schema must put group-key columns first, in the same order as MinMaxRecomputeExec.KeyInputColIDs.
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
	updateRows                   []int
	updateOpIndexes              []int
	updateChanged                []bool
	minMaxRecomputeRowsByMapping [][]int
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
		workerIdx := i
		g.Go(func() error {
			defer workerWG.Done()
			return e.runWorker(gctx, inputCh, resultCh, workerIdx)
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
	workerIdx int,
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

		result, err := e.mergeOneChunk(ctx, chk, &workerData, workerIdx)
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
				for keyIdx, keyCol := range worker.KeyCols {
					if keyCol == nil || keyCol.Data == nil {
						return errors.Errorf("MinMaxRecompute mapping %d single-row worker %d key column %d is not initialized", mappingIdx, workerIdx, keyIdx)
					}
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
			if len(recompute.BatchResultColIdxes) != len(agg.ColID) {
				return errors.Errorf("MinMaxRecompute mapping %d batch result column mismatch: Mapping.ColID=%d BatchResultColIdxes=%d", mappingIdx, len(agg.ColID), len(recompute.BatchResultColIdxes))
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
	minMaxOutputCols := make(map[int]struct{})
	for i := range e.AggMappings {
		mapping := e.AggMappings[i]
		if mapping.AggFunc == nil {
			continue
		}
		if !isMinMaxAgg(mapping.AggFunc.Name) {
			continue
		}
		for _, outputColID := range mapping.ColID {
			minMaxOutputCols[outputColID] = struct{}{}
		}
	}
	if len(minMaxOutputCols) > 0 {
		for mappingIdx := range e.AggMappings {
			mapping := e.AggMappings[mappingIdx]
			for _, depColID := range mapping.DependencyColID {
				if _, isMinMaxOutput := minMaxOutputCols[depColID]; isMinMaxOutput {
					return errors.Errorf(
						"AggMappings[%d] depends on MIN/MAX output col %d, which is unsupported in stage1",
						mappingIdx,
						depColID,
					)
				}
			}
		}
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
			if e.MinMaxRecompute == nil {
				err = errors.Errorf("%s merge requires MinMaxRecompute metadata", aggName)
				break
			}
			merger, err = e.buildMinMaxMerger(i, mapping, colID2ComputedIdx, childTypes)
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

func (e *Exec) mergeOneChunk(ctx context.Context, chk *chunk.Chunk, workerData *mvMergeAggWorkerData, workerIdx int) (*ChunkResult, error) {
	if workerData != nil {
		workerData.prepareMinMaxRecomputeRows(len(e.AggMappings))
	}
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
		if err := merger.mergeChunk(chk, computedByOrder[:computedBuiltCnt], outputCols, workerData); err != nil {
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
	if err := e.recomputeMinMaxRows(ctx, chk, computedByColID, workerData, workerIdx); err != nil {
		return nil, err
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

func (d *mvMergeAggWorkerData) prepareMinMaxRecomputeRows(mappingCnt int) {
	if mappingCnt <= 0 {
		d.minMaxRecomputeRowsByMapping = nil
		return
	}
	if len(d.minMaxRecomputeRowsByMapping) < mappingCnt {
		old := d.minMaxRecomputeRowsByMapping
		d.minMaxRecomputeRowsByMapping = make([][]int, mappingCnt)
		copy(d.minMaxRecomputeRowsByMapping, old)
	}
	d.minMaxRecomputeRowsByMapping = d.minMaxRecomputeRowsByMapping[:mappingCnt]
	for idx := range d.minMaxRecomputeRowsByMapping {
		d.minMaxRecomputeRowsByMapping[idx] = d.minMaxRecomputeRowsByMapping[idx][:0]
	}
}

func (e *Exec) recomputeMinMaxRows(
	ctx context.Context,
	input *chunk.Chunk,
	computedByColID []*chunk.Column,
	workerData *mvMergeAggWorkerData,
	workerIdx int,
) error {
	if e.MinMaxRecompute == nil || workerData == nil {
		return nil
	}
	if len(workerData.minMaxRecomputeRowsByMapping) == 0 {
		return nil
	}
	if len(e.aggOutputColIDs) == 0 {
		return errors.New("min/max recompute requires aggregate outputs")
	}
	countStarOutputColID := e.aggOutputColIDs[0]
	if countStarOutputColID < 0 || countStarOutputColID >= len(computedByColID) {
		return errors.Errorf("count(*) output col %d out of computed-by-col-id range [0,%d)", countStarOutputColID, len(computedByColID))
	}
	countStarCol := computedByColID[countStarOutputColID]
	if countStarCol == nil {
		return errors.Errorf("count(*) output col %d is missing in computed columns", countStarOutputColID)
	}
	countStarVals := countStarCol.Int64s()
	childTypes := e.Children(0).RetFieldTypes()
	overrides := make([]*mappingRecomputeOverride, len(e.AggMappings))
	batchMappings := make([]int, 0)

	for mappingIdx, rows := range workerData.minMaxRecomputeRowsByMapping {
		if len(rows) == 0 {
			continue
		}
		if mappingIdx < 0 || mappingIdx >= len(e.AggMappings) {
			return errors.Errorf("min/max recompute mapping idx %d out of range [0,%d)", mappingIdx, len(e.AggMappings))
		}
		recomputeMeta := e.MinMaxRecompute.Mappings[mappingIdx]
		if recomputeMeta == nil {
			return errors.Errorf("missing MinMaxRecompute mapping for agg index %d", mappingIdx)
		}
		switch recomputeMeta.Strategy {
		case MinMaxRecomputeSingleRow:
			if err := e.recomputeMinMaxSingleRow(
				ctx,
				input,
				childTypes,
				countStarVals,
				workerIdx,
				mappingIdx,
				rows,
				overrides,
			); err != nil {
				return err
			}
		case MinMaxRecomputeBatch:
			batchMappings = append(batchMappings, mappingIdx)
		default:
			return errors.Errorf("unknown MinMaxRecompute strategy %d for mapping %d", recomputeMeta.Strategy, mappingIdx)
		}
	}
	if len(batchMappings) > 0 {
		if err := e.recomputeMinMaxBatch(
			ctx,
			input,
			childTypes,
			countStarVals,
			batchMappings,
			workerData.minMaxRecomputeRowsByMapping,
			overrides,
		); err != nil {
			return err
		}
	}
	return e.applyMinMaxRecomputeOverrides(computedByColID, childTypes, overrides)
}

func (e *Exec) recomputeMinMaxSingleRow(
	ctx context.Context,
	input *chunk.Chunk,
	childTypes []*types.FieldType,
	countStarVals []int64,
	workerIdx int,
	mappingIdx int,
	recomputeRows []int,
	overrides []*mappingRecomputeOverride,
) error {
	recomputeMeta := e.MinMaxRecompute.Mappings[mappingIdx]
	if recomputeMeta == nil || recomputeMeta.SingleRow == nil {
		return errors.Errorf("single-row MinMaxRecompute metadata is missing for mapping %d", mappingIdx)
	}
	if workerIdx < 0 || workerIdx >= len(recomputeMeta.SingleRow.Workers) {
		return errors.Errorf(
			"min/max single-row worker idx %d out of range [0,%d) for mapping %d",
			workerIdx,
			len(recomputeMeta.SingleRow.Workers),
			mappingIdx,
		)
	}
	worker := recomputeMeta.SingleRow.Workers[workerIdx]
	if worker.Exec == nil {
		return errors.Errorf("min/max single-row worker %d executor is nil for mapping %d", workerIdx, mappingIdx)
	}
	keyColIDs := e.MinMaxRecompute.KeyInputColIDs
	if len(worker.KeyCols) != len(keyColIDs) {
		return errors.Errorf("min/max single-row key column mismatch for mapping %d: expect %d, got %d", mappingIdx, len(keyColIDs), len(worker.KeyCols))
	}
	keyTypes := make([]*types.FieldType, len(keyColIDs))
	for keyPos, keyColID := range keyColIDs {
		if keyColID < 0 || keyColID >= len(childTypes) {
			return errors.Errorf("min/max single-row key col id %d out of range [0,%d)", keyColID, len(childTypes))
		}
		if worker.KeyCols[keyPos] == nil || worker.KeyCols[keyPos].Data == nil {
			return errors.Errorf("min/max single-row worker %d key col %d is not initialized", workerIdx, keyPos)
		}
		keyTypes[keyPos] = childTypes[keyColID]
	}
	resultChk := exec.NewFirstChunk(worker.Exec)
	mapping := e.AggMappings[mappingIdx]
	if _, err := ensureMappingOverride(overrides, mappingIdx, len(mapping.ColID), len(recomputeRows)); err != nil {
		return err
	}
	rowValues := make([]types.Datum, len(mapping.ColID))

	for _, rowIdx := range recomputeRows {
		if rowIdx < 0 || rowIdx >= input.NumRows() {
			return errors.Errorf("min/max single-row recompute row idx %d out of range [0,%d)", rowIdx, input.NumRows())
		}
		if countStarVals[rowIdx] < 0 {
			return errors.Errorf("count(*) becomes negative (%d) at row %d", countStarVals[rowIdx], rowIdx)
		}
		if countStarVals[rowIdx] == 0 {
			continue
		}
		inputRow := input.GetRow(rowIdx)
		for keyPos, keyColID := range keyColIDs {
			keyDatum := inputRow.GetDatum(keyColID, keyTypes[keyPos])
			keyDatum.Copy(worker.KeyCols[keyPos].Data)
		}

		if err := exec.Open(ctx, worker.Exec); err != nil {
			return err
		}
		resultChk.Reset()
		nextErr := exec.Next(ctx, worker.Exec, resultChk)
		closeErr := worker.Exec.Close()
		if nextErr != nil {
			return nextErr
		}
		if closeErr != nil {
			return closeErr
		}
		if resultChk.NumRows() == 0 {
			return errors.Errorf("min/max single-row recompute returns no row for mapping %d row %d", mappingIdx, rowIdx)
		}

		resultRow := resultChk.GetRow(0)
		for colPos, outputColID := range mapping.ColID {
			if outputColID < 0 || outputColID >= len(childTypes) {
				return errors.Errorf("mapping %d output col id %d out of range [0,%d)", mappingIdx, outputColID, len(childTypes))
			}
			rowValues[colPos] = resultRow.GetDatum(colPos, childTypes[outputColID])
		}
		if err := appendMappingOverrideFromValues(overrides, mappingIdx, rowIdx, rowValues); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exec) recomputeMinMaxBatch(
	ctx context.Context,
	input *chunk.Chunk,
	childTypes []*types.FieldType,
	countStarVals []int64,
	batchMappings []int,
	recomputeRowsByMapping [][]int,
	overrides []*mappingRecomputeOverride,
) (retErr error) {
	if e.MinMaxRecompute.BatchBuilder == nil {
		return errors.New("min/max batch recompute requires BatchBuilder")
	}
	tz := e.Ctx().GetSessionVars().StmtCtx.TimeZone()
	keyColIDs := e.MinMaxRecompute.KeyInputColIDs
	keyTypes := make([]*types.FieldType, len(keyColIDs))
	for keyPos, keyColID := range keyColIDs {
		if keyColID < 0 || keyColID >= len(childTypes) {
			return errors.Errorf("min/max batch key col id %d out of range [0,%d)", keyColID, len(childTypes))
		}
		if childTypes[keyColID] == nil {
			return errors.Errorf("min/max batch key col id %d type is unavailable", keyColID)
		}
		keyTypes[keyPos] = childTypes[keyColID]
	}
	rowCnt := input.NumRows()

	lookupKeys := make([]*MinMaxBatchLookupContent, 0)
	keyIdxByEncoded := make(map[string]int)
	refsByLookupKey := make([][]batchRowRef, 0)
	seenByMapping := make([][]bool, len(e.AggMappings))
	keyBuffer := make([]types.Datum, len(keyColIDs))

	for _, mappingIdx := range batchMappings {
		rows := recomputeRowsByMapping[mappingIdx]
		if len(rows) == 0 {
			continue
		}
		mapping := e.AggMappings[mappingIdx]
		if _, err := ensureMappingOverride(overrides, mappingIdx, len(mapping.ColID), len(rows)); err != nil {
			return err
		}
		seenByMapping[mappingIdx] = make([]bool, rowCnt)
	}

	for _, mappingIdx := range batchMappings {
		rows := recomputeRowsByMapping[mappingIdx]
		for _, rowIdx := range rows {
			if rowIdx < 0 || rowIdx >= rowCnt {
				return errors.Errorf("min/max batch recompute row idx %d out of range [0,%d)", rowIdx, rowCnt)
			}
			if countStarVals[rowIdx] < 0 {
				return errors.Errorf("count(*) becomes negative (%d) at row %d", countStarVals[rowIdx], rowIdx)
			}
			if countStarVals[rowIdx] == 0 {
				continue
			}
			inputRow := input.GetRow(rowIdx)
			for keyPos, keyColID := range keyColIDs {
				d := inputRow.GetDatum(keyColID, keyTypes[keyPos])
				d.Copy(&keyBuffer[keyPos])
			}
			encodedKey, err := encodeDatumKey(tz, keyBuffer)
			if err != nil {
				return err
			}
			keyIdx, exists := keyIdxByEncoded[encodedKey]
			if !exists {
				keyIdx = len(lookupKeys)
				keyIdxByEncoded[encodedKey] = keyIdx
				keyDatums := make([]types.Datum, len(keyBuffer))
				for i := range keyBuffer {
					keyBuffer[i].Copy(&keyDatums[i])
				}
				lookupKeys = append(lookupKeys, &MinMaxBatchLookupContent{Keys: keyDatums})
				refsByLookupKey = append(refsByLookupKey, nil)
			}
			refsByLookupKey[keyIdx] = append(refsByLookupKey[keyIdx], batchRowRef{mappingIdx: mappingIdx, rowIdx: rowIdx})
		}
	}
	if len(lookupKeys) == 0 {
		return nil
	}

	batchExec, err := e.MinMaxRecompute.BatchBuilder.Build(ctx, &MinMaxBatchBuildRequest{LookupKeys: lookupKeys})
	if err != nil {
		return err
	}
	if batchExec == nil {
		return errors.New("min/max batch recompute builder returns nil executor")
	}
	defer func() {
		closeErr := batchExec.Close()
		if retErr == nil && closeErr != nil {
			retErr = closeErr
		}
	}()

	batchTypes := exec.RetTypes(batchExec)
	if len(batchTypes) < len(keyColIDs) {
		return errors.Errorf(
			"min/max batch recompute result schema too small: key columns=%d, result columns=%d",
			len(keyColIDs),
			len(batchTypes),
		)
	}

	resultKeyBuffer := make([]types.Datum, len(keyColIDs))
	resultChk := exec.NewFirstChunk(batchExec)
	for {
		resultChk.Reset()
		if retErr = exec.Next(ctx, batchExec, resultChk); retErr != nil {
			return retErr
		}
		if resultChk.NumRows() == 0 {
			break
		}
		for rowIdx := 0; rowIdx < resultChk.NumRows(); rowIdx++ {
			resultRow := resultChk.GetRow(rowIdx)
			for keyPos := range keyColIDs {
				resultRow.DatumWithBuffer(keyPos, batchTypes[keyPos], &resultKeyBuffer[keyPos])
			}
			encodedKey, err := encodeDatumKey(tz, resultKeyBuffer)
			if err != nil {
				return err
			}
			keyIdx, exists := keyIdxByEncoded[encodedKey]
			if !exists {
				continue
			}
			refs := refsByLookupKey[keyIdx]
			if len(refs) == 0 {
				continue
			}
			for _, ref := range refs {
				seenRows := seenByMapping[ref.mappingIdx]
				if seenRows == nil {
					return errors.Errorf("min/max batch recompute seen-rows bitmap is missing for mapping %d", ref.mappingIdx)
				}
				if seenRows[ref.rowIdx] {
					return errors.Errorf("min/max batch recompute has duplicate result for mapping %d row %d", ref.mappingIdx, ref.rowIdx)
				}
				recomputeMeta := e.MinMaxRecompute.Mappings[ref.mappingIdx]
				mapping := e.AggMappings[ref.mappingIdx]
				if err := appendMappingOverrideFromResultRow(
					overrides,
					ref.mappingIdx,
					ref.rowIdx,
					resultRow,
					childTypes,
					mapping,
					recomputeMeta.BatchResultColIdxes,
					len(batchTypes),
				); err != nil {
					return err
				}
				seenRows[ref.rowIdx] = true
			}
		}
	}

	for _, mappingIdx := range batchMappings {
		seenRows := seenByMapping[mappingIdx]
		if seenRows == nil {
			return errors.Errorf("min/max batch recompute seen-rows bitmap is missing for mapping %d", mappingIdx)
		}
		rows := recomputeRowsByMapping[mappingIdx]
		for _, rowIdx := range rows {
			if countStarVals[rowIdx] == 0 {
				continue
			}
			if !seenRows[rowIdx] {
				return errors.Errorf("min/max batch recompute misses row %d for mapping %d", rowIdx, mappingIdx)
			}
		}
	}
	return nil
}

func (e *Exec) applyMinMaxRecomputeOverrides(
	computedByColID []*chunk.Column,
	childTypes []*types.FieldType,
	overrides []*mappingRecomputeOverride,
) error {
	for mappingIdx, override := range overrides {
		if override == nil || len(override.rowIdxes) == 0 {
			continue
		}
		mapping := e.AggMappings[mappingIdx]
		if len(override.valuesByOutput) != len(mapping.ColID) {
			return errors.Errorf(
				"min/max override output count mismatch for mapping %d: override=%d mapping=%d",
				mappingIdx,
				len(override.valuesByOutput),
				len(mapping.ColID),
			)
		}
		for colPos, outputColID := range mapping.ColID {
			if outputColID < 0 || outputColID >= len(computedByColID) {
				return errors.Errorf("min/max override output col id %d out of range [0,%d)", outputColID, len(computedByColID))
			}
			if outputColID < 0 || outputColID >= len(childTypes) {
				return errors.Errorf("min/max override output col id %d out of field type range [0,%d)", outputColID, len(childTypes))
			}
			if len(override.valuesByOutput[colPos]) != len(override.rowIdxes) {
				return errors.Errorf(
					"min/max override row/value count mismatch for mapping %d output position %d: rows=%d values=%d",
					mappingIdx,
					colPos,
					len(override.rowIdxes),
					len(override.valuesByOutput[colPos]),
				)
			}
			oldCol := computedByColID[outputColID]
			if oldCol == nil {
				return errors.Errorf("min/max override target output col %d is nil", outputColID)
			}
			ft := childTypes[outputColID]
			if ft == nil {
				return errors.Errorf("min/max override output col %d type is unavailable", outputColID)
			}
			newCol, err := rebuildColumnWithOverrides(oldCol, ft, override.rowIdxes, override.valuesByOutput[colPos])
			if err != nil {
				return err
			}
			computedByColID[outputColID] = newCol
		}
	}
	return nil
}

type mappingRecomputeOverride struct {
	rowIdxes       []int
	valuesByOutput [][]types.Datum
}

type batchRowRef struct {
	mappingIdx int
	rowIdx     int
}

func ensureMappingOverride(
	overrides []*mappingRecomputeOverride,
	mappingIdx int,
	outputCnt int,
	rowCap int,
) (*mappingRecomputeOverride, error) {
	if mappingIdx < 0 || mappingIdx >= len(overrides) {
		return nil, errors.Errorf("mapping idx %d out of override range [0,%d)", mappingIdx, len(overrides))
	}
	if outputCnt < 0 {
		return nil, errors.Errorf("output count must be non-negative, got %d", outputCnt)
	}
	if rowCap < 0 {
		rowCap = 0
	}
	override := overrides[mappingIdx]
	if override == nil {
		override = &mappingRecomputeOverride{
			rowIdxes:       make([]int, 0, rowCap),
			valuesByOutput: make([][]types.Datum, outputCnt),
		}
		for i := range override.valuesByOutput {
			override.valuesByOutput[i] = make([]types.Datum, 0, rowCap)
		}
		overrides[mappingIdx] = override
		return override, nil
	}
	if len(override.valuesByOutput) != outputCnt {
		return nil, errors.Errorf(
			"override output column count mismatch for mapping %d: override=%d values=%d",
			mappingIdx,
			len(override.valuesByOutput),
			outputCnt,
		)
	}
	return override, nil
}

func appendMappingOverrideFromValues(
	overrides []*mappingRecomputeOverride,
	mappingIdx int,
	rowIdx int,
	values []types.Datum,
) error {
	override, err := ensureMappingOverride(overrides, mappingIdx, len(values), 0)
	if err != nil {
		return err
	}
	override.rowIdxes = append(override.rowIdxes, rowIdx)
	for idx := range values {
		var copied types.Datum
		values[idx].Copy(&copied)
		override.valuesByOutput[idx] = append(override.valuesByOutput[idx], copied)
	}
	return nil
}

func appendMappingOverrideFromResultRow(
	overrides []*mappingRecomputeOverride,
	mappingIdx int,
	rowIdx int,
	resultRow chunk.Row,
	childTypes []*types.FieldType,
	mapping Mapping,
	resultColIdxes []int,
	resultColCnt int,
) error {
	override, err := ensureMappingOverride(overrides, mappingIdx, len(mapping.ColID), 0)
	if err != nil {
		return err
	}
	if len(resultColIdxes) != len(mapping.ColID) {
		return errors.Errorf(
			"batch result column count mismatch for mapping %d: result=%d mapping=%d",
			mappingIdx,
			len(resultColIdxes),
			len(mapping.ColID),
		)
	}
	override.rowIdxes = append(override.rowIdxes, rowIdx)
	for colPos, outputColID := range mapping.ColID {
		resultColIdx := resultColIdxes[colPos]
		if resultColIdx < 0 || resultColIdx >= resultColCnt {
			return errors.Errorf(
				"min/max batch recompute result col idx %d out of range [0,%d) for mapping %d",
				resultColIdx,
				resultColCnt,
				mappingIdx,
			)
		}
		if outputColID < 0 || outputColID >= len(childTypes) {
			return errors.Errorf("mapping %d output col id %d out of range [0,%d)", mappingIdx, outputColID, len(childTypes))
		}
		var copied types.Datum
		d := resultRow.GetDatum(resultColIdx, childTypes[outputColID])
		d.Copy(&copied)
		override.valuesByOutput[colPos] = append(override.valuesByOutput[colPos], copied)
	}
	return nil
}

func encodeDatumKey(tz *time.Location, datums []types.Datum) (string, error) {
	encoded, err := codec.EncodeKey(tz, nil, datums...)
	if err != nil {
		return "", err
	}
	return string(hack.String(encoded)), nil
}

func rebuildColumnWithOverrides(
	oldCol *chunk.Column,
	ft *types.FieldType,
	rowIdxes []int,
	values []types.Datum,
) (*chunk.Column, error) {
	if oldCol == nil {
		return nil, errors.New("cannot rebuild nil column")
	}
	if len(rowIdxes) != len(values) {
		return nil, errors.Errorf("override row/value count mismatch: rows=%d values=%d", len(rowIdxes), len(values))
	}
	rowCnt := oldCol.Rows()
	newCol := chunk.NewColumn(ft, rowCnt)
	overridePos := 0
	prevOverrideRow := -1
	for rowIdx := 0; rowIdx < rowCnt; rowIdx++ {
		if overridePos < len(rowIdxes) {
			overrideRow := rowIdxes[overridePos]
			if overrideRow < prevOverrideRow {
				return nil, errors.Errorf("override rows are not in ascending order: prev=%d curr=%d", prevOverrideRow, overrideRow)
			}
			if overrideRow < rowIdx {
				return nil, errors.Errorf("override row %d repeats or is out of order", overrideRow)
			}
			if overrideRow == rowIdx {
				if err := appendDatumToColumn(newCol, &values[overridePos], ft); err != nil {
					return nil, err
				}
				prevOverrideRow = overrideRow
				overridePos++
				continue
			}
		}
		newCol.AppendCellNTimes(oldCol, rowIdx, 1)
	}
	if overridePos != len(rowIdxes) {
		return nil, errors.Errorf("override rows out of range: consumed=%d total=%d rows=%d", overridePos, len(rowIdxes), rowCnt)
	}
	return newCol, nil
}

func appendDatumToColumn(col *chunk.Column, d *types.Datum, ft *types.FieldType) error {
	if d == nil || d.IsNull() {
		col.AppendNull()
		return nil
	}
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			col.AppendUint64(d.GetUint64())
		} else {
			col.AppendInt64(d.GetInt64())
		}
	case mysql.TypeYear:
		col.AppendInt64(d.GetInt64())
	case mysql.TypeFloat:
		col.AppendFloat32(d.GetFloat32())
	case mysql.TypeDouble:
		col.AppendFloat64(d.GetFloat64())
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		col.AppendString(d.GetString())
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		col.AppendTime(d.GetMysqlTime())
	case mysql.TypeDuration:
		col.AppendDuration(d.GetMysqlDuration())
	case mysql.TypeNewDecimal:
		col.AppendMyDecimal(d.GetMysqlDecimal())
	case mysql.TypeEnum:
		col.AppendEnum(d.GetMysqlEnum())
	case mysql.TypeSet:
		col.AppendSet(d.GetMysqlSet())
	case mysql.TypeBit:
		col.AppendBytes(d.GetBytes())
	case mysql.TypeJSON:
		col.AppendJSON(d.GetMysqlJSON())
	case mysql.TypeTiDBVectorFloat32:
		col.AppendVectorFloat32(d.GetVectorFloat32())
	default:
		return errors.Errorf("unsupported MySQL type %d when appending recompute datum", ft.GetType())
	}
	return nil
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
