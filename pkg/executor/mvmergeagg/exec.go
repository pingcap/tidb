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

package mvmergeagg

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

// MVMergeAggMapping describes one aggregate merge rule.
//
// ColID contains the target aggregate column positions in child output schema.
// DependencyColID can reference:
// 1. child chunk delta-agg columns (col id < DeltaAggColCount), or
// 2. columns already produced by previous mappings.
type MVMergeAggMapping struct {
	// AggFunc must be set; aggregate function name is read from AggFunc.Name.
	AggFunc         *aggregation.AggFuncDesc
	ColID           []int
	DependencyColID []int
}

// MinMaxRecomputeExec keeps the recompute plan for min/max fallback.
// Stage 1 does not execute this path yet.
type MinMaxRecomputeExec struct {
	keyColIDs []int
	KeyCols   [][]*expression.CorrelatedColumn
	exec      []exec.Executor
}

// MVMergeAggRowOpType is the row operation for MV table write.
type MVMergeAggRowOpType uint8

const (
	// MVMergeAggRowOpNoOp means there is no MV row to touch.
	MVMergeAggRowOpNoOp MVMergeAggRowOpType = iota
	// MVMergeAggRowOpInsert means insert a new MV row.
	MVMergeAggRowOpInsert
	// MVMergeAggRowOpUpdate means update an existing MV row.
	MVMergeAggRowOpUpdate
	// MVMergeAggRowOpDelete means delete an existing MV row.
	MVMergeAggRowOpDelete
)

// MVMergeAggRowOp describes one row-level write action.
type MVMergeAggRowOp struct {
	RowIdx int
	Tp     MVMergeAggRowOpType
}

// MVMergeAggChunkResult contains worker result for one input chunk.
type MVMergeAggChunkResult struct {
	// Input is the original joined chunk (delta agg + MV columns).
	Input *chunk.Chunk
	// ComputedCols is indexed by input column position.
	// A nil item means this column is not computed by merge mappings.
	ComputedCols []*chunk.Column
	// RowOps indicates insert/update/delete rows in Input.
	RowOps []MVMergeAggRowOp
}

// MVMergeAggResultWriter consumes merged chunk results and writes to MV table.
type MVMergeAggResultWriter interface {
	WriteChunk(ctx context.Context, result *MVMergeAggChunkResult) error
}

// MVMergeAggExec is the sink executor for incremental MV merge.
// It currently supports COUNT / SUM merge.
type MVMergeAggExec struct {
	exec.BaseExecutor

	AggMappings      []MVMergeAggMapping
	DeltaAggColCount int
	MinMaxRecompute  map[int]MinMaxRecomputeExec

	WorkerCnt int
	Writer    MVMergeAggResultWriter

	TargetTable table.Table
	TargetInfo  *model.TableInfo
	// TargetHandleCols is used to build row handle for update/delete.
	TargetHandleCols plannerutil.HandleCols
	// TargetWritableColIDs maps target writable col index -> input chunk col index.
	// The mapped input column provides old/new base values for this writable column.
	// For aggregate writable columns, new values are taken from computed result when available.
	TargetWritableColIDs []int

	compiledMergers      []aggMerger
	compiledOutputColCnt int
	aggOutputColIDs      []int
	prepared             bool
	executed             bool
}

type mvMergeAggWorkerData struct {
	changedMask []bool
}

// Open implements the Executor interface.
func (e *MVMergeAggExec) Open(ctx context.Context) error {
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
// MVMergeAggExec is a sink executor and always returns an empty chunk.
func (e *MVMergeAggExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		return errors.New("MVMergeAggExec is not opened")
	}
	if e.executed {
		return nil
	}
	e.executed = true
	return e.runMergePipeline(ctx)
}

// Close implements the Executor interface.
func (e *MVMergeAggExec) Close() error {
	e.compiledMergers = nil
	e.compiledOutputColCnt = 0
	e.aggOutputColIDs = nil
	e.prepared = false
	e.executed = false
	return e.BaseExecutor.Close()
}

func (e *MVMergeAggExec) runMergePipeline(ctx context.Context) error {
	workerCnt := e.WorkerCnt
	if workerCnt <= 0 {
		workerCnt = 1
	}

	inputBufSize := max(workerCnt*2, 2)

	inputCh := make(chan *chunk.Chunk, inputBufSize)
	freeInputCh := make(chan *chunk.Chunk, inputBufSize)
	resultCh := make(chan *MVMergeAggChunkResult, workerCnt)

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

func (e *MVMergeAggExec) runReader(
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

func (e *MVMergeAggExec) runWorker(
	ctx context.Context,
	inputCh <-chan *chunk.Chunk,
	resultCh chan<- *MVMergeAggChunkResult,
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

func (e *MVMergeAggExec) runWriter(
	ctx context.Context,
	resultCh <-chan *MVMergeAggChunkResult,
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
	// Require a strict, compile-time non-NULL constant.
	// ParamMarker/DeferredExpr are context-dependent and may be NULL at runtime.
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

func (e *MVMergeAggExec) prepareMergers() error {
	if e.ChildrenLen() != 1 {
		return errors.Errorf("MVMergeAggExec expects exactly 1 child, got %d", e.ChildrenLen())
	}
	if len(e.AggMappings) == 0 {
		return errors.New("MVMergeAggExec requires non-empty AggMappings")
	}

	childTypes := exec.RetTypes(e.Children(0))
	if e.DeltaAggColCount < 0 || e.DeltaAggColCount > len(childTypes) {
		return errors.Errorf("DeltaAggColCount %d out of range [0,%d]", e.DeltaAggColCount, len(childTypes))
	}
	colID2ComputedIdx := make(map[int]int, len(e.AggMappings))
	e.compiledMergers = make([]aggMerger, 0, len(e.AggMappings))
	e.compiledOutputColCnt = 0
	e.aggOutputColIDs = make([]int, 0, len(e.AggMappings))

	firstAgg := e.AggMappings[0].AggFunc
	if firstAgg == nil {
		return errors.New("MVMergeAgg mapping requires AggFunc")
	}
	if !isFirstAggCountAllRows(firstAgg) {
		return errors.Errorf("the first MVMergeAgg mapping must be COUNT(*)/COUNT(non-NULL constant), got %s", aggFuncForErr(firstAgg))
	}
	if len(e.AggMappings[0].ColID) != 1 {
		return errors.Errorf("the first MVMergeAgg mapping must output exactly 1 column, got %d", len(e.AggMappings[0].ColID))
	}

	for i := range e.AggMappings {
		mapping := e.AggMappings[i]
		if mapping.AggFunc == nil {
			return errors.New("MVMergeAgg mapping requires AggFunc")
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
			err = errors.Errorf("unsupported agg function in MVMergeAggExec: %s", aggName)
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

func (e *MVMergeAggExec) mergeOneChunk(chk *chunk.Chunk, workerData *mvMergeAggWorkerData) (*MVMergeAggChunkResult, error) {
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

	rowOps, err := e.buildRowOps(chk, computedByColID, workerData)
	if err != nil {
		return nil, err
	}
	return &MVMergeAggChunkResult{
		Input:        chk,
		ComputedCols: computedByColID,
		RowOps:       rowOps,
	}, nil
}

func (e *MVMergeAggExec) buildRowOps(input *chunk.Chunk, computedByColID []*chunk.Column, workerData *mvMergeAggWorkerData) ([]MVMergeAggRowOp, error) {
	if len(e.aggOutputColIDs) == 0 {
		return nil, errors.New("no aggregate outputs in MVMergeAggExec")
	}
	countStarOutputColID := e.aggOutputColIDs[0]
	if countStarOutputColID < 0 || countStarOutputColID >= input.NumCols() {
		return nil, errors.Errorf("count(*) output col %d out of input chunk range", countStarOutputColID)
	}
	if countStarOutputColID >= len(computedByColID) {
		return nil, errors.Errorf("count(*) output col %d out of computed-by-col-id range [0,%d)", countStarOutputColID, len(computedByColID))
	}
	newCountStarCol := computedByColID[countStarOutputColID]
	if newCountStarCol == nil {
		return nil, errors.Errorf("count(*) output col %d is missing in computed columns", countStarOutputColID)
	}

	oldCountStarCol := input.Column(countStarOutputColID)
	newCountStarVals := newCountStarCol.Int64s()
	changedMask, err := e.buildAggChangedMask(input, computedByColID, workerData)
	if err != nil {
		return nil, err
	}
	rowOps := make([]MVMergeAggRowOp, 0, input.NumRows())
	for rowIdx := 0; rowIdx < input.NumRows(); rowIdx++ {
		newCount := newCountStarVals[rowIdx]
		if newCount < 0 {
			return nil, errors.Errorf("count(*) becomes negative (%d) at row %d", newCount, rowIdx)
		}
		// Left outer join: old MV row is absent when this side is NULL.
		oldExists := !oldCountStarCol.IsNull(rowIdx)

		opTp := MVMergeAggRowOpNoOp
		switch {
		case newCount == 0:
			if oldExists {
				opTp = MVMergeAggRowOpDelete
			}
		case !oldExists:
			opTp = MVMergeAggRowOpInsert
		default:
			if changedMask[rowIdx] {
				opTp = MVMergeAggRowOpUpdate
			}
		}

		if opTp != MVMergeAggRowOpNoOp {
			rowOps = append(rowOps, MVMergeAggRowOp{
				RowIdx: rowIdx,
				Tp:     opTp,
			})
		}
	}
	return rowOps, nil
}

func (e *MVMergeAggExec) buildAggChangedMask(input *chunk.Chunk, computedByColID []*chunk.Column, workerData *mvMergeAggWorkerData) ([]bool, error) {
	rowCnt := input.NumRows()
	var changedMask []bool
	if workerData != nil {
		changedMask = workerData.changedMask
	}
	if cap(changedMask) < rowCnt {
		changedMask = make([]bool, rowCnt)
	} else {
		changedMask = changedMask[:rowCnt]
		clear(changedMask)
	}
	if workerData != nil {
		workerData.changedMask = changedMask
	}
	if rowCnt == 0 {
		return changedMask, nil
	}

	fieldTypes := e.Children(0).RetFieldTypes()
	for _, colID := range e.aggOutputColIDs {
		if colID < 0 || colID >= input.NumCols() {
			return nil, errors.Errorf("agg output col %d out of input chunk range", colID)
		}
		if colID >= len(computedByColID) {
			return nil, errors.Errorf("agg output col %d out of computed-by-col-id range [0,%d)", colID, len(computedByColID))
		}
		if colID >= len(fieldTypes) {
			return nil, errors.Errorf("agg output col %d out of field type range [0,%d)", colID, len(fieldTypes))
		}
		oldCol := input.Column(colID)
		newCol := computedByColID[colID]
		if newCol == nil {
			return nil, errors.Errorf("computed agg col %d is nil", colID)
		}
		if err := markChangedRowsByColumn(changedMask, oldCol, newCol, fieldTypes[colID]); err != nil {
			return nil, err
		}
	}
	return changedMask, nil
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
