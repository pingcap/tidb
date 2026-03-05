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
	"cmp"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

func (e *Exec) buildMinMaxMerger(
	mappingIdx int,
	mapping Mapping,
	colID2ComputedIdx map[int]int,
	childTypes []*types.FieldType,
) (aggMerger, error) {
	if len(mapping.ColID) != 1 {
		return nil, errors.Errorf("%s mapping expects exactly 1 output column, got %d", mapping.AggFunc.Name, len(mapping.ColID))
	}
	exprNullable, err := e.isMinMaxExprNullable(mapping)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping expression nullability", mapping.AggFunc.Name)
	}
	if exprNullable {
		if len(mapping.DependencyColID) != 5 {
			return nil, errors.Errorf(
				"%s(nullable expr) requires final-count dependency (exactly 5 dependencies), got %d",
				mapping.AggFunc.Name,
				len(mapping.DependencyColID),
			)
		}
	} else if len(mapping.DependencyColID) != 4 {
		return nil, errors.Errorf(
			"%s(non-nullable expr) must not set final-count dependency (exactly 4 dependencies), got %d",
			mapping.AggFunc.Name,
			len(mapping.DependencyColID),
		)
	}

	outputColID := mapping.ColID[0]
	retTp, err := resolveFieldTypeByColID(outputColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping output col %d", mapping.AggFunc.Name, outputColID)
	}
	addedColID := mapping.DependencyColID[0]
	addedTp, err := resolveFieldTypeByColID(addedColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping added dependency col %d", mapping.AggFunc.Name, addedColID)
	}
	removedColID := mapping.DependencyColID[2]
	removedTp, err := resolveFieldTypeByColID(removedColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed dependency col %d", mapping.AggFunc.Name, removedColID)
	}
	if err := validateMinMaxValueTypes(retTp, addedTp, removedTp); err != nil {
		return nil, errors.Annotatef(err, "%s mapping value dependencies", mapping.AggFunc.Name)
	}
	addedRef, err := resolveDepRef(addedColID, colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping added dependency col %d", mapping.AggFunc.Name, addedColID)
	}
	if err := validateDepRefSource(addedRef, depFromInput); err != nil {
		return nil, errors.Annotatef(err, "%s mapping added dependency col %d", mapping.AggFunc.Name, addedColID)
	}
	addedCntColID := mapping.DependencyColID[1]
	addedCntTp, err := resolveFieldTypeByColID(addedCntColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping added-count dependency col %d", mapping.AggFunc.Name, addedCntColID)
	}
	if err := validateSignedIntType(addedCntTp); err != nil {
		return nil, errors.Annotatef(err, "%s mapping added-count dependency col %d", mapping.AggFunc.Name, addedCntColID)
	}
	addedCntRef, err := resolveDepRef(addedCntColID, colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping added-count dependency col %d", mapping.AggFunc.Name, addedCntColID)
	}
	if err := validateDepRefSource(addedCntRef, depFromInput); err != nil {
		return nil, errors.Annotatef(err, "%s mapping added-count dependency col %d", mapping.AggFunc.Name, addedCntColID)
	}
	removedRef, err := resolveDepRef(removedColID, colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed dependency col %d", mapping.AggFunc.Name, removedColID)
	}
	if err := validateDepRefSource(removedRef, depFromInput); err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed dependency col %d", mapping.AggFunc.Name, removedColID)
	}
	removedCntColID := mapping.DependencyColID[3]
	removedCntTp, err := resolveFieldTypeByColID(removedCntColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed-count dependency col %d", mapping.AggFunc.Name, removedCntColID)
	}
	if err := validateSignedIntType(removedCntTp); err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed-count dependency col %d", mapping.AggFunc.Name, removedCntColID)
	}
	removedCntRef, err := resolveDepRef(removedCntColID, colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed-count dependency col %d", mapping.AggFunc.Name, removedCntColID)
	}
	if err := validateDepRefSource(removedCntRef, depFromInput); err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed-count dependency col %d", mapping.AggFunc.Name, removedCntColID)
	}

	base := minMaxMergerBase{
		outputCols:    []int{outputColID},
		addedRef:      addedRef,
		addedCntRef:   addedCntRef,
		removedRef:    removedRef,
		removedCntRef: removedCntRef,
		mappingIdx:    mappingIdx,
		isMax:         mapping.AggFunc.Name == ast.AggFuncMax,
		retTp:         retTp,
		countRef:      depRef{},
	}
	if exprNullable {
		countColID := mapping.DependencyColID[4]
		countExprTp, err := resolveFieldTypeByColID(countColID, childTypes)
		if err != nil {
			return nil, errors.Annotatef(err, "%s mapping final-count dependency col %d", mapping.AggFunc.Name, countColID)
		}
		if err := validateSignedIntType(countExprTp); err != nil {
			return nil, errors.Annotatef(err, "%s mapping final-count dependency col %d", mapping.AggFunc.Name, countColID)
		}
		base.countRef, err = resolveDepRef(countColID, colID2ComputedIdx, e.DeltaAggColCount)
		if err != nil {
			return nil, errors.Annotatef(err, "%s mapping final-count dependency col %d", mapping.AggFunc.Name, countColID)
		}
		if err := validateDepRefSource(base.countRef, depFromComputed); err != nil {
			return nil, errors.Annotatef(err, "%s mapping final-count dependency col %d", mapping.AggFunc.Name, countColID)
		}
	} else {
		countAllRowsColID := e.AggMappings[0].ColID[0]
		countAllRowsTp, err := resolveFieldTypeByColID(countAllRowsColID, childTypes)
		if err != nil {
			return nil, errors.Annotatef(err, "%s mapping fallback count(*) output col %d", mapping.AggFunc.Name, countAllRowsColID)
		}
		if err := validateSignedIntType(countAllRowsTp); err != nil {
			return nil, errors.Annotatef(err, "%s mapping fallback count(*) output col %d", mapping.AggFunc.Name, countAllRowsColID)
		}
		base.countRef, err = resolveDepRef(countAllRowsColID, colID2ComputedIdx, e.DeltaAggColCount)
		if err != nil {
			return nil, errors.Annotatef(err, "%s mapping fallback count(*) output col %d", mapping.AggFunc.Name, countAllRowsColID)
		}
		if err := validateDepRefSource(base.countRef, depFromComputed); err != nil {
			return nil, errors.Annotatef(err, "%s mapping fallback count(*) output col %d", mapping.AggFunc.Name, countAllRowsColID)
		}
	}

	switch retTp.EvalType() {
	case types.ETInt:
		if mysql.HasUnsignedFlag(retTp.GetFlag()) {
			return &minMaxUintMerger{minMaxMergerBase: base}, nil
		}
		return &minMaxIntMerger{minMaxMergerBase: base}, nil
	case types.ETReal:
		if retTp.GetType() == mysql.TypeFloat {
			return &minMaxFloat32Merger{minMaxMergerBase: base}, nil
		}
		return &minMaxFloat64Merger{minMaxMergerBase: base}, nil
	case types.ETDecimal:
		return &minMaxDecimalMerger{minMaxMergerBase: base}, nil
	case types.ETDatetime, types.ETTimestamp:
		return &minMaxTimeMerger{minMaxMergerBase: base}, nil
	case types.ETDuration:
		return &minMaxDurationMerger{minMaxMergerBase: base}, nil
	case types.ETString:
		switch retTp.GetType() {
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			return &minMaxStringMerger{
				minMaxMergerBase: base,
				collator:         collate.GetCollator(retTp.GetCollate()),
			}, nil
		default:
			return nil, errors.Errorf("%s merge does not support string type %d", mapping.AggFunc.Name, retTp.GetType())
		}
	default:
		return nil, errors.Errorf("%s merge does not support eval type %s", mapping.AggFunc.Name, retTp.EvalType())
	}
}

func validateMinMaxValueTypes(outputTp, addedTp, removedTp *types.FieldType) error {
	if outputTp == nil || addedTp == nil || removedTp == nil {
		return errors.New("type is unavailable")
	}
	if !outputTp.Equal(addedTp) || !outputTp.Equal(removedTp) {
		return errors.Errorf(
			"value type mismatch: output=%s added=%s removed=%s",
			outputTp.String(),
			addedTp.String(),
			removedTp.String(),
		)
	}
	return nil
}

func (e *Exec) isMinMaxExprNullable(mapping Mapping) (bool, error) {
	if mapping.AggFunc == nil {
		return false, errors.New("AggFunc is nil")
	}
	if len(mapping.AggFunc.Args) != 1 || mapping.AggFunc.Args[0] == nil {
		return false, errors.Errorf("expects exactly 1 non-nil argument, got %d", len(mapping.AggFunc.Args))
	}
	argTp := mapping.AggFunc.Args[0].GetType(e.Ctx().GetExprCtx().GetEvalCtx())
	if argTp == nil {
		return false, errors.New("argument type is unavailable")
	}
	return !mysql.HasNotNullFlag(argTp.GetFlag()), nil
}

type minMaxMergerBase struct {
	outputCols []int

	addedRef      depRef
	addedCntRef   depRef
	removedRef    depRef
	removedCntRef depRef

	countRef depRef

	mappingIdx int
	isMax      bool
	retTp      *types.FieldType
}

func (m *minMaxMergerBase) outputColIDs() []int {
	return m.outputCols
}

func (m *minMaxMergerBase) resolveColumns(
	input *chunk.Chunk,
	computedByOrder []*chunk.Column,
) (oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol *chunk.Column, err error) {
	outputColID := m.outputCols[0]
	if outputColID < 0 || outputColID >= input.NumCols() {
		err = errors.Errorf("min/max output col %d out of input range", outputColID)
		return
	}
	oldCol = input.Column(outputColID)
	addedCol, err = getDepColumn(input, computedByOrder, m.addedRef)
	if err != nil {
		return
	}
	addedCntCol, err = getDepColumn(input, computedByOrder, m.addedCntRef)
	if err != nil {
		return
	}
	removedCol, err = getDepColumn(input, computedByOrder, m.removedRef)
	if err != nil {
		return
	}
	removedCntCol, err = getDepColumn(input, computedByOrder, m.removedCntRef)
	if err != nil {
		return
	}
	countCol, err = getDepColumn(input, computedByOrder, m.countRef)
	if err != nil {
		return
	}
	return
}

func (m *minMaxMergerBase) rowCounts(rowIdx int, countCol, addedCntCol, removedCntCol *chunk.Column) (shouldExist bool, addedCnt, removedCnt int64, err error) {
	finalCnt, err := readNonNegativeCount(countCol, rowIdx)
	if err != nil {
		return false, 0, 0, err
	}
	if finalCnt == 0 {
		return false, 0, 0, nil
	}
	addedCnt, err = readNonNegativeCount(addedCntCol, rowIdx)
	if err != nil {
		return
	}
	removedCnt, err = readNonNegativeCount(removedCntCol, rowIdx)
	if err != nil {
		return
	}
	shouldExist = true
	return
}

func readNonNegativeCount(col *chunk.Column, rowIdx int) (int64, error) {
	if col.IsNull(rowIdx) {
		return 0, errors.Errorf("count is null at row %d", rowIdx)
	}
	v := col.Int64s()[rowIdx]
	if v < 0 {
		return 0, errors.Errorf("count becomes negative (%d) at row %d", v, rowIdx)
	}
	return v, nil
}

type minMaxDecision uint8

const (
	minMaxDecisionUseOld minMaxDecision = iota
	minMaxDecisionUseAdded
	minMaxDecisionUseNull
	minMaxDecisionRecompute
)

func chooseDeltaSource(isMax bool, addExists, delExists bool, cmpAddDel int) (deltaExists, addIsDelta, delIsDelta, chooseAdded bool) {
	switch {
	case addExists && delExists:
		switch {
		case cmpAddDel == 0:
			return true, true, true, true
		case cmpAddDel > 0:
			if isMax {
				return true, true, false, true
			}
			return true, false, true, false
		default:
			if isMax {
				return true, false, true, false
			}
			return true, true, false, true
		}
	case addExists:
		return true, true, false, true
	case delExists:
		return true, false, true, false
	default:
		return false, false, false, false
	}
}

func decideMinMaxFast(
	isMax bool,
	oldExists bool,
	addIsDelta bool,
	delIsDelta bool,
	cmpDeltaOld int,
	addedCnt int64,
	removedCnt int64,
) minMaxDecision {
	deltaExists := addIsDelta || delIsDelta
	if !oldExists {
		if !deltaExists {
			return minMaxDecisionRecompute
		}
		// With no old value, a delete-only dominator cannot produce a valid fast result.
		if !addIsDelta {
			return minMaxDecisionRecompute
		}
		// Dominator may disappear when added and removed are tied on value.
		if delIsDelta && addedCnt <= removedCnt {
			return minMaxDecisionRecompute
		}
		return minMaxDecisionUseAdded
	}
	if !deltaExists {
		return minMaxDecisionUseOld
	}

	if isMax {
		switch {
		case cmpDeltaOld < 0:
			return minMaxDecisionUseOld
		case cmpDeltaOld > 0:
			if !delIsDelta {
				return minMaxDecisionUseAdded
			}
			if addIsDelta && addedCnt > removedCnt {
				return minMaxDecisionUseAdded
			}
			return minMaxDecisionRecompute
		default:
			if delIsDelta && (!addIsDelta || removedCnt > addedCnt) {
				return minMaxDecisionRecompute
			}
			return minMaxDecisionUseOld
		}
	}

	switch {
	case cmpDeltaOld > 0:
		return minMaxDecisionUseOld
	case cmpDeltaOld < 0:
		if !delIsDelta {
			return minMaxDecisionUseAdded
		}
		if addIsDelta && addedCnt > removedCnt {
			return minMaxDecisionUseAdded
		}
		return minMaxDecisionRecompute
	default:
		if delIsDelta && (!addIsDelta || removedCnt > addedCnt) {
			return minMaxDecisionRecompute
		}
		return minMaxDecisionUseOld
	}
}

func prepareMinMaxRecomputeRows(workerData *mvMergeAggWorkerData, mappingIdx int) ([]int, error) {
	if workerData == nil {
		return nil, errors.New("min/max recompute requires worker data")
	}
	if mappingIdx < 0 || mappingIdx >= len(workerData.minMaxRecomputeRowsByMapping) {
		return nil, errors.Errorf("min/max mapping idx %d out of recompute slice range [0,%d)", mappingIdx, len(workerData.minMaxRecomputeRowsByMapping))
	}
	return workerData.minMaxRecomputeRowsByMapping[mappingIdx], nil
}

func storeMinMaxRecomputeRows(workerData *mvMergeAggWorkerData, mappingIdx int, rows []int) {
	workerData.minMaxRecomputeRowsByMapping[mappingIdx] = rows
}

func cmpFloat32(a, b float32) int {
	switch {
	case a > b:
		return 1
	case a < b:
		return -1
	default:
		return 0
	}
}

func cmpFloat64(a, b float64) int {
	switch {
	case a > b:
		return 1
	case a < b:
		return -1
	default:
		return 0
	}
}

type minMaxIntMerger struct {
	minMaxMergerBase
}

func (m *minMaxIntMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error {
	if len(outputCols) != 1 {
		return errors.Errorf("min/max merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	recomputeRows, err := prepareMinMaxRecomputeRows(workerData, m.mappingIdx)
	if err != nil {
		return err
	}
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeInt64(numRows, false)
	resultVals := resultCol.Int64s()
	oldVals := oldCol.Int64s()
	addedVals := addedCol.Int64s()
	removedVals := removedCol.Int64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countCol, addedCntCol, removedCntCol)
		if err != nil {
			return err
		}
		if !shouldExist {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		delExists := !removedCol.IsNull(rowIdx)

		cmpAddDel := 0
		if addExists && delExists {
			cmpAddDel = cmp.Compare(addedVals[rowIdx], removedVals[rowIdx])
		}
		deltaExists, addIsDelta, delIsDelta, chooseAdded := chooseDeltaSource(m.isMax, addExists, delExists, cmpAddDel)
		cmpDeltaOld := 0
		if oldExists && deltaExists {
			if chooseAdded {
				cmpDeltaOld = cmp.Compare(addedVals[rowIdx], oldVals[rowIdx])
			} else {
				cmpDeltaOld = cmp.Compare(removedVals[rowIdx], oldVals[rowIdx])
			}
		}

		switch decideMinMaxFast(m.isMax, oldExists, addIsDelta, delIsDelta, cmpDeltaOld, addedCnt, removedCnt) {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			switch {
			case addExists:
				resultVals[rowIdx] = addedVals[rowIdx]
			case delExists:
				resultVals[rowIdx] = removedVals[rowIdx]
			default:
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseNull:
			resultCol.SetNull(rowIdx, true)
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxUintMerger struct {
	minMaxMergerBase
}

func (m *minMaxUintMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error {
	if len(outputCols) != 1 {
		return errors.Errorf("min/max merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	recomputeRows, err := prepareMinMaxRecomputeRows(workerData, m.mappingIdx)
	if err != nil {
		return err
	}
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeUint64(numRows, false)
	resultVals := resultCol.Uint64s()
	oldVals := oldCol.Uint64s()
	addedVals := addedCol.Uint64s()
	removedVals := removedCol.Uint64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countCol, addedCntCol, removedCntCol)
		if err != nil {
			return err
		}
		if !shouldExist {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		delExists := !removedCol.IsNull(rowIdx)

		cmpAddDel := 0
		if addExists && delExists {
			cmpAddDel = cmp.Compare(addedVals[rowIdx], removedVals[rowIdx])
		}
		deltaExists, addIsDelta, delIsDelta, chooseAdded := chooseDeltaSource(m.isMax, addExists, delExists, cmpAddDel)
		cmpDeltaOld := 0
		if oldExists && deltaExists {
			if chooseAdded {
				cmpDeltaOld = cmp.Compare(addedVals[rowIdx], oldVals[rowIdx])
			} else {
				cmpDeltaOld = cmp.Compare(removedVals[rowIdx], oldVals[rowIdx])
			}
		}

		switch decideMinMaxFast(m.isMax, oldExists, addIsDelta, delIsDelta, cmpDeltaOld, addedCnt, removedCnt) {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			switch {
			case addExists:
				resultVals[rowIdx] = addedVals[rowIdx]
			case delExists:
				resultVals[rowIdx] = removedVals[rowIdx]
			default:
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseNull:
			resultCol.SetNull(rowIdx, true)
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxFloat32Merger struct {
	minMaxMergerBase
}

func (m *minMaxFloat32Merger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error {
	if len(outputCols) != 1 {
		return errors.Errorf("min/max merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	recomputeRows, err := prepareMinMaxRecomputeRows(workerData, m.mappingIdx)
	if err != nil {
		return err
	}
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeFloat32(numRows, false)
	resultVals := resultCol.Float32s()
	oldVals := oldCol.Float32s()
	addedVals := addedCol.Float32s()
	removedVals := removedCol.Float32s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countCol, addedCntCol, removedCntCol)
		if err != nil {
			return err
		}
		if !shouldExist {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		delExists := !removedCol.IsNull(rowIdx)

		cmpAddDel := 0
		if addExists && delExists {
			cmpAddDel = cmpFloat32(addedVals[rowIdx], removedVals[rowIdx])
		}
		deltaExists, addIsDelta, delIsDelta, chooseAdded := chooseDeltaSource(m.isMax, addExists, delExists, cmpAddDel)
		cmpDeltaOld := 0
		if oldExists && deltaExists {
			if chooseAdded {
				cmpDeltaOld = cmpFloat32(addedVals[rowIdx], oldVals[rowIdx])
			} else {
				cmpDeltaOld = cmpFloat32(removedVals[rowIdx], oldVals[rowIdx])
			}
		}

		switch decideMinMaxFast(m.isMax, oldExists, addIsDelta, delIsDelta, cmpDeltaOld, addedCnt, removedCnt) {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			switch {
			case addExists:
				resultVals[rowIdx] = addedVals[rowIdx]
			case delExists:
				resultVals[rowIdx] = removedVals[rowIdx]
			default:
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseNull:
			resultCol.SetNull(rowIdx, true)
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxFloat64Merger struct {
	minMaxMergerBase
}

func (m *minMaxFloat64Merger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error {
	if len(outputCols) != 1 {
		return errors.Errorf("min/max merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	recomputeRows, err := prepareMinMaxRecomputeRows(workerData, m.mappingIdx)
	if err != nil {
		return err
	}
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeFloat64(numRows, false)
	resultVals := resultCol.Float64s()
	oldVals := oldCol.Float64s()
	addedVals := addedCol.Float64s()
	removedVals := removedCol.Float64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countCol, addedCntCol, removedCntCol)
		if err != nil {
			return err
		}
		if !shouldExist {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		delExists := !removedCol.IsNull(rowIdx)

		cmpAddDel := 0
		if addExists && delExists {
			cmpAddDel = cmpFloat64(addedVals[rowIdx], removedVals[rowIdx])
		}
		deltaExists, addIsDelta, delIsDelta, chooseAdded := chooseDeltaSource(m.isMax, addExists, delExists, cmpAddDel)
		cmpDeltaOld := 0
		if oldExists && deltaExists {
			if chooseAdded {
				cmpDeltaOld = cmpFloat64(addedVals[rowIdx], oldVals[rowIdx])
			} else {
				cmpDeltaOld = cmpFloat64(removedVals[rowIdx], oldVals[rowIdx])
			}
		}

		switch decideMinMaxFast(m.isMax, oldExists, addIsDelta, delIsDelta, cmpDeltaOld, addedCnt, removedCnt) {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			switch {
			case addExists:
				resultVals[rowIdx] = addedVals[rowIdx]
			case delExists:
				resultVals[rowIdx] = removedVals[rowIdx]
			default:
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseNull:
			resultCol.SetNull(rowIdx, true)
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxDecimalMerger struct {
	minMaxMergerBase
}

func (m *minMaxDecimalMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error {
	if len(outputCols) != 1 {
		return errors.Errorf("min/max merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	recomputeRows, err := prepareMinMaxRecomputeRows(workerData, m.mappingIdx)
	if err != nil {
		return err
	}
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeDecimal(numRows, false)
	resultVals := resultCol.Decimals()
	oldVals := oldCol.Decimals()
	addedVals := addedCol.Decimals()
	removedVals := removedCol.Decimals()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countCol, addedCntCol, removedCntCol)
		if err != nil {
			return err
		}
		if !shouldExist {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		delExists := !removedCol.IsNull(rowIdx)

		cmpAddDel := 0
		if addExists && delExists {
			cmpAddDel = addedVals[rowIdx].Compare(&removedVals[rowIdx])
		}
		deltaExists, addIsDelta, delIsDelta, chooseAdded := chooseDeltaSource(m.isMax, addExists, delExists, cmpAddDel)
		cmpDeltaOld := 0
		if oldExists && deltaExists {
			if chooseAdded {
				cmpDeltaOld = addedVals[rowIdx].Compare(&oldVals[rowIdx])
			} else {
				cmpDeltaOld = removedVals[rowIdx].Compare(&oldVals[rowIdx])
			}
		}

		switch decideMinMaxFast(m.isMax, oldExists, addIsDelta, delIsDelta, cmpDeltaOld, addedCnt, removedCnt) {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			switch {
			case addExists:
				resultVals[rowIdx] = addedVals[rowIdx]
			case delExists:
				resultVals[rowIdx] = removedVals[rowIdx]
			default:
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseNull:
			resultCol.SetNull(rowIdx, true)
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxTimeMerger struct {
	minMaxMergerBase
}

func (m *minMaxTimeMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error {
	if len(outputCols) != 1 {
		return errors.Errorf("min/max merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	recomputeRows, err := prepareMinMaxRecomputeRows(workerData, m.mappingIdx)
	if err != nil {
		return err
	}
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeTime(numRows, false)
	resultVals := resultCol.Times()
	oldVals := oldCol.Times()
	addedVals := addedCol.Times()
	removedVals := removedCol.Times()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countCol, addedCntCol, removedCntCol)
		if err != nil {
			return err
		}
		if !shouldExist {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		delExists := !removedCol.IsNull(rowIdx)

		cmpAddDel := 0
		if addExists && delExists {
			cmpAddDel = addedVals[rowIdx].Compare(removedVals[rowIdx])
		}
		deltaExists, addIsDelta, delIsDelta, chooseAdded := chooseDeltaSource(m.isMax, addExists, delExists, cmpAddDel)
		cmpDeltaOld := 0
		if oldExists && deltaExists {
			if chooseAdded {
				cmpDeltaOld = addedVals[rowIdx].Compare(oldVals[rowIdx])
			} else {
				cmpDeltaOld = removedVals[rowIdx].Compare(oldVals[rowIdx])
			}
		}

		switch decideMinMaxFast(m.isMax, oldExists, addIsDelta, delIsDelta, cmpDeltaOld, addedCnt, removedCnt) {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			switch {
			case addExists:
				resultVals[rowIdx] = addedVals[rowIdx]
			case delExists:
				resultVals[rowIdx] = removedVals[rowIdx]
			default:
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseNull:
			resultCol.SetNull(rowIdx, true)
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxDurationMerger struct {
	minMaxMergerBase
}

func (m *minMaxDurationMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error {
	if len(outputCols) != 1 {
		return errors.Errorf("min/max merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	recomputeRows, err := prepareMinMaxRecomputeRows(workerData, m.mappingIdx)
	if err != nil {
		return err
	}
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeGoDuration(numRows, false)
	resultVals := resultCol.GoDurations()
	oldVals := oldCol.GoDurations()
	addedVals := addedCol.GoDurations()
	removedVals := removedCol.GoDurations()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countCol, addedCntCol, removedCntCol)
		if err != nil {
			return err
		}
		if !shouldExist {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		delExists := !removedCol.IsNull(rowIdx)

		cmpAddDel := 0
		if addExists && delExists {
			cmpAddDel = cmp.Compare(int64(addedVals[rowIdx]), int64(removedVals[rowIdx]))
		}
		deltaExists, addIsDelta, delIsDelta, chooseAdded := chooseDeltaSource(m.isMax, addExists, delExists, cmpAddDel)
		cmpDeltaOld := 0
		if oldExists && deltaExists {
			if chooseAdded {
				cmpDeltaOld = cmp.Compare(int64(addedVals[rowIdx]), int64(oldVals[rowIdx]))
			} else {
				cmpDeltaOld = cmp.Compare(int64(removedVals[rowIdx]), int64(oldVals[rowIdx]))
			}
		}

		switch decideMinMaxFast(m.isMax, oldExists, addIsDelta, delIsDelta, cmpDeltaOld, addedCnt, removedCnt) {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			switch {
			case addExists:
				resultVals[rowIdx] = addedVals[rowIdx]
			case delExists:
				resultVals[rowIdx] = removedVals[rowIdx]
			default:
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseNull:
			resultCol.SetNull(rowIdx, true)
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxStringMerger struct {
	minMaxMergerBase
	collator collate.Collator
}

func (m *minMaxStringMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error {
	if len(outputCols) != 1 {
		return errors.Errorf("min/max merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	recomputeRows, err := prepareMinMaxRecomputeRows(workerData, m.mappingIdx)
	if err != nil {
		return err
	}
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countCol, addedCntCol, removedCntCol)
		if err != nil {
			return err
		}
		if !shouldExist {
			resultCol.AppendNull()
			continue
		}
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		delExists := !removedCol.IsNull(rowIdx)

		var addVal, delVal, oldVal string
		if addExists {
			addVal = addedCol.GetString(rowIdx)
		}
		if delExists {
			delVal = removedCol.GetString(rowIdx)
		}
		if oldExists {
			oldVal = oldCol.GetString(rowIdx)
		}

		cmpAddDel := 0
		if addExists && delExists {
			cmpAddDel = m.collator.Compare(addVal, delVal)
		}
		deltaExists, addIsDelta, delIsDelta, chooseAdded := chooseDeltaSource(m.isMax, addExists, delExists, cmpAddDel)
		cmpDeltaOld := 0
		if oldExists && deltaExists {
			if chooseAdded {
				cmpDeltaOld = m.collator.Compare(addVal, oldVal)
			} else {
				cmpDeltaOld = m.collator.Compare(delVal, oldVal)
			}
		}

		switch decideMinMaxFast(m.isMax, oldExists, addIsDelta, delIsDelta, cmpDeltaOld, addedCnt, removedCnt) {
		case minMaxDecisionUseOld:
			if oldExists {
				resultCol.AppendString(oldVal)
			} else {
				resultCol.AppendNull()
			}
		case minMaxDecisionUseAdded:
			switch {
			case addExists:
				resultCol.AppendString(addVal)
			case delExists:
				resultCol.AppendString(delVal)
			default:
				resultCol.AppendNull()
			}
		case minMaxDecisionUseNull:
			resultCol.AppendNull()
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			if oldExists {
				resultCol.AppendString(oldVal)
			} else {
				resultCol.AppendNull()
			}
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}
