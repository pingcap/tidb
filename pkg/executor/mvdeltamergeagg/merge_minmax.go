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
	if len(mapping.DependencyColID) != 4 && len(mapping.DependencyColID) != 5 {
		return nil, errors.Errorf("%s mapping expects 4 or 5 dependencies, got %d", mapping.AggFunc.Name, len(mapping.DependencyColID))
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
	if err := validateMinMaxValueTypes(mapping.AggFunc.Name, retTp, addedTp, removedTp); err != nil {
		return nil, err
	}
	addedRef, err := resolveDepRef(addedColID, colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping added dependency col %d", mapping.AggFunc.Name, addedColID)
	}
	addedCntColID := mapping.DependencyColID[1]
	addedCntTp, err := resolveFieldTypeByColID(addedCntColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping added-count dependency col %d", mapping.AggFunc.Name, addedCntColID)
	}
	if err := validateMinMaxCountType(mapping.AggFunc.Name, addedCntColID, addedCntTp); err != nil {
		return nil, err
	}
	addedCntRef, err := resolveDepRef(addedCntColID, colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping added-count dependency col %d", mapping.AggFunc.Name, addedCntColID)
	}
	removedRef, err := resolveDepRef(removedColID, colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed dependency col %d", mapping.AggFunc.Name, removedColID)
	}
	removedCntColID := mapping.DependencyColID[3]
	removedCntTp, err := resolveFieldTypeByColID(removedCntColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed-count dependency col %d", mapping.AggFunc.Name, removedCntColID)
	}
	if err := validateMinMaxCountType(mapping.AggFunc.Name, removedCntColID, removedCntTp); err != nil {
		return nil, err
	}
	removedCntRef, err := resolveDepRef(removedCntColID, colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping removed-count dependency col %d", mapping.AggFunc.Name, removedCntColID)
	}

	base := minMaxMergerBase{
		outputCols:     []int{outputColID},
		addedRef:       addedRef,
		addedCntRef:    addedCntRef,
		removedRef:     removedRef,
		removedCntRef:  removedCntRef,
		mappingIdx:     mappingIdx,
		isMax:          mapping.AggFunc.Name == ast.AggFuncMax,
		retTp:          retTp,
		hasCountExpr:   len(mapping.DependencyColID) == 5,
		countExprRef:   depRef{},
		countExprColID: -1,
	}
	if base.hasCountExpr {
		base.countExprColID = mapping.DependencyColID[4]
		countExprTp, err := resolveFieldTypeByColID(base.countExprColID, childTypes)
		if err != nil {
			return nil, errors.Annotatef(err, "%s mapping final-count dependency col %d", mapping.AggFunc.Name, base.countExprColID)
		}
		if err := validateMinMaxCountType(mapping.AggFunc.Name, base.countExprColID, countExprTp); err != nil {
			return nil, err
		}
		base.countExprRef, err = resolveDepRef(base.countExprColID, colID2ComputedIdx, e.DeltaAggColCount)
		if err != nil {
			return nil, errors.Annotatef(err, "%s mapping final-count dependency col %d", mapping.AggFunc.Name, base.countExprColID)
		}
		if base.countExprRef.source != depFromComputed {
			return nil, errors.Errorf("%s(nullable expr) requires final COUNT(expr) from previously computed columns", mapping.AggFunc.Name)
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

func validateMinMaxValueTypes(aggName string, outputTp, addedTp, removedTp *types.FieldType) error {
	if outputTp == nil || addedTp == nil || removedTp == nil {
		return errors.Errorf("%s mapping type is unavailable", aggName)
	}
	if !outputTp.Equal(addedTp) || !outputTp.Equal(removedTp) {
		return errors.Errorf(
			"%s mapping value type mismatch: output=%s added=%s removed=%s",
			aggName,
			outputTp.String(),
			addedTp.String(),
			removedTp.String(),
		)
	}
	return nil
}

func validateMinMaxCountType(aggName string, colID int, tp *types.FieldType) error {
	if tp == nil {
		return errors.Errorf("%s mapping count col %d type is unavailable", aggName, colID)
	}
	if tp.EvalType() != types.ETInt {
		return errors.Errorf("%s mapping count col %d must be integer, got %s", aggName, colID, tp.EvalType())
	}
	if mysql.HasUnsignedFlag(tp.GetFlag()) {
		return errors.Errorf("%s mapping count col %d must be signed integer", aggName, colID)
	}
	return nil
}

type minMaxMergerBase struct {
	outputCols []int

	addedRef      depRef
	addedCntRef   depRef
	removedRef    depRef
	removedCntRef depRef

	hasCountExpr   bool
	countExprRef   depRef
	countExprColID int

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
) (oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol *chunk.Column, err error) {
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
	if m.hasCountExpr {
		countExprCol, err = getDepColumn(input, computedByOrder, m.countExprRef)
		if err != nil {
			return
		}
	}
	return
}

func (m *minMaxMergerBase) rowCounts(rowIdx int, countExprCol, addedCntCol, removedCntCol *chunk.Column) (shouldExist bool, addedCnt, removedCnt int64, err error) {
	shouldExist = true
	addedCnt, err = readNonNegativeCount(addedCntCol, rowIdx)
	if err != nil {
		return
	}
	removedCnt, err = readNonNegativeCount(removedCntCol, rowIdx)
	if err != nil {
		return
	}
	if m.hasCountExpr {
		finalCnt, readErr := readNonNegativeCount(countExprCol, rowIdx)
		if readErr != nil {
			err = readErr
			return
		}
		if finalCnt == 0 {
			shouldExist = false
		}
	}
	return
}

func readNonNegativeCount(col *chunk.Column, rowIdx int) (int64, error) {
	if col == nil || col.IsNull(rowIdx) {
		return 0, nil
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

func appendMinMaxRecomputeRow(workerData *mvMergeAggWorkerData, mappingIdx int, rowIdx int) error {
	if workerData == nil {
		return errors.New("min/max recompute requires worker data")
	}
	if mappingIdx < 0 || mappingIdx >= len(workerData.minMaxRecomputeRowsByMapping) {
		return errors.Errorf("min/max mapping idx %d out of recompute slice range [0,%d)", mappingIdx, len(workerData.minMaxRecomputeRowsByMapping))
	}
	workerData.minMaxRecomputeRowsByMapping[mappingIdx] = append(workerData.minMaxRecomputeRowsByMapping[mappingIdx], rowIdx)
	return nil
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
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol, err := m.resolveColumns(input, computedByOrder)
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
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countExprCol, addedCntCol, removedCntCol)
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
			if err := appendMinMaxRecomputeRow(workerData, m.mappingIdx, rowIdx); err != nil {
				return err
			}
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
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
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol, err := m.resolveColumns(input, computedByOrder)
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
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countExprCol, addedCntCol, removedCntCol)
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
			if err := appendMinMaxRecomputeRow(workerData, m.mappingIdx, rowIdx); err != nil {
				return err
			}
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
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
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol, err := m.resolveColumns(input, computedByOrder)
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
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countExprCol, addedCntCol, removedCntCol)
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
			if err := appendMinMaxRecomputeRow(workerData, m.mappingIdx, rowIdx); err != nil {
				return err
			}
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
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
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol, err := m.resolveColumns(input, computedByOrder)
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
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countExprCol, addedCntCol, removedCntCol)
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
			if err := appendMinMaxRecomputeRow(workerData, m.mappingIdx, rowIdx); err != nil {
				return err
			}
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
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
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol, err := m.resolveColumns(input, computedByOrder)
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
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countExprCol, addedCntCol, removedCntCol)
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
			if err := appendMinMaxRecomputeRow(workerData, m.mappingIdx, rowIdx); err != nil {
				return err
			}
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
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
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol, err := m.resolveColumns(input, computedByOrder)
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
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countExprCol, addedCntCol, removedCntCol)
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
			if err := appendMinMaxRecomputeRow(workerData, m.mappingIdx, rowIdx); err != nil {
				return err
			}
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
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
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol, err := m.resolveColumns(input, computedByOrder)
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
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countExprCol, addedCntCol, removedCntCol)
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
			if err := appendMinMaxRecomputeRow(workerData, m.mappingIdx, rowIdx); err != nil {
				return err
			}
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		}
	}
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
	oldCol, addedCol, addedCntCol, removedCol, removedCntCol, countExprCol, err := m.resolveColumns(input, computedByOrder)
	if err != nil {
		return err
	}
	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		shouldExist, addedCnt, removedCnt, err := m.rowCounts(rowIdx, countExprCol, addedCntCol, removedCntCol)
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
			if err := appendMinMaxRecomputeRow(workerData, m.mappingIdx, rowIdx); err != nil {
				return err
			}
			if oldExists {
				resultCol.AppendString(oldVal)
			} else {
				resultCol.AppendNull()
			}
		}
	}
	outputCols[0] = resultCol
	return nil
}
