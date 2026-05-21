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

package mviewdeltamergeagg

import (
	"cmp"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
)

// MIN/MAX delta-merge update overview:
//
// old:
// current MIN/MAX stored in the MView row.
// add/remove:
// MIN/MAX aggregate outputs of inserted/deleted base rows in the current delta batch.
// addCnt/removeCnt:
// counts of rows equal to add/remove extremum in those delta aggregates.
// countRef:
// final group count reference used by merger (COUNT(expr) when provided; otherwise COUNT(*)).
//
// Decision flow:
//  1. Derive oldExists/addExists/removeExists from value nullability and non-negative counts.
//  2. Compute chooseAddRemove:
//     +1 => add side dominates, -1 => remove side dominates, 0 => no dominant side
//     (both sides absent, or same extremum with balanced counts).
//  3. Use fast-path update when old/add/remove (+ counts) is sufficient to prove final extremum.
//  4. Otherwise fallback to recomputing, because delta aggregates do not include the successor extremum.
//
// This preserves semantics equivalent to recomputing MIN/MAX from base rows after applying delta.
func (e *Exec) buildMinMaxMerger(
	mappingIdx int,
	mapping Mapping,
	colID2ComputedIdx map[int]int,
	childTypes []*types.FieldType,
) (aggMerger, error) {
	if len(mapping.ColID) != 1 {
		return nil, errors.Errorf("%s mapping expects exactly 1 output column, got %d", mapping.AggFunc.Name, len(mapping.ColID))
	}
	exprNullable, err := e.isMinMaxArgNullable(mapping)
	if err != nil {
		return nil, errors.Annotatef(err, "%s mapping expression nullability", mapping.AggFunc.Name)
	}
	if exprNullable {
		if len(mapping.DependencyColID) != 4 && len(mapping.DependencyColID) != 5 {
			return nil, errors.Errorf(
				"%s(nullable expr) expects 4 or 5 dependencies, got %d",
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
	if exprNullable && len(mapping.DependencyColID) == 5 {
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
		// Fall back to count(*) when there is no explicit COUNT(expr) dependency. For nullable
		// MIN/MAX this may trigger extra recomputes, but remains semantically correct.
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

func (e *Exec) isMinMaxArgNullable(mapping Mapping) (bool, error) {
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

func (*minMaxMergerBase) validateCountColumns(
	countCol, addedCntCol, removedCntCol *chunk.Column,
) error {
	if countCol.HasNull() {
		return errors.New("min/max final count contains null")
	}
	if addedCntCol.HasNull() {
		return errors.New("min/max added count contains null")
	}
	if removedCntCol.HasNull() {
		return errors.New("min/max removed count contains null")
	}
	if neg, ok := firstNegativeCountValue(countCol.Int64s()); ok {
		return errors.Errorf("min/max final count becomes negative (%d)", neg)
	}
	if neg, ok := firstNegativeCountValue(addedCntCol.Int64s()); ok {
		return errors.Errorf("min/max added count becomes negative (%d)", neg)
	}
	if neg, ok := firstNegativeCountValue(removedCntCol.Int64s()); ok {
		return errors.Errorf("min/max removed count becomes negative (%d)", neg)
	}
	return nil
}

type minMaxDecision uint8

const (
	minMaxDecisionUseOld minMaxDecision = iota
	minMaxDecisionUseAdded
	minMaxDecisionRecompute
)

func prepareMinMaxRecomputeRows(workerData *mergeWorkerData, mappingIdx int) ([]int, error) {
	if mappingIdx < 0 || mappingIdx >= len(workerData.minMaxMappingIdxToRows) {
		return nil, errors.Errorf("min/max mapping idx %d out of recompute slice range [0,%d)", mappingIdx, len(workerData.minMaxMappingIdxToRows))
	}
	return workerData.minMaxMappingIdxToRows[mappingIdx], nil
}

func storeMinMaxRecomputeRows(workerData *mergeWorkerData, mappingIdx int, rows []int) {
	workerData.minMaxMappingIdxToRows[mappingIdx] = rows
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

func redactUint64Value(v uint64) string {
	return redact.Value(strconv.FormatUint(v, 10))
}

func redactInt64Value(v int64) string {
	return redact.Value(strconv.FormatInt(v, 10))
}

func redactFloat32Value(v float32) string {
	return redact.Value(strconv.FormatFloat(float64(v), 'g', -1, 32))
}

func redactFloat64Value(v float64) string {
	return redact.Value(strconv.FormatFloat(v, 'g', -1, 64))
}

func redactDecimalValue(v *types.MyDecimal) string {
	return redact.Value(v.String())
}

func redactTimeValue(v types.Time) string {
	return redact.Value(v.String())
}

func redactDurationValue(v int64) string {
	return redactInt64Value(v)
}

type minMaxIntMerger struct {
	minMaxMergerBase
}

func (m *minMaxIntMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mergeWorkerData) error {
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
	if err := m.validateCountColumns(countCol, addedCntCol, removedCntCol); err != nil {
		return err
	}
	numRows := input.NumRows()
	countVals := countCol.Int64s()
	addedCntVals := addedCntCol.Int64s()
	removedCntVals := removedCntCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeInt64(numRows, false)
	resultVals := resultCol.Int64s()
	oldVals := oldCol.Int64s()
	addedVals := addedCol.Int64s()
	removedVals := removedCol.Int64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		finalCnt := countVals[rowIdx]
		if finalCnt == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		addedCnt := addedCntVals[rowIdx]
		removedCnt := removedCntVals[rowIdx]
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		removeExists := !removedCol.IsNull(rowIdx)

		// chooseAddRemove indicates which delta side dominates the candidate extremum.
		// 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
		// 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
		chooseAddRemove := 0
		// sameExtremumValue means added and removed share the same extremum value.
		sameExtremumValue := false
		if addExists != removeExists {
			if addExists {
				chooseAddRemove = 1
			} else {
				chooseAddRemove = -1
			}
		} else if addExists && removeExists {
			if m.isMax {
				chooseAddRemove = cmp.Compare(addedVals[rowIdx], removedVals[rowIdx])
			} else {
				chooseAddRemove = cmp.Compare(removedVals[rowIdx], addedVals[rowIdx])
			}
			if chooseAddRemove == 0 {
				chooseAddRemove = cmp.Compare(addedCnt, removedCnt)
				sameExtremumValue = true
			}
		}

		decision := minMaxDecisionUseOld
		if chooseAddRemove == 0 {
			if sameExtremumValue {
				// added/removed have the same extremum value and the same count.
				// If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
				// than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
				// aggregates do not carry the replacement extremum value.
				// In this case, recompute is required.
				if !oldExists {
					decision = minMaxDecisionRecompute
				} else {
					cmpOld := cmp.Compare(addedVals[rowIdx], oldVals[rowIdx])
					if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
						decision = minMaxDecisionRecompute
					}
				}
			}
			// If sameExtremumValue is false, it means both sides are absent, and old value should be kept
		} else if chooseAddRemove > 0 {
			if !oldExists {
				decision = minMaxDecisionUseAdded
			} else {
				cmpOld := cmp.Compare(addedVals[rowIdx], oldVals[rowIdx])
				if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
					decision = minMaxDecisionUseAdded
				}
			}
		} else {
			if !oldExists {
				logutil.BgLogger().Error(
					"invalid min/max int fast-path state (remove-dominant without old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("add_val", redactInt64Value(addedVals[rowIdx])),
					zap.String("remove_val", redactInt64Value(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max int fast-path state (remove-dominant without old value) at row %d",
					rowIdx,
				)
			}
			cmpOld := cmp.Compare(removedVals[rowIdx], oldVals[rowIdx])
			if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
				logutil.BgLogger().Error(
					"invalid min/max int fast-path state (remove-dominant outranks old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int("cmp_old", cmpOld),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("old_val", redactInt64Value(oldVals[rowIdx])),
					zap.String("add_val", redactInt64Value(addedVals[rowIdx])),
					zap.String("remove_val", redactInt64Value(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max int fast-path state (remove-dominant outranks old value) at row %d",
					rowIdx,
				)
			}
			if cmpOld == 0 {
				decision = minMaxDecisionRecompute
			}
		}

		switch decision {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			resultVals[rowIdx] = addedVals[rowIdx]
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			resultCol.SetNull(rowIdx, true)
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxUintMerger struct {
	minMaxMergerBase
}

func (m *minMaxUintMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mergeWorkerData) error {
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
	if err := m.validateCountColumns(countCol, addedCntCol, removedCntCol); err != nil {
		return err
	}
	numRows := input.NumRows()
	countVals := countCol.Int64s()
	addedCntVals := addedCntCol.Int64s()
	removedCntVals := removedCntCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeUint64(numRows, false)
	resultVals := resultCol.Uint64s()
	oldVals := oldCol.Uint64s()
	addedVals := addedCol.Uint64s()
	removedVals := removedCol.Uint64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		finalCnt := countVals[rowIdx]
		if finalCnt == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		addedCnt := addedCntVals[rowIdx]
		removedCnt := removedCntVals[rowIdx]
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		removeExists := !removedCol.IsNull(rowIdx)

		// chooseAddRemove indicates which delta side dominates the candidate extremum.
		// 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
		// 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
		chooseAddRemove := 0
		// sameExtremumValue means added and removed share the same extremum value.
		sameExtremumValue := false
		if addExists != removeExists {
			if addExists {
				chooseAddRemove = 1
			} else {
				chooseAddRemove = -1
			}
		} else if addExists && removeExists {
			if m.isMax {
				chooseAddRemove = cmp.Compare(addedVals[rowIdx], removedVals[rowIdx])
			} else {
				chooseAddRemove = cmp.Compare(removedVals[rowIdx], addedVals[rowIdx])
			}
			if chooseAddRemove == 0 {
				chooseAddRemove = cmp.Compare(addedCnt, removedCnt)
				sameExtremumValue = true
			}
		}

		decision := minMaxDecisionUseOld
		if chooseAddRemove == 0 {
			if sameExtremumValue {
				// added/removed have the same extremum value and the same count.
				// If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
				// than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
				// aggregates do not carry the replacement extremum value.
				// In this case, recompute is required.
				if !oldExists {
					decision = minMaxDecisionRecompute
				} else {
					cmpOld := cmp.Compare(addedVals[rowIdx], oldVals[rowIdx])
					if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
						decision = minMaxDecisionRecompute
					}
				}
			}
			// If sameExtremumValue is false, it means both sides are absent, and old value should be kept
		} else if chooseAddRemove > 0 {
			if !oldExists {
				decision = minMaxDecisionUseAdded
			} else {
				cmpOld := cmp.Compare(addedVals[rowIdx], oldVals[rowIdx])
				if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
					decision = minMaxDecisionUseAdded
				}
			}
		} else {
			if !oldExists {
				logutil.BgLogger().Error(
					"invalid min/max uint fast-path state (remove-dominant without old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("add_val", redactUint64Value(addedVals[rowIdx])),
					zap.String("remove_val", redactUint64Value(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max uint fast-path state (remove-dominant without old value) at row %d",
					rowIdx,
				)
			}
			cmpOld := cmp.Compare(removedVals[rowIdx], oldVals[rowIdx])
			if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
				logutil.BgLogger().Error(
					"invalid min/max uint fast-path state (remove-dominant outranks old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int("cmp_old", cmpOld),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("old_val", redactUint64Value(oldVals[rowIdx])),
					zap.String("add_val", redactUint64Value(addedVals[rowIdx])),
					zap.String("remove_val", redactUint64Value(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max uint fast-path state (remove-dominant outranks old value) at row %d",
					rowIdx,
				)
			}
			if cmpOld == 0 {
				decision = minMaxDecisionRecompute
			}
		}

		switch decision {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			resultVals[rowIdx] = addedVals[rowIdx]
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			resultCol.SetNull(rowIdx, true)
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxFloat32Merger struct {
	minMaxMergerBase
}

func (m *minMaxFloat32Merger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mergeWorkerData) error {
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
	if err := m.validateCountColumns(countCol, addedCntCol, removedCntCol); err != nil {
		return err
	}
	numRows := input.NumRows()
	countVals := countCol.Int64s()
	addedCntVals := addedCntCol.Int64s()
	removedCntVals := removedCntCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeFloat32(numRows, false)
	resultVals := resultCol.Float32s()
	oldVals := oldCol.Float32s()
	addedVals := addedCol.Float32s()
	removedVals := removedCol.Float32s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		finalCnt := countVals[rowIdx]
		if finalCnt == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		addedCnt := addedCntVals[rowIdx]
		removedCnt := removedCntVals[rowIdx]
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		removeExists := !removedCol.IsNull(rowIdx)

		// chooseAddRemove indicates which delta side dominates the candidate extremum.
		// 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
		// 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
		chooseAddRemove := 0
		// sameExtremumValue means added and removed share the same extremum value.
		sameExtremumValue := false
		if addExists != removeExists {
			if addExists {
				chooseAddRemove = 1
			} else {
				chooseAddRemove = -1
			}
		} else if addExists && removeExists {
			if m.isMax {
				chooseAddRemove = cmpFloat32(addedVals[rowIdx], removedVals[rowIdx])
			} else {
				chooseAddRemove = cmpFloat32(removedVals[rowIdx], addedVals[rowIdx])
			}
			if chooseAddRemove == 0 {
				chooseAddRemove = cmp.Compare(addedCnt, removedCnt)
				sameExtremumValue = true
			}
		}

		decision := minMaxDecisionUseOld
		if chooseAddRemove == 0 {
			if sameExtremumValue {
				// added/removed have the same extremum value and the same count.
				// If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
				// than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
				// aggregates do not carry the replacement extremum value.
				// In this case, recompute is required.
				if !oldExists {
					decision = minMaxDecisionRecompute
				} else {
					cmpOld := cmpFloat32(addedVals[rowIdx], oldVals[rowIdx])
					if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
						decision = minMaxDecisionRecompute
					}
				}
			}
			// If sameExtremumValue is false, it means both sides are absent, and old value should be kept
		} else if chooseAddRemove > 0 {
			if !oldExists {
				decision = minMaxDecisionUseAdded
			} else {
				cmpOld := cmpFloat32(addedVals[rowIdx], oldVals[rowIdx])
				if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
					decision = minMaxDecisionUseAdded
				}
			}
		} else {
			if !oldExists {
				logutil.BgLogger().Error(
					"invalid min/max float32 fast-path state (remove-dominant without old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("add_val", redactFloat32Value(addedVals[rowIdx])),
					zap.String("remove_val", redactFloat32Value(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max float32 fast-path state (remove-dominant without old value) at row %d",
					rowIdx,
				)
			}
			cmpOld := cmpFloat32(removedVals[rowIdx], oldVals[rowIdx])
			if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
				logutil.BgLogger().Error(
					"invalid min/max float32 fast-path state (remove-dominant outranks old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int("cmp_old", cmpOld),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("old_val", redactFloat32Value(oldVals[rowIdx])),
					zap.String("add_val", redactFloat32Value(addedVals[rowIdx])),
					zap.String("remove_val", redactFloat32Value(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max float32 fast-path state (remove-dominant outranks old value) at row %d",
					rowIdx,
				)
			}
			if cmpOld == 0 {
				decision = minMaxDecisionRecompute
			}
		}

		switch decision {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			resultVals[rowIdx] = addedVals[rowIdx]
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			resultCol.SetNull(rowIdx, true)
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxFloat64Merger struct {
	minMaxMergerBase
}

func (m *minMaxFloat64Merger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mergeWorkerData) error {
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
	if err := m.validateCountColumns(countCol, addedCntCol, removedCntCol); err != nil {
		return err
	}
	numRows := input.NumRows()
	countVals := countCol.Int64s()
	addedCntVals := addedCntCol.Int64s()
	removedCntVals := removedCntCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeFloat64(numRows, false)
	resultVals := resultCol.Float64s()
	oldVals := oldCol.Float64s()
	addedVals := addedCol.Float64s()
	removedVals := removedCol.Float64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		finalCnt := countVals[rowIdx]
		if finalCnt == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		addedCnt := addedCntVals[rowIdx]
		removedCnt := removedCntVals[rowIdx]
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		removeExists := !removedCol.IsNull(rowIdx)

		// chooseAddRemove indicates which delta side dominates the candidate extremum.
		// 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
		// 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
		chooseAddRemove := 0
		// sameExtremumValue means added and removed share the same extremum value.
		sameExtremumValue := false
		if addExists != removeExists {
			if addExists {
				chooseAddRemove = 1
			} else {
				chooseAddRemove = -1
			}
		} else if addExists && removeExists {
			if m.isMax {
				chooseAddRemove = cmpFloat64(addedVals[rowIdx], removedVals[rowIdx])
			} else {
				chooseAddRemove = cmpFloat64(removedVals[rowIdx], addedVals[rowIdx])
			}
			if chooseAddRemove == 0 {
				chooseAddRemove = cmp.Compare(addedCnt, removedCnt)
				sameExtremumValue = true
			}
		}

		decision := minMaxDecisionUseOld
		if chooseAddRemove == 0 {
			if sameExtremumValue {
				// added/removed have the same extremum value and the same count.
				// If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
				// than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
				// aggregates do not carry the replacement extremum value.
				// In this case, recompute is required.
				if !oldExists {
					decision = minMaxDecisionRecompute
				} else {
					cmpOld := cmpFloat64(addedVals[rowIdx], oldVals[rowIdx])
					if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
						decision = minMaxDecisionRecompute
					}
				}
			}
			// If sameExtremumValue is false, it means both sides are absent, and old value should be kept
		} else if chooseAddRemove > 0 {
			if !oldExists {
				decision = minMaxDecisionUseAdded
			} else {
				cmpOld := cmpFloat64(addedVals[rowIdx], oldVals[rowIdx])
				if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
					decision = minMaxDecisionUseAdded
				}
			}
		} else {
			if !oldExists {
				logutil.BgLogger().Error(
					"invalid min/max float64 fast-path state (remove-dominant without old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("add_val", redactFloat64Value(addedVals[rowIdx])),
					zap.String("remove_val", redactFloat64Value(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max float64 fast-path state (remove-dominant without old value) at row %d",
					rowIdx,
				)
			}
			cmpOld := cmpFloat64(removedVals[rowIdx], oldVals[rowIdx])
			if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
				logutil.BgLogger().Error(
					"invalid min/max float64 fast-path state (remove-dominant outranks old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int("cmp_old", cmpOld),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("old_val", redactFloat64Value(oldVals[rowIdx])),
					zap.String("add_val", redactFloat64Value(addedVals[rowIdx])),
					zap.String("remove_val", redactFloat64Value(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max float64 fast-path state (remove-dominant outranks old value) at row %d",
					rowIdx,
				)
			}
			if cmpOld == 0 {
				decision = minMaxDecisionRecompute
			}
		}

		switch decision {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			resultVals[rowIdx] = addedVals[rowIdx]
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			resultCol.SetNull(rowIdx, true)
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxDecimalMerger struct {
	minMaxMergerBase
}

func (m *minMaxDecimalMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mergeWorkerData) error {
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
	if err := m.validateCountColumns(countCol, addedCntCol, removedCntCol); err != nil {
		return err
	}
	numRows := input.NumRows()
	countVals := countCol.Int64s()
	addedCntVals := addedCntCol.Int64s()
	removedCntVals := removedCntCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeDecimal(numRows, false)
	resultVals := resultCol.Decimals()
	oldVals := oldCol.Decimals()
	addedVals := addedCol.Decimals()
	removedVals := removedCol.Decimals()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		finalCnt := countVals[rowIdx]
		if finalCnt == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		addedCnt := addedCntVals[rowIdx]
		removedCnt := removedCntVals[rowIdx]
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		removeExists := !removedCol.IsNull(rowIdx)

		// chooseAddRemove indicates which delta side dominates the candidate extremum.
		// 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
		// 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
		chooseAddRemove := 0
		// sameExtremumValue means added and removed share the same extremum value.
		sameExtremumValue := false
		if addExists != removeExists {
			if addExists {
				chooseAddRemove = 1
			} else {
				chooseAddRemove = -1
			}
		} else if addExists && removeExists {
			if m.isMax {
				chooseAddRemove = addedVals[rowIdx].Compare(&removedVals[rowIdx])
			} else {
				chooseAddRemove = removedVals[rowIdx].Compare(&addedVals[rowIdx])
			}
			if chooseAddRemove == 0 {
				chooseAddRemove = cmp.Compare(addedCnt, removedCnt)
				sameExtremumValue = true
			}
		}

		decision := minMaxDecisionUseOld
		if chooseAddRemove == 0 {
			if sameExtremumValue {
				// added/removed have the same extremum value and the same count.
				// If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
				// than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
				// aggregates do not carry the replacement extremum value.
				// In this case, recompute is required.
				if !oldExists {
					decision = minMaxDecisionRecompute
				} else {
					cmpOld := addedVals[rowIdx].Compare(&oldVals[rowIdx])
					if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
						decision = minMaxDecisionRecompute
					}
				}
			}
			// If sameExtremumValue is false, it means both sides are absent, and old value should be kept
		} else if chooseAddRemove > 0 {
			if !oldExists {
				decision = minMaxDecisionUseAdded
			} else {
				cmpOld := addedVals[rowIdx].Compare(&oldVals[rowIdx])
				if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
					decision = minMaxDecisionUseAdded
				}
			}
		} else {
			if !oldExists {
				logutil.BgLogger().Error(
					"invalid min/max decimal fast-path state (remove-dominant without old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("add_val", redactDecimalValue(&addedVals[rowIdx])),
					zap.String("remove_val", redactDecimalValue(&removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max decimal fast-path state (remove-dominant without old value) at row %d",
					rowIdx,
				)
			}
			cmpOld := removedVals[rowIdx].Compare(&oldVals[rowIdx])
			if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
				logutil.BgLogger().Error(
					"invalid min/max decimal fast-path state (remove-dominant outranks old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int("cmp_old", cmpOld),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("old_val", redactDecimalValue(&oldVals[rowIdx])),
					zap.String("add_val", redactDecimalValue(&addedVals[rowIdx])),
					zap.String("remove_val", redactDecimalValue(&removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max decimal fast-path state (remove-dominant outranks old value) at row %d",
					rowIdx,
				)
			}
			if cmpOld == 0 {
				decision = minMaxDecisionRecompute
			}
		}

		switch decision {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			resultVals[rowIdx] = addedVals[rowIdx]
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			resultCol.SetNull(rowIdx, true)
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxTimeMerger struct {
	minMaxMergerBase
}

func (m *minMaxTimeMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mergeWorkerData) error {
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
	if err := m.validateCountColumns(countCol, addedCntCol, removedCntCol); err != nil {
		return err
	}
	numRows := input.NumRows()
	countVals := countCol.Int64s()
	addedCntVals := addedCntCol.Int64s()
	removedCntVals := removedCntCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeTime(numRows, false)
	resultVals := resultCol.Times()
	oldVals := oldCol.Times()
	addedVals := addedCol.Times()
	removedVals := removedCol.Times()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		finalCnt := countVals[rowIdx]
		if finalCnt == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		addedCnt := addedCntVals[rowIdx]
		removedCnt := removedCntVals[rowIdx]
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		removeExists := !removedCol.IsNull(rowIdx)

		// chooseAddRemove indicates which delta side dominates the candidate extremum.
		// 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
		// 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
		chooseAddRemove := 0
		// sameExtremumValue means added and removed share the same extremum value.
		sameExtremumValue := false
		if addExists != removeExists {
			if addExists {
				chooseAddRemove = 1
			} else {
				chooseAddRemove = -1
			}
		} else if addExists && removeExists {
			if m.isMax {
				chooseAddRemove = addedVals[rowIdx].Compare(removedVals[rowIdx])
			} else {
				chooseAddRemove = removedVals[rowIdx].Compare(addedVals[rowIdx])
			}
			if chooseAddRemove == 0 {
				chooseAddRemove = cmp.Compare(addedCnt, removedCnt)
				sameExtremumValue = true
			}
		}

		decision := minMaxDecisionUseOld
		if chooseAddRemove == 0 {
			if sameExtremumValue {
				// added/removed have the same extremum value and the same count.
				// If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
				// than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
				// aggregates do not carry the replacement extremum value.
				// In this case, recompute is required.
				if !oldExists {
					decision = minMaxDecisionRecompute
				} else {
					cmpOld := addedVals[rowIdx].Compare(oldVals[rowIdx])
					if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
						decision = minMaxDecisionRecompute
					}
				}
			}
			// If sameExtremumValue is false, it means both sides are absent, and old value should be kept
		} else if chooseAddRemove > 0 {
			if !oldExists {
				decision = minMaxDecisionUseAdded
			} else {
				cmpOld := addedVals[rowIdx].Compare(oldVals[rowIdx])
				if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
					decision = minMaxDecisionUseAdded
				}
			}
		} else {
			if !oldExists {
				logutil.BgLogger().Error(
					"invalid min/max time fast-path state (remove-dominant without old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("add_val", redactTimeValue(addedVals[rowIdx])),
					zap.String("remove_val", redactTimeValue(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max time fast-path state (remove-dominant without old value) at row %d",
					rowIdx,
				)
			}
			cmpOld := removedVals[rowIdx].Compare(oldVals[rowIdx])
			if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
				logutil.BgLogger().Error(
					"invalid min/max time fast-path state (remove-dominant outranks old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int("cmp_old", cmpOld),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("old_val", redactTimeValue(oldVals[rowIdx])),
					zap.String("add_val", redactTimeValue(addedVals[rowIdx])),
					zap.String("remove_val", redactTimeValue(removedVals[rowIdx])),
				)
				return errors.Errorf(
					"invalid min/max time fast-path state (remove-dominant outranks old value) at row %d",
					rowIdx,
				)
			}
			if cmpOld == 0 {
				decision = minMaxDecisionRecompute
			}
		}

		switch decision {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			resultVals[rowIdx] = addedVals[rowIdx]
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			resultCol.SetNull(rowIdx, true)
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}

type minMaxDurationMerger struct {
	minMaxMergerBase
}

func (m *minMaxDurationMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mergeWorkerData) error {
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
	if err := m.validateCountColumns(countCol, addedCntCol, removedCntCol); err != nil {
		return err
	}
	numRows := input.NumRows()
	countVals := countCol.Int64s()
	addedCntVals := addedCntCol.Int64s()
	removedCntVals := removedCntCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeGoDuration(numRows, false)
	resultVals := resultCol.GoDurations()
	oldVals := oldCol.GoDurations()
	addedVals := addedCol.GoDurations()
	removedVals := removedCol.GoDurations()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		finalCnt := countVals[rowIdx]
		if finalCnt == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		addedCnt := addedCntVals[rowIdx]
		removedCnt := removedCntVals[rowIdx]
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		removeExists := !removedCol.IsNull(rowIdx)

		// chooseAddRemove indicates which delta side dominates the candidate extremum.
		// 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
		// 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
		chooseAddRemove := 0
		// sameExtremumValue means added and removed share the same extremum value.
		sameExtremumValue := false
		if addExists != removeExists {
			if addExists {
				chooseAddRemove = 1
			} else {
				chooseAddRemove = -1
			}
		} else if addExists && removeExists {
			if m.isMax {
				chooseAddRemove = cmp.Compare(int64(addedVals[rowIdx]), int64(removedVals[rowIdx]))
			} else {
				chooseAddRemove = cmp.Compare(int64(removedVals[rowIdx]), int64(addedVals[rowIdx]))
			}
			if chooseAddRemove == 0 {
				chooseAddRemove = cmp.Compare(addedCnt, removedCnt)
				sameExtremumValue = true
			}
		}

		decision := minMaxDecisionUseOld
		if chooseAddRemove == 0 {
			if sameExtremumValue {
				// added/removed have the same extremum value and the same count.
				// If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
				// than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
				// aggregates do not carry the replacement extremum value.
				// In this case, recompute is required.
				if !oldExists {
					decision = minMaxDecisionRecompute
				} else {
					cmpOld := cmp.Compare(int64(addedVals[rowIdx]), int64(oldVals[rowIdx]))
					if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
						decision = minMaxDecisionRecompute
					}
				}
			}
			// If sameExtremumValue is false, it means both sides are absent, and old value should be kept
		} else if chooseAddRemove > 0 {
			if !oldExists {
				decision = minMaxDecisionUseAdded
			} else {
				cmpOld := cmp.Compare(int64(addedVals[rowIdx]), int64(oldVals[rowIdx]))
				if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
					decision = minMaxDecisionUseAdded
				}
			}
		} else {
			if !oldExists {
				logutil.BgLogger().Error(
					"invalid min/max duration fast-path state (remove-dominant without old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("add_val", redactDurationValue(int64(addedVals[rowIdx]))),
					zap.String("remove_val", redactDurationValue(int64(removedVals[rowIdx]))),
				)
				return errors.Errorf(
					"invalid min/max duration fast-path state (remove-dominant without old value) at row %d",
					rowIdx,
				)
			}
			cmpOld := cmp.Compare(int64(removedVals[rowIdx]), int64(oldVals[rowIdx]))
			if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
				logutil.BgLogger().Error(
					"invalid min/max duration fast-path state (remove-dominant outranks old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int("cmp_old", cmpOld),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("old_val", redactDurationValue(int64(oldVals[rowIdx]))),
					zap.String("add_val", redactDurationValue(int64(addedVals[rowIdx]))),
					zap.String("remove_val", redactDurationValue(int64(removedVals[rowIdx]))),
				)
				return errors.Errorf(
					"invalid min/max duration fast-path state (remove-dominant outranks old value) at row %d",
					rowIdx,
				)
			}
			if cmpOld == 0 {
				decision = minMaxDecisionRecompute
			}
		}

		switch decision {
		case minMaxDecisionUseOld:
			if oldExists {
				resultVals[rowIdx] = oldVals[rowIdx]
			} else {
				resultCol.SetNull(rowIdx, true)
			}
		case minMaxDecisionUseAdded:
			resultVals[rowIdx] = addedVals[rowIdx]
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			resultCol.SetNull(rowIdx, true)
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

func (m *minMaxStringMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mergeWorkerData) error {
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
	if err := m.validateCountColumns(countCol, addedCntCol, removedCntCol); err != nil {
		return err
	}
	numRows := input.NumRows()
	countVals := countCol.Int64s()
	addedCntVals := addedCntCol.Int64s()
	removedCntVals := removedCntCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		finalCnt := countVals[rowIdx]
		if finalCnt == 0 {
			resultCol.AppendNull()
			continue
		}
		addedCnt := addedCntVals[rowIdx]
		removedCnt := removedCntVals[rowIdx]
		oldExists := !oldCol.IsNull(rowIdx)
		addExists := !addedCol.IsNull(rowIdx)
		removeExists := !removedCol.IsNull(rowIdx)

		var addVal, removeVal, oldVal string
		if addExists {
			addVal = addedCol.GetString(rowIdx)
		}
		if removeExists {
			removeVal = removedCol.GetString(rowIdx)
		}
		if oldExists {
			oldVal = oldCol.GetString(rowIdx)
		}

		// chooseAddRemove indicates which delta side dominates the candidate extremum.
		// 1 => add dominates, -1 => remove dominates, 0 => no dominant side.
		// 0 occurs when both sides are absent, or when both sides share the same extremum with balanced counts.
		chooseAddRemove := 0
		// sameExtremumValue means added and removed share the same extremum value.
		sameExtremumValue := false
		if addExists != removeExists {
			if addExists {
				chooseAddRemove = 1
			} else {
				chooseAddRemove = -1
			}
		} else if addExists && removeExists {
			if m.isMax {
				chooseAddRemove = m.collator.Compare(addVal, removeVal)
			} else {
				chooseAddRemove = m.collator.Compare(removeVal, addVal)
			}
			if chooseAddRemove == 0 {
				chooseAddRemove = cmp.Compare(addedCnt, removedCnt)
				sameExtremumValue = true
			}
		}

		decision := minMaxDecisionUseOld
		if chooseAddRemove == 0 {
			if sameExtremumValue {
				// added/removed have the same extremum value and the same count.
				// If old is NULL, or old is strictly smaller (MAX) / strictly larger (MIN)
				// than that value, this canceled value cannot remain the final extremum after merge, and MView-log delta
				// aggregates do not carry the replacement extremum value.
				// In this case, recompute is required.
				if !oldExists {
					decision = minMaxDecisionRecompute
				} else {
					cmpOld := m.collator.Compare(addVal, oldVal)
					if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
						decision = minMaxDecisionRecompute
					}
				}
			}
			// If sameExtremumValue is false, it means both sides are absent, and old value should be kept
		} else if chooseAddRemove > 0 {
			if !oldExists {
				decision = minMaxDecisionUseAdded
			} else {
				cmpOld := m.collator.Compare(addVal, oldVal)
				if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
					decision = minMaxDecisionUseAdded
				}
			}
		} else {
			if !oldExists {
				logutil.BgLogger().Error(
					"invalid min/max string fast-path state (remove-dominant without old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("add_val", redact.Value(addVal)),
					zap.String("remove_val", redact.Value(removeVal)),
				)
				return errors.Errorf(
					"invalid min/max string fast-path state (remove-dominant without old value) at row %d",
					rowIdx,
				)
			}
			cmpOld := m.collator.Compare(removeVal, oldVal)
			if (m.isMax && cmpOld > 0) || (!m.isMax && cmpOld < 0) {
				logutil.BgLogger().Error(
					"invalid min/max string fast-path state (remove-dominant outranks old value)",
					zap.Bool("is_max", m.isMax),
					zap.Int("cmp_old", cmpOld),
					zap.Int64("added_cnt", addedCnt),
					zap.Int64("removed_cnt", removedCnt),
					zap.String("old_val", redact.Value(oldVal)),
					zap.String("add_val", redact.Value(addVal)),
					zap.String("remove_val", redact.Value(removeVal)),
				)
				return errors.Errorf(
					"invalid min/max string fast-path state (remove-dominant outranks old value) at row %d",
					rowIdx,
				)
			}
			if cmpOld == 0 {
				decision = minMaxDecisionRecompute
			}
		}

		switch decision {
		case minMaxDecisionUseOld:
			if oldExists {
				resultCol.AppendString(oldVal)
			} else {
				resultCol.AppendNull()
			}
		case minMaxDecisionUseAdded:
			resultCol.AppendString(addVal)
		default:
			recomputeRows = append(recomputeRows, rowIdx)
			resultCol.AppendNull()
		}
	}
	storeMinMaxRecomputeRows(workerData, m.mappingIdx, recomputeRows)
	outputCols[0] = resultCol
	return nil
}
