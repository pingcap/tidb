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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func validateSumValueTypes(outputTp, deltaTp *types.FieldType) error {
	if outputTp == nil || deltaTp == nil {
		return errors.New("SUM mapping type is unavailable")
	}
	outputEvalTp := outputTp.EvalType()
	deltaEvalTp := deltaTp.EvalType()
	if outputEvalTp != deltaEvalTp {
		return errors.Errorf("SUM mapping type mismatch: output eval type %s differs from delta eval type %s", outputEvalTp, deltaEvalTp)
	}
	if outputEvalTp == types.ETInt && mysql.HasUnsignedFlag(deltaTp.GetFlag()) {
		return errors.New("SUM int merge requires signed delta dependency")
	}
	return nil
}

func (e *MVMergeAggExec) buildSumMerger(
	mapping MVMergeAggMapping,
	colID2ComputedIdx map[int]int,
	childTypes []*types.FieldType,
) (aggMerger, error) {
	if len(mapping.ColID) != 1 {
		return nil, errors.Errorf("SUM mapping expects exactly 1 output column, got %d", len(mapping.ColID))
	}
	if len(mapping.DependencyColID) < 1 || len(mapping.DependencyColID) > 2 {
		return nil, errors.Errorf("SUM mapping expects 1 or 2 dependencies, got %d", len(mapping.DependencyColID))
	}
	outputColID := mapping.ColID[0]
	deltaRef, err := resolveDepRef(mapping.DependencyColID[0], colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, err
	}
	deltaColID := mapping.DependencyColID[0]
	deltaTp, err := resolveFieldTypeByColID(deltaColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "SUM mapping dependency col %d", deltaColID)
	}
	retTp, err := resolveFieldTypeByColID(outputColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "SUM mapping output col %d", outputColID)
	}
	if err := validateSumValueTypes(retTp, deltaTp); err != nil {
		return nil, err
	}
	isNullableSum := len(mapping.DependencyColID) == 2

	var countRef depRef
	if isNullableSum {
		countRef, err = resolveDepRef(mapping.DependencyColID[1], colID2ComputedIdx, e.DeltaAggColCount)
		if err != nil {
			return nil, err
		}
		if countRef.source != depFromComputed {
			return nil, errors.New("SUM(nullable expr) requires final COUNT(expr) from previously computed columns")
		}
		countColID := mapping.DependencyColID[1]
		countTp, err := resolveFieldTypeByColID(countColID, childTypes)
		if err != nil {
			return nil, errors.Annotatef(err, "SUM mapping count dependency col %d", countColID)
		}
		if countTp.EvalType() != types.ETInt {
			return nil, errors.Errorf("SUM mapping count dependency col %d must be integer, got %s", countColID, countTp.EvalType())
		}
	}

	switch retTp.EvalType() {
	case types.ETInt:
		if deltaTp.EvalType() != types.ETInt {
			return nil, errors.Errorf("SUM int merge expects integer delta dependency, got %s", deltaTp.EvalType())
		}
		if mysql.HasUnsignedFlag(retTp.GetFlag()) {
			return &sumUintMerger{
				outputCols:  []int{outputColID},
				deltaRef:    deltaRef,
				countRef:    countRef,
				hasCountRef: isNullableSum,
				retTp:       retTp,
			}, nil
		}
		return &sumIntMerger{
			outputCols:  []int{outputColID},
			deltaRef:    deltaRef,
			countRef:    countRef,
			hasCountRef: isNullableSum,
			retTp:       retTp,
		}, nil
	case types.ETReal:
		return &sumRealMerger{
			outputCols:  []int{outputColID},
			deltaRef:    deltaRef,
			countRef:    countRef,
			hasCountRef: isNullableSum,
			retTp:       retTp,
		}, nil
	case types.ETDecimal:
		return &sumDecimalMerger{
			outputCols:  []int{outputColID},
			deltaRef:    deltaRef,
			countRef:    countRef,
			hasCountRef: isNullableSum,
			retTp:       retTp,
		}, nil
	default:
		return nil, errors.Errorf("SUM merge does not support eval type %s", retTp.EvalType())
	}
}

type sumIntMerger struct {
	outputCols  []int
	deltaRef    depRef
	countRef    depRef
	hasCountRef bool
	retTp       *types.FieldType
}

func (m *sumIntMerger) outputColIDs() []int {
	return m.outputCols
}

func (m *sumIntMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column) error {
	if len(outputCols) != 1 {
		return errors.Errorf("sum merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	outputColID := m.outputCols[0]
	if outputColID < 0 || outputColID >= input.NumCols() {
		return errors.Errorf("sum output col %d out of input range", outputColID)
	}
	oldCol := input.Column(outputColID)
	deltaCol, err := getDepColumn(input, computedByOrder, m.deltaRef)
	if err != nil {
		return err
	}
	var countCol *chunk.Column
	if m.hasCountRef {
		countCol, err = getDepColumn(input, computedByOrder, m.countRef)
		if err != nil {
			return err
		}
	}

	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeInt64(numRows, false)
	resultVals := resultCol.Int64s()

	oldVals := oldCol.Int64s()
	deltaVals := deltaCol.Int64s()
	if !m.hasCountRef {
		// Fast path for SUM(non-null expr):
		// delta SUM is expected to be non-null by plan contract, so we only need
		// to handle old NULL (newly inserted group where MV side is absent).
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if oldCol.IsNull(rowIdx) {
				resultVals[rowIdx] = deltaVals[rowIdx]
				continue
			}
			sum, err := types.AddInt64(oldVals[rowIdx], deltaVals[rowIdx])
			if err != nil {
				return err
			}
			resultVals[rowIdx] = sum
		}
		outputCols[0] = resultCol
		return nil
	}

	countVals := countCol.Int64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if countVals[rowIdx] < 0 {
			return errors.Errorf("count(expr) becomes negative (%d) at row %d", countVals[rowIdx], rowIdx)
		}
		if countVals[rowIdx] == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}

		oldIsNull := oldCol.IsNull(rowIdx)
		deltaIsNull := deltaCol.IsNull(rowIdx)
		switch {
		case oldIsNull && deltaIsNull:
			resultCol.SetNull(rowIdx, true)
		case oldIsNull:
			resultVals[rowIdx] = deltaVals[rowIdx]
		case deltaIsNull:
			resultVals[rowIdx] = oldVals[rowIdx]
		default:
			sum, err := types.AddInt64(oldVals[rowIdx], deltaVals[rowIdx])
			if err != nil {
				return err
			}
			resultVals[rowIdx] = sum
		}
	}
	outputCols[0] = resultCol
	return nil
}

type sumUintMerger struct {
	outputCols  []int
	deltaRef    depRef
	countRef    depRef
	hasCountRef bool
	retTp       *types.FieldType
}

func (m *sumUintMerger) outputColIDs() []int {
	return m.outputCols
}

func (m *sumUintMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column) error {
	if len(outputCols) != 1 {
		return errors.Errorf("sum merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	outputColID := m.outputCols[0]
	if outputColID < 0 || outputColID >= input.NumCols() {
		return errors.Errorf("sum output col %d out of input range", outputColID)
	}
	oldCol := input.Column(outputColID)
	deltaCol, err := getDepColumn(input, computedByOrder, m.deltaRef)
	if err != nil {
		return err
	}
	var countCol *chunk.Column
	if m.hasCountRef {
		countCol, err = getDepColumn(input, computedByOrder, m.countRef)
		if err != nil {
			return err
		}
	}

	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeUint64(numRows, false)
	resultVals := resultCol.Uint64s()

	oldVals := oldCol.Uint64s()
	deltaVals := deltaCol.Int64s()
	if !m.hasCountRef {
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if oldCol.IsNull(rowIdx) {
				sum, err := types.AddInteger(0, deltaVals[rowIdx])
				if err != nil {
					return err
				}
				resultVals[rowIdx] = sum
				continue
			}
			sum, err := types.AddInteger(oldVals[rowIdx], deltaVals[rowIdx])
			if err != nil {
				return err
			}
			resultVals[rowIdx] = sum
		}
		outputCols[0] = resultCol
		return nil
	}

	countVals := countCol.Int64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if countVals[rowIdx] < 0 {
			return errors.Errorf("count(expr) becomes negative (%d) at row %d", countVals[rowIdx], rowIdx)
		}
		if countVals[rowIdx] == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}

		oldIsNull := oldCol.IsNull(rowIdx)
		deltaIsNull := deltaCol.IsNull(rowIdx)
		switch {
		case oldIsNull && deltaIsNull:
			resultCol.SetNull(rowIdx, true)
		case oldIsNull:
			sum, err := types.AddInteger(0, deltaVals[rowIdx])
			if err != nil {
				return err
			}
			resultVals[rowIdx] = sum
		case deltaIsNull:
			resultVals[rowIdx] = oldVals[rowIdx]
		default:
			sum, err := types.AddInteger(oldVals[rowIdx], deltaVals[rowIdx])
			if err != nil {
				return err
			}
			resultVals[rowIdx] = sum
		}
	}
	outputCols[0] = resultCol
	return nil
}

type sumRealMerger struct {
	outputCols  []int
	deltaRef    depRef
	countRef    depRef
	hasCountRef bool
	retTp       *types.FieldType
}

func (m *sumRealMerger) outputColIDs() []int {
	return m.outputCols
}

func (m *sumRealMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column) error {
	if len(outputCols) != 1 {
		return errors.Errorf("sum merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	outputColID := m.outputCols[0]
	if outputColID < 0 || outputColID >= input.NumCols() {
		return errors.Errorf("sum output col %d out of input range", outputColID)
	}
	oldCol := input.Column(outputColID)
	deltaCol, err := getDepColumn(input, computedByOrder, m.deltaRef)
	if err != nil {
		return err
	}
	var countCol *chunk.Column
	if m.hasCountRef {
		countCol, err = getDepColumn(input, computedByOrder, m.countRef)
		if err != nil {
			return err
		}
	}

	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeFloat64(numRows, false)
	resultVals := resultCol.Float64s()

	oldVals := oldCol.Float64s()
	deltaVals := deltaCol.Float64s()
	if !m.hasCountRef {
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if oldCol.IsNull(rowIdx) {
				resultVals[rowIdx] = deltaVals[rowIdx]
				continue
			}
			resultVals[rowIdx] = oldVals[rowIdx] + deltaVals[rowIdx]
		}
		outputCols[0] = resultCol
		return nil
	}

	countVals := countCol.Int64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if countVals[rowIdx] < 0 {
			return errors.Errorf("count(expr) becomes negative (%d) at row %d", countVals[rowIdx], rowIdx)
		}
		if countVals[rowIdx] == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldIsNull := oldCol.IsNull(rowIdx)
		deltaIsNull := deltaCol.IsNull(rowIdx)
		switch {
		case oldIsNull && deltaIsNull:
			resultCol.SetNull(rowIdx, true)
		case oldIsNull:
			resultVals[rowIdx] = deltaVals[rowIdx]
		case deltaIsNull:
			resultVals[rowIdx] = oldVals[rowIdx]
		default:
			resultVals[rowIdx] = oldVals[rowIdx] + deltaVals[rowIdx]
		}
	}
	outputCols[0] = resultCol
	return nil
}

type sumDecimalMerger struct {
	outputCols  []int
	deltaRef    depRef
	countRef    depRef
	hasCountRef bool
	retTp       *types.FieldType
}

func (m *sumDecimalMerger) outputColIDs() []int {
	return m.outputCols
}

func (m *sumDecimalMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column) error {
	if len(outputCols) != 1 {
		return errors.Errorf("sum merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	outputColID := m.outputCols[0]
	if outputColID < 0 || outputColID >= input.NumCols() {
		return errors.Errorf("sum output col %d out of input range", outputColID)
	}
	oldCol := input.Column(outputColID)
	deltaCol, err := getDepColumn(input, computedByOrder, m.deltaRef)
	if err != nil {
		return err
	}
	var countCol *chunk.Column
	if m.hasCountRef {
		countCol, err = getDepColumn(input, computedByOrder, m.countRef)
		if err != nil {
			return err
		}
	}

	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeDecimal(numRows, false)
	resultVals := resultCol.Decimals()

	oldVals := oldCol.Decimals()
	deltaVals := deltaCol.Decimals()
	if !m.hasCountRef {
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if oldCol.IsNull(rowIdx) {
				resultVals[rowIdx] = deltaVals[rowIdx]
				continue
			}
			var sum types.MyDecimal
			if err := types.DecimalAdd(&oldVals[rowIdx], &deltaVals[rowIdx], &sum); err != nil {
				return err
			}
			resultVals[rowIdx] = sum
		}
		outputCols[0] = resultCol
		return nil
	}

	countVals := countCol.Int64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if countVals[rowIdx] < 0 {
			return errors.Errorf("count(expr) becomes negative (%d) at row %d", countVals[rowIdx], rowIdx)
		}
		if countVals[rowIdx] == 0 {
			resultCol.SetNull(rowIdx, true)
			continue
		}
		oldIsNull := oldCol.IsNull(rowIdx)
		deltaIsNull := deltaCol.IsNull(rowIdx)
		switch {
		case oldIsNull && deltaIsNull:
			resultCol.SetNull(rowIdx, true)
		case oldIsNull:
			resultVals[rowIdx] = deltaVals[rowIdx]
		case deltaIsNull:
			resultVals[rowIdx] = oldVals[rowIdx]
		default:
			var sum types.MyDecimal
			if err := types.DecimalAdd(&oldVals[rowIdx], &deltaVals[rowIdx], &sum); err != nil {
				return err
			}
			resultVals[rowIdx] = sum
		}
	}
	outputCols[0] = resultCol
	return nil
}
