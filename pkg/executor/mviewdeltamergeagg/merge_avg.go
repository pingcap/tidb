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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (e *Exec) buildAvgMerger(
	mapping Mapping,
	colID2ComputedIdx map[int]int,
	childTypes []*types.FieldType,
	definitionDivPrecisionIncrement int,
) (aggMerger, error) {
	if len(mapping.ColID) != 1 {
		return nil, errors.Errorf("AVG mapping expects exactly 1 output column, got %d", len(mapping.ColID))
	}
	if len(mapping.DependencyColID) != 3 {
		return nil, errors.Errorf("AVG mapping expects SUM, COUNT(expr), COUNT(*) dependencies, got %d", len(mapping.DependencyColID))
	}
	outputColID := mapping.ColID[0]
	retTp, err := resolveFieldTypeByColID(outputColID, childTypes)
	if err != nil {
		return nil, errors.Annotate(err, "AVG mapping output")
	}
	retEvalType := retTp.EvalType()
	if retEvalType != types.ETDecimal && retEvalType != types.ETReal {
		return nil, errors.Errorf("AVG mapping output must be decimal or real, got %s", retEvalType)
	}

	refs := make([]depRef, len(mapping.DependencyColID))
	var sumTp *types.FieldType
	for i, depID := range mapping.DependencyColID {
		refs[i], err = resolveDepRef(depID, colID2ComputedIdx, e.DeltaAggColCount)
		if err != nil {
			return nil, errors.Annotatef(err, "AVG mapping dependency col %d", depID)
		}
		if err := validateDepRefSource(refs[i], depFromComputed); err != nil {
			return nil, errors.Annotatef(err, "AVG mapping dependency col %d", depID)
		}
		tp, typeErr := resolveFieldTypeByColID(depID, childTypes)
		if typeErr != nil {
			return nil, typeErr
		}
		switch i {
		case 0:
			sumTp = tp
			sumEvalType := tp.EvalType()
			if sumEvalType != types.ETDecimal && sumEvalType != types.ETInt && sumEvalType != types.ETReal {
				return nil, errors.Errorf("AVG SUM dependency must be decimal, integer, or real, got %s", sumEvalType)
			}
		case 1, 2:
			if err := validateSignedIntType(tp); err != nil {
				return nil, errors.Annotatef(err, "AVG count dependency %d", i)
			}
		}
	}
	return &avgMerger{
		outputCols:            []int{outputColID},
		sumRef:                refs[0],
		countExprRef:          refs[1],
		countStarRef:          refs[2],
		retTp:                 retTp,
		sumTp:                 sumTp,
		divPrecisionIncrement: definitionDivPrecisionIncrement,
	}, nil
}

type avgMerger struct {
	outputCols            []int
	sumRef                depRef
	countExprRef          depRef
	countStarRef          depRef
	retTp                 *types.FieldType
	sumTp                 *types.FieldType
	divPrecisionIncrement int
}

func (m *avgMerger) outputColIDs() []int { return m.outputCols }

func (m *avgMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, _ *mergeWorkerData) error {
	_, err := resolveSingleOutputOldColumn("avg", input, outputCols, m.outputCols)
	if err != nil {
		return err
	}
	// AVG is finalized from the updated SUM/COUNT state, never from the old AVG value.
	sumCol, err := getDepColumn(input, computedByOrder, m.sumRef)
	if err != nil {
		return err
	}
	countExprCol, err := getDepColumn(input, computedByOrder, m.countExprRef)
	if err != nil {
		return err
	}
	countStarCol, err := getDepColumn(input, computedByOrder, m.countStarRef)
	if err != nil {
		return err
	}
	if countExprCol.HasNull() || countStarCol.HasNull() {
		return errors.New("AVG count dependency contains null")
	}
	numRows := input.NumRows()
	countExprVals := countExprCol.Int64s()
	countStarVals := countStarCol.Int64s()
	retEvalType := m.retTp.EvalType()
	sumEvalType := m.sumTp.EvalType()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	if retEvalType == types.ETReal {
		resultCol.ResizeFloat64(numRows, true)
	} else {
		resultCol.ResizeDecimal(numRows, true)
	}
	var resultFloatVals []float64
	var resultDecimalVals []types.MyDecimal
	if retEvalType == types.ETReal {
		resultFloatVals = resultCol.Float64s()
	} else {
		resultDecimalVals = resultCol.Decimals()
	}
	var sumFloatVals []float64
	var sumDecimalVals []types.MyDecimal
	var sumIntVals []int64
	var sumUintVals []uint64
	switch sumEvalType {
	case types.ETReal:
		sumFloatVals = sumCol.Float64s()
	case types.ETDecimal:
		sumDecimalVals = sumCol.Decimals()
	case types.ETInt:
		if mysql.HasUnsignedFlag(m.sumTp.GetFlag()) {
			sumUintVals = sumCol.Uint64s()
		} else {
			sumIntVals = sumCol.Int64s()
		}
	}
	frac := m.retTp.GetDecimal()
	if frac == types.UnspecifiedLength {
		frac = mysql.MaxDecimalScale
	}

	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		countExpr := countExprVals[rowIdx]
		countStar := countStarVals[rowIdx]
		if countExpr < 0 || countStar < 0 {
			return errors.Errorf("AVG count becomes negative: count(expr)=%d count(*)=%d", countExpr, countStar)
		}
		if countExpr > countStar {
			return errors.Errorf("AVG count invariant violated: count(expr)=%d exceeds count(*)=%d", countExpr, countStar)
		}
		sumIsNull := sumCol.IsNull(rowIdx)
		if countExpr == 0 {
			if !sumIsNull {
				return errors.New("AVG state invariant violated: zero COUNT(expr) with non-NULL SUM")
			}
			resultCol.SetNull(rowIdx, true)
			continue
		}
		if sumIsNull {
			return errors.New("AVG state invariant violated: positive COUNT(expr) with NULL SUM")
		}

		if sumEvalType == types.ETReal {
			if retEvalType != types.ETReal {
				return errors.Errorf("AVG real SUM dependency requires real output, got %s", retEvalType)
			}
			resultFloatVals[rowIdx] = sumFloatVals[rowIdx] / float64(countExpr)
			resultCol.SetNull(rowIdx, false)
			continue
		}

		var sum types.MyDecimal
		switch sumEvalType {
		case types.ETDecimal:
			sum = sumDecimalVals[rowIdx]
		case types.ETInt:
			if sumUintVals != nil {
				sum = *types.NewDecFromUint(sumUintVals[rowIdx])
			} else {
				sum = *types.NewDecFromInt(sumIntVals[rowIdx])
			}
		default:
			return errors.Errorf("AVG SUM dependency has unsupported eval type %s", sumEvalType)
		}
		avg := new(types.MyDecimal)
		if err := types.DecimalDiv(&sum, types.NewDecFromInt(countExpr), avg, m.divPrecisionIncrement); err != nil {
			return err
		}
		if err := avg.Round(avg, frac, types.ModeHalfUp); err != nil {
			return err
		}
		candidate := *avg
		materialized, err := types.ProduceDecWithSpecifiedTp(types.StrictContext, &candidate, m.retTp)
		if err != nil {
			return errors.Annotate(err, "AVG result cannot be represented by the MV output type")
		}
		if materialized.Compare(avg) != 0 {
			return errors.Errorf("AVG result would change value %s to %s", avg.String(), materialized.String())
		}
		resultDecimalVals[rowIdx] = *materialized
		resultCol.SetNull(rowIdx, false)
	}
	outputCols[0] = resultCol
	return nil
}
