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
) (aggMerger, error) {
	if e.TargetInfo == nil || e.TargetInfo.MaterializedView == nil {
		return nil, errors.New("AVG merge requires materialized view metadata")
	}
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
			if sumEvalType := tp.EvalType(); sumEvalType != retEvalType {
				return nil, errors.Errorf("AVG SUM dependency type %s does not match AVG output type %s", sumEvalType, retEvalType)
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
		divPrecisionIncrement: e.TargetInfo.MaterializedView.DefinitionDivPrecisionIncrement,
	}, nil
}

type avgMerger struct {
	outputCols            []int
	sumRef                depRef
	countExprRef          depRef
	countStarRef          depRef
	retTp                 *types.FieldType
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
	isReal := m.retTp.EvalType() == types.ETReal
	resultCol := chunk.NewColumn(m.retTp, numRows)
	var sumFloatVals, resultFloatVals []float64
	var sumDecimalVals, resultDecimalVals []types.MyDecimal
	if isReal {
		resultCol.ResizeFloat64(numRows, true)
		sumFloatVals = sumCol.Float64s()
		resultFloatVals = resultCol.Float64s()
	} else {
		resultCol.ResizeDecimal(numRows, true)
		sumDecimalVals = sumCol.Decimals()
		resultDecimalVals = resultCol.Decimals()
	}
	frac := m.retTp.GetDecimal()
	if frac == types.UnspecifiedLength {
		frac = mysql.MaxDecimalScale
	}

	for rowIdx := range numRows {
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

		if isReal {
			resultFloatVals[rowIdx] = sumFloatVals[rowIdx] / float64(countExpr)
			resultCol.SetNull(rowIdx, false)
			continue
		}

		sum := sumDecimalVals[rowIdx]
		var avg, count types.MyDecimal
		if err := types.DecimalDiv(&sum, count.FromInt(countExpr), &avg, m.divPrecisionIncrement); err != nil {
			return err
		}
		if err := avg.Round(&avg, frac, types.ModeHalfUp); err != nil {
			return err
		}
		resultDecimalVals[rowIdx] = avg
		resultCol.SetNull(rowIdx, false)
	}
	outputCols[0] = resultCol
	return nil
}
