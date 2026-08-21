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
	if retTp.EvalType() != types.ETDecimal {
		return nil, errors.Errorf("AVG mapping output must be decimal, got %s", retTp.EvalType())
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
			if tp.EvalType() != types.ETDecimal && tp.EvalType() != types.ETInt {
				return nil, errors.Errorf("AVG SUM dependency must be decimal or integer, got %s", tp.EvalType())
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
		divPrecisionIncrement: mapping.DivPrecisionIncrement,
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
	countExprVals := countExprCol.Int64s()
	countStarVals := countStarCol.Int64s()
	resultCol := chunk.NewColumn(m.retTp, input.NumRows())
	resultCol.ResizeDecimal(input.NumRows(), true)

	for rowIdx := 0; rowIdx < input.NumRows(); rowIdx++ {
		countExpr := countExprVals[rowIdx]
		countStar := countStarVals[rowIdx]
		if countExpr < 0 || countStar < 0 {
			return errors.Errorf("AVG count becomes negative: count(expr)=%d count(*)=%d", countExpr, countStar)
		}
		if countExpr > countStar {
			return errors.Errorf("AVG count invariant violated: count(expr)=%d exceeds count(*)=%d", countExpr, countStar)
		}
		if countExpr == 0 {
			if !sumCol.IsNull(rowIdx) {
				return errors.New("AVG state invariant violated: zero COUNT(expr) with non-NULL SUM")
			}
			resultCol.SetNull(rowIdx, true)
			continue
		}
		if sumCol.IsNull(rowIdx) {
			return errors.New("AVG state invariant violated: positive COUNT(expr) with NULL SUM")
		}

		var sum types.MyDecimal
		switch m.sumTp.EvalType() {
		case types.ETDecimal:
			sum = sumCol.Decimals()[rowIdx]
		case types.ETInt:
			if mysql.HasUnsignedFlag(m.sumTp.GetFlag()) {
				sum = *types.NewDecFromUint(sumCol.Uint64s()[rowIdx])
			} else {
				sum = *types.NewDecFromInt(sumCol.Int64s()[rowIdx])
			}
		default:
			return errors.Errorf("AVG SUM dependency has unsupported eval type %s", m.sumTp.EvalType())
		}
		avg := new(types.MyDecimal)
		if err := types.DecimalDiv(&sum, types.NewDecFromInt(countExpr), avg, m.divPrecisionIncrement); err != nil {
			return err
		}
		frac := m.retTp.GetDecimal()
		if frac == types.UnspecifiedLength {
			frac = mysql.MaxDecimalScale
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
		resultCol.Decimals()[rowIdx] = *materialized
		resultCol.SetNull(rowIdx, false)
	}
	outputCols[0] = resultCol
	return nil
}
