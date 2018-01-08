// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/ranger"
)

// If one condition can't be calculated, we will assume that the selectivity of this condition is 0.8.
const selectionFactor = 0.8

// exprSet is used for calculating selectivity.
type exprSet struct {
	tp int
	ID int64
	// mask is a bit pattern whose ith bit will indicate whether the ith expression is covered by this index/column.
	mask int64
	// ranges contains all the ranges we got.
	ranges []*ranger.NewRange
}

// The type of the exprSet.
const (
	indexType = iota
	pkType
	colType
)

// checkColumnConstant receives two expressions and makes sure one of them is column and another is constant.
func checkColumnConstant(e []expression.Expression) bool {
	if len(e) != 2 {
		return false
	}
	_, ok1 := e[0].(*expression.Column)
	_, ok2 := e[1].(*expression.Constant)
	if ok1 && ok2 {
		return true
	}
	_, ok1 = e[1].(*expression.Column)
	_, ok2 = e[0].(*expression.Constant)
	return ok1 && ok2
}

func pseudoSelectivity(exprs []expression.Expression) float64 {
	minFactor := selectionFactor
	for _, expr := range exprs {
		if fun, ok := expr.(*expression.ScalarFunction); ok && checkColumnConstant(fun.GetArgs()) {
			switch fun.FuncName.L {
			case ast.EQ, ast.NullEQ:
				minFactor = math.Min(minFactor, 1.0/pseudoEqualRate)
			case ast.GE, ast.GT, ast.LE, ast.LT:
				minFactor = math.Min(minFactor, 1.0/pseudoLessRate)
				// FIXME: To resolve the between case.
			}
		}
	}
	return minFactor
}

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// TODO: support expressions that the top layer is a DNF.
// Currently the time complexity is o(n^2).
func (t *Table) Selectivity(ctx context.Context, exprs []expression.Expression) (float64, error) {
	if t.Count == 0 {
		return 1, nil
	}
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if t.Pseudo || len(exprs) > 63 || (len(t.Columns) == 0 && len(t.Indices) == 0) {
		return pseudoSelectivity(exprs), nil
	}
	if len(exprs) == 0 {
		return 1.0, nil
	}
	var sets []*exprSet
	sc := ctx.GetSessionVars().StmtCtx

	extractedCols := make([]*expression.Column, 0, len(t.Columns))
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, exprs, nil)
	for _, colInfo := range t.Columns {
		col := expression.ColInfo2Col(extractedCols, colInfo.Info)
		// This column should have histogram.
		if col != nil && !t.ColumnIsInvalid(ctx.GetSessionVars().StmtCtx, col.ID) {
			maskCovered, ranges, err := getMaskAndRanges(ctx, exprs, ranger.ColumnRangeType, nil, col)
			if err != nil {
				return 0, errors.Trace(err)
			}
			sets = append(sets, &exprSet{tp: colType, ID: col.ID, mask: maskCovered, ranges: ranges})
			if mysql.HasPriKeyFlag(colInfo.Info.Flag) {
				sets[len(sets)-1].tp = pkType
			}
		}
	}
	for _, idxInfo := range t.Indices {
		idxCols, lengths := expression.IndexInfo2Cols(extractedCols, idxInfo.Info)
		// This index should have histogram.
		if len(idxCols) > 0 && idxInfo.Histogram.Len() > 0 {
			maskCovered, ranges, err := getMaskAndRanges(ctx, exprs, ranger.IndexRangeType, lengths, idxCols...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			sets = append(sets, &exprSet{tp: indexType, ID: idxInfo.ID, mask: maskCovered, ranges: ranges})
		}
	}
	sets = getUsableSetsByGreedy(sets)
	ret := 1.0
	// Initialize the mask with the full set.
	mask := (int64(1) << uint(len(exprs))) - 1
	for _, set := range sets {
		mask ^= set.mask
		var (
			rowCount float64
			err      error
		)
		switch set.tp {
		case pkType, colType:
			rowCount, err = t.GetRowCountByColumnRanges(sc, set.ID, set.ranges)
		case indexType:
			rowCount, err = t.GetRowCountByIndexRanges(sc, set.ID, set.ranges)
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		ret *= rowCount / float64(t.Count)
	}
	// If there's still conditions which cannot be calculated, we will multiply a selectionFactor.
	if mask > 0 {
		ret *= selectionFactor
	}
	return ret, nil
}

func getMaskAndRanges(ctx context.Context, exprs []expression.Expression, rangeType ranger.RangeType,
	lengths []int, cols ...*expression.Column) (mask int64, ranges []*ranger.NewRange, err error) {
	accessConds := ranger.ExtractAccessConditions(exprs, rangeType, cols, lengths)
	sc := ctx.GetSessionVars().StmtCtx
	switch rangeType {
	case ranger.ColumnRangeType:
		ranges, err = ranger.BuildColumnRange(accessConds, sc, cols[0].RetType)
	case ranger.IndexRangeType:
		ranges, err = ranger.BuildIndexRange(sc, cols, lengths, accessConds)
	default:
		panic("should never be here")
	}
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	for i := range exprs {
		for j := range accessConds {
			if exprs[i].Equal(accessConds[j], ctx) {
				mask |= 1 << uint64(i)
				break
			}
		}
	}
	return mask, ranges, nil
}

// getUsableSetsByGreedy will select the indices and pk used for calculate selectivity by greedy algorithm.
func getUsableSetsByGreedy(sets []*exprSet) (newBlocks []*exprSet) {
	mask := int64(math.MaxInt64)
	for {
		// Choose the index that covers most.
		bestID, bestCount, bestTp := -1, 0, colType
		for i, set := range sets {
			set.mask &= mask
			bits := popCount(set.mask)
			if (bestTp == colType && set.tp < colType) || bestCount < bits {
				bestID, bestCount, bestTp = i, bits, set.tp
			}
		}
		if bestCount == 0 {
			break
		} else {
			// update the mask, remove the bit that sets[bestID].mask has.
			mask &^= sets[bestID].mask

			newBlocks = append(newBlocks, sets[bestID])
			// remove the chosen one
			sets = append(sets[:bestID], sets[bestID+1:]...)
		}
	}
	return
}

// popCount is the digit sum of the binary representation of the number x.
func popCount(x int64) int {
	ret := 0
	// x -= x & -x, remove the lowest bit of the x.
	// e.g. result will be 2 if x is 3.
	for ; x > 0; x -= x & -x {
		ret++
	}
	return ret
}
