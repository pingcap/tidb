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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
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
	ranges []*ranger.Range
}

// The type of the exprSet.
const (
	indexType = iota
	pkType
	colType
)

const unknownColumnID = math.MinInt64

// getConstantColumnID receives two expressions and if one of them is column and another is constant, it returns the
// ID of the column.
func getConstantColumnID(e []expression.Expression) int64 {
	if len(e) != 2 {
		return unknownColumnID
	}
	col, ok1 := e[0].(*expression.Column)
	_, ok2 := e[1].(*expression.Constant)
	if ok1 && ok2 {
		return col.ID
	}
	col, ok1 = e[1].(*expression.Column)
	_, ok2 = e[0].(*expression.Constant)
	if ok1 && ok2 {
		return col.ID
	}
	return unknownColumnID
}

func pseudoSelectivity(t *Table, exprs []expression.Expression) float64 {
	minFactor := selectionFactor
	uniqueCol := make(map[string]bool)
	for _, expr := range exprs {
		fun, ok := expr.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		colID := getConstantColumnID(fun.GetArgs())
		if colID == unknownColumnID {
			continue
		}
		switch fun.FuncName.L {
		case ast.EQ, ast.NullEQ, ast.In:
			minFactor = math.Min(minFactor, 1.0/pseudoEqualRate)
			col, ok := t.Columns[colID]
			if !ok {
				continue
			}
			if mysql.HasUniKeyFlag(col.Info.Flag) {
				uniqueCol[col.Info.Name.L] = true
			}
			if mysql.HasPriKeyFlag(col.Info.Flag) {
				if t.PKIsHandle {
					return 1.0 / float64(t.Count)
				}
				uniqueCol[col.Info.Name.L] = true
			}
		case ast.GE, ast.GT, ast.LE, ast.LT:
			minFactor = math.Min(minFactor, 1.0/pseudoLessRate)
			// FIXME: To resolve the between case.
		}
	}
	if len(uniqueCol) == 0 {
		return minFactor
	}
	// use the unique key info
	for _, idx := range t.Indices {
		if !idx.Info.Unique {
			continue
		}
		unique := true
		for _, col := range idx.Info.Columns {
			if !uniqueCol[col.Name.L] {
				unique = false
				break
			}
		}
		if unique {
			return 1.0 / float64(t.Count)
		}
	}
	return minFactor
}

// isColEqCorCol checks if the expression is a eq function that one side is correlated column and another is column.
// If so, it will return the column's reference. Otherwise return nil instead.
func isColEqCorCol(filter expression.Expression) *expression.Column {
	f, ok := filter.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		return nil
	}
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		if _, ok := f.GetArgs()[1].(*expression.CorrelatedColumn); ok {
			return c
		}
	}
	if c, ok := f.GetArgs()[1].(*expression.Column); ok {
		if _, ok := f.GetArgs()[0].(*expression.CorrelatedColumn); ok {
			return c
		}
	}
	return nil
}

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// TODO: support expressions that the top layer is a DNF.
// Currently the time complexity is o(n^2).
func (t *Table) Selectivity(ctx sessionctx.Context, exprs []expression.Expression) (float64, error) {
	// If table's count is zero or conditions are empty, we should return 100% selectivity.
	if t.Count == 0 || len(exprs) == 0 {
		return 1, nil
	}
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if len(exprs) > 63 || (len(t.Columns) == 0 && len(t.Indices) == 0) {
		return pseudoSelectivity(t, exprs), nil
	}
	ret := 1.0
	var sets []*exprSet
	sc := ctx.GetSessionVars().StmtCtx

	remainedExprs := make([]expression.Expression, 0, len(exprs))

	// Deal with the correlated column.
	for _, expr := range exprs {
		if c := isColEqCorCol(expr); c != nil && !t.ColumnIsInvalid(sc, c.ID) {
			colHist := t.Columns[c.ID]
			if colHist.NDV > 0 {
				ret *= 1 / float64(colHist.NDV)
			}
		} else {
			remainedExprs = append(remainedExprs, expr)
		}
	}

	extractedCols := make([]*expression.Column, 0, len(t.Columns))
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, remainedExprs, nil)
	for _, colInfo := range t.Columns {
		col := expression.ColInfo2Col(extractedCols, colInfo.Info)
		if col != nil {
			maskCovered, ranges, err := getMaskAndRanges(ctx, remainedExprs, ranger.ColumnRangeType, nil, col)
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
		if len(idxCols) > 0 {
			maskCovered, ranges, err := getMaskAndRanges(ctx, remainedExprs, ranger.IndexRangeType, lengths, idxCols...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			sets = append(sets, &exprSet{tp: indexType, ID: idxInfo.ID, mask: maskCovered, ranges: ranges})
		}
	}
	sets = getUsableSetsByGreedy(sets)
	// Initialize the mask with the full set.
	mask := (int64(1) << uint(len(remainedExprs))) - 1
	for _, set := range sets {
		mask ^= set.mask
		var (
			rowCount float64
			err      error
		)
		switch set.tp {
		case pkType:
			rowCount, err = t.GetRowCountByIntColumnRanges(sc, set.ID, set.ranges)
		case colType:
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

func getMaskAndRanges(ctx sessionctx.Context, exprs []expression.Expression, rangeType ranger.RangeType,
	lengths []int, cols ...*expression.Column) (mask int64, ranges []*ranger.Range, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	var accessConds []expression.Expression
	switch rangeType {
	case ranger.ColumnRangeType:
		accessConds = ranger.ExtractAccessConditionsForColumn(exprs, cols[0].ColName)
		ranges, err = ranger.BuildColumnRange(accessConds, sc, cols[0].RetType)
	case ranger.IndexRangeType:
		ranges, accessConds, err = ranger.DetachSimpleCondAndBuildRangeForIndex(ctx, exprs, cols, lengths)
	default:
		panic("should never be here")
	}
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	for i := range exprs {
		for j := range accessConds {
			if exprs[i].Equal(ctx, accessConds[j]) {
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
			// This set cannot cover any thing, just skip it.
			if bits == 0 {
				continue
			}
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
