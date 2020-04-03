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
	"math/bits"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
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
	// numCols is the number of columns contained in the index or column(which is always 1).
	numCols int
	// partCover indicates whether the bit in the mask is for a full cover or partial cover. It is only true
	// when the condition is a DNF expression on index, and the expression is not totally extracted as access condition.
	partCover bool
}

// The type of the exprSet.
const (
	indexType = iota
	pkType
	colType
)

func compareType(l, r int) int {
	if l == r {
		return 0
	}
	if l == colType {
		return -1
	}
	if l == pkType {
		return 1
	}
	if r == colType {
		return 1
	}
	return -1
}

// MockExprSet is only used for test.
func MockExprSet(id int64, m int64, num int) *exprSet {
	return &exprSet{ID: id, mask: m, numCols: num}
}

// MockExprSetSlice is only used for test.
func MockExprSetSlice(l int) []*exprSet {
	return make([]*exprSet, l)
}

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

func pseudoSelectivity(coll *HistColl, exprs []expression.Expression) float64 {
	minFactor := selectionFactor
	colExists := make(map[string]bool)
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
			col, ok := coll.Columns[colID]
			if !ok {
				continue
			}
			colExists[col.Info.Name.L] = true
			if mysql.HasUniKeyFlag(col.Info.Flag) {
				return 1.0 / float64(coll.Count)
			}
		case ast.GE, ast.GT, ast.LE, ast.LT:
			minFactor = math.Min(minFactor, 1.0/pseudoLessRate)
			// FIXME: To resolve the between case.
		}
	}
	if len(colExists) == 0 {
		return minFactor
	}
	// use the unique key info
	for _, idx := range coll.Indices {
		if !idx.Info.Unique {
			continue
		}
		unique := true
		for _, col := range idx.Info.Columns {
			if !colExists[col.Name.L] {
				unique = false
				break
			}
		}
		if unique {
			return 1.0 / float64(coll.Count)
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
// Currently the time complexity is o(n^2).
func (coll *HistColl) Selectivity(ctx sessionctx.Context, exprs []expression.Expression) (float64, error) {
	// If table's count is zero or conditions are empty, we should return 100% selectivity.
	if coll.Count == 0 || len(exprs) == 0 {
		return 1, nil
	}
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if len(exprs) > 63 || (len(coll.Columns) == 0 && len(coll.Indices) == 0) {
		return pseudoSelectivity(coll, exprs), nil
	}
	ret := 1.0
	var sets []*exprSet
	sc := ctx.GetSessionVars().StmtCtx

	remainedExprs := make([]expression.Expression, 0, len(exprs))

	// Deal with the correlated column.
	for _, expr := range exprs {
		if c := isColEqCorCol(expr); c != nil && !coll.ColumnIsInvalid(sc, c.UniqueID) {
			colHist := coll.Columns[c.UniqueID]
			if colHist.NDV > 0 {
				ret *= 1 / float64(colHist.NDV)
			}
		} else {
			remainedExprs = append(remainedExprs, expr)
		}
	}

	extractedCols := make([]*expression.Column, 0, len(coll.Columns))
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, remainedExprs, nil)
	for id, colInfo := range coll.Columns {
		col := expression.ColInfo2Col(extractedCols, colInfo.Info)
		if col != nil {
			maskCovered, ranges, _, err := getMaskAndRanges(ctx, remainedExprs, ranger.ColumnRangeType, nil, col)
			if err != nil {
				return 0, errors.Trace(err)
			}
			sets = append(sets, &exprSet{tp: colType, ID: id, mask: maskCovered, ranges: ranges, numCols: 1})
			if colInfo.isHandle {
				sets[len(sets)-1].tp = pkType
			}
		}
	}
	for id, idxInfo := range coll.Indices {
		idxCols := expression.FindPrefixOfIndex(extractedCols, coll.Idx2ColumnIDs[id])
		if len(idxCols) > 0 {
			lengths := make([]int, 0, len(idxCols))
			for i := 0; i < len(idxCols); i++ {
				lengths = append(lengths, idxInfo.Info.Columns[i].Length)
			}
			maskCovered, ranges, partCover, err := getMaskAndRanges(ctx, remainedExprs, ranger.IndexRangeType, lengths, idxCols...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			sets = append(sets, &exprSet{tp: indexType, ID: id, mask: maskCovered, ranges: ranges, numCols: len(idxInfo.Info.Columns), partCover: partCover})
		}
	}
	sets = GetUsableSetsByGreedy(sets)
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
			rowCount, err = coll.GetRowCountByIntColumnRanges(sc, set.ID, set.ranges)
		case colType:
			rowCount, err = coll.GetRowCountByColumnRanges(sc, set.ID, set.ranges)
		case indexType:
			rowCount, err = coll.GetRowCountByIndexRanges(sc, set.ID, set.ranges)
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		ret *= rowCount / float64(coll.Count)
		// If `partCover` is true, it means that the conditions are in DNF form, and only part
		// of the DNF expressions are extracted as access conditions, so besides from the selectivity
		// of the extracted access conditions, we multiply another selectionFactor for the residual
		// conditions.
		if set.partCover {
			ret *= selectionFactor
		}
	}
	// If there's still conditions which cannot be calculated, we will multiply a selectionFactor.
	if mask > 0 {
		ret *= selectionFactor
	}
	return ret, nil
}

func getMaskAndRanges(ctx sessionctx.Context, exprs []expression.Expression, rangeType ranger.RangeType,
	lengths []int, cols ...*expression.Column) (mask int64, ranges []*ranger.Range, partCover bool, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	isDNF := false
	var accessConds, remainedConds []expression.Expression
	switch rangeType {
	case ranger.ColumnRangeType:
		accessConds = ranger.ExtractAccessConditionsForColumn(exprs, cols[0].UniqueID)
		ranges, err = ranger.BuildColumnRange(accessConds, sc, cols[0].RetType)
	case ranger.IndexRangeType:
		var res *ranger.DetachRangeResult
		res, err = ranger.DetachCondAndBuildRangeForIndex(ctx, exprs, cols, lengths)
		ranges, accessConds, remainedConds, isDNF = res.Ranges, res.AccessConds, res.RemainedConds, res.IsDNFCond
	default:
		panic("should never be here")
	}
	if err != nil {
		return 0, nil, false, err
	}
	if isDNF && len(accessConds) > 0 {
		mask |= 1
		return mask, ranges, len(remainedConds) > 0, nil
	}
	for i := range exprs {
		for j := range accessConds {
			if exprs[i].Equal(ctx, accessConds[j]) {
				mask |= 1 << uint64(i)
				break
			}
		}
	}
	return mask, ranges, false, nil
}

// GetUsableSetsByGreedy will select the indices and pk used for calculate selectivity by greedy algorithm.
func GetUsableSetsByGreedy(sets []*exprSet) (newBlocks []*exprSet) {
	sort.Slice(sets, func(i int, j int) bool {
		if r := compareType(sets[i].tp, sets[j].tp); r != 0 {
			return r < 0
		}
		return sets[i].ID < sets[j].ID
	})
	marked := make([]bool, len(sets))
	mask := int64(math.MaxInt64)
	for {
		// Choose the index that covers most.
		bestID, bestCount, bestTp, bestNumCols, bestMask := -1, 0, colType, 0, int64(0)
		for i, set := range sets {
			if marked[i] {
				continue
			}
			curMask := set.mask & mask
			if curMask != set.mask {
				marked[i] = true
				continue
			}
			bits := bits.OnesCount64(uint64(curMask))
			// This set cannot cover any thing, just skip it.
			if bits == 0 {
				marked[i] = true
				continue
			}
			// We greedy select the stats info based on:
			// (1): The stats type, always prefer the primary key or index.
			// (2): The number of expression that it covers, the more the better.
			// (3): The number of columns that it contains, the less the better.
			if (bestTp == colType && set.tp != colType) || bestCount < bits || (bestCount == bits && bestNumCols > set.numCols) {
				bestID, bestCount, bestTp, bestNumCols, bestMask = i, bits, set.tp, set.numCols, curMask
			}
		}
		if bestCount == 0 {
			break
		}

		// update the mask, remove the bit that sets[bestID].mask has.
		mask &^= bestMask

		newBlocks = append(newBlocks, sets[bestID])
		marked[bestID] = true
	}
	return
}
