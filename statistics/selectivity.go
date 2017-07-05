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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/types"
)

// If one condition can't be calculated, we will assume that the selectivity of this condition is 0.8.
const selectionFactor = 0.8

// exprSet is used for calculating selectivity.
type exprSet struct {
	tp int
	ID int64
	// The ith bit of `mask` will tell whether the ith expression is covered by this index/column.
	mask int64
	// This stores ranges we get.
	ranges []types.Range
}

// The type of the exprSet.
const (
	indexType = iota
	pkType
	colType
)

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// TODO: support expressions that the top layer is a DNF.
// Currently the time complexity is o(n^2).
func (t *Table) Selectivity(ctx context.Context, exprs []expression.Expression) (float64, error) {
	if t.Count == 0 {
		return 0, nil
	}
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if t.Pseudo || len(exprs) > 63 {
		return selectionFactor, nil
	}
	var sets []*exprSet
	sc := ctx.GetSessionVars().StmtCtx
	extractedCols := expression.ExtractColumns(expression.ComposeCNFCondition(ctx, exprs...))
	for _, colInfo := range t.Columns {
		col := expression.ColInfo2Col(extractedCols, colInfo.Info)
		// This column should have histogram.
		if col != nil && len(colInfo.Histogram.Buckets) > 0 {
			maskCovered, ranges, err := getMaskAndRanges(sc, exprs, ranger.ColumnRangeType, nil, col)
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
		if len(idxCols) > 0 && len(idxInfo.Histogram.Buckets) > 0 {
			maskCovered, ranges, err := getMaskAndRanges(sc, exprs, ranger.IndexRangeType, lengths, idxCols...)
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
			ranges := ranger.Ranges2ColumnRanges(set.ranges)
			rowCount, err = t.GetRowCountByColumnRanges(sc, set.ID, ranges)
		case indexType:
			ranges := ranger.Ranges2IndexRanges(set.ranges)
			rowCount, err = t.GetRowCountByIndexRanges(sc, set.ID, ranges)
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

func getMaskAndRanges(sc *variable.StatementContext, exprs []expression.Expression, rangeType int,
	lengths []int, cols ...*expression.Column) (int64, []types.Range, error) {
	exprsClone := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		exprsClone = append(exprsClone, expr.Clone())
	}
	ranges, accessConds, _, err := ranger.BuildRange(sc, exprsClone, rangeType, cols, lengths)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	mask := int64(0)
	for i := range exprs {
		for j := range accessConds {
			if exprs[i].Equal(accessConds[j], nil) {
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
		bestID := -1
		bestCount := 0
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
