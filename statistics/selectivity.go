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

// calcBlock is used for calculating selectivity.
type calcBlock struct {
	tp int
	ID int64
	// the ith bit of `cover` will tell whether the ith expression is covered by this index.
	cover int64
	// ranges calculated.
	ranges []types.Range
}

// The type of the calcBlock.
const (
	IndexBlock = iota
	PkBlock
	ColumnBlock
)

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// TODO: support expressions that the top layer is a DNF.
// Currently the time complexity if o(n^2).
func (t *Table) Selectivity(ctx context.Context, exprs []expression.Expression) (float64, error) {
	if t.Count == 0 {
		return 0, nil
	}
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if t.Pseudo || len(exprs) > 63 {
		return selectionFactor, nil
	}
	var blocks []*calcBlock
	sc := ctx.GetSessionVars().StmtCtx
	extractedCols := expression.ExtractColumns(expression.ComposeCNFCondition(ctx, exprs...))
	for _, colInfo := range t.Columns {
		col := expression.ColInfo2Col(extractedCols, colInfo.Info)
		// This column should have histogram.
		if col != nil && len(colInfo.Histogram.Buckets) > 0 {
			covered, ranges, err := getMaskAndRanges(sc, exprs, ranger.ColumnRangeType, nil, col)
			if err != nil {
				return 0, errors.Trace(err)
			}
			blocks = append(blocks, &calcBlock{tp: ColumnBlock, ID: col.ID, cover: covered, ranges: ranges})
			if mysql.HasPriKeyFlag(colInfo.Info.Flag) {
				blocks[len(blocks)-1].tp = PkBlock
			}
		}
	}
	for _, idxInfo := range t.Indices {
		idxCols, lengths := expression.IndexInfo2Cols(extractedCols, idxInfo.Info)
		// This index should have histogram.
		if len(idxCols) > 0 && len(idxInfo.Histogram.Buckets) > 0 {
			covered, ranges, err := getMaskAndRanges(sc, exprs, ranger.IndexRangeType, lengths, idxCols...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			blocks = append(blocks, &calcBlock{tp: IndexBlock, ID: idxInfo.ID, cover: covered, ranges: ranges})
		}
	}
	blocks = getUsedBlocksByGreedy(blocks)
	ret := 1.0
	// cut off the higher bit of mask
	mask := int64(math.MaxInt64) >> uint64(63-len(exprs))
	for _, block := range blocks {
		mask ^= block.cover
		var (
			rowCount float64
			err      error
		)
		switch block.tp {
		case PkBlock, ColumnBlock:
			ranges := ranger.Ranges2ColumnRanges(block.ranges)
			rowCount, err = t.GetRowCountByColumnRanges(sc, block.ID, ranges)
		case IndexBlock:
			ranges := ranger.Ranges2IndexRanges(block.ranges)
			rowCount, err = t.GetRowCountByIndexRanges(sc, block.ID, ranges)
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

// getUsedBlocksByGreedy will select the indices and pk used for calculate selectivity by greedy algorithm.
func getUsedBlocksByGreedy(blocks []*calcBlock) (newBlocks []*calcBlock) {
	mask := int64(math.MaxInt64)
	for {
		// Choose the index that covers most.
		bestID := -1
		bestCount := 0
		bestID, bestCount, bestTp := -1, 0, ColumnBlock
		for i, block := range blocks {
			block.cover &= mask
			bits := popcount(block.cover)
			if (bestTp == ColumnBlock && block.tp < ColumnBlock) || bestCount < bits {
				bestID, bestCount, bestTp = i, bits, block.tp
			}
		}
		if bestCount == 0 {
			break
		} else {
			// update the mask, remove the bit that blocks[bestID].cover has.
			mask &^= blocks[bestID].cover

			newBlocks = append(newBlocks, blocks[bestID])
			// remove the chosen one
			blocks = append(blocks[:bestID], blocks[bestID+1:]...)
		}
	}
	return
}

// popcount is the digit sum of the binary representation of the number x.
func popcount(x int64) int {
	ret := 0
	// x = x & -x, cut the highest bit of the x.
	for ; x > 0; x -= x & -x {
		ret++
	}
	return ret
}
