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

// The type of the SelUnit if it isn't a index.
const (
	SelPkUnit     = -1
	SelColumnUnit = -2
)

// SelUnit is used as a param passed by Selectivity.
// When ID >= 0, this unit is a index. And the ID, cols, lengths is the information of this index.
// When ID is SelPkUnit, cols' len is 0, and it stores the pk column.
// When ID is SelColumnUnit, cols stores the columns used for column sampling calculation.
type SelUnit struct {
	ID      int64
	cols    []*expression.Column
	lengths []int
}

// MakeSelUnit generates a SelUnit by the params.
func MakeSelUnit(id int64, lengths []int, cols ...*expression.Column) *SelUnit {
	return &SelUnit{ID: id, cols: cols, lengths: lengths}
}

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// TODO: support expressions that the top layer is a DNF.
// Currently the time complexity if o(n^2).
func Selectivity(ctx context.Context, exprs []expression.Expression, units []*SelUnit, statsTbl *Table) (float64, error) {
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if statsTbl.Pseudo || len(exprs) > 63 {
		return selectionFactor, nil
	}
	var blocks []*calcBlock
	sc := ctx.GetSessionVars().StmtCtx
	for _, unit := range units {
		switch unit.ID {
		case SelPkUnit:
			c := statsTbl.Columns[unit.cols[0].ID]
			// pk should have histogram.
			if c != nil && len(c.Buckets) > 0 {
				covered, ranges, err := getMaskAndRanges(sc, exprs, ranger.IntRangeType, nil, unit.cols...)
				if err != nil {
					return 0, errors.Trace(err)
				}
				blocks = append(blocks, &calcBlock{
					tp:     ranger.IntRangeType,
					ID:     unit.cols[0].ID,
					cover:  covered,
					ranges: ranges,
				})
			}
		case SelColumnUnit:
			for _, col := range unit.cols {
				c := statsTbl.Columns[col.ID]
				// This column should have histogram.
				if c != nil && len(c.Buckets) > 0 {
					covered, ranges, err := getMaskAndRanges(sc, exprs, ranger.ColumnRangeType, nil, col)
					if err != nil {
						return 0, errors.Trace(err)
					}
					blocks = append(blocks, &calcBlock{
						tp:     ranger.ColumnRangeType,
						ID:     col.ID,
						cover:  covered,
						ranges: ranges,
					})
				}
			}
		default:
			c := statsTbl.Indices[unit.ID]
			// This index should have histogram.
			if c != nil && len(c.Buckets) > 0 {
				covered, ranges, err := getMaskAndRanges(sc, exprs, ranger.IndexRangeType, unit.lengths, unit.cols...)
				if err != nil {
					return 0, errors.Trace(err)
				}
				blocks = append(blocks, &calcBlock{
					tp:     ranger.IndexRangeType,
					ID:     unit.ID,
					cover:  covered,
					ranges: ranges,
				})
			}
		}
	}
	blocks = getUsedBlocksByGreedy(blocks)
	ret := 1.0
	// cut off the higher bit of mask
	mask := int64(math.MaxInt64) >> uint64(63-len(exprs))
	for _, block := range blocks {
		mask &^= block.cover
		var (
			rowCount float64
			err      error
		)
		switch block.tp {
		case ranger.IntRangeType:
			ranges := ranger.Ranges2IntRanges(block.ranges)
			rowCount, err = statsTbl.GetRowCountByIntColumnRanges(sc, block.ID, ranges)
		case ranger.ColumnRangeType:
			/*
				ranges := ranger.Ranges2ColumnRanges(block.ranges)
				rowCount, err = statsTbl.GetRowCountByColumnRanges(sc, block.ID, ranges)
				if err != nil {
					return 0, errors.Trace(err)
				}
			*/
		default:
			ranges := ranger.Ranges2IndexRanges(block.ranges)
			rowCount, err = statsTbl.GetRowCountByIndexRanges(sc, block.ID, ranges)
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		ret *= rowCount / float64(statsTbl.Count)
	}
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
		bestID, bestCount, bestTp := -1, 0, ranger.ColumnRangeType
		for i, block := range blocks {
			block.cover &= mask
			bits := popcount(block.cover)
			if (bestTp == ranger.ColumnRangeType && block.tp < ranger.ColumnRangeType) || bestCount < bits {
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
