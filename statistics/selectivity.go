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

// calcBlock is used when calculating selectivity.
type calcBlock struct {
	tp int
	// the position that this index is in the index slice, if is -1, then this is primary key.
	pos int
	// the ith bit of `cover` will tell whether the ith expression is covered by this index.
	cover uint64
	// ranges calculated by this index or pk.
	ranges []types.Range
}

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// TODO: support expressions that the top layer is a DNF.
// Currently the time complexity if o(n^2).
func Selectivity(ctx context.Context, exprs []expression.Expression, keys []expression.KeyInfo,
	keyIDs []int64, hasPk bool, lengths [][]int, cols []*expression.Column, statsTbl *Table) (float64, error) {
	// TODO: If len(exprs) is bigger than 64, we could use bitset structure to replace the uint64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if statsTbl.Pseudo || len(exprs) > 64 {
		return selectionFactor, nil
	}
	var blocks []*calcBlock
	sc := ctx.GetSessionVars().StmtCtx
	idxBeginID := 0
	if hasPk {
		idxBeginID = 1
		c := statsTbl.Columns[keyIDs[0]]
		// pk should have histogram.
		if c != nil && len(c.Buckets) > 0 {
			covered, ranges, err := getMaskAndRanges(sc, exprs, ranger.IntRangeType, nil, keys[0][0])
			if err != nil {
				return 0, errors.Trace(err)
			}
			blocks = append(blocks, &calcBlock{
				tp:     ranger.IntRangeType,
				pos:    0,
				cover:  covered,
				ranges: ranges,
			})
		}
	}
	for i := idxBeginID; i < len(keys); i++ {
		c := statsTbl.Indices[keyIDs[i]]
		// This index should have histogram.
		if c != nil && len(c.Buckets) > 0 {
			// covered, ranges, err := getMaskByIndex(sc, exprs, keys[i], lengths)
			covered, ranges, err := getMaskAndRanges(sc, exprs, ranger.IndexRangeType, lengths[i], keys[i]...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			blocks = append(blocks, &calcBlock{
				tp:     ranger.IndexRangeType,
				pos:    i,
				cover:  covered,
				ranges: ranges,
			})
		}
	}
	for i, col := range cols {
		c := statsTbl.Columns[col.ID]
		// This column should have histogram.
		if c != nil && len(c.Buckets) > 0 {
			covered, ranges, err := getMaskAndRanges(sc, exprs, ranger.ColumnRangeType, nil, col)
			if err != nil {
				return 0, errors.Trace(err)
			}
			blocks = append(blocks, &calcBlock{
				tp: ranger.ColumnRangeType,
				pos: i,
				cover: covered,
				ranges: ranges,
			})
		}
	}
	blocks = getUsedBlocksByGreedy(blocks)
	ret := 1.0
	// cut off the higher bit of mask
	mask := uint64(math.MaxUint64) >> uint64(64-len(exprs))
	for _, block := range blocks {
		mask &^= block.cover
		var (
			rowCount float64
			err      error
		)
		if block.pos == 0 && hasPk {
			ranges := ranger.Ranges2IntRanges(block.ranges)
			rowCount, err = statsTbl.GetRowCountByIntColumnRanges(sc, keyIDs[0], ranges)
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			ranges := ranger.Ranges2IndexRanges(block.ranges)
			rowCount, err = statsTbl.GetRowCountByIndexRanges(sc, keyIDs[block.pos], ranges)
		}
		ret *= rowCount / float64(statsTbl.Count)
	}
	if mask > 0 {
		ret *= selectionFactor
	}
	return ret, nil
}

func getMaskAndRanges(sc *variable.StatementContext, exprs []expression.Expression, rangeType int,
	lengths []int, cols ...*expression.Column) (uint64, []types.Range, error) {
	exprsClone := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		exprsClone = append(exprsClone, expr.Clone())
	}
	ranges, accessConds, _, err := ranger.BuildRange(sc, exprsClone, rangeType, cols, lengths)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	mask := uint64(0)
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
	mask := uint64(math.MaxUint64)
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
func popcount(x uint64) int {
	ret := 0
	for ; x > 0; x = x >> 1 {
		if (x & 1) > 0 {
			ret++
		}
	}
	return ret
}
