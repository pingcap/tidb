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

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/ranger"
)

// If one condition can't be calculated, we will assume that the selectivity of this condition is 0.8.
const selectionFactor = 0.8

// indexBlock is used when calculating selectivity.
type indexBlock struct {
	// the position that this index is in the index slice, if is -1, then this is primary key.
	pos int
	// the ith bit of `cover` will tell whether the ith expression is covered by this index.
	cover uint64
	// pk won't use this, only index will use. It's the count of `in` or `equal` expressions.
	inAndEqCnt int
}

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// TODO: support expressions that the top layer is a DNF.
func Selectivity(ctx context.Context, exprs []expression.Expression, indices []*model.IndexInfo, pkColID int64,
	tbl *model.TableInfo, statsTbl *Table) (float64, error) {
	// TODO: If len(exprs) is bigger than 64, we could use bitset structure to replace the uint64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if statsTbl.Pseudo || len(exprs) > 64 {
		return selectionFactor, nil
	}
	var blocks []*indexBlock
	sc := ctx.GetSessionVars().StmtCtx
	if tbl.PKIsHandle {
		c := statsTbl.Columns[pkColID]
		// pk should have histogram.
		if c != nil && len(c.Buckets) > 0 {
			covered, _ := getMaskByColumn(ctx, exprs, tbl.GetPkName())
			if covered != 0 {
				blocks = append(blocks, &indexBlock{pos: -1, cover: covered})
			}
		}
	}
	for id, index := range indices {
		c := statsTbl.Indices[index.ID]
		// This index should have histogram.
		if c != nil && len(c.Buckets) > 0 {
			covered, inAndEqCnt, _ := getMaskByIndex(ctx, exprs, index)
			if covered > 0 {
				blocks = append(blocks, &indexBlock{pos: id, cover: covered, inAndEqCnt: inAndEqCnt})
			}
		}
	}
	blocks = getUsedIndicesByGreedy(blocks)
	ret := 1.0
	// cut off the higher bit of mask.
	mask := uint64(math.MaxUint64) >> uint64(64-len(exprs))
	for _, block := range blocks {
		conds := getCondThroughMask(block.cover, exprs)
		// update the mask.
		mask &^= block.cover
		var rowCount float64
		if block.pos == -1 {
			ranges, err := ranger.BuildTableRange(conds, sc)
			if err != nil {
				return 0, err
			}
			rowCount, err = statsTbl.GetRowCountByIntColumnRanges(sc, pkColID, ranges)
			if err != nil {
				return 0, err
			}
		} else {
			ranges, err := ranger.BuildIndexRange(sc, tbl, indices[block.pos], block.inAndEqCnt, conds)
			if err != nil {
				return 0, err
			}
			rowCount, err = statsTbl.GetRowCountByIndexRanges(sc, indices[block.pos].ID, ranges, block.inAndEqCnt)
			if err != nil {
				return 0, err
			}
		}
		ret *= rowCount / float64(statsTbl.Count)
	}
	// TODO: add the calculation of column sampling.
	if mask > 0 {
		ret *= float64(selectionFactor)
	}
	return ret, nil
}

func getMaskByColumn(ctx context.Context, exprs []expression.Expression, colName model.CIStr) (uint64, []expression.Expression) {
	accessConds, _ := ranger.DetachTableScanConditions(exprs, colName)
	mask := uint64(0)
	// There accessConds is a sub sequence of exprs. i.e. If exprs is `[ a > 1, b > 1, a < 10]`,
	// accessConds could be `[ a > 1, b > 1 ]` or `[a > 1, a < 10]`, couldn't be `[ a < 10, a > 1 ]`
	// So we can use the following o(n) algorithm.
	i, j := 0, 0
	for i < len(exprs) && j < len(accessConds) {
		if exprs[i].Equal(accessConds[j], ctx) {
			mask |= 1 << uint64(i)
			i++
			j++
		} else {
			i++
		}
	}
	return mask, accessConds
}

func getMaskByIndex(ctx context.Context, exprs []expression.Expression, index *model.IndexInfo) (uint64, int, []expression.Expression) {
	exprsClone := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		exprsClone = append(exprsClone, expr.Clone())
	}
	accessConds, _, _, inAndEqCnt := ranger.DetachIndexScanConditions(exprsClone, index)
	mask := uint64(0)
	for i := range exprs {
		for j := range accessConds {
			if exprs[i].Equal(accessConds[j], ctx) {
				mask |= 1 << uint64(i)
				break
			}
		}
	}
	return mask, inAndEqCnt, accessConds
}

// getUsedIndicesByGreedy will select the indices and pk used for calculate selectivity by greedy algorithm.
func getUsedIndicesByGreedy(blocks []*indexBlock) (newBlocks []*indexBlock) {
	mask := uint64(math.MaxUint64)
	for {
		// Choose the index that covers most.
		bestID := -1
		bestCount := 0
		for i, block := range blocks {
			block.cover &= mask
			bits := popcount(block.cover)
			if bestCount < bits {
				bestCount = bits
				bestID = i
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

func getCondThroughMask(x uint64, conds []expression.Expression) []expression.Expression {
	accessConds := make([]expression.Expression, 0, popcount(x))
	for i := 0; x > 0; i++ {
		if (x & 1) > 0 {
			accessConds = append(accessConds, conds[i])
		}
		x = x >> 1
	}
	return accessConds
}
