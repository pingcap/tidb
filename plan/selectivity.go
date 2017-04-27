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

package plan

import (
	"math"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
)

// indexBlock is used when calculating selectivity
type indexBlock struct {
	// the position that this index is in the index slice
	pos int
	// the ith bit of `cover` will tell whether the ith expression is covered by this index.
	cover uint64
	// pk won't use this, only index will use.
	inAndEqCnt int
}

// selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
func selectivity(exprs []expression.Expression, indices []*model.IndexInfo, ds *DataSource, pkColID int64) (float64, error) {
	// TODO: If len(exprs) is bigger than 64, we could use bitset to replace the uint64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if ds.statisticTable.Pseudo || len(exprs) > 64 {
		return selectionFactor, nil
	}
	var blocks []*indexBlock
	sc := ds.ctx.GetSessionVars().StmtCtx
	if ds.tableInfo.PKIsHandle {
		c := ds.statisticTable.Columns[pkColID]
		// pk should have histogram.
		if c != nil && len(c.Buckets) > 0 {
			covered, _ := getMaskByColumn(ds.ctx, exprs, GetPkName(ds.tableInfo))
			if covered != 0 {
				blocks = append(blocks, &indexBlock{pos: -1, cover: covered})
			}
		}
	}
	for id, index := range indices {
		c := ds.statisticTable.Indices[index.ID]
		// This index should have histogram.
		if c != nil && len(c.Buckets) > 0 {
			covered, inAndEqCnt, _ := getMaskByIndex(ds.ctx, exprs, index)
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
		mask &^= block.cover
		var rowCount float64
		if block.pos == -1 {
			ranges, err := BuildTableRange(conds, sc)
			if err != nil {
				return 0.0, err
			}
			rowCount, err = ds.statisticTable.GetRowCountByIntColumnRanges(sc, pkColID, ranges)
			if err != nil {
				return 0.0, err
			}
		} else {
			ranges, err := BuildIndexRange(sc, ds.tableInfo, indices[block.pos], block.inAndEqCnt, conds)
			if err != nil {
				return 0.0, err
			}
			rowCount, err = ds.statisticTable.GetRowCountByIndexRanges(sc, indices[block.pos].ID, ranges, block.inAndEqCnt)
			if err != nil {
				return 0.0, err
			}
		}
		ret *= rowCount / float64(ds.statisticTable.Count)
	}
	// TODO: add the calculation of column sampling.
	if mask > 0 {
		//ret *= float64(selectionFactor)
		ret = ret * selectionFactor
	}
	return ret, nil
}

func getMaskByColumn(ctx context.Context, exprs []expression.Expression, colName model.CIStr) (uint64, []expression.Expression) {
	accessConds, _ := DetachTableScanConditions(exprs, colName)
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
	accessConds, _, _, inAndEqCnt := DetachIndexScanConditions(exprsClone, index)
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

// It's the digit sum of the binary representation of the number x.
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
