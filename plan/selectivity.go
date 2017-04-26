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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"math"
	"github.com/pingcap/tidb/context"
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
	mask := uint64(math.MaxUint64)
	mask = mask >> uint64(64 - len(exprs))
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
				return 0.0, nil
			}
			rowCount, err = ds.statisticTable.GetRowCountByIndexRanges(sc, indices[block.pos].ID, ranges, block.inAndEqCnt)
			if err != nil {
				return 0.0, nil
			}
		}
		ret *= rowCount / float64(ds.statisticTable.Count)
	}
	if mask > 0 {
		ret *= selectionFactor
	}
	return ret, nil
}

func getMaskByColumn(ctx context.Context, exprs []expression.Expression, colName model.CIStr) (uint64, []expression.Expression) {
	accessConds, _ := DetachTableScanConditions(exprs, colName)
	mask := uint64(0)
	// There accessConds is a sub sequence of exprs. i.e. If exprs is `[ a > 1, b > 1, a < 10]`,
	// accessConds could be `[ a > 1, b > 1 ]` or `[a > 1, a < 10]`, couldn't be `[ a < 10, a > 1 ]`
	for i := 0; i < len(exprs); {
		for j := 0; j < len(accessConds); {
			if exprs[i].Equal(accessConds[j], ctx) {
				mask = mask | (1 << uint64(i))
				i++
				j++
			} else {
				i++
			}
		}
	}
	return mask, accessConds
}

func getMaskByIndex(ctx context.Context, exprs []expression.Expression, index *model.IndexInfo) (uint64, int, []expression.Expression) {
	accessConds, _, _, inAndEqCnt := DetachIndexScanConditions(exprs, index)
	mask := uint64(0)
	for i := range exprs {
		for j := range exprs {
			if exprs[i].Equal(accessConds[j], ctx) {
				mask = mask | (1 << uint64(i))
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
			bits := countOneBits(block.cover)
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

func countOneBits(x uint64) int {
	ret := 0
	for ; x > 0; x = x >> 1 {
		if (x & 1) > 0 {
			ret++
		}
	}
	return ret
}

func getCondThroughMask(x uint64, conds []expression.Expression) []expression.Expression {
	accessConds := make([]expression.Expression, 0, countOneBits(x))
	for i := 0; x > 0; i++ {
		if (x & 1) > 0 {
			accessConds = append(accessConds, conds[i])
		}
		x = x >> 1
	}
	return accessConds
}
