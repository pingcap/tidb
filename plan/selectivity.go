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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"math"
)

// indexBlock is used when calculating selectivity
type indexBlock struct {
	// the position that this index is in the index slice
	pos int
	// cover will tell which expressions is covered by this index.
	cover uint64
	// pk won't use this, only index will use.
	inAndEqCnt int
}

// selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should hold when you call this.
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
			copiedExprs := make([]expression.Expression, 0, len(exprs))
			for _, expr := range exprs {
				copiedExprs = append(copiedExprs, expr.Clone())
			}
			accessConds, _ := DetachTableScanConditions(copiedExprs, ds.tableInfo)
			covered := uint64(0)
			for i := 0; i < len(exprs); {
				for j := 0; j < len(accessConds); {
					if exprs[i].Equal(accessConds[j], ds.ctx) {
						covered = covered | (1 << uint64(i))
						i++
						j++
					} else {
						j++
					}
				}
			}
			if covered != 0 {
				blocks = append(blocks, &indexBlock{pos: -1, cover: covered})
			}
		}
	}
	for id, index := range indices {
		c := ds.statisticTable.Indices[index.ID]
		// This index should have histogram.
		if c != nil && len(c.Buckets) > 0 {
			copiedExprs := make([]expression.Expression, 0, len(exprs))
			for _, expr := range exprs {
				copiedExprs = append(copiedExprs, expr.Clone())
			}
			covered := uint64(0)
			accessConds, _, _, inAndEqCnt := DetachIndexScanConditions(copiedExprs, index)
			for i := range exprs {
				for j := range accessConds {
					if exprs[i].Equal(accessConds[j], ds.ctx) {
						covered = covered | (1 << uint64(i))
					}
				}
			}
			if covered != 0 {
				blocks = append(blocks, &indexBlock{pos: id, cover: covered, inAndEqCnt: inAndEqCnt})
			}
		}
	}
	blocks = getUsedIndicesByGreedy(blocks)
	log.Warnf("block len: %v", len(blocks))
	ret := 1.0
	for _, block := range blocks {
		conds := getCondThroughCompressBits(block.cover, exprs)
		var rowCount float64
		if block.pos == -1 {
			ranges, err := BuildTableRange(conds, sc)
			if err != nil {
				return 0.0, err
			}
			log.Warnf("ranges: %v", ranges)
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
	return ret, nil
}

// getUsedIndicesByGreedy will get the indices used for calculate selectivity by greedy algorithm.
// New covers and usedIdxs will be returned.
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

func getCondThroughCompressBits(x uint64, conds []expression.Expression) []expression.Expression {
	accessConds := make([]expression.Expression, 0, countOneBits(x))
	for i := 0; x > 0; i++ {
		if (x & 1) > 0 {
			accessConds = append(accessConds, conds[i])
		}
		x = x >> 1
	}
	return accessConds
}
