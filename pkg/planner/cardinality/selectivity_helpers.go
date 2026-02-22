// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cardinality

import (
	"cmp"
	"math"
	"math/bits"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	planutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// The type of the StatsNode.
const (
	IndexType = iota
	PkType
	ColType
)

func compareType(l, r int) int {
	if l == r {
		return 0
	}
	if l == ColType {
		return -1
	}
	if l == PkType {
		return 1
	}
	if r == ColType {
		return 1
	}
	return -1
}

const unknownColumnID = math.MinInt64

// staleLastBucketThreshold is the threshold for detecting stale last bucket estimates.
// If the last bucket's count is less than 30% of the average bucket count, we consider
// it potentially stale.
const staleLastBucketThreshold = 0.3

// valueAwareRowAddedThreshold is the minimum ratio of newly added rows relative to
// the average value count required to trigger the stale bucket heuristic.
// If new rows >= (avgValueCount * threshold), we consider applying the heuristic.
const valueAwareRowAddedThreshold = 0.5

// IsLastBucketEndValueUnderrepresented detects when the last value (upper bound) of the last bucket
// has a suspiciously low count that may be stale due to concentrated writes after ANALYZE.
func IsLastBucketEndValueUnderrepresented(sctx planctx.PlanContext, hg *statistics.Histogram, val types.Datum,
	histCnt float64, histNDV float64, realtimeRowCount, modifyCount int64) bool {
	if modifyCount <= 0 || len(hg.Buckets) == 0 || histNDV <= 0 {
		return false
	}

	// This represents data changes since ANALYZE - we use absolute difference as a proxy for
	// activity level since we cannot distinguish between inserts, deletes, and updates
	newRowsAdded := hg.AbsRowCountDifference(realtimeRowCount)

	// Calculate average count per distinct value
	avgValueCount := hg.NotNullCount() / histNDV

	// Only apply heuristic when new rows are significant relative to average value count
	if newRowsAdded < avgValueCount*valueAwareRowAddedThreshold {
		return false
	}

	// Use LocateBucket to check if this value is at the last bucket's upper bound
	_, bucketIdx, inBucket, matchLastValue := hg.LocateBucket(sctx, val)

	// Check if this is the last bucket's upper bound value (end value)
	isLastBucketEndValue := (bucketIdx == len(hg.Buckets)-1) && inBucket && matchLastValue
	if !isLastBucketEndValue {
		return false
	}

	// If count is much less than average value count, it's likely underrepresented
	return histCnt < avgValueCount*staleLastBucketThreshold
}

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

// GetUsableSetsByGreedy will select the indices and pk used for calculate selectivity by greedy algorithm.
func GetUsableSetsByGreedy(nodes []*StatsNode) (newBlocks []*StatsNode) {
	slices.SortFunc(nodes, func(i, j *StatsNode) int {
		if r := compareType(i.Tp, j.Tp); r != 0 {
			return r
		}
		return cmp.Compare(i.ID, j.ID)
	})
	marked := make([]bool, len(nodes))
	mask := int64(math.MaxInt64)
	for {
		// Choose the index that covers most.
		bestMask := int64(0)
		best := &statsNodeForGreedyChoice{
			StatsNode: &StatsNode{
				Tp:                       ColType,
				Selectivity:              0,
				numCols:                  0,
				partCover:                true,
				minAccessCondsForDNFCond: -1,
			},
			idx:        -1,
			coverCount: 0,
		}
		for i, set := range nodes {
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
			current := &statsNodeForGreedyChoice{
				StatsNode:  set,
				idx:        i,
				coverCount: bits,
			}
			if current.isBetterThan(best) {
				best = current
				bestMask = curMask
			}
		}
		if best.coverCount == 0 {
			break
		}

		// Update the mask, remove the bit that nodes[best.idx].mask has.
		mask &^= bestMask

		newBlocks = append(newBlocks, nodes[best.idx])
		marked[best.idx] = true
	}
	return
}

type statsNodeForGreedyChoice struct {
	*StatsNode
	idx        int
	coverCount int
}

func (s *statsNodeForGreedyChoice) isBetterThan(other *statsNodeForGreedyChoice) bool {
	// none of them should be nil
	if s == nil || other == nil {
		return false
	}
	// 1. The stats type, always prefer the primary key or index.
	if s.Tp != ColType && other.Tp == ColType {
		return true
	}
	// 2. The number of expression that it covers, the more, the better.
	if s.coverCount > other.coverCount {
		return true
	}
	// Worse or equal. We return false for both cases. The same for the following rules.
	if s.coverCount != other.coverCount {
		return false
	}
	// 3. It's only for DNF. Full cover is better than partial cover
	if !s.partCover && other.partCover {
		return true
	}
	if s.partCover != other.partCover {
		return false
	}
	// 4. It's only for DNF. The minimum number of access conditions among all DNF items, the more, the better.
	// s.coverCount is not enough for DNF, so we use this field to make the judgment more accurate.
	if s.minAccessCondsForDNFCond > other.minAccessCondsForDNFCond {
		return true
	}
	if s.minAccessCondsForDNFCond != other.minAccessCondsForDNFCond {
		return false
	}

	// 5. The number of columns that it contains, the less, the better.
	if s.numCols < other.numCols {
		return true
	}
	if s.numCols != other.numCols {
		return false
	}
	// 6. The selectivity of the covered conditions, the less, the better.
	// The rationale behind is that lower selectivity tends to reflect more functional dependencies
	// between columns. It's hard to decide the priority of this rule against rules above, in order
	// to avoid massive plan changes between tidb-server versions, I adopt this conservative strategy
	// to impose this rule after rules above.
	if s.Selectivity < other.Selectivity {
		return true
	}
	return false
}

// isColEqCorCol checks if the expression is an eq function that one side is correlated column and another is column.
// If so, it will return the column's reference. Otherwise, return nil instead.
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

// findPrefixOfIndex will find columns in index by checking the unique id.
// So it will return at once no matching column is found.
func findPrefixOfIndex(cols []*expression.Column, idxColIDs []int64) []*expression.Column {
	retCols := make([]*expression.Column, 0, len(idxColIDs))
idLoop:
	for _, id := range idxColIDs {
		for _, col := range cols {
			if col.UniqueID == id {
				retCols = append(retCols, col)
				continue idLoop
			}
		}
		// If no matching column is found, just return.
		return retCols
	}
	return retCols
}

// findPrefixOfIndexByCol will find columns in index by checking the unique id or the virtual expression.
// So it will return at once no matching column is found.
func findPrefixOfIndexByCol(ctx planctx.PlanContext, cols []*expression.Column, idxColIDs []int64,
	cachedPath *planutil.AccessPath) []*expression.Column {
	if cachedPath != nil {
		evalCtx := ctx.GetExprCtx().GetEvalCtx()
		idxCols := cachedPath.IdxCols
		retCols := make([]*expression.Column, 0, len(idxCols))
	idLoop:
		for _, idCol := range idxCols {
			for _, col := range cols {
				if col.EqualByExprAndID(evalCtx, idCol) {
					retCols = append(retCols, col)
					continue idLoop
				}
			}
			// If no matching column is found, just return.
			return retCols
		}
		return retCols
	}
	return findPrefixOfIndex(cols, idxColIDs)
}

func getMaskAndRanges(ctx planctx.PlanContext, exprs []expression.Expression, rangeType ranger.RangeType,
	lengths []int, cachedPath *planutil.AccessPath, cols ...*expression.Column) (
	mask int64, ranges []*ranger.Range, partCover bool, minAccessCondsForDNFCond int, err error) {
	isDNF := false
	var accessConds, remainedConds []expression.Expression
	switch rangeType {
	case ranger.ColumnRangeType:
		accessConds = ranger.ExtractAccessConditionsForColumn(ctx.GetRangerCtx(), exprs, cols[0])
		ranges, accessConds, _, err = ranger.BuildColumnRange(accessConds, ctx.GetRangerCtx(), cols[0].RetType,
			types.UnspecifiedLength, ctx.GetSessionVars().RangeMaxSize)
	case ranger.IndexRangeType:
		if cachedPath != nil {
			ranges = cachedPath.Ranges
			accessConds = cachedPath.AccessConds
			remainedConds = cachedPath.TableFilters
			isDNF = cachedPath.IsDNFCond
			minAccessCondsForDNFCond = cachedPath.MinAccessCondsForDNFCond
			break
		}
		var res *ranger.DetachRangeResult
		res, err = ranger.DetachCondAndBuildRangeForIndex(ctx.GetRangerCtx(), exprs, cols, lengths, ctx.GetSessionVars().RangeMaxSize)
		if err != nil {
			return 0, nil, false, 0, err
		}
		ranges = res.Ranges
		accessConds = res.AccessConds
		remainedConds = res.RemainedConds
		isDNF = res.IsDNFCond
		minAccessCondsForDNFCond = res.MinAccessCondsForDNFCond
	default:
		panic("should never be here")
	}
	if err != nil {
		return 0, nil, false, 0, err
	}
	if isDNF && len(accessConds) > 0 {
		mask |= 1
		return mask, ranges, len(remainedConds) > 0, minAccessCondsForDNFCond, nil
	}
	for i := range exprs {
		for j := range accessConds {
			if exprs[i].Equal(ctx.GetExprCtx().GetEvalCtx(), accessConds[j]) {
				mask |= 1 << uint64(i)
				break
			}
		}
	}
	return mask, ranges, false, 0, nil
}

func getMaskAndSelectivityForMVIndex(
	ctx planctx.PlanContext,
	coll *statistics.HistColl,
	id int64,
	exprs []expression.Expression,
) (float64, int64, bool) {
	cols := coll.MVIdx2Columns[id]
	if len(cols) == 0 {
		return 1.0, 0, false
	}
	// You can find more examples and explanations in comments for collectFilters4MVIndex() and
	// buildPartialPaths4MVIndex() in planner/core.
	accessConds, _, _ := CollectFilters4MVIndex(ctx, exprs, cols)
	paths, isIntersection, ok, err := BuildPartialPaths4MVIndex(ctx, accessConds, cols, coll.GetIdx(id).Info, coll)
	if err != nil || !ok {
		return 1.0, 0, false
	}
	totalSelectivity := CalcTotalSelectivityForMVIdxPath(coll, paths, isIntersection)
	var mask int64
	for i := range exprs {
		for _, accessCond := range accessConds {
			if exprs[i].Equal(ctx.GetExprCtx().GetEvalCtx(), accessCond) {
				mask |= 1 << uint64(i)
				break
			}
		}
	}
	return totalSelectivity, mask, true
}

