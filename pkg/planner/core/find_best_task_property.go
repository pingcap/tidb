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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// candidatePath is used to maintain required info for skyline pruning.
type candidatePath struct {
	path              *util.AccessPath
	accessCondsColMap util.Col2Len // accessCondsColMap maps Column.UniqueID to column length for the columns in AccessConds.
	indexCondsColMap  util.Col2Len // indexCondsColMap maps Column.UniqueID to column length for the columns in AccessConds and indexFilters.
	matchPropResult   property.PhysicalPropMatchResult
	// partialOrderMatch records the partial order match result for TopN optimization.
	// When the matched is true, it means this path can provide partial order using prefix index.
	partialOrderMatchResult property.PartialOrderMatchResult // Result of matching partial order property
	indexJoinCols           int                              // how many index columns are used in access conditions in this IndexJoin.
}

func compareBool(l, r bool) int {
	if l == r {
		return 0
	}
	if !l {
		return -1
	}
	return 1
}

func compareIndexBack(lhs, rhs *candidatePath) (int, bool) {
	result := compareBool(lhs.path.IsSingleScan, rhs.path.IsSingleScan)
	if result == 0 && !lhs.path.IsSingleScan {
		// if both lhs and rhs need to access table after IndexScan, we utilize the set of columns that occurred in AccessConds and IndexFilters
		// to compare how many table rows will be accessed.
		return util.CompareCol2Len(lhs.indexCondsColMap, rhs.indexCondsColMap)
	}
	return result, true
}

func compareGlobalIndex(lhs, rhs *candidatePath) int {
	if lhs.path.IsTablePath() || rhs.path.IsTablePath() ||
		len(lhs.path.PartialIndexPaths) != 0 || len(rhs.path.PartialIndexPaths) != 0 {
		return 0
	}
	return compareBool(lhs.path.Index.Global, rhs.path.Index.Global)
}

func compareRiskRatio(lhs, rhs *candidatePath) (int, float64) {
	lhsRiskRatio, rhsRiskRatio := 0.0, 0.0
	// MaxCountAfterAccess tracks the worst case "CountAfterAccess", accounting for scenarios that could
	// increase our row estimation, thus lhs/rhsRiskRatio represents the "risk" of the CountAfterAccess value.
	// Lower value means less risk that the actual row count is higher than the estimated one.
	if lhs.path.MaxCountAfterAccess > lhs.path.CountAfterAccess && lhs.path.CountAfterAccess > 0 {
		lhsRiskRatio = lhs.path.MaxCountAfterAccess / lhs.path.CountAfterAccess
	}
	if rhs.path.MaxCountAfterAccess > rhs.path.CountAfterAccess && rhs.path.CountAfterAccess > 0 {
		rhsRiskRatio = rhs.path.MaxCountAfterAccess / rhs.path.CountAfterAccess
	}
	sumLHS := lhs.path.CountAfterAccess + lhs.path.MaxCountAfterAccess
	sumRHS := rhs.path.CountAfterAccess + rhs.path.MaxCountAfterAccess
	// lhs has lower risk
	if lhsRiskRatio < rhsRiskRatio {
		if lhs.path.CountAfterAccess <= rhs.path.CountAfterAccess {
			return 1, lhsRiskRatio
		}
		if sumLHS < sumRHS && lhs.path.MinCountAfterAccess > 0 &&
			(lhs.path.MinCountAfterAccess <= rhs.path.MinCountAfterAccess || lhs.path.CountAfterIndex <= rhs.path.CountAfterIndex) {
			return 1, lhsRiskRatio
		}
	}
	// rhs has lower risk
	if rhsRiskRatio < lhsRiskRatio {
		if rhs.path.CountAfterAccess <= lhs.path.CountAfterAccess {
			return -1, rhsRiskRatio
		}
		if sumRHS < sumLHS && rhs.path.MinCountAfterAccess > 0 &&
			(rhs.path.MinCountAfterAccess <= lhs.path.MinCountAfterAccess || rhs.path.CountAfterIndex <= lhs.path.CountAfterIndex) {
			return -1, rhsRiskRatio
		}
	}
	return 0, 0
}

// compareCandidates is the core of skyline pruning, which is used to decide which candidate path is better.
// The first return value is 1 if lhs is better, -1 if rhs is better, 0 if they are equivalent or not comparable.
// The 2nd return value indicates whether the "better path" is missing statistics or not.
func compareCandidates(sctx base.PlanContext, statsTbl *statistics.Table, prop *property.PhysicalProperty, lhs, rhs *candidatePath, preferRange bool) (int, bool) {
	// Due to #50125, full scan on MVIndex has been disabled, so MVIndex path might lead to 'can't find a proper plan' error at the end.
	// Avoid MVIndex path to exclude all other paths and leading to 'can't find a proper plan' error, see #49438 for an example.
	if isMVIndexPath(lhs.path) || isMVIndexPath(rhs.path) {
		return 0, false
	}
	// lhsPseudo == lhs has pseudo (no) stats for the table or index for the lhs path.
	// rhsPseudo == rhs has pseudo (no) stats for the table or index for the rhs path.
	//
	// For the return value - if lhs wins (1), we return lhsPseudo. If rhs wins (-1), we return rhsPseudo.
	// If there is no winner (0), we return false.
	//
	// This return value is used later in SkyLinePruning to determine whether we should preference an index scan
	// over a table scan. Allowing indexes without statistics to survive means they can win via heuristics where
	// they otherwise would have lost on cost.
	lhsPseudo, rhsPseudo, tablePseudo := false, false, false
	if statsTbl != nil {
		tablePseudo = statsTbl.HistColl.Pseudo
		lhsPseudo, rhsPseudo = isCandidatesPseudo(lhs, rhs, statsTbl)
	}
	// matchResult: comparison result of whether LHS vs RHS matches the required properties (1=LHS better, -1=RHS better, 0=equal)
	// globalResult: comparison result of global index vs local index preference (1=LHS better, -1=RHS better, 0=equal)
	matchResult, globalResult := compareBool(lhs.matchPropResult.Matched(), rhs.matchPropResult.Matched()), compareGlobalIndex(lhs, rhs)
	// accessResult: comparison result of access condition coverage (1=LHS better, -1=RHS better, 0=equal)
	// comparable1: whether the access conditions are comparable between LHS and RHS
	accessResult, comparable1 := util.CompareCol2Len(lhs.accessCondsColMap, rhs.accessCondsColMap)
	// scanResult: comparison result of index back scan efficiency (1=LHS better, -1=RHS better, 0=equal)
	//             scanResult will always be true for a table scan (because it is a single scan).
	//             This has the effect of allowing the table scan plan to not be pruned.
	// comparable2: whether the index back scan characteristics are comparable between LHS and RHS
	scanResult, comparable2 := compareIndexBack(lhs, rhs)
	// riskResult: comparison result of risk factor (1=LHS better, -1=RHS better, 0=equal)
	riskResult, _ := compareRiskRatio(lhs, rhs)
	// eqOrInResult: comparison result of equal/IN predicate coverage (1=LHS better, -1=RHS better, 0=equal)
	eqOrInResult, lhsEqOrInCount, rhsEqOrInCount := compareEqOrIn(lhs, rhs)

	// predicateResult is separated out. An index may "win" because it has a better
	// accessResult - but that access has high risk.
	// accessResult does not differentiate between range or equal/IN predicates.
	// Summing these 3 metrics ensures that a "high risk" index wont win ONLY on
	// accessResult. The high risk will negate that accessResult with erOrIn being the
	// tiebreaker or equalizer.
	predicateResult := accessResult + riskResult + eqOrInResult

	// totalSum is the aggregate score of all comparison metrics
	totalSum := accessResult + scanResult + matchResult + globalResult

	pseudoResult := 0
	// Determine winner if one index doesn't have statistics and another has statistics
	if (lhsPseudo || rhsPseudo) && !tablePseudo && // At least one index doesn't have statistics
		(lhsEqOrInCount > 0 || rhsEqOrInCount > 0) { // At least one index has equal/IN predicates
		lhsFullMatch := isFullIndexMatch(lhs)
		rhsFullMatch := isFullIndexMatch(rhs)
		pseudoResult = comparePseudo(lhsPseudo, rhsPseudo, lhsFullMatch, rhsFullMatch, eqOrInResult, lhsEqOrInCount, rhsEqOrInCount, preferRange)
		if pseudoResult > 0 && totalSum >= 0 {
			return pseudoResult, lhsPseudo
		}
		if pseudoResult < 0 && totalSum <= 0 {
			return pseudoResult, rhsPseudo
		}
	}

	// This rule is empirical but not always correct.
	// If x's range row count is significantly lower than y's, for example, 1000 times, we think x is better.
	if lhs.path.CountAfterAccess > 100 && rhs.path.CountAfterAccess > 100 && // to prevent some extreme cases, e.g. 0.01 : 10
		len(lhs.path.PartialIndexPaths) == 0 && len(rhs.path.PartialIndexPaths) == 0 && // not IndexMerge since its row count estimation is not accurate enough
		prop.ExpectedCnt == math.MaxFloat64 { // Limit may affect access row count
		threshold := float64(fixcontrol.GetIntWithDefault(sctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix45132, 1000))
		sctx.GetSessionVars().RecordRelevantOptFix(fixcontrol.Fix45132)
		if threshold > 0 { // set it to 0 to disable this rule
			// corrResult is included to ensure we don't preference to a higher risk plan given that
			// this rule does not check the other criteria included below.
			if lhs.path.CountAfterAccess/rhs.path.CountAfterAccess > threshold && riskResult <= 0 {
				return -1, rhsPseudo // right wins - also return whether it has statistics (pseudo) or not
			}
			if rhs.path.CountAfterAccess/lhs.path.CountAfterAccess > threshold && riskResult >= 0 {
				return 1, lhsPseudo // left wins - also return whether it has statistics (pseudo) or not
			}
		}
	}

	leftDidNotLose := predicateResult >= 0 && scanResult >= 0 && matchResult >= 0 && globalResult >= 0
	rightDidNotLose := predicateResult <= 0 && scanResult <= 0 && matchResult <= 0 && globalResult <= 0
	if !comparable1 || !comparable2 {
		// These aren't comparable - meaning that they have different combinations of columns in
		// the access conditions or filters.
		// One or more predicates could carry high risk - so we want to compare that risk and other
		// metrics to see if we can determine a clear winner.
		// The 2 key metrics here are riskResult and predicateResult.
		// - riskResult tells us which candidate has lower risk
		// - predicateResult already includes risk - we need ">1" or "<-1" to counteract the risk factor.
		// "DidNotLose" and totalSum are also factored in to ensure that the winner is better overall."
		if riskResult > 0 && leftDidNotLose && totalSum >= 0 && predicateResult > 1 {
			return 1, lhsPseudo // left wins - also return whether it has statistics (pseudo) or not
		}
		if riskResult < 0 && rightDidNotLose && totalSum <= 0 && predicateResult < -1 {
			return -1, rhsPseudo // right wins - also return whether it has statistics (pseudo) or not
		}
		return 0, false // No winner (0). Do not return the pseudo result
	}
	// leftDidNotLose, but one of the metrics is a win
	if leftDidNotLose && totalSum > 0 {
		return 1, lhsPseudo // left wins - also return whether it has statistics (pseudo) or not
	}
	// rightDidNotLose, but one of the metrics is a win
	if rightDidNotLose && totalSum < 0 {
		return -1, rhsPseudo // right wins - also return whether it has statistics (pseudo) or not
	}
	return 0, false // No winner (0). Do not return the pseudo result
}

func isCandidatesPseudo(lhs, rhs *candidatePath, statsTbl *statistics.Table) (lhsPseudo, rhsPseudo bool) {
	lhsPseudo, rhsPseudo = statsTbl.HistColl.Pseudo, statsTbl.HistColl.Pseudo
	if len(lhs.path.PartialIndexPaths) == 0 && len(rhs.path.PartialIndexPaths) == 0 {
		if !lhs.path.IsTablePath() && lhs.path.Index != nil {
			if statsTbl.ColAndIdxExistenceMap.HasAnalyzed(lhs.path.Index.ID, true) {
				lhsPseudo = false // We have statistics for the lhs index
			} else {
				lhsPseudo = true
			}
		}
		if !rhs.path.IsTablePath() && rhs.path.Index != nil {
			if statsTbl.ColAndIdxExistenceMap.HasAnalyzed(rhs.path.Index.ID, true) {
				rhsPseudo = false // We have statistics on the rhs index
			} else {
				rhsPseudo = true
			}
		}
	}
	return lhsPseudo, rhsPseudo
}

func comparePseudo(lhsPseudo, rhsPseudo, lhsFullMatch, rhsFullMatch bool, eqOrInResult, lhsEqOrInCount, rhsEqOrInCount int, preferRange bool) int {
	// TO-DO: Consider a separate set of rules for global indexes.
	// If one index has statistics and the other does not, choose the index with statistics if it
	// has the same or higher number of equal/IN predicates.
	if !lhsPseudo && lhsEqOrInCount > 0 && eqOrInResult >= 0 {
		return 1 // left wins
	}
	if !rhsPseudo && rhsEqOrInCount > 0 && eqOrInResult <= 0 {
		return -1 // right wins
	}
	if preferRange {
		// keep an index without statistics if that index has more equal/IN predicates, AND:
		// 1) there are at least 2 equal/INs
		// 2) OR - it's a full index match for all index predicates
		if lhsPseudo && eqOrInResult > 0 &&
			(lhsEqOrInCount > 1 || lhsFullMatch) {
			return 1 // left wins
		}
		if rhsPseudo && eqOrInResult < 0 &&
			(rhsEqOrInCount > 1 || rhsFullMatch) {
			return -1 // right wins
		}
	}
	return 0
}

// Return the index with the higher EqOrInCondCount as winner (1 for lhs, -1 for rhs, 0 for tie),
// and the count for each. For example:
//
//	where a=1 and b=1 and c=1 and d=1
//	lhs == idx(a, b, e) <-- lhsEqOrInCount == 2 (loser)
//	rhs == idx(d, c, b) <-- rhsEqOrInCount == 3 (winner)
func compareEqOrIn(lhs, rhs *candidatePath) (predCompare, lhsEqOrInCount, rhsEqOrInCount int) {
	if len(lhs.path.PartialIndexPaths) > 0 || len(rhs.path.PartialIndexPaths) > 0 {
		// If either path has partial index paths, we cannot reliably compare EqOrIn conditions.
		return 0, 0, 0
	}
	lhsEqOrInCount = lhs.equalPredicateCount()
	rhsEqOrInCount = rhs.equalPredicateCount()
	if lhsEqOrInCount > rhsEqOrInCount {
		return 1, lhsEqOrInCount, rhsEqOrInCount
	}
	if lhsEqOrInCount < rhsEqOrInCount {
		return -1, lhsEqOrInCount, rhsEqOrInCount
	}
	// We didn't find a winner, but return both counts for use by the caller
	return 0, lhsEqOrInCount, rhsEqOrInCount
}

func isFullIndexMatch(candidate *candidatePath) bool {
	// Check if the DNF condition is a full match
	if candidate.path.IsDNFCond && candidate.hasOnlyEqualPredicatesInDNF() {
		return candidate.path.MinAccessCondsForDNFCond >= len(candidate.path.Index.Columns)
	}
	// Check if the index covers all access conditions for non-DNF conditions
	return candidate.path.EqOrInCondCount > 0 && len(candidate.indexCondsColMap) >= len(candidate.path.Index.Columns)
}

func matchProperty(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) property.PhysicalPropMatchResult {
	if ds.Table.Type().IsClusterTable() && !prop.IsSortItemEmpty() {
		// TableScan with cluster table can't keep order.
		return property.PropNotMatched
	}
	if prop.VectorProp.VSInfo != nil && path.Index != nil && path.Index.VectorInfo != nil {
		if ds.TableInfo.Columns[path.Index.Columns[0].Offset].ID != prop.VectorProp.Column.ID {
			return property.PropNotMatched
		}

		if model.IndexableFnNameToDistanceMetric[prop.VectorProp.DistanceFnName.L] != path.Index.VectorInfo.DistanceMetric {
			return property.PropNotMatched
		}
		return property.PropMatched
	}
	if path.IsIntHandlePath {
		pkCol := ds.GetPKIsHandleCol()
		if len(prop.SortItems) != 1 || pkCol == nil {
			return property.PropNotMatched
		}
		item := prop.SortItems[0]
		if !item.Col.EqualColumn(pkCol) ||
			path.StoreType == kv.TiFlash && item.Desc {
			return property.PropNotMatched
		}
		return property.PropMatched
	}

	// Match SortItems physical property.
	all, _ := prop.AllSameOrder()
	// When the prop is empty or `all` is false, `matchProperty` is better to be `PropNotMatched` because
	// it needs not to keep order for index scan.
	if prop.IsSortItemEmpty() || !all || len(path.IdxCols) < len(prop.SortItems) {
		return property.PropNotMatched
	}

	// Basically, if `prop.SortItems` is the prefix of `path.IdxCols`, then the property is matched.
	// However, we need to consider the situations when some columns of `path.IdxCols` are evaluated as constant.
	// For example:
	// ```
	// create table t(a int, b int, c int, d int, index idx_a_b_c(a, b, c), index idx_d_c_b_a(d, c, b, a));
	// select * from t where a = 1 order by b, c;
	// select * from t where b = 1 order by a, c;
	// select * from t where d = 1 and b = 2 order by c, a;
	// select * from t where d = 1 and b = 2 order by c, b, a;
	// ```
	// In the first two `SELECT` statements, `idx_a_b_c` matches the sort order.
	// In the last two `SELECT` statements, `idx_d_c_b_a` matches the sort order.
	// Hence, we use `path.ConstCols` to deal with the above situations. This corresponds to Case 2 in the code below.
	//
	// Moreover, we also need to consider the situation when some columns of `path.IdxCols` are a list of constant
	// values. For example, for query `SELECT * FROM t WHERE a IN (1,2,3) ORDER BY b, c`, we can use `idx_a_b_c` to
	// access 3 ranges and use a merge sort to satisfy `ORDER BY b, c`. This corresponds to Case 3 in the code below.
	matchResult := property.PropMatched
	groupByColIdxs := make([]int, 0)
	colIdx := 0
	for _, sortItem := range prop.SortItems {
		found := false
		for ; colIdx < len(path.IdxCols); colIdx++ {
			// Case 1: this sort item is satisfied by the index column, go to match the next sort item.
			if path.IdxColLens[colIdx] == types.UnspecifiedLength && sortItem.Col.EqualColumn(path.IdxCols[colIdx]) {
				found = true
				colIdx++
				break
			}
			// Below: this sort item is not satisfied by the index column

			// Case 2: the accessed range on this index column is a single constant value, so we can skip this
			// column and try to use the next index column to satisfy this sort item.
			if path.ConstCols != nil && colIdx < len(path.ConstCols) && path.ConstCols[colIdx] {
				continue
			}

			// Check if the accessed range on this index column is a single constant value **for each range**.
			allRangesPoint := true
			for _, ran := range path.Ranges {
				if len(ran.LowVal) <= colIdx || len(ran.HighVal) <= colIdx {
					allRangesPoint = false
					break
				}
				cmpResult, err := ran.LowVal[colIdx].Compare(
					ds.SCtx().GetSessionVars().StmtCtx.TypeCtx(),
					&ran.HighVal[colIdx],
					ran.Collators[colIdx],
				)
				if err != nil || cmpResult != 0 {
					allRangesPoint = false
					break
				}
			}
			// Case 3: the accessed range on this index column is a constant value in each range respectively,
			// we can also skip this column and try the next index column. But we need to record this column
			// as a "group by" column, because we need to do a merge sort on these ranges later.
			// So `len(groupByColIdxs) > 0` means we need to do a merge sort to satisfy the required property.
			if allRangesPoint {
				groupByColIdxs = append(groupByColIdxs, colIdx)
				continue
			}
			// Case 4: cannot satisfy this sort item, the path cannot match the required property.
			intest.Assert(!found)
			break
		}
		if !found {
			matchResult = property.PropNotMatched
			break
		}
	}
	if len(groupByColIdxs) > 0 && matchResult == property.PropMatched {
		groups, err := GroupRangesByCols(path.Ranges, groupByColIdxs)
		if err != nil {
			return property.PropNotMatched
		}
		// Multiple ranges can be put into one cop task. But when we group them, they are forced to be split into
		// multiple cop tasks. This can cause large overhead besides the merge sort operation itself.
		// After some performance tests, we set a threshold here to limit the max number of groups to prevent this
		// overhead becomes too large unexpectedly.
		const maxGroupCount = 2048
		if len(groups) > maxGroupCount {
			return property.PropNotMatched
		}
		if len(groups) > 0 {
			path.GroupedRanges = groups
			path.GroupByColIdxs = groupByColIdxs
			return property.PropMatchedNeedMergeSort
		}
	}
	return matchResult
}
