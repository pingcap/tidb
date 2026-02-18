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
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/ranger"
	sliceutil "github.com/pingcap/tidb/pkg/util/slice"
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

// matchPartialOrderProperty checks if the index can provide partial order for TopN optimization.
// Unlike matchProperty, this function allows prefix index to match ORDER BY columns.
// partialOrderInfo being non-nil indicates that partial order optimization is enabled.
// The current implementation primarily supports cases where
// the index can **completely match** the **prefix** of the order by column.
// Case 1: Prefix index INDEX idx(col(N)) matches ORDER BY col
// For example:
//
//	query: order by a, b
//	index: a(10)
//	index: a, b(10)
//
// On success, this function will return `PartialOrderMatchResult.Matched`= true, `PartialOrderMatchResult.PrefixCol` and `PartialOrderMatchResult.PrefixLen`.
// Otherwise it returns false and not initialize `PrefixCol and PrefixLen`.
// TODO
// Case 2: Composite index INDEX idx(a, b) matches ORDER BY a, b, c (index provides order for a, b)
// In fact, there is a Case3 that can also be supported, but it will not be explained in detail here.
// Please refer to the design document for details.
func matchPartialOrderProperty(path *util.AccessPath, partialOrderInfo *property.PartialOrderInfo) property.PartialOrderMatchResult {
	emptyResult := property.PartialOrderMatchResult{Matched: false}

	if partialOrderInfo == nil || path.Index == nil || len(path.IdxCols) == 0 {
		return emptyResult
	}

	sortItems := partialOrderInfo.SortItems
	if len(sortItems) == 0 {
		return emptyResult
	}

	allSameOrder, _ := partialOrderInfo.AllSameOrder()
	if !allSameOrder {
		return emptyResult
	}

	// Case 1: Prefix index INDEX idx(col(N)) matches ORDER BY col
	// Check if index columns can match ORDER BY columns (allowing prefix index)
	//
	// NOTE: We use path.Index.Columns to get the actual index definition columns count,
	// because path.FullIdxCols may include additional handle columns (e.g., primary key `id`)
	// appended for non-unique indexes on tables with PKIsHandle.
	// For example, for `index idx_name_prefix (name(10))` on a table with `id int primary key`,
	// path.FullIdxCols = [name, id] but path.Index.Columns only contains [name].
	// We should only consider the actual index definition columns for partial order matching.
	indexColCount := len(path.Index.Columns)

	// Constraint 0: The full idx cols length must >= all the index definition columns length, no matter column is pruned or not
	// Theoretically, the preceding function logic ”IndexInfo2FullCols“ can guarantee that this constraint will always hold.
	// Therefore, this is merely a defensive check to prevent the array from going out of bounds in the subsequent for loop.
	if len(path.FullIdxCols) < indexColCount || len(path.FullIdxColLens) < indexColCount {
		return emptyResult
	}

	// Constraint 1: The number of index definition columns must be <= the number of ORDER BY columns
	if indexColCount > len(sortItems) {
		return emptyResult
	}
	// Constraint 2: The last column of the index definition must be a prefix column
	lastIdxColLen := path.Index.Columns[indexColCount-1].Length
	if lastIdxColLen == types.UnspecifiedLength {
		// The last column is not a prefix column, skip this index
		return emptyResult
	}
	// Extract ORDER BY columns
	orderByCols := make([]*expression.Column, 0, len(sortItems))
	for _, item := range sortItems {
		orderByCols = append(orderByCols, item.Col)
	}

	// Only iterate over the actual index definition columns, not the appended handle columns
	for i := range indexColCount {
		idxCol := path.FullIdxCols[i]
		if idxCol == nil {
			return emptyResult
		}
		// check if the same column
		if !orderByCols[i].EqualColumn(idxCol) {
			return emptyResult
		}

		// meet prefix index column, match termination
		if path.FullIdxColLens[i] != types.UnspecifiedLength {
			// If we meet a prefix column but it's not the last index definition column, it's not supported.
			// e.g. prefix(a), b cannot provide partial order for ORDER BY a, b.
			if i != indexColCount-1 {
				return emptyResult
			}
			// Encountered a prefix index column.
			// This prefix index column can provide partial order, but subsequent columns cannot match.
			return property.PartialOrderMatchResult{
				Matched:   true,
				PrefixCol: idxCol,
				PrefixLen: path.FullIdxColLens[i],
			}
		}
	}

	return emptyResult
}

// GroupRangesByCols groups the ranges by the values of the columns specified by groupByColIdxs.
func GroupRangesByCols(ranges []*ranger.Range, groupByColIdxs []int) ([][]*ranger.Range, error) {
	groups := make(map[string][]*ranger.Range)
	for _, ran := range ranges {
		var datums []types.Datum
		for _, idx := range groupByColIdxs {
			datums = append(datums, ran.LowVal[idx])
		}
		// We just use it to group the values, so any time zone is ok.
		keyBytes, err := codec.EncodeValue(time.UTC, nil, datums...)
		intest.AssertNoError(err)
		if err != nil {
			return nil, err
		}
		key := string(keyBytes)
		groups[key] = append(groups[key], ran)
	}

	// Convert map to slice
	result := slices.Collect(maps.Values(groups))
	return result, nil
}

// matchPropForIndexMergeAlternatives will match the prop with inside PartialAlternativeIndexPaths, and choose
// 1 matched alternative to be a determined index merge partial path for each dimension in PartialAlternativeIndexPaths.
// finally, after we collected the all decided index merge partial paths, we will output a concrete index merge path
// with field PartialIndexPaths is fulfilled here.
//
// as we mentioned before, after deriveStats is done, the normal index OR path will be generated like below:
//
//	    `create table t (a int, b int, c int, key a(a), key b(b), key ac(a, c), key bc(b, c))`
//		`explain format='verbose' select * from t where a=1 or b=1 order by c`
//
// like the case here:
// normal index merge OR path should be:
// for a=1, it has two partial alternative paths: [a, ac]
// for b=1, it has two partial alternative paths: [b, bc]
// and the index merge path:
//
//	indexMergePath: {
//	    PartialIndexPaths: empty                          // 1D array here, currently is not decided yet.
//	    PartialAlternativeIndexPaths: [[a, ac], [b, bc]]  // 2D array here, each for one DNF item choices.
//	}
//
// let's say we have a prop requirement like sort by [c] here, we will choose the better one [ac] (because it can keep
// order) for the first batch [a, ac] from PartialAlternativeIndexPaths; and choose the better one [bc] (because it can
// keep order too) for the second batch [b, bc] from PartialAlternativeIndexPaths. Finally we output a concrete index
// merge path as
//
//	indexMergePath: {
//	    PartialIndexPaths: [ac, bc]                       // just collected since they match the prop.
//	    ...
//	}
//
// how about the prop is empty? that means the choice to be decided from [a, ac] and [b, bc] is quite random just according
// to their countAfterAccess. That's why we use a slices.SortFunc(matchIdxes, func(a, b int){}) inside there. After sort,
// the ASC order of matchIdxes of matched paths are ordered by their countAfterAccess, choosing the first one is straight forward.
//
// there is another case shown below, just the pick the first one after matchIdxes is ordered is not always right, as shown:
// special logic for alternative paths:
//
//	index merge:
//	   matched paths-1: {pk, index1}
//	   matched paths-2: {pk}
//
// if we choose first one as we talked above, says pk here in the first matched paths, then path2 has no choice(avoiding all same
// index logic inside) but pk, this will result in all single index failure. so we need to sort the matchIdxes again according to
// their matched paths length, here mean:
//
//	index merge:
//	   matched paths-1: {pk, index1}
//	   matched paths-2: {pk}
//
// and let matched paths-2 to be the first to make their determination --- choosing pk here, then next turn is matched paths-1 to
// make their choice, since pk is occupied, avoiding-all-same-index-logic inside will try to pick index1 here, so work can be done.
//
// at last, according to determinedIndexPartialPaths to rewrite their real countAfterAccess, this part is move from deriveStats to
// here.
func matchPropForIndexMergeAlternatives(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) (*util.AccessPath, property.PhysicalPropMatchResult) {
	// target:
	//	1: index merge case, try to match the every alternative partial path to the order property as long as
	//	possible, and generate that property-matched index merge path out if any.
	//	2: If the prop is empty (means no sort requirement), we will generate a random index partial combination
	//	path from all alternatives in case that no index merge path comes out.

	// Execution part doesn't support the merge operation for intersection case yet.
	if path.IndexMergeIsIntersection {
		return nil, property.PropNotMatched
	}

	noSortItem := prop.IsSortItemEmpty()
	allSame, _ := prop.AllSameOrder()
	if !allSame {
		return nil, property.PropNotMatched
	}
	// step1: match the property from all the index partial alternative paths.
	determinedIndexPartialPaths := make([]*util.AccessPath, 0, len(path.PartialAlternativeIndexPaths))
	usedIndexMap := make(map[int64]struct{}, 1)
	useMVIndex := false
	for _, oneORBranch := range path.PartialAlternativeIndexPaths {
		matchIdxes := make([]int, 0, 1)
		for i, oneAlternative := range oneORBranch {
			// if there is some sort items and this path doesn't match this prop, continue.
			match := true
			for _, oneAccessPath := range oneAlternative {
				// Satisfying the property by a merge sort is not supported for partial paths of index merge.
				if !noSortItem && matchProperty(ds, oneAccessPath, prop) != property.PropMatched {
					match = false
				}
			}
			if !match {
				continue
			}
			// two possibility here:
			// 1. no sort items requirement.
			// 2. matched with sorted items.
			matchIdxes = append(matchIdxes, i)
		}
		if len(matchIdxes) == 0 {
			// if all index alternative of one of the cnf item's couldn't match the sort property,
			// the entire index merge union path can be ignored for this sort property, return false.
			return nil, property.PropNotMatched
		}
		if len(matchIdxes) > 1 {
			// if matchIdxes greater than 1, we should sort this match alternative path by its CountAfterAccess.
			alternatives := oneORBranch
			slices.SortStableFunc(matchIdxes, func(a, b int) int {
				res := cmpAlternatives(ds.SCtx().GetSessionVars())(alternatives[a], alternatives[b])
				if res != 0 {
					return res
				}
				// If CountAfterAccess is same, any path is global index should be the first one.
				var lIsGlobalIndex, rIsGlobalIndex int
				if !alternatives[a][0].IsTablePath() && alternatives[a][0].Index.Global {
					lIsGlobalIndex = 1
				}
				if !alternatives[b][0].IsTablePath() && alternatives[b][0].Index.Global {
					rIsGlobalIndex = 1
				}
				return -cmp.Compare(lIsGlobalIndex, rIsGlobalIndex)
			})
		}
		lowestCountAfterAccessIdx := matchIdxes[0]
		determinedIndexPartialPaths = append(determinedIndexPartialPaths, sliceutil.DeepClone(oneORBranch[lowestCountAfterAccessIdx])...)
		// record the index usage info to avoid choosing a single index for all partial paths
		var indexID int64
		if oneORBranch[lowestCountAfterAccessIdx][0].IsTablePath() {
			indexID = -1
		} else {
			indexID = oneORBranch[lowestCountAfterAccessIdx][0].Index.ID
			// record mv index because it's not affected by the all single index limitation.
			if oneORBranch[lowestCountAfterAccessIdx][0].Index.MVIndex {
				useMVIndex = true
			}
		}
		// record the lowestCountAfterAccessIdx's chosen index.
		usedIndexMap[indexID] = struct{}{}
	}
	// since all the choice is done, check the all single index limitation, skip check for mv index.
	// since ds index merge hints will prune other path ahead, lift the all single index limitation here.
	if len(usedIndexMap) == 1 && !useMVIndex && len(ds.IndexMergeHints) <= 0 {
		// if all partial path are using a same index, meaningless and fail over.
		return nil, property.PropNotMatched
	}

	// check if any of the partial paths is not cacheable.
	notCachableReason := ""
	for _, p := range determinedIndexPartialPaths {
		if p.NoncacheableReason != "" {
			notCachableReason = p.NoncacheableReason
			break
		}
	}

	// step2: gen a new **concrete** index merge path.
	indexMergePath := &util.AccessPath{
		IndexMergeAccessMVIndex:  useMVIndex,
		PartialIndexPaths:        determinedIndexPartialPaths,
		IndexMergeIsIntersection: false,
		// inherit those determined can't pushed-down table filters.
		TableFilters:       path.TableFilters,
		NoncacheableReason: notCachableReason,
	}
	// path.ShouldBeKeptCurrentFilter record that whether there are some part of the cnf item couldn't be pushed down to tikv already.
	shouldKeepCurrentFilter := path.KeepIndexMergeORSourceFilter
	for _, p := range determinedIndexPartialPaths {
		if p.KeepIndexMergeORSourceFilter {
			shouldKeepCurrentFilter = true
			break
		}
	}
	if shouldKeepCurrentFilter {
		// add the cnf expression back as table filer.
		indexMergePath.TableFilters = append(indexMergePath.TableFilters, path.IndexMergeORSourceFilter)
	}

	// step3: after the index merge path is determined, compute the countAfterAccess as usual.
	indexMergePath.CountAfterAccess = estimateCountAfterAccessForIndexMergeOR(ds, determinedIndexPartialPaths)
	if noSortItem {
		// since there is no sort property, index merge case is generated by random combination, each alternative with the lower/lowest
		// countAfterAccess, here the returned matchProperty should be PropNotMatched.
		return indexMergePath, property.PropNotMatched
	}
	return indexMergePath, property.PropMatched
}

func isMatchPropForIndexMerge(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) property.PhysicalPropMatchResult {
	// Execution part doesn't support the merge operation for intersection case yet.
	if path.IndexMergeIsIntersection {
		return property.PropNotMatched
	}
	allSame, _ := prop.AllSameOrder()
	if !allSame {
		return property.PropNotMatched
	}
	for _, partialPath := range path.PartialIndexPaths {
		// Satisfying the property by a merge sort is not supported for partial paths of index merge.
		if matchProperty(ds, partialPath, prop) != property.PropMatched {
			return property.PropNotMatched
		}
	}
	return property.PropMatched
}

func getTableCandidate(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.matchPropResult = matchProperty(ds, path, prop)
	candidate.accessCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), path.AccessConds, nil, nil)
	return candidate
}

func getIndexCandidate(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.matchPropResult = matchProperty(ds, path, prop)
	// Because the skyline pruning already prune the indexes that cannot provide partial order
	// when prop has PartialOrderInfo physical property,
	// So here we just need to record the partial order match result(prefixCol, prefixLen).
	// The partialOrderMatchResult.Matched() will be always true after skyline pruning.
	if ds.SCtx().GetSessionVars().IsPartialOrderedIndexForTopNEnabled() && prop.PartialOrderInfo != nil {
		candidate.partialOrderMatchResult = matchPartialOrderProperty(path, prop.PartialOrderInfo)
	}
	candidate.accessCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), path.AccessConds, path.IdxCols, path.IdxColLens)
	candidate.indexCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), append(path.AccessConds, path.IndexFilters...), path.FullIdxCols, path.FullIdxColLens)
	return candidate
}

func getIndexCandidateForIndexJoin(sctx planctx.PlanContext, path *util.AccessPath, indexJoinCols int) *candidatePath {
	candidate := &candidatePath{path: path, indexJoinCols: indexJoinCols}
	candidate.matchPropResult = property.PropNotMatched
	candidate.accessCondsColMap = util.ExtractCol2Len(sctx.GetExprCtx().GetEvalCtx(), path.AccessConds, path.IdxCols, path.IdxColLens)
	candidate.indexCondsColMap = util.ExtractCol2Len(sctx.GetExprCtx().GetEvalCtx(), append(path.AccessConds, path.IndexFilters...), path.FullIdxCols, path.FullIdxColLens)
	// AccessConds could miss some predicates since the DataSource can't see join predicates.
	// For example, `where t1.a=t2.a and t2.b=1`, `t1=a=t2.a` is not pushed down to t2's accessConds since it's a join
	// predicate. We need to set columns used as join keys to accessCondsColMap/indexCondsColMap manually.
	for i := range indexJoinCols {
		candidate.accessCondsColMap[path.IdxCols[i].UniqueID] = path.IdxColLens[i]
		candidate.indexCondsColMap[path.IdxCols[i].UniqueID] = path.IdxColLens[i]
	}
	return candidate
}

func convergeIndexMergeCandidate(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	// since the all index path alternative paths is collected and undetermined, and we should determine a possible and concrete path for this prop.
	possiblePath, match := matchPropForIndexMergeAlternatives(ds, path, prop)
	if possiblePath == nil {
		return nil
	}
	candidate := &candidatePath{path: possiblePath, matchPropResult: match}
	return candidate
}

func getIndexMergeCandidate(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.matchPropResult = isMatchPropForIndexMerge(ds, path, prop)
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func skylinePruning(ds *logicalop.DataSource, prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	idxMissingStats := false
	// tidb_opt_prefer_range_scan is the master switch to control index preferencing
	preferRange := ds.SCtx().GetSessionVars().GetAllowPreferRangeScan()
	for _, path := range ds.PossibleAccessPaths {
		// We should check whether the possible access path is valid first.
		if path.StoreType != kv.TiFlash && prop.IsFlashProp() {
			continue
		}
		if len(path.PartialAlternativeIndexPaths) > 0 {
			// OR normal index merge path, try to determine every index partial path for this property.
			candidate := convergeIndexMergeCandidate(ds, path, prop)
			if candidate != nil {
				candidates = append(candidates, candidate)
			}
			continue
		}
		if path.PartialIndexPaths != nil {
			candidates = append(candidates, getIndexMergeCandidate(ds, path, prop))
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 {
			return []*candidatePath{{path: path}}
		}
		var currentCandidate *candidatePath
		if path.IsTablePath() {
			if prop.PartialOrderInfo != nil {
				// skyline pruning table path with partial order property is not supported yet.
				// TODO: support it in the future after we support prefix column as partial order.
				continue
			}
			currentCandidate = getTableCandidate(ds, path, prop)
		} else {
			// Check if this path can be used for partial order optimization
			var matchPartialOrderIndex bool
			if ds.SCtx().GetSessionVars().IsPartialOrderedIndexForTopNEnabled() &&
				prop.PartialOrderInfo != nil {
				if !matchPartialOrderProperty(path, prop.PartialOrderInfo).Matched {
					// skyline pruning all indexes that cannot provide partial order when we are looking for
					continue
				}
				matchPartialOrderIndex = true
				// If the index can match partial order requirement and user use "use/force index" in hint.
				// If the index can't match partial order requirement and use use "use/force index" and enable partial order optimization together,
				// the behavior will degenerate into normal index use behavior without considering partial order optimization.
				// If path is force but it use the hint /no_order_index/ which ForceNoKeepOrder is true,
				// we won't consider it for partial order optimization, and it will be treated as normal forced index.
				if path.Forced && !path.ForceNoKeepOrder {
					path.ForcePartialOrder = true
				}
			}

			// We will use index to generate physical plan if any of the following conditions is satisfied:
			// 1. This path's access cond is not nil.
			// 2. We have a non-empty prop to match.
			// 3. This index is forced to choose.
			// 4. The needed columns are all covered by index columns(and handleCol).
			// 5. Match PartialOrderInfo physical property to be considered for partial order optimization (new condition).
			keepIndex := len(path.AccessConds) > 0 || !prop.IsSortItemEmpty() || path.Forced || path.IsSingleScan || matchPartialOrderIndex
			if !keepIndex {
				// If none of the above conditions are met, this index will be directly pruned here.
				continue
			}

			// After passing the check, generate the candidate
			currentCandidate = getIndexCandidate(ds, path, prop)
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			if candidates[i].path.StoreType == kv.TiFlash {
				continue
			}
			result, missingStats := compareCandidates(ds.SCtx(), ds.StatisticTable, prop, candidates[i], currentCandidate, preferRange)
			if missingStats {
				idxMissingStats = true // Ensure that we track idxMissingStats across all iterations
			}
			if result == 1 {
				pruned = true
				// We can break here because the current candidate lost to another plan.
				// This means that we won't add it to the candidates below.
				break
			} else if result == -1 {
				// The current candidate is better - so remove the old one from "candidates"
				candidates = slices.Delete(candidates, i, i+1)
			}
		}
		if !pruned {
			candidates = append(candidates, currentCandidate)
		}
	}

	// If we've forced an index merge - we want to keep these plans
	preferMerge := rule.ShouldPreferIndexMerge(ds)
	if preferRange {
		// Override preferRange with the following limitations to scope
		preferRange = preferMerge || idxMissingStats || ds.TableStats.HistColl.Pseudo || ds.TableStats.RowCount < 1
	}
	if preferRange && len(candidates) > 1 {
		// If a candidate path is TiFlash-path or forced-path or MV index or global index, we just keep them. For other
		// candidate paths, if there exists any range scan path, we remove full scan paths and keep range scan paths.
		preferredPaths := make([]*candidatePath, 0, len(candidates))
		var hasRangeScanPath, hasMultiRange bool
		for _, c := range candidates {
			if len(c.path.Ranges) > 1 {
				hasMultiRange = true
			}
			if c.path.Forced || c.path.StoreType == kv.TiFlash || (c.path.Index != nil && (c.path.Index.Global || c.path.Index.MVIndex)) {
				preferredPaths = append(preferredPaths, c)
				continue
			}
			// Preference plans with equals/IN predicates or where there is more filtering in the index than against the table
			indexFilters := c.equalPredicateCount() > 0 || len(c.path.TableFilters) < len(c.path.IndexFilters)
			if preferMerge || ((c.path.IsSingleScan || indexFilters) && (prop.IsSortItemEmpty() || c.matchPropResult.Matched())) {
				if !c.path.IsFullScanRange(ds.TableInfo) {
					preferredPaths = append(preferredPaths, c)
					hasRangeScanPath = true
				}
			}
		}
		if hasMultiRange {
			// Only log the fix control if we had multiple ranges
			ds.SCtx().GetSessionVars().RecordRelevantOptFix(fixcontrol.Fix52869)
		}
		if hasRangeScanPath {
			return preferredPaths
		}
	}

	return candidates
}

// hasOnlyEqualPredicatesInDNF checks if all access conditions in DNF form contain at least one equal predicate
func (c *candidatePath) hasOnlyEqualPredicatesInDNF() bool {
	// Helper function to check if a condition is an equal/IN predicate or a LogicOr of equal/IN predicates
	var isEqualPredicateOrOr func(expr expression.Expression) bool
	isEqualPredicateOrOr = func(expr expression.Expression) bool {
		sf, ok := expr.(*expression.ScalarFunction)
		if !ok {
			return false
		}
		switch sf.FuncName.L {
		case ast.UnaryNot:
			// Reject NOT operators - they can make predicates non-equal
			return false
		case ast.LogicOr, ast.LogicAnd:
			for _, arg := range sf.GetArgs() {
				if !isEqualPredicateOrOr(arg) {
					return false
				}
			}
			return true
		case ast.EQ, ast.In:
			// Check if it's an equal predicate (eq) or IN predicate (in)
			// Also reject any other comparison operators that are not equal/IN
			return true
		default:
			// Reject all other comparison operators (LT, GT, LE, GE, NE, etc.)
			// and any other functions that are not equal/IN predicates
			return false
		}
	}

	// Check all access conditions
	for _, cond := range c.path.AccessConds {
		if !isEqualPredicateOrOr(cond) {
			return false
		}
	}
	return true
}

func (c *candidatePath) equalPredicateCount() int {
	if c.indexJoinCols > 0 { // this candidate path is for Index Join
		// Specially handle indexes under Index Join, since these indexes can't see the join key themselves, we can't
		// use path.EqOrInCondCount here.
		// For example, for "where t1.a=t2.a and t2.b=1" and index "idx(t2, a, b)", since the DataSource of t2 can't
		// see the join key "t1.a=t2.a", it only considers "t2.b=1" as access condition, so its EqOrInCondCount is 0,
		// but actually it should be 2.
		return c.indexJoinCols
	}

	// Exit if this isn't a DNF condition or has no access conditions
	if !c.path.IsDNFCond || len(c.path.AccessConds) == 0 {
		return c.path.EqOrInCondCount
	}
	if c.hasOnlyEqualPredicatesInDNF() {
		return c.path.MinAccessCondsForDNFCond
	}
	return max(0, c.path.MinAccessCondsForDNFCond-1)
}

func getPruningInfo(ds *logicalop.DataSource, candidates []*candidatePath, prop *property.PhysicalProperty) string {
	if len(candidates) == len(ds.PossibleAccessPaths) {
		return ""
	}
	if len(candidates) == 1 && len(candidates[0].path.Ranges) == 0 {
		// For TableDual, we don't need to output pruning info.
		return ""
	}
	names := make([]string, 0, len(candidates))
	var tableName string
	if ds.TableAsName.O == "" {
		tableName = ds.TableInfo.Name.O
	} else {
		tableName = ds.TableAsName.O
	}
	getSimplePathName := func(path *util.AccessPath) string {
		if path.IsTablePath() {
			if path.StoreType == kv.TiFlash {
				return tableName + "(tiflash)"
			}
			return tableName
		}
		return path.Index.Name.O
	}
	for _, cand := range candidates {
		if cand.path.PartialIndexPaths != nil {
			partialNames := make([]string, 0, len(cand.path.PartialIndexPaths))
			for _, partialPath := range cand.path.PartialIndexPaths {
				partialNames = append(partialNames, getSimplePathName(partialPath))
			}
			names = append(names, fmt.Sprintf("IndexMerge{%s}", strings.Join(partialNames, ",")))
		} else {
			names = append(names, getSimplePathName(cand.path))
		}
	}
	items := make([]string, 0, len(prop.SortItems))
	for _, item := range prop.SortItems {
		items = append(items, item.String())
	}
	return fmt.Sprintf("[%s] remain after pruning paths for %s given Prop{SortItems: [%s], TaskTp: %s}",
		strings.Join(names, ","), tableName, strings.Join(items, " "), prop.TaskTp)
}

