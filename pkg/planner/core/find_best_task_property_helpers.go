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
	"maps"
	"slices"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/ranger"
	sliceutil "github.com/pingcap/tidb/pkg/util/slice"
)


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

