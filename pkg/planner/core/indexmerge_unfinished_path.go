// Copyright 2024 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// generateORIndexMerge handles all (MV and non-MV index) OR type IndexMerge path generation.
// The input filters are implicitly connected by AND.
func generateORIndexMerge(ds *logicalop.DataSource, filters []expression.Expression) error {
	usedIndexCount := len(ds.PossibleAccessPaths)
	// 1. Iterate the input filters and try to find an OR list.
	for k, cond := range filters {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.LogicOr {
			continue
		}

		dnfFilters := expression.SplitDNFItems(sf)
		candidatesAccessPaths := ds.PossibleAccessPaths[:usedIndexCount]

		// 2. Try to collect usable filters for each candidate access path using the OR list.
		unfinishedIndexMergePath := genUnfinishedPathFromORList(ds, dnfFilters, candidatesAccessPaths)
		// 3. Try to collect more usable filters from the top level AND list and build it into a valid AccessPath.
		indexMergePath := handleTopLevelANDList(ds, filters, k, candidatesAccessPaths, unfinishedIndexMergePath)
		if indexMergePath != nil {
			ds.PossibleAccessPaths = append(ds.PossibleAccessPaths, indexMergePath)
		}
	}
	return nil
}

// unfinishedAccessPath collects usable filters in preparation for building an OR type IndexMerge access path.
// It maintains the information during iterating all filters. Importantly, it maintains incomplete access filters, which
// means they may not be able to build a valid range, but could build a valid range after collecting more access filters.
// After iterating all filters, we can check and build it into a valid util.AccessPath.
// Similar to AccessPath, unfinishedAccessPath has 2 meanings:
// 1. When orBranches is nil, it collects usable filters for a single candidate access path.
// 2. When orBranches is not nil, it's a container of partial paths, and each element in the slice corresponds to one
// OR branch in the input expression.
type unfinishedAccessPath struct {
	index *model.IndexInfo

	usableFilters []expression.Expression

	// To avoid regression and keep the same behavior as the previous implementation, we collect access filters in two
	// methods:
	//
	// 1. Use the same functions as the previous implementation to collect access filters. They are able to handle some
	// complicated expressions, but the expressions must be able to build into a valid range at once.
	// In this case, idxColHasUsableFilter will be nil and initedWithValidRange will be true.
	//
	// 2. Use the new logic, which is to collect access filters for each column respectively, gradually collect more
	// access filters during iterating all filters and try to form a valid range at last.
	// In this case, initedWithValidRange will be false, and idxColHasUsableFilter will record if we have collected
	// usable filters for each column of the index.
	idxColHasUsableFilter []bool
	initedWithValidRange  bool

	// needKeepFilter means the OR list need to become a filter in the final Selection.
	needKeepFilter bool

	// Similar to AccessPath.PartialIndexPaths, each element in the slice is for one OR branch.
	// It will build into AccessPath.PartialAlternativeIndexPaths.
	orBranches []unfinishedAccessPathList
}

// unfinishedAccessPathList collects usable filters for a slice of candidate access paths in preparation for building an
// OR type IndexMerge access path.
type unfinishedAccessPathList []*unfinishedAccessPath

// genUnfinishedPathFromORList handles a list of filters connected by OR, collects access filters for
// each candidate access path, and returns an unfinishedAccessPath, which must be an index merge OR unfinished path,
// each partial path of which corresponds to one filter in the input orList.
/*
Example:
 Input:
   orList: 1 member of j->'$.a' OR 2 member of j->'$.b'
   candidateAccessPaths: [idx1(a, j->'$.a' unsigned array), idx2(j->'$.b' unsigned array, a)]
 Output:
   unfinishedAccessPath{
     orBranches: [
       // Collect usable filters for (1 member of j->'$.a') using two candidates respectively.
       [ unfinishedAccessPath{idx1,1 member of j->'$.a'}, nil (no usable filters for idx2)                ]
       // Collect usable filters for (2 member of j->'$.b') using two candidates respectively.
       [ nil (no usable filters for idx1)               , unfinishedAccessPath{idx2,2 member of j->'$.b'} ]
     ]
  }
*/
func genUnfinishedPathFromORList(
	ds *logicalop.DataSource,
	orList []expression.Expression,
	candidateAccessPaths []*util.AccessPath,
) *unfinishedAccessPath {
	if len(orList) < 2 {
		return nil
	}
	unfinishedPartialPaths := make([]unfinishedAccessPathList, 0, len(orList))
	for _, singleFilter := range orList {
		unfinishedPathList := initUnfinishedPathsFromExpr(ds, candidateAccessPaths, singleFilter)
		if unfinishedPathList == nil {
			return nil
		}
		unfinishedPartialPaths = append(unfinishedPartialPaths, unfinishedPathList)
	}
	return &unfinishedAccessPath{
		orBranches: unfinishedPartialPaths,
	}
}

// initUnfinishedPathsFromExpr tries to collect access filters from the input filter for each candidate access path,
// and returns them as a slice of unfinishedAccessPath, each of which corresponds to an input candidate access path.
// If we failed to collect access filters for one candidate access path, the corresponding element in the return slice
// will be nil.
// If we failed to collect access filters for all candidate access paths, this function will return nil.
/*
Example1 (consistent with the one in genUnfinishedPathFromORList()):
  Input:
    expr: 1 member of j->'$.a'
    candidateAccessPaths: [idx1(a, j->'$.a' unsigned array), idx2(j->'$.b' unsigned array, a)]
  Output:
    [unfinishedAccessPath{idx1,1 member of j->'$.a'}, nil]

Example2:
  Input:
    expr: a = 3
    candidateAccessPaths: [idx1(a, j->'$.a' unsigned array), idx2(j->'$.b' unsigned array, a)]
  Output:
    [unfinishedAccessPath{idx1,a=3}, unfinishedAccessPath{idx2,a=3}]
*/
func initUnfinishedPathsFromExpr(
	ds *logicalop.DataSource,
	candidateAccessPaths []*util.AccessPath,
	expr expression.Expression,
) unfinishedAccessPathList {
	retValues := make([]unfinishedAccessPath, len(candidateAccessPaths))
	ret := make([]*unfinishedAccessPath, 0, len(candidateAccessPaths))
	for i := range candidateAccessPaths {
		ret = append(ret, &retValues[i])
	}
	for i, path := range candidateAccessPaths {
		ret[i].index = path.Index
		// case 1: try to use the previous logic to handle non-mv index
		if !isMVIndexPath(path) {
			partialPath, needSelection := generateNormalIndexPartialPath(
				ds,
				expr,
				path,
			)
			if partialPath != nil {
				ret[i].initedWithValidRange = true
				ret[i].needKeepFilter = needSelection
				ret[i].usableFilters = []expression.Expression{expr}
				continue
			}
		}
		if path.IsTablePath() {
			continue
		}
		idxCols, ok := PrepareIdxColsAndUnwrapArrayType(ds.Table.Meta(), path.Index, ds.TblColsByID, false)
		if !ok {
			continue
		}
		cnfItems := expression.SplitCNFItems(expr)
		pushDownCtx := util.GetPushDownCtx(ds.SCtx())
		for _, cnfItem := range cnfItems {
			if !expression.CanExprsPushDown(pushDownCtx, []expression.Expression{cnfItem}, kv.TiKV) {
				ret[i].needKeepFilter = true
			}
		}

		// case 2: try to use the previous logic to handle mv index
		if isMVIndexPath(path) {
			accessFilters, remainingFilters, tp := collectFilters4MVIndex(ds.SCtx(), cnfItems, idxCols)
			if len(accessFilters) > 0 && (tp == multiValuesOROnMVColTp || tp == singleValueOnMVColTp) {
				ret[i].initedWithValidRange = true
				ret[i].usableFilters = accessFilters
				ret[i].needKeepFilter = len(remainingFilters) > 0
				continue
			}
		}

		// case 3: use the new logic if the previous logic didn't succeed to collect access filters that can build a
		// valid range directly.
		ret[i].idxColHasUsableFilter = make([]bool, len(idxCols))
		ret[i].needKeepFilter = true
		for j, col := range idxCols {
			for _, cnfItem := range cnfItems {
				if ok, tp := checkAccessFilter4IdxCol(ds.SCtx(), cnfItem, col); ok &&
					// Since we only handle the OR list nested in the AND list, and only generate IndexMerge OR path,
					// we disable the multiValuesANDOnMVColTp case here.
					(tp == eqOnNonMVColTp || tp == multiValuesOROnMVColTp || tp == singleValueOnMVColTp) {
					ret[i].usableFilters = append(ret[i].usableFilters, cnfItem)
					ret[i].idxColHasUsableFilter[j] = true
					// Once we find one valid access filter for this column, we directly go to the next column without
					// looking into other filters.
					break
				}
			}
		}
	}

	validCnt := 0
	// remove useless paths
	for i, path := range ret {
		if !path.initedWithValidRange &&
			!slices.Contains(path.idxColHasUsableFilter, true) {
			ret[i] = nil
		} else {
			validCnt++
		}
	}
	if validCnt == 0 {
		return nil
	}
	return ret
}

// handleTopLevelANDList is expected to be used together with genUnfinishedPathFromORList() to handle the expression
// like ... AND (... OR ... OR ...) AND ... for mv index.
// It will try to collect possible access filters from other items in the top level AND list and try to merge them into
// the unfinishedAccessPath from genUnfinishedPathFromORList(), and try to build it into a valid
// util.AccessPath.
// The input candidateAccessPaths argument should be the same with genUnfinishedPathFromORList().
func handleTopLevelANDList(
	ds *logicalop.DataSource,
	allConds []expression.Expression,
	orListIdxInAllConds int,
	candidateAccessPaths []*util.AccessPath,
	unfinishedIndexMergePath *unfinishedAccessPath,
) *util.AccessPath {
	for i, cnfItem := range allConds {
		// Skip the (... OR ... OR ...) in the list.
		if i == orListIdxInAllConds {
			continue
		}
		// Collect access filters from one AND item.
		pathListFromANDItem := initUnfinishedPathsFromExpr(ds, candidateAccessPaths, cnfItem)
		// Try to merge useful access filters in them into unfinishedIndexMergePath, which is from the nested OR list.
		unfinishedIndexMergePath = mergeANDItemIntoUnfinishedIndexMergePath(unfinishedIndexMergePath, pathListFromANDItem)
	}
	if unfinishedIndexMergePath == nil {
		return nil
	}
	return buildIntoAccessPath(
		ds,
		candidateAccessPaths,
		unfinishedIndexMergePath,
		allConds,
		orListIdxInAllConds,
	)
}

/*
Example (consistent with the one in genUnfinishedPathFromORList()):

	idx1: (a, j->'$.a' unsigned array)  idx2: (j->'$.b' unsigned array, a)
	Input:
	  indexMergePath:
	    unfinishedAccessPath{ orBranches:[
	      [ unfinishedAccessPath{idx1,1 member of j->'$.a'}, nil                                             ]
	      [ nil                                            , unfinishedAccessPath{idx2,2 member of j->'$.b'} ]
	    ]}
	  pathListFromANDItem:
	    [unfinishedAccessPath{idx1,a=3}, unfinishedAccessPath{idx2,a=3}]
	Output:
	  unfinishedAccessPath{ orBranches:[
	    [ unfinishedAccessPath{idx1,1 member of j->'$.a', a=3}, nil                                                  ]
	    [ nil                                                 , unfinishedAccessPath{idx2,2 member of j->'$.b', a=3} ]
	  ]}
*/
func mergeANDItemIntoUnfinishedIndexMergePath(
	indexMergePath *unfinishedAccessPath,
	pathListFromANDItem unfinishedAccessPathList,
) *unfinishedAccessPath {
	// Currently, we only handle the case where indexMergePath is an index merge OR unfinished path and
	// pathListFromANDItem is a normal unfinished path or nil
	if indexMergePath == nil || len(indexMergePath.orBranches) == 0 {
		return nil
	}
	// This means we failed to find any valid access filter from other expressions in the top level AND list.
	// In this case, we ignore them and only rely on the nested OR list to try to build a IndexMerge OR path.
	if pathListFromANDItem == nil {
		return indexMergePath
	}
	for _, pathListForSinglePartialPath := range indexMergePath.orBranches {
		if len(pathListForSinglePartialPath) != len(pathListFromANDItem) {
			continue
		}
		for i, path := range pathListForSinglePartialPath {
			if path == nil || pathListFromANDItem[i] == nil {
				continue
			}
			// We don't do precise checks. As long as any columns have valid access filters, we collect the entire
			// access filters from the AND item.
			// We just collect as many possibly useful access filters as possible, buildIntoAccessPath() should handle
			// them correctly.
			if pathListFromANDItem[i].initedWithValidRange ||
				slices.Contains(pathListFromANDItem[i].idxColHasUsableFilter, true) {
				path.usableFilters = append(path.usableFilters, pathListFromANDItem[i].usableFilters...)
			}
		}
	}
	return indexMergePath
}

func buildIntoAccessPath(
	ds *logicalop.DataSource,
	originalPaths []*util.AccessPath,
	indexMergePath *unfinishedAccessPath,
	allConds []expression.Expression,
	orListIdxInAllConds int,
) *util.AccessPath {
	if indexMergePath == nil || len(indexMergePath.orBranches) == 0 {
		return nil
	}
	// 1. Use the collected usable filters to build partial paths for each alternative of each OR branch.
	allAlternativePaths := make([][][]*util.AccessPath, 0, len(indexMergePath.orBranches))

	// for each OR branch
	for _, orBranch := range indexMergePath.orBranches {
		var alternativesForORBranch [][]*util.AccessPath

		// for each alternative of this OR branch
		for i, unfinishedPath := range orBranch {
			if unfinishedPath == nil {
				continue
			}
			var oneAlternative []*util.AccessPath
			var needSelection bool
			if unfinishedPath.index != nil && unfinishedPath.index.MVIndex {
				// case 1: mv index
				idxCols, ok := PrepareIdxColsAndUnwrapArrayType(
					ds.Table.Meta(),
					unfinishedPath.index,
					ds.TblColsByID,
					true,
				)
				if !ok {
					continue
				}
				accessFilters, remainingFilters, _ := collectFilters4MVIndex(
					ds.SCtx(),
					unfinishedPath.usableFilters,
					idxCols,
				)
				if len(accessFilters) == 0 {
					continue
				}
				var isIntersection bool
				var err error
				oneAlternative, isIntersection, ok, err = buildPartialPaths4MVIndex(
					ds.SCtx(),
					accessFilters,
					idxCols,
					unfinishedPath.index,
					ds.TableStats.HistColl,
				)
				if err != nil || !ok || (isIntersection && len(oneAlternative) > 1) {
					continue
				}
				needSelection = len(remainingFilters) > 0
			} else {
				// case 2: non-mv index
				var path *util.AccessPath
				// Reuse the previous implementation. The same usage as in initUnfinishedPathsFromExpr().
				path, needSelection = generateNormalIndexPartialPath(
					ds,
					expression.ComposeCNFCondition(
						ds.SCtx().GetExprCtx(),
						unfinishedPath.usableFilters...,
					),
					originalPaths[i],
				)
				if path == nil {
					continue
				}
				oneAlternative = []*util.AccessPath{path}
			}
			needSelection = needSelection || unfinishedPath.needKeepFilter

			if needSelection {
				// only need to set one of the paths to true
				oneAlternative[0].KeepIndexMergeORSourceFilter = true
			}
			alternativesForORBranch = append(alternativesForORBranch, oneAlternative)
		}
		if len(alternativesForORBranch) == 0 {
			return nil
		}
		allAlternativePaths = append(allAlternativePaths, alternativesForORBranch)
	}

	// 2. Some extra setup and checks.

	pushDownCtx := util.GetPushDownCtx(ds.SCtx())
	possibleIdxIDs := make(map[int64]struct{}, len(allAlternativePaths))
	var containMVPath bool
	// We do two things in this loop:
	// 1. Clean/Set KeepIndexMergeORSourceFilter, InexFilters and TableFilters for each partial path.
	// 2. Collect all index IDs and check if there is any MV index.
	for _, p := range util.SliceRecursiveFlattenIter[*util.AccessPath](allAlternativePaths) {
		// A partial path can handle TableFilters only if it's a table path, and the filters can be pushed to TiKV.
		// Otherwise, we should clear TableFilters and set KeepIndexMergeORSourceFilter to true.
		if len(p.TableFilters) > 0 {
			// Note: Theoretically, we don't need to set KeepIndexMergeORSourceFilter to true if we can handle the
			// TableFilters. But filters that contain non-handle columns will be unexpectedly removed in
			// convertToPartialTableScan(). Not setting it to true will cause the final plan to miss those filters.
			// The behavior related to convertToPartialTableScan() needs more investigation.
			// Anyway, now we set it to true here, and it's also consistent with the previous implementation.
			p.KeepIndexMergeORSourceFilter = true
			if !expression.CanExprsPushDown(pushDownCtx, p.TableFilters, kv.TiKV) || !p.IsTablePath() {
				p.TableFilters = nil
			}
		}
		// A partial path can handle IndexFilters if the filters can be pushed to TiKV.
		if len(p.IndexFilters) != 0 && !expression.CanExprsPushDown(pushDownCtx, p.IndexFilters, kv.TiKV) {
			p.KeepIndexMergeORSourceFilter = true
			p.IndexFilters = nil
		}

		if p.IsTablePath() {
			possibleIdxIDs[-1] = struct{}{}
		} else {
			possibleIdxIDs[p.Index.ID] = struct{}{}
		}
		if isMVIndexPath(p) {
			containMVPath = true
		}
	}

	if !containMVPath && len(possibleIdxIDs) <= 1 {
		return nil
	}

	// Keep this filter as a part of table filters for safety if it has any parameter.
	needKeepORSourceFilter := expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), allConds[orListIdxInAllConds])

	// 3. Build the final access path.
	possiblePath := &util.AccessPath{
		PartialAlternativeIndexPaths: allAlternativePaths,
		TableFilters:                 slices.Delete(slices.Clone(allConds), orListIdxInAllConds, orListIdxInAllConds+1),
		IndexMergeORSourceFilter:     allConds[orListIdxInAllConds],
		KeepIndexMergeORSourceFilter: needKeepORSourceFilter,
	}

	// For estimation, we need the decided partial paths. So we use a simple heuristic to choose the partial paths by
	// comparing the row count just for estimation here.
	pathsForEstimate := make([]*util.AccessPath, 0, len(allAlternativePaths))
	for _, oneORBranch := range allAlternativePaths {
		pathsWithMinRowCount := slices.MinFunc(oneORBranch, cmpAlternatives(ds.SCtx().GetSessionVars()))
		pathsForEstimate = append(pathsForEstimate, pathsWithMinRowCount...)
	}
	possiblePath.CountAfterAccess = estimateCountAfterAccessForIndexMergeOR(ds, pathsForEstimate)

	return possiblePath
}

func cmpAlternatives(sessionVars *variable.SessionVars) func(lhs, rhs []*util.AccessPath) int {
	allPointOrEmptyRange := func(paths []*util.AccessPath) bool {
		// Prefer the path with empty range or all point ranges.
		for _, path := range paths {
			// 1. It's not empty range.
			if len(path.Ranges) > 0 &&
				// 2-1. It's not point range on table path.
				((path.IsTablePath() &&
					!path.OnlyPointRange(sessionVars.StmtCtx.TypeCtx())) ||
					// 2-2. It's not point range on unique index.
					(!path.IsTablePath() &&
						len(path.Ranges) > 0 &&
						!(path.OnlyPointRange(sessionVars.StmtCtx.TypeCtx()) && path.Index.Unique))) {
				return false
			}
		}
		return true
	}
	// If one alternative consists of multiple AccessPath, we use the maximum row count of them to compare.
	getMaxRowCountFromPaths := func(paths []*util.AccessPath) float64 {
		maxRowCount := 0.0
		for _, path := range paths {
			rowCount := path.CountAfterAccess
			if len(path.IndexFilters) > 0 {
				rowCount = path.CountAfterIndex
			}
			maxRowCount = max(maxRowCount, rowCount)
		}
		return maxRowCount
	}
	return func(a, b []*util.AccessPath) int {
		lhsBetterRange := allPointOrEmptyRange(a)
		rhsBetterRange := allPointOrEmptyRange(b)
		if lhsBetterRange != rhsBetterRange {
			if lhsBetterRange {
				return -1
			}
			return 1
		}
		lhsRowCount := getMaxRowCountFromPaths(a)
		rhsRowCount := getMaxRowCountFromPaths(b)
		return cmp.Compare(lhsRowCount, rhsRowCount)
	}
}

func estimateCountAfterAccessForIndexMergeOR(ds *logicalop.DataSource, decidedPartialPaths []*util.AccessPath) float64 {
	accessConds := make([]expression.Expression, 0, len(decidedPartialPaths))
	containMVPath := false
	for _, p := range decidedPartialPaths {
		if isMVIndexPath(p) {
			containMVPath = true
		}
		indexCondsForP := p.AccessConds[:]
		indexCondsForP = append(indexCondsForP, p.IndexFilters...)
		if len(indexCondsForP) > 0 {
			accessConds = append(accessConds, expression.ComposeCNFCondition(ds.SCtx().GetExprCtx(), indexCondsForP...))
		}
	}
	accessDNF := expression.ComposeDNFCondition(ds.SCtx().GetExprCtx(), accessConds...)
	var sel float64
	if containMVPath {
		sel = cardinality.CalcTotalSelectivityForMVIdxPath(ds.TableStats.HistColl,
			decidedPartialPaths,
			false,
		)
	} else {
		var err error
		sel, _, err = cardinality.Selectivity(
			ds.SCtx(),
			ds.TableStats.HistColl,
			[]expression.Expression{accessDNF},
			nil,
		)
		if err != nil {
			logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
			sel = cost.SelectionFactor
		}
	}
	return sel * ds.TableStats.RowCount
}
