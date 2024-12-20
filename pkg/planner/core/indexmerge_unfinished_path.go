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
	"math"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
)

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
	orBranches []unfinishedAccessPathList
}

// unfinishedAccessPathList collects usable filters for a slice of candidate access paths in preparation for building an
// OR type IndexMerge access path.
type unfinishedAccessPathList []*unfinishedAccessPath

// generateUnfinishedIndexMergePathFromORList handles a list of filters connected by OR, collects access filters for
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
			// generateNormalIndexPartialPaths4DNF is introduced for handle a slice of DNF items and a slice of
			// candidate AccessPaths before, now we reuse it to handle single filter and single candidate AccessPath,
			// so we need to wrap them in a slice here.
			partialPath, needSelection := generateNormalIndexPartialPath(
				ds,
				expr,
				path,
			)
			if partialPath != nil {
				ret[i].initedWithValidRange = true
				ret[i].usableFilters = partialPath.AccessConds
				ret[i].needKeepFilter = needSelection
				// Here is a special case, if this expr is always false and this path is a dual path, it will run to
				// this point, and paths[0].AccessConds and paths[0].Ranges will be nil.
				// In this case, we set the accessFilters to the original expr.
				if len(ret[i].usableFilters) <= 0 {
					ret[i].usableFilters = []expression.Expression{expr}
				}
				continue
			}
		}
		if path.IsTablePath() {
			continue
		}
		idxCols, ok := PrepareIdxColsAndUnwrapArrayType(ds.Table.Meta(), path.Index, ds.TblCols, false)
		if !ok {
			continue
		}
		cnfItems := expression.SplitCNFItems(expr)

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

	idx1: (a, j->'$.a' unsigned array)  idx2: (j->'$.b' unsigned array, a
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
	var needSelectionGlobal bool

	// 1. Generate one or more partial access path for each partial unfinished path (access filter on mv index may
	// produce several partial paths).
	partialPaths := make([]*util.AccessPath, 0, len(indexMergePath.orBranches))

	// for each partial path
	for _, unfinishedPathList := range indexMergePath.orBranches {
		var (
			bestPaths            []*util.AccessPath
			bestCountAfterAccess float64
			bestNeedSelection    bool
		)

		// for each possible access path of this partial path
		for i, unfinishedPath := range unfinishedPathList {
			if unfinishedPath == nil {
				continue
			}
			var paths []*util.AccessPath
			var needSelection bool
			if unfinishedPath.index != nil && unfinishedPath.index.MVIndex {
				// case 1: mv index
				idxCols, ok := PrepareIdxColsAndUnwrapArrayType(
					ds.Table.Meta(),
					unfinishedPath.index,
					ds.TblCols,
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
				paths, isIntersection, ok, err = buildPartialPaths4MVIndex(
					ds.SCtx(),
					accessFilters,
					idxCols,
					unfinishedPath.index,
					ds.TableStats.HistColl,
				)
				if err != nil || !ok || (isIntersection && len(paths) > 1) {
					continue
				}
				needSelection = len(remainingFilters) > 0 || len(unfinishedPath.idxColHasUsableFilter) > 0
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
				paths = []*util.AccessPath{path}
			}
			needSelection = needSelection || unfinishedPath.needKeepFilter
			// If there are several partial paths, we use the max CountAfterAccess for comparison.
			maxCountAfterAccess := -1.0
			for _, p := range paths {
				maxCountAfterAccess = math.Max(maxCountAfterAccess, p.CountAfterAccess)
			}
			// Choose the best partial path for this partial path.
			if len(bestPaths) == 0 {
				bestPaths = paths
				bestCountAfterAccess = maxCountAfterAccess
				bestNeedSelection = needSelection
			} else if bestCountAfterAccess > maxCountAfterAccess {
				bestPaths = paths
				bestCountAfterAccess = maxCountAfterAccess
				bestNeedSelection = needSelection
			}
		}
		if len(bestPaths) == 0 {
			return nil
		}
		// Succeeded to get valid path(s) for this partial path.
		partialPaths = append(partialPaths, bestPaths...)
		needSelectionGlobal = needSelectionGlobal || bestNeedSelection
	}

	// 2. Collect the final table filter
	// We always put all filters in the top level AND list except for the OR list into the final table filters.
	// Whether to put the OR list into the table filters also depends on the needSelectionGlobal.
	tableFilter := slices.Clone(allConds)
	if !needSelectionGlobal {
		tableFilter = slices.Delete(tableFilter, orListIdxInAllConds, orListIdxInAllConds+1)
	}

	// 3. Build the final access path
	ret := buildPartialPathUp4MVIndex(partialPaths, false, tableFilter, ds.TableStats.HistColl)
	return ret
}
