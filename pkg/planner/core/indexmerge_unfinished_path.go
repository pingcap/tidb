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
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/util"
)

type unfinishedAccessPathList []*unfinishedAccessPath

type unfinishedAccessPath struct {
	Index *model.IndexInfo

	accessFilters         []expression.Expression
	IdxColHasAccessFilter []bool
	initedAsFinished      bool

	needKeepFilter bool

	indexMergeOrPartialPaths []unfinishedAccessPathList
}

func generateUnfinishedPathsFromExpr(
	ds *DataSource,
	originalPaths []*util.AccessPath,
	expr expression.Expression,
) unfinishedAccessPathList {
	retValues := make([]unfinishedAccessPath, len(originalPaths))
	ret := make([]*unfinishedAccessPath, 0, len(originalPaths))
	for i := range originalPaths {
		ret = append(ret, &retValues[i])
	}
	for i, path := range originalPaths {
		ret[i].Index = path.Index
		// case 1
		if !isMVIndexPath(path) {
			paths, needSelection, usedMap := ds.generateNormalIndexPartialPaths4DNF(
				[]expression.Expression{expr},
				[]*util.AccessPath{path},
			)
			if len(usedMap) == 1 && usedMap[0] && len(paths) == 1 {
				ret[i].initedAsFinished = true
				ret[i].accessFilters = paths[0].AccessConds
				ret[i].needKeepFilter = needSelection
				continue
			}
		}
		if path.IsTablePath() {
			continue
		}
		idxCols, ok := collectIdxCols(ds.table.Meta(), path.Index, ds.TblCols)
		if !ok {
			continue
		}
		cnfItems := expression.SplitCNFItems(expr)

		// case 2
		if isMVIndexPath(path) {
			accessFilters, remainingFilters, tp := collectFilters4MVIndex(ds.SCtx(), cnfItems, idxCols)
			if len(accessFilters) > 0 && (tp == MultiValuesOROnMVColTp || tp == SingleValueOnMVColTp) {
				ret[i].initedAsFinished = true
				ret[i].accessFilters = accessFilters
				ret[i].needKeepFilter = len(remainingFilters) > 0
				continue
			}
		}
		ret[i].IdxColHasAccessFilter = make([]bool, len(idxCols))

		// case 3
		// todo: invalid handling
		for j, col := range idxCols {
			for _, cnfItem := range cnfItems {
				if ok, tp := checkFilter4MVIndexColumn(ds.SCtx(), cnfItem, col); ok &&
					(tp == EQOnNonMVColTp || tp == MultiValuesOROnMVColTp || tp == SingleValueOnMVColTp) {
					ret[i].accessFilters = append(ret[i].accessFilters, cnfItem)
					ret[i].IdxColHasAccessFilter[j] = true
					break
				}
			}
		}
	}

	validCnt := 0
	// reset useless paths
	for i, path := range ret {
		if !path.initedAsFinished &&
			!slices.Contains(path.IdxColHasAccessFilter, true) {
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

func mergeUnfinishedPathsWithOR(path1, path2 unfinishedAccessPathList) unfinishedAccessPathList {
	if path1 == nil || path2 == nil {
		return nil
	}
	// case 1: append into existing index merge path
	if len(path1) == 1 && len(path1[0].indexMergeOrPartialPaths) > 0 {
		path1[0].indexMergeOrPartialPaths = append(path1[0].indexMergeOrPartialPaths, path2)
		return path1
	}
	// case 2: new index merge path
	ret := &unfinishedAccessPath{
		indexMergeOrPartialPaths: []unfinishedAccessPathList{path1, path2},
	}
	return unfinishedAccessPathList{ret}
}

func mergeUnfinishedPathsWithAND(indexMergePath *unfinishedAccessPath, pathListFromANDItem unfinishedAccessPathList) *unfinishedAccessPath {
	// currently, we only handle the case where indexMergePath is an index merge OR unfinished path and
	// pathListFromANDItem is a normal unfinished path or nil
	if indexMergePath == nil || len(indexMergePath.indexMergeOrPartialPaths) == 0 {
		return nil
	}
	if pathListFromANDItem == nil {
		return indexMergePath
	}
	for _, partialPathList := range indexMergePath.indexMergeOrPartialPaths {
		if len(partialPathList) != len(pathListFromANDItem) {
			continue
		}
		for i, path := range partialPathList {
			if path == nil || pathListFromANDItem[i] == nil {
				continue
			}
			if pathListFromANDItem[i].initedAsFinished {
				path.accessFilters = append(path.accessFilters, pathListFromANDItem[i].accessFilters...)
				continue
			}
			for j, hasAccessFilter := range pathListFromANDItem[i].IdxColHasAccessFilter {
				// handle the index column where the pathListFromANDItem has point access and the path from partial path
				// doesn't have point access
				if hasAccessFilter == true && path.IdxColHasAccessFilter[j] == false {
					// append access cond
					path.accessFilters = append(path.accessFilters, pathListFromANDItem[i].accessFilters...)
					break
				}
			}
		}
	}
	return indexMergePath
}

func buildAccessPathFromUnfinishedPath(
	ds *DataSource,
	originalPaths []*util.AccessPath,
	indexMergePath *unfinishedAccessPath,
	allConds []expression.Expression,
	curIdx int,
) *util.AccessPath {
	if indexMergePath == nil || len(indexMergePath.indexMergeOrPartialPaths) == 0 {
		return nil
	}
	var needSelectionGlobal bool
	partialPaths := make([]*util.AccessPath, 0, len(indexMergePath.indexMergeOrPartialPaths))
	for _, unfinishedPathList := range indexMergePath.indexMergeOrPartialPaths {
		// generate one or more partial access path for each partial unfinished path
		var (
			bestPaths            []*util.AccessPath
			bestCountAfterAccess float64
			bestNeedSelection    bool
		)
		for i, unfinishedPath := range unfinishedPathList {
			if unfinishedPath == nil {
				continue
			}
			var paths []*util.AccessPath
			var needSelection bool
			if unfinishedPath.Index != nil && unfinishedPath.Index.MVIndex {
				// case 1: mv index
				idxCols, ok := PrepareCols4MVIndex(ds.table.Meta(), unfinishedPath.Index, ds.TblCols)
				if !ok {
					continue
				}
				// for every cnfCond, try to map it into possible mv index path.
				// remainingFilters is not cared here, because it will be all suspended on the table side.
				accessFilters, remainingFilters, _ := collectFilters4MVIndex(
					ds.SCtx(),
					unfinishedPath.accessFilters,
					idxCols)
				if len(accessFilters) == 0 {
					continue
				}
				var isIntersection bool
				var err error
				paths, isIntersection, ok, err = buildPartialPaths4MVIndex(
					ds.SCtx(),
					accessFilters,
					idxCols,
					unfinishedPath.Index,
					ds.tableStats.HistColl)
				if err != nil || !ok || (isIntersection && len(paths) > 1) {
					continue
				}
				needSelection = len(remainingFilters) > 0 || len(unfinishedPath.IdxColHasAccessFilter) > 0
			} else {
				// case 2: non-mv index
				var usedMap []bool
				paths, needSelection, usedMap = ds.generateNormalIndexPartialPaths4DNF(
					[]expression.Expression{
						expression.ComposeCNFCondition(
							ds.SCtx().GetExprCtx(),
							unfinishedPath.accessFilters...,
						),
					},
					[]*util.AccessPath{originalPaths[i]},
				)
				if len(paths) == 0 || slices.Contains(usedMap, false) {
					continue
				}
			}
			needSelection = needSelection || unfinishedPath.needKeepFilter
			maxCountAfterAccess := -1.0
			for _, p := range paths {
				maxCountAfterAccess = math.Max(maxCountAfterAccess, p.CountAfterAccess)
			}
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
			// failed to get valid path(s) for this partial path
			return nil
		}
		// this unfinishedPathList from unfinishedAccessPath.indexMergeOrPartialPaths finished
		partialPaths = append(partialPaths, bestPaths...)
		needSelectionGlobal = needSelectionGlobal || bestNeedSelection
	}
	tableFilter := allConds[:]
	if !needSelectionGlobal {
		tableFilter = slices.Delete(tableFilter, curIdx, curIdx+1)
	}
	ret := ds.buildPartialPathUp4MVIndex(
		partialPaths,
		false,
		tableFilter,
		ds.tableStats.HistColl,
	)
	return ret
}

func collectIdxCols(
	tableInfo *model.TableInfo,
	mvIndex *model.IndexInfo,
	tblCols []*expression.Column,
) (idxCols []*expression.Column, ok bool) {
	for i := range mvIndex.Columns {
		colOffset := mvIndex.Columns[i].Offset
		colMeta := tableInfo.Cols()[colOffset]
		var col *expression.Column
		for _, c := range tblCols {
			if c.ID == colMeta.ID {
				col = c
				break
			}
		}
		if col == nil { // unexpected, no vir-col on this MVIndex
			return nil, false
		}
		if col.GetType().IsArray() {
			col = col.Clone().(*expression.Column)
			col.RetType = col.GetType().ArrayType() // use the underlying type directly: JSON-ARRAY(INT) --> INT
			col.RetType.SetCharset(charset.CharsetBin)
			col.RetType.SetCollate(charset.CollationBin)
		}
		idxCols = append(idxCols, col)
	}
	return idxCols, true
}
