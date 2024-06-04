// Copyright 2022 PingCAP, Inc.
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
	"math"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// generateIndexMergePath generates IndexMerge AccessPaths on this DataSource.
func (ds *DataSource) generateIndexMergePath() error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	var warningMsg string
	stmtCtx := ds.SCtx().GetSessionVars().StmtCtx
	defer func() {
		if len(ds.indexMergeHints) > 0 && warningMsg != "" {
			ds.indexMergeHints = nil
			stmtCtx.AppendWarning(errors.NewNoStackError(warningMsg))
			logutil.BgLogger().Debug(warningMsg)
		}
	}()

	// Consider the IndexMergePath. Now, we just generate `IndexMergePath` in DNF case.
	// Use allConds instread of pushedDownConds,
	// because we want to use IndexMerge even if some expr cannot be pushed to TiKV.
	// We will create new Selection for exprs that cannot be pushed in convertToIndexMergeScan.
	indexMergeConds := make([]expression.Expression, 0, len(ds.allConds))
	for _, expr := range ds.allConds {
		indexMergeConds = append(indexMergeConds, expression.PushDownNot(ds.SCtx().GetExprCtx(), expr))
	}

	sessionAndStmtPermission := (ds.SCtx().GetSessionVars().GetEnableIndexMerge() || len(ds.indexMergeHints) > 0) && !stmtCtx.NoIndexMergeHint
	if !sessionAndStmtPermission {
		warningMsg = "IndexMerge is inapplicable or disabled. Got no_index_merge hint or tidb_enable_index_merge is off."
		return nil
	}

	if ds.tableInfo.TempTableType == model.TempTableLocal {
		warningMsg = "IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table."
		return nil
	}

	regularPathCount := len(ds.possibleAccessPaths)
	var err error
	if warningMsg, err = ds.generateIndexMerge4NormalIndex(regularPathCount, indexMergeConds); err != nil {
		return err
	}
	if err := ds.generateIndexMerge4MVIndex(regularPathCount, indexMergeConds); err != nil {
		return err
	}
	oldIndexMergeCount := len(ds.possibleAccessPaths)
	if err := ds.generateIndexMerge4ComposedIndex(regularPathCount, indexMergeConds); err != nil {
		return err
	}

	// Because index merge access paths are built from ds.allConds, sometimes they can help us consider more filters than
	// the ds.stats, which is calculated from ds.pushedDownConds before this point.
	// So we use a simple and naive method to update ds.stats here using the largest row count from index merge paths.
	// This can help to avoid some cases where the row count of operator above IndexMerge is larger than IndexMerge.
	// TODO: Probably we should directly consider ds.allConds when calculating ds.stats in the future.
	if len(ds.possibleAccessPaths) > regularPathCount && len(ds.allConds) > len(ds.pushedDownConds) {
		var maxRowCount float64
		for i := regularPathCount; i < len(ds.possibleAccessPaths); i++ {
			maxRowCount = max(maxRowCount, ds.possibleAccessPaths[i].CountAfterAccess)
		}
		if ds.StatsInfo().RowCount > maxRowCount {
			ds.SetStats(ds.tableStats.ScaleByExpectCnt(maxRowCount))
		}
	}

	// If without hints, it means that `enableIndexMerge` is true
	if len(ds.indexMergeHints) != 0 {
		// If len(indexMergeHints) > 0, then add warnings if index-merge hints cannot work.
		if regularPathCount == len(ds.possibleAccessPaths) {
			if warningMsg == "" {
				warningMsg = "IndexMerge is inapplicable"
			}
			return nil
		}

		// If len(indexMergeHints) > 0 and some index-merge paths were added, then prune all other non-index-merge paths.
		// if len(ds.possibleAccessPaths) > oldIndexMergeCount, it means composed index merge path is generated, prune others.
		if len(ds.possibleAccessPaths) > oldIndexMergeCount {
			ds.possibleAccessPaths = ds.possibleAccessPaths[oldIndexMergeCount:]
		} else {
			ds.possibleAccessPaths = ds.possibleAccessPaths[regularPathCount:]
		}
	}

	// If there is a multi-valued index hint, remove all paths which don't use the specified index.
	ds.cleanAccessPathForMVIndexHint()

	return nil
}

func (ds *DataSource) generateNormalIndexPartialPaths4DNF(
	dnfItems []expression.Expression,
	candidatePaths []*util.AccessPath,
) (paths []*util.AccessPath, needSelection bool, usedMap []bool) {
	paths = make([]*util.AccessPath, 0, len(dnfItems))
	usedMap = make([]bool, len(dnfItems))
	pushDownCtx := GetPushDownCtx(ds.SCtx())
	for offset, item := range dnfItems {
		cnfItems := expression.SplitCNFItems(item)
		pushedDownCNFItems := make([]expression.Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			if expression.CanExprsPushDown(pushDownCtx, []expression.Expression{cnfItem}, kv.TiKV) {
				pushedDownCNFItems = append(pushedDownCNFItems, cnfItem)
			} else {
				needSelection = true
			}
		}
		itemPaths := ds.accessPathsForConds(pushedDownCNFItems, candidatePaths)
		if len(itemPaths) == 0 {
			// for this dnf item, we couldn't generate an index merge partial path.
			// (1 member of (a)) or (3 member of (b)) or d=1; if one dnf item like d=1 here could walk index path,
			// the entire index merge is not valid anymore.
			return nil, false, usedMap
		}
		partialPath := buildIndexMergePartialPath(itemPaths)
		if partialPath == nil {
			// for this dnf item, we couldn't generate an index merge partial path.
			// (1 member of (a)) or (3 member of (b)) or d=1; if one dnf item like d=1 here could walk index path,
			// the entire index merge is not valid anymore.
			return nil, false, usedMap
		}

		// identify whether all pushedDownCNFItems are fully used.
		// If any partial path contains table filters, we need to keep the whole DNF filter in the Selection.
		if len(partialPath.TableFilters) > 0 {
			needSelection = true
			partialPath.TableFilters = nil
		}
		// If any partial path's index filter cannot be pushed to TiKV, we should keep the whole DNF filter.
		if len(partialPath.IndexFilters) != 0 && !expression.CanExprsPushDown(pushDownCtx, partialPath.IndexFilters, kv.TiKV) {
			needSelection = true
			// Clear IndexFilter, the whole filter will be put in indexMergePath.TableFilters.
			partialPath.IndexFilters = nil
		}
		// Keep this filter as a part of table filters for safety if it has any parameter.
		if expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), cnfItems) {
			needSelection = true
		}
		usedMap[offset] = true
		paths = append(paths, partialPath)
	}
	return paths, needSelection, usedMap
}

// getIndexMergeOrPath generates all possible IndexMergeOrPaths.
// For index merge union case, the order property from its partial
// path can be kept and multi-way merged and output. So we don't
// generate a concrete index merge path out, but an un-determined
// alternatives set index merge path instead.
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
func (ds *DataSource) generateIndexMergeOrPaths(filters []expression.Expression) error {
	usedIndexCount := len(ds.possibleAccessPaths)
	pushDownCtx := GetPushDownCtx(ds.SCtx())
	for k, cond := range filters {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.LogicOr {
			continue
		}
		// shouldKeepCurrentFilter means the partial paths can't cover the current filter completely, so we must add
		// the current filter into a Selection after partial paths.
		shouldKeepCurrentFilter := false
		var partialAlternativePaths = make([][]*util.AccessPath, 0, usedIndexCount)
		dnfItems := expression.FlattenDNFConditions(sf)
		for _, item := range dnfItems {
			cnfItems := expression.SplitCNFItems(item)

			pushedDownCNFItems := make([]expression.Expression, 0, len(cnfItems))
			for _, cnfItem := range cnfItems {
				if expression.CanExprsPushDown(pushDownCtx, []expression.Expression{cnfItem}, kv.TiKV) {
					pushedDownCNFItems = append(pushedDownCNFItems, cnfItem)
				} else {
					shouldKeepCurrentFilter = true
				}
			}

			itemPaths := ds.accessPathsForConds(pushedDownCNFItems, ds.possibleAccessPaths[:usedIndexCount])
			if len(itemPaths) == 0 {
				partialAlternativePaths = nil
				break
			}
			// we don't prune other possible index merge path here.
			// keep all the possible index merge partial paths here to let the property choose.
			partialAlternativePaths = append(partialAlternativePaths, itemPaths)
		}
		if len(partialAlternativePaths) <= 1 {
			continue
		}
		// in this loop we do two things.
		// 1: If all the partialPaths use the same index, we will not use the indexMerge.
		// 2: Compute a theoretical best countAfterAccess(pick its accessConds) for every alternative path(s).
		indexMap := make(map[int64]struct{}, 1)
		accessConds := make([]expression.Expression, 0, len(partialAlternativePaths))
		for _, oneAlternativeSet := range partialAlternativePaths {
			// 1: mark used map.
			for _, oneAlternativePath := range oneAlternativeSet {
				if oneAlternativePath.IsTablePath() {
					// table path
					indexMap[-1] = struct{}{}
				} else {
					// index path
					indexMap[oneAlternativePath.Index.ID] = struct{}{}
				}
			}
			// 2.1: trade off on countAfterAccess.
			minCountAfterAccessPath := buildIndexMergePartialPath(oneAlternativeSet)
			indexCondsForP := minCountAfterAccessPath.AccessConds[:]
			indexCondsForP = append(indexCondsForP, minCountAfterAccessPath.IndexFilters...)
			if len(indexCondsForP) > 0 {
				accessConds = append(accessConds, expression.ComposeCNFCondition(ds.SCtx().GetExprCtx(), indexCondsForP...))
			}
		}
		if len(indexMap) == 1 {
			continue
		}
		// 2.2 get the theoretical whole count after access for index merge.
		accessDNF := expression.ComposeDNFCondition(ds.SCtx().GetExprCtx(), accessConds...)
		sel, _, err := cardinality.Selectivity(ds.SCtx(), ds.tableStats.HistColl, []expression.Expression{accessDNF}, nil)
		if err != nil {
			logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
			sel = cost.SelectionFactor
		}

		possiblePath := buildIndexMergeOrPath(filters, partialAlternativePaths, k, shouldKeepCurrentFilter)
		if possiblePath == nil {
			return nil
		}
		possiblePath.CountAfterAccess = sel * ds.tableStats.RowCount
		// only after all partial path is determined, can the countAfterAccess be done, delay it to converging.
		ds.possibleAccessPaths = append(ds.possibleAccessPaths, possiblePath)
	}
	return nil
}

// isInIndexMergeHints returns true if the input index name is not excluded by the IndexMerge hints, which means either
// (1) there's no IndexMerge hint, (2) there's IndexMerge hint but no specified index names, or (3) the input index
// name is specified in the IndexMerge hints.
func (ds *DataSource) isInIndexMergeHints(name string) bool {
	// if no index merge hints, all mv index is accessible
	if len(ds.indexMergeHints) == 0 {
		return true
	}
	for _, hint := range ds.indexMergeHints {
		if hint.IndexHint == nil || len(hint.IndexHint.IndexNames) == 0 {
			return true
		}
		for _, hintName := range hint.IndexHint.IndexNames {
			if strings.EqualFold(strings.ToLower(name), strings.ToLower(hintName.String())) {
				return true
			}
		}
	}
	return false
}

// indexMergeHintsHasSpecifiedIdx returns true if there's IndexMerge hint, and it has specified index names.
func (ds *DataSource) indexMergeHintsHasSpecifiedIdx() bool {
	for _, hint := range ds.indexMergeHints {
		if hint.IndexHint == nil || len(hint.IndexHint.IndexNames) == 0 {
			continue
		}
		if len(hint.IndexHint.IndexNames) > 0 {
			return true
		}
	}
	return false
}

// indexMergeHintsHasSpecifiedIdx return true if the input index name is specified in the IndexMerge hint.
func (ds *DataSource) isSpecifiedInIndexMergeHints(name string) bool {
	for _, hint := range ds.indexMergeHints {
		if hint.IndexHint == nil || len(hint.IndexHint.IndexNames) == 0 {
			continue
		}
		for _, hintName := range hint.IndexHint.IndexNames {
			if strings.EqualFold(strings.ToLower(name), strings.ToLower(hintName.String())) {
				return true
			}
		}
	}
	return false
}

// accessPathsForConds generates all possible index paths for conditions.
func (ds *DataSource) accessPathsForConds(
	conditions []expression.Expression,
	candidatePaths []*util.AccessPath,
) []*util.AccessPath {
	var results = make([]*util.AccessPath, 0, len(candidatePaths))
	for _, path := range candidatePaths {
		newPath := &util.AccessPath{}
		if path.IsTablePath() {
			if !ds.isInIndexMergeHints("primary") {
				continue
			}
			if ds.tableInfo.IsCommonHandle {
				newPath.IsCommonHandlePath = true
				newPath.Index = path.Index
			} else {
				newPath.IsIntHandlePath = true
			}
			err := ds.deriveTablePathStats(newPath, conditions, true)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			var unsignedIntHandle bool
			if newPath.IsIntHandlePath && ds.tableInfo.PKIsHandle {
				if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
					unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
				}
			}
			// If the newPath contains a full range, ignore it.
			if ranger.HasFullRange(newPath.Ranges, unsignedIntHandle) {
				continue
			}
			// If we have point or empty range, just remove other possible paths.
			if len(newPath.Ranges) == 0 || newPath.OnlyPointRange(ds.SCtx().GetSessionVars().StmtCtx.TypeCtx()) {
				if len(results) == 0 {
					results = append(results, newPath)
				} else {
					results[0] = newPath
					results = results[:1]
				}
				break
			}
		} else {
			newPath.Index = path.Index
			if !ds.isInIndexMergeHints(newPath.Index.Name.L) {
				continue
			}
			err := ds.fillIndexPath(newPath, conditions)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			ds.deriveIndexPathStats(newPath, conditions, true)
			// If the newPath contains a full range, ignore it.
			if ranger.HasFullRange(newPath.Ranges, false) {
				continue
			}
			// If we have empty range, or point range on unique index, just remove other possible paths.
			if len(newPath.Ranges) == 0 || (newPath.OnlyPointRange(ds.SCtx().GetSessionVars().StmtCtx.TypeCtx()) && newPath.Index.Unique) {
				if len(results) == 0 {
					results = append(results, newPath)
				} else {
					results[0] = newPath
					results = results[:1]
				}
				break
			}
		}
		results = append(results, newPath)
	}
	return results
}

// buildIndexMergePartialPath chooses the best index path from all possible paths.
// Now we choose the index with minimal estimate row count.
func buildIndexMergePartialPath(indexAccessPaths []*util.AccessPath) *util.AccessPath {
	if len(indexAccessPaths) == 1 {
		return indexAccessPaths[0]
	}

	minEstRowIndex := 0
	minEstRow := math.MaxFloat64
	for i := 0; i < len(indexAccessPaths); i++ {
		rc := indexAccessPaths[i].CountAfterAccess
		if len(indexAccessPaths[i].IndexFilters) > 0 {
			rc = indexAccessPaths[i].CountAfterIndex
		}
		if rc < minEstRow {
			minEstRowIndex = i
			minEstRow = rc
		}
	}
	return indexAccessPaths[minEstRowIndex]
}

// buildIndexMergeOrPath generates one possible IndexMergePath.
func buildIndexMergeOrPath(
	filters []expression.Expression,
	partialAlternativePaths [][]*util.AccessPath,
	current int,
	shouldKeepCurrentFilter bool,
) *util.AccessPath {
	indexMergePath := &util.AccessPath{PartialAlternativeIndexPaths: partialAlternativePaths}
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, filters[:current]...)
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, filters[current+1:]...)
	// since shouldKeepCurrentFilter may be changed in alternative paths converging, kept the filer expression anyway here.
	indexMergePath.KeepIndexMergeORSourceFilter = shouldKeepCurrentFilter
	// this filter will be merged into indexPath's table filters when converging.
	indexMergePath.IndexMergeORSourceFilter = filters[current]
	return indexMergePath
}

func (ds *DataSource) generateNormalIndexPartialPath4And(normalPathCnt int, usedAccessMap map[string]expression.Expression) []*util.AccessPath {
	if res := ds.generateIndexMergeAndPaths(normalPathCnt, usedAccessMap); res != nil {
		return res.PartialIndexPaths
	}
	return nil
}

// generateIndexMergeAndPaths generates IndexMerge paths for `AND` (a.k.a. intersection type IndexMerge)
func (ds *DataSource) generateIndexMergeAndPaths(normalPathCnt int, usedAccessMap map[string]expression.Expression) *util.AccessPath {
	// For now, we only consider intersection type IndexMerge when the index names are specified in the hints.
	if !ds.indexMergeHintsHasSpecifiedIdx() {
		return nil
	}
	composedWithMvIndex := len(usedAccessMap) != 0

	// 1. Collect partial paths from normal paths.
	var partialPaths []*util.AccessPath
	for i := 0; i < normalPathCnt; i++ {
		originalPath := ds.possibleAccessPaths[i]
		// No need to consider table path as a partial path.
		if ds.possibleAccessPaths[i].IsTablePath() {
			continue
		}
		// since this code path is only for normal index, skip mv index here.
		if ds.possibleAccessPaths[i].Index.MVIndex {
			continue
		}
		if !ds.isSpecifiedInIndexMergeHints(originalPath.Index.Name.L) {
			continue
		}
		// If the path contains a full range, ignore it.
		if ranger.HasFullRange(originalPath.Ranges, false) {
			continue
		}
		if composedWithMvIndex {
			// case:
			// idx1: mv(c, cast(`a` as signed array))
			// idx2: idx(c)
			// idx3: idx(c, d)
			// for condition: (1 member of a) AND (c = 1) AND (d = 2), we should pick idx1 and idx3,
			// since idx2's access cond has already been covered by idx1.
			containRelation := true
			for _, access := range originalPath.AccessConds {
				if _, ok := usedAccessMap[string(access.HashCode())]; !ok {
					// some condition is not covered in previous mv index partial path, use it!
					containRelation = false
					break
				}
			}
			if containRelation {
				continue
			}
			// for this picked normal index, mark its access conds.
			for _, access := range originalPath.AccessConds {
				if _, ok := usedAccessMap[string(access.HashCode())]; !ok {
					usedAccessMap[string(access.HashCode())] = access
				}
			}
		}
		newPath := originalPath.Clone()
		partialPaths = append(partialPaths, newPath)
	}
	if len(partialPaths) < 1 {
		return nil
	}
	if len(partialPaths) == 1 && !composedWithMvIndex {
		// even single normal index path here, it can be composed with other mv index partial paths.
		return nil
	}

	// 2. Collect filters that can't be covered by the partial paths and deduplicate them.
	finalFilters := make([]expression.Expression, 0)
	partialFilters := make([]expression.Expression, 0, len(partialPaths))
	hashCodeSet := make(map[string]struct{})
	pushDownCtx := GetPushDownCtx(ds.SCtx())
	for _, path := range partialPaths {
		// Classify filters into coveredConds and notCoveredConds.
		coveredConds := make([]expression.Expression, 0, len(path.AccessConds)+len(path.IndexFilters))
		notCoveredConds := make([]expression.Expression, 0, len(path.IndexFilters)+len(path.TableFilters))
		// AccessConds can be covered by partial path.
		coveredConds = append(coveredConds, path.AccessConds...)
		for i, cond := range path.IndexFilters {
			// IndexFilters can be covered by partial path if it can be pushed down to TiKV.
			if !expression.CanExprsPushDown(pushDownCtx, []expression.Expression{cond}, kv.TiKV) {
				path.IndexFilters = append(path.IndexFilters[:i], path.IndexFilters[i+1:]...)
				notCoveredConds = append(notCoveredConds, cond)
			} else {
				coveredConds = append(coveredConds, cond)
			}
		}
		// TableFilters can't be covered by partial path.
		notCoveredConds = append(notCoveredConds, path.TableFilters...)

		// Record covered filters in hashCodeSet.
		// Note that we only record filters that not appear in the notCoveredConds. It's possible that a filter appear
		// in both coveredConds and notCoveredConds (e.g. because of prefix index). So we need this extra check to
		// avoid wrong deduplication.
		notCoveredHashCodeSet := make(map[string]struct{})
		for _, cond := range notCoveredConds {
			hashCode := string(cond.HashCode())
			notCoveredHashCodeSet[hashCode] = struct{}{}
		}
		for _, cond := range coveredConds {
			hashCode := string(cond.HashCode())
			if _, ok := notCoveredHashCodeSet[hashCode]; !ok {
				hashCodeSet[hashCode] = struct{}{}
			}
		}

		finalFilters = append(finalFilters, notCoveredConds...)
		partialFilters = append(partialFilters, coveredConds...)
	}

	// Remove covered filters from finalFilters and deduplicate finalFilters.
	dedupedFinalFilters := make([]expression.Expression, 0, len(finalFilters))
	for _, cond := range finalFilters {
		hashCode := string(cond.HashCode())
		if _, ok := hashCodeSet[hashCode]; !ok {
			dedupedFinalFilters = append(dedupedFinalFilters, cond)
			hashCodeSet[hashCode] = struct{}{}
		}
	}

	// Keep these partial filters as a part of table filters for safety if there is any parameter.
	if expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), partialFilters) {
		dedupedFinalFilters = append(dedupedFinalFilters, partialFilters...)
	}

	// 3. Estimate the row count after partial paths.
	sel, _, err := cardinality.Selectivity(ds.SCtx(), ds.tableStats.HistColl, partialFilters, nil)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		sel = cost.SelectionFactor
	}

	indexMergePath := &util.AccessPath{
		PartialIndexPaths:        partialPaths,
		IndexMergeIsIntersection: true,
		TableFilters:             dedupedFinalFilters,
		CountAfterAccess:         sel * ds.tableStats.RowCount,
	}
	return indexMergePath
}

// generateMVIndexMergePartialPaths4And try to find mv index merge partial path from a collection of cnf conditions.
func (ds *DataSource) generateMVIndexMergePartialPaths4And(normalPathCnt int, indexMergeConds []expression.Expression, histColl *statistics.HistColl) ([]*util.AccessPath, map[string]expression.Expression, error) {
	// step1: collect all mv index paths
	possibleMVIndexPaths := make([]*util.AccessPath, 0, len(ds.possibleAccessPaths))
	for idx := 0; idx < normalPathCnt; idx++ {
		if !isMVIndexPath(ds.possibleAccessPaths[idx]) {
			continue // not a MVIndex path
		}
		if !ds.isInIndexMergeHints(ds.possibleAccessPaths[idx].Index.Name.L) {
			continue
		}
		possibleMVIndexPaths = append(possibleMVIndexPaths, ds.possibleAccessPaths[idx])
	}
	// step2: mapping index merge conditions into possible mv index path
	mvAndPartialPath := make([]*util.AccessPath, 0, len(possibleMVIndexPaths))
	usedAccessCondsMap := make(map[string]expression.Expression, len(indexMergeConds))
	// fill the possible indexMergeConds down to possible index merge paths. If two index merge path
	// share same accessFilters, pick the one with minimum countAfterAccess.
	type record struct {
		originOffset     int
		paths            []*util.AccessPath
		countAfterAccess float64
	}
	// mm is a map here used for de-duplicate partial paths which is derived from **same** accessFilters, not necessary to keep them both.
	mm := make(map[string]*record, 0)
	for idx := 0; idx < len(possibleMVIndexPaths); idx++ {
		idxCols, ok := PrepareIdxColsAndUnwrapArrayType(
			ds.table.Meta(),
			possibleMVIndexPaths[idx].Index,
			ds.TblCols,
			true,
		)
		if !ok {
			continue
		}
		accessFilters, _, mvColOffset, mvFilterMutations := CollectFilters4MVIndexMutations(ds.SCtx(), indexMergeConds, idxCols)
		if len(accessFilters) == 0 { // cannot use any filter on this MVIndex
			continue
		}
		// record the all hashcodes before accessFilters is mutated.
		allHashCodes := make([]string, 0, len(accessFilters)+len(mvFilterMutations)-1)
		for _, accessF := range accessFilters {
			allHashCodes = append(allHashCodes, string(accessF.HashCode()))
		}
		for i, mvF := range mvFilterMutations {
			if i == 0 {
				// skip the first one, because it has already in accessFilters.
				continue
			}
			allHashCodes = append(allHashCodes, string(mvF.HashCode()))
		}
		// in traveling of these mv index conditions, we can only use one of them to build index merge path, just fetch it out first.
		// build index merge partial path for every mutation combination access filters.
		var partialPaths4ThisMvIndex []*util.AccessPath
		for _, mvFilterMu := range mvFilterMutations {
			// derive each mutation access filters
			accessFilters[mvColOffset] = mvFilterMu

			partialPaths, isIntersection, ok, err := buildPartialPaths4MVIndex(ds.SCtx(), accessFilters, idxCols, possibleMVIndexPaths[idx].Index, ds.tableStats.HistColl)
			if err != nil {
				logutil.BgLogger().Debug("build index merge partial mv index paths failed", zap.Error(err))
				return nil, nil, err
			}
			if !ok {
				// for this mutation we couldn't build index merge partial path correctly, skip it.
				continue
			}
			// how we can merge mv index paths with normal index paths from index merge (intersection)?
			// 1: len(partialPaths) = 1, no matter what it type is:
			//		And(path1, path2, Or(path3)) => And(path1, path2, path3, merge(table-action like filters))
			// 2: it's an original intersection type:
			//		And(path1, path2, And(path3, path4)) => And(path1, path2, path3, path4, merge(table-action like filter)
			if len(partialPaths) == 1 || isIntersection {
				for _, accessF := range accessFilters {
					usedAccessCondsMap[string(accessF.HashCode())] = accessF
				}
				partialPaths4ThisMvIndex = append(partialPaths4ThisMvIndex, partialPaths...)
			}
		}
		// sort allHashCodes to make the unified hashcode for slices of all accessFilters.
		slices.Sort(allHashCodes)
		allHashCodesKey := strings.Join(allHashCodes, "")
		countAfterAccess := float64(histColl.RealtimeCount) * cardinality.CalcTotalSelectivityForMVIdxPath(histColl, partialPaths4ThisMvIndex, true)
		if rec, ok := mm[allHashCodesKey]; !ok {
			// compute the count after access from those intersection partial paths, for this mv index usage.
			mm[allHashCodesKey] = &record{idx, partialPaths4ThisMvIndex, countAfterAccess}
		} else {
			// pick the minimum countAfterAccess's paths.
			if rec.countAfterAccess > countAfterAccess {
				mm[allHashCodesKey] = &record{idx, partialPaths4ThisMvIndex, countAfterAccess}
			}
		}
	}
	// after all mv index is traversed, pick those remained paths which has already been de-duplicated for its accessFilters.
	recordsCollection := maps.Values(mm)
	// according origin offset to stable the partial paths order. (golang map is not order stable)
	slices.SortFunc(recordsCollection, func(a, b *record) int {
		return cmp.Compare(a.originOffset, b.originOffset)
	})
	for _, one := range recordsCollection {
		mvAndPartialPath = append(mvAndPartialPath, one.paths...)
	}
	return mvAndPartialPath, usedAccessCondsMap, nil
}

func (ds *DataSource) generateIndexMerge4NormalIndex(regularPathCount int, indexMergeConds []expression.Expression) (string, error) {
	isPossibleIdxMerge := len(indexMergeConds) > 0 && // have corresponding access conditions, and
		len(ds.possibleAccessPaths) > 1 // have multiple index paths
	if !isPossibleIdxMerge {
		return "IndexMerge is inapplicable or disabled. No available filter or available index.", nil
	}

	// We current do not consider `IndexMergePath`:
	// 1. If there is an index path.
	// 2. TODO: If there exists exprs that cannot be pushed down. This is to avoid wrongly estRow of Selection added by rule_predicate_push_down.
	stmtCtx := ds.SCtx().GetSessionVars().StmtCtx
	needConsiderIndexMerge := true
	// if current index merge hint is nil, once there is a no-access-cond in one of possible access path.
	if len(ds.indexMergeHints) == 0 {
		skipRangeScanCheck := fixcontrol.GetBoolWithDefault(
			ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(),
			fixcontrol.Fix52869,
			false,
		)
		if !skipRangeScanCheck {
			for i := 1; i < len(ds.possibleAccessPaths); i++ {
				if len(ds.possibleAccessPaths[i].AccessConds) != 0 {
					needConsiderIndexMerge = false
					break
				}
			}
		}
		if needConsiderIndexMerge {
			// PushDownExprs() will append extra warnings, which is annoying. So we reset warnings here.
			warnings := stmtCtx.GetWarnings()
			extraWarnings := stmtCtx.GetExtraWarnings()
			_, remaining := expression.PushDownExprs(GetPushDownCtx(ds.SCtx()), indexMergeConds, kv.UnSpecified)
			stmtCtx.SetWarnings(warnings)
			stmtCtx.SetExtraWarnings(extraWarnings)
			if len(remaining) > 0 {
				needConsiderIndexMerge = false
			}
		}
	}

	if !needConsiderIndexMerge {
		return "IndexMerge is inapplicable or disabled. ", nil // IndexMerge is inapplicable
	}

	// 1. Generate possible IndexMerge paths for `OR`.
	err := ds.generateIndexMergeOrPaths(indexMergeConds)
	if err != nil {
		return "", err
	}
	// 2. Generate possible IndexMerge paths for `AND`.
	indexMergeAndPath := ds.generateIndexMergeAndPaths(regularPathCount, nil)
	if indexMergeAndPath != nil {
		ds.possibleAccessPaths = append(ds.possibleAccessPaths, indexMergeAndPath)
	}
	return "", nil
}

// generateIndexMergeOnDNF4MVIndex generates IndexMerge paths for MVIndex upon DNF filters.
/*
	select * from t where ((1 member of (a) and b=1) or (2 member of (a) and b=2)) and (c > 10)
		IndexMerge(OR)
			IndexRangeScan(a, b, [1 1, 1 1])
			IndexRangeScan(a, b, [2 2, 2 2])
			Selection(c > 10)
				TableRowIdScan(t)
	Two limitations now:
	1). all filters in the DNF have to be used as access-filters: ((1 member of (a)) or (2 member of (a)) or b > 10) cannot be used to access the MVIndex.
	2). cannot support json_contains: (json_contains(a, '[1, 2]') or json_contains(a, '[3, 4]')) is not supported since a single IndexMerge cannot represent this SQL.
*/
func (ds *DataSource) generateIndexMergeOnDNF4MVIndex(normalPathCnt int, filters []expression.Expression) (mvIndexPaths []*util.AccessPath, err error) {
	for idx := 0; idx < normalPathCnt; idx++ {
		if !isMVIndexPath(ds.possibleAccessPaths[idx]) {
			continue // not a MVIndex path
		}

		// for single MV index usage, if specified use the specified one, if not, all can be access and chosen by cost model.
		if !ds.isInIndexMergeHints(ds.possibleAccessPaths[idx].Index.Name.L) {
			continue
		}

		for current, filter := range filters {
			sf, ok := filter.(*expression.ScalarFunction)
			if !ok || sf.FuncName.L != ast.LogicOr {
				continue
			}
			dnfFilters := expression.SplitDNFItems(sf) // [(1 member of (a) and b=1), (2 member of (a) and b=2)]

			unfinishedIndexMergePath := generateUnfinishedIndexMergePathFromORList(
				ds,
				dnfFilters,
				[]*util.AccessPath{ds.possibleAccessPaths[idx]},
			)
			finishedIndexMergePath := handleTopLevelANDListAndGenFinishedPath(
				ds,
				filters,
				current,
				[]*util.AccessPath{ds.possibleAccessPaths[idx]},
				unfinishedIndexMergePath,
			)
			if finishedIndexMergePath != nil {
				mvIndexPaths = append(mvIndexPaths, finishedIndexMergePath)
			}
		}
	}
	return
}

// generateIndexMerge4ComposedIndex generates index path composed of multi indexes including multivalued index from
// (json_member_of / json_overlaps / json_contains) and single-valued index from normal indexes.
/*
CNF path
	1. select * from t where ((1 member of (a) and c=1) and (2 member of (b) and d=2) and (other index predicates))
		flatten as: select * from t where 1 member of (a) and 2 member of (b) and c=1 and c=2 and other index predicates
		analyze: find and utilize index access filter items as much as possible:
		IndexMerge(AND-INTERSECTION)                  --- ROOT
			IndexRangeScan(mv-index-a)(1)             --- COP   ---> expand as: IndexMerge(AND)          ---> simplify: since memberof only have 1 partial index plan, we can defer the TableRowIdScan(t).
			IndexRangeScan(mv-index-b)(2)             --- COP						IndexRangeScan(a, [1,1])
			IndexRangeScan(non-mv-index-if-any)(?)    --- COP		        		TableRowIdScan(t)
			Selection(remained-non-index-predicates)  --- COP
				TableRowIdScan(t)  					  --- COP
	2. select * from t where ((1 member of (a) and c=1) and (json_contains(b, '[1, 2, 3]') and d=2) and (other index predicates))
		flatten as: select * from t where 1 member of (a) and json_contains(b, '[1, 2, 3]') and c=1 and c=2 and other index predicates
		analyze: find and utilize index access filter items as much as possible:
		IndexMerge(AND-INTERSECTION)                  --- ROOT
			IndexRangeScan(mv-index-a)(1)             --- COP
			IndexMerge(mv-index-b AND-INTERSECTION)   --- ROOT (embedded index merge) ---> simplify: we can defer the TableRowIdScan(t) to the outer index merge.
				IndexRangeScan(a, [1,1])              --- COP
				IndexRangeScan(a, [2,2])              --- COP
				IndexRangeScan(a, [3,3])              --- COP
			IndexRangeScan(non-mv-index-if-any)(?)    --- COP
			TableRowIdScan(t)                         --- COP
	3. select * from t where ((1 member of (a) and c=1) and (json_overlap(a, '[1, 2, 3]') and d=2) and (other index predicates))
		flatten as: select * from t where 1 member of (a) and json_overlap(a, '[1, 2, 3]') and c=1 and c=2 and other index predicates
		analyze: find and utilize index access filter items as much as possible:
		IndexMerge(AND-INTERSECTION)                  --- ROOT
			IndexRangeScan(mv-index-a)(1)             --- COP
			IndexMerge(mv-index-b OR-UNION)           --- ROOT (embedded index merge) ---> no simplify
				IndexRangeScan(a, [1,1])              --- COP
				IndexRangeScan(a, [2,2])              --- COP
				IndexRangeScan(a, [3,3])              --- COP
			IndexRangeScan(non-mv-index-if-any)(?)    --- COP
			TableRowIdScan(t)                         --- COP
DNF path
	Note: in DNF pattern, every dnf item should be utilized as index path with full range or prefix range. Otherwise,index merge is invalid.
	1. select * from t where ((1 member of (a)) or (2 member of (b)) or (other index predicates))
		analyze: find and utilize index access filter items as much as possible:
		IndexMerge(OR-UNION)                         --- ROOT
			IndexRangeScan(mv-index-a)(1)             --- COP  ---> simplified from member-of index merge, defer TableRowIdScan(t) to the outer index merge.
			IndexRangeScan(mv-index-b)(2)             --- COP
			IndexRangeScan(non-mv-index-if-any)(?)    --- COP
			TableRowIdScan(t)                         --- COP
	2. select * from t where ((1 member of (a)) or (json_contains(b, '[1, 2, 3]')) or (other index predicates))
		analyze: find and utilize index access filter items as much as possible:
		IndexMerge(OR-UNION)                          --- ROOT
			IndexRangeScan(mv-index-a)(1)             --- COP
			IndexMerge(mv-index-b AND-INTERSECTION)   --- ROOT (embedded index merge) ---> can't be simplified
				IndexRangeScan(a, [1,1])              --- COP
				IndexRangeScan(a, [2,2])              --- COP
				IndexRangeScan(a, [3,3])              --- COP
			IndexRangeScan(non-mv-index-if-any)(?)    --- COP
			TableRowIdScan(t)                         --- COP
	3. select * from t where ((1 member of (a) and c=1) or (json_overlap(a, '[1, 2,3]') and d=2) or (other index predicates)
		analyze: find and utilize index access filter items as much as possible:
		IndexMerge(OR-UNION)                          --- ROOT
			IndexRangeScan(mv-index-a)(1)             --- COP
			IndexMerge(mv-index-b OR-UNION)           --- ROOT (embedded index merge) ---> simplify: we can merge with outer index union merge, and defer TableRowIdScan(t).
				IndexRangeScan(a, [1,1])              --- COP
				IndexRangeScan(a, [2,2])              --- COP
				IndexRangeScan(a, [3,3])              --- COP
			IndexRangeScan(non-mv-index-if-any)(?)    --- COP
			TableRowIdScan(t)                         --- COP
*/
func (ds *DataSource) generateIndexMerge4ComposedIndex(normalPathCnt int, indexMergeConds []expression.Expression) error {
	isPossibleIdxMerge := len(indexMergeConds) > 0 && // have corresponding access conditions, and
		len(ds.possibleAccessPaths) > 1 // have multiple index paths
	if !isPossibleIdxMerge {
		return nil
	}

	// Collect access paths that satisfy the hints, and make sure there is at least one MV index path.
	var mvIndexPathCnt int
	candidateAccessPaths := make([]*util.AccessPath, 0, len(ds.possibleAccessPaths))
	for idx := 0; idx < normalPathCnt; idx++ {
		if (ds.possibleAccessPaths[idx].IsTablePath() &&
			!ds.isInIndexMergeHints("primary")) ||
			(!ds.possibleAccessPaths[idx].IsTablePath() &&
				!ds.isInIndexMergeHints(ds.possibleAccessPaths[idx].Index.Name.L)) {
			continue
		}
		if isMVIndexPath(ds.possibleAccessPaths[idx]) {
			mvIndexPathCnt++
		}
		candidateAccessPaths = append(candidateAccessPaths, ds.possibleAccessPaths[idx])
	}
	if mvIndexPathCnt == 0 {
		return nil
	}

	for current, filter := range indexMergeConds {
		// DNF path.
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.LogicOr {
			// targeting: cond1 or cond2 or cond3
			continue
		}
		dnfFilters := expression.SplitDNFItems(sf)

		unfinishedIndexMergePath := generateUnfinishedIndexMergePathFromORList(
			ds,
			dnfFilters,
			candidateAccessPaths,
		)
		finishedIndexMergePath := handleTopLevelANDListAndGenFinishedPath(
			ds,
			indexMergeConds,
			current,
			candidateAccessPaths,
			unfinishedIndexMergePath,
		)
		if finishedIndexMergePath == nil {
			return nil
		}

		var mvIndexPartialPathCnt, normalIndexPartialPathCnt int
		for _, path := range finishedIndexMergePath.PartialIndexPaths {
			if isMVIndexPath(path) {
				mvIndexPartialPathCnt++
			} else {
				normalIndexPartialPathCnt++
			}
		}

		// Keep the same behavior with previous implementation, we only handle the "composed" case here.
		if mvIndexPartialPathCnt == 0 || (mvIndexPartialPathCnt == 1 && normalIndexPartialPathCnt == 0) {
			return nil
		}
		ds.possibleAccessPaths = append(ds.possibleAccessPaths, finishedIndexMergePath)
		return nil
	}
	// CNF path.

	// after fillIndexPath, all cnf items are filled into the suitable index paths, for these normal index paths,
	// fetch them out as partialIndexPaths of a outerScope index merge.
	// note that:
	// 1: for normal index idx(b): b=2 and b=1 will lead a constant folding, finally leading an invalid range.
	// 		so it's possible that multi condition about the column b can be filled into same index b path.
	// 2: for mv index mv(j): 1 member of (j->'$.path') and 2 member of (j->'$.path') couldn't be classified
	// 		into a single index path conditions in fillIndexPath calling.
	// 3: The predicate of mv index can not converge into a linear interval range at physical phase like EQ and
	//		GT in normal index. Among the predicates in mv index (member-of/contains/overlap), multi conditions
	//		about them should be built as self-independent index path, deriving the final intersection/union handles,
	//		which means a mv index path may be reused for multi related conditions.
	// so we do this:
	// step1: firstly collect all the potential normal index partial paths.
	// step2: secondly collect all the potential mv index partial path, and merge them into one if possible.
	// step3: thirdly merge normal index paths and mv index paths together to compose a bigger index merge path.
	mvIndexPartialPaths, usedAccessMap, err := ds.generateMVIndexMergePartialPaths4And(normalPathCnt, indexMergeConds, ds.tableStats.HistColl)
	if err != nil {
		return err
	}
	if len(mvIndexPartialPaths) == 0 {
		return nil
	}
	normalIndexPartialPaths := ds.generateNormalIndexPartialPath4And(normalPathCnt, usedAccessMap)
	// since multi normal index merge path is handled before, here focus on multi mv index merge, or mv and normal mixed index merge
	composed := (len(mvIndexPartialPaths) > 1) || (len(mvIndexPartialPaths) == 1 && len(normalIndexPartialPaths) >= 1)
	if !composed {
		return nil
	}
	// todo: make this as the portal of all index merge case.
	combinedPartialPaths := append(normalIndexPartialPaths, mvIndexPartialPaths...)
	if len(combinedPartialPaths) == 0 {
		return nil
	}

	// collect the remained CNF conditions
	var remainedCNFs []expression.Expression
	for _, CNFItem := range indexMergeConds {
		if _, ok := usedAccessMap[string(CNFItem.HashCode())]; !ok {
			remainedCNFs = append(remainedCNFs, CNFItem)
		}
	}
	mvp := ds.buildPartialPathUp4MVIndex(combinedPartialPaths, true, remainedCNFs, ds.tableStats.HistColl)

	ds.possibleAccessPaths = append(ds.possibleAccessPaths, mvp)
	return nil
}

// generateIndexMerge4MVIndex generates paths for (json_member_of / json_overlaps / json_contains) on multi-valued index.
/*
	1. select * from t where 1 member of (a)
		IndexMerge(AND)
			IndexRangeScan(a, [1,1])
			TableRowIdScan(t)
	2. select * from t where json_contains(a, '[1, 2, 3]')
		IndexMerge(AND)
			IndexRangeScan(a, [1,1])
			IndexRangeScan(a, [2,2])
			IndexRangeScan(a, [3,3])
			TableRowIdScan(t)
	3. select * from t where json_overlap(a, '[1, 2, 3]')
		IndexMerge(OR)
			IndexRangeScan(a, [1,1])
			IndexRangeScan(a, [2,2])
			IndexRangeScan(a, [3,3])
			TableRowIdScan(t)
*/
func (ds *DataSource) generateIndexMerge4MVIndex(normalPathCnt int, filters []expression.Expression) error {
	dnfMVIndexPaths, err := ds.generateIndexMergeOnDNF4MVIndex(normalPathCnt, filters)
	if err != nil {
		return err
	}
	ds.possibleAccessPaths = append(ds.possibleAccessPaths, dnfMVIndexPaths...)

	for idx := 0; idx < normalPathCnt; idx++ {
		if !isMVIndexPath(ds.possibleAccessPaths[idx]) {
			continue // not a MVIndex path
		}

		// for single MV index usage, if specified use the specified one, if not, all can be access and chosen by cost model.
		if !ds.isInIndexMergeHints(ds.possibleAccessPaths[idx].Index.Name.L) {
			continue
		}

		idxCols, ok := PrepareIdxColsAndUnwrapArrayType(
			ds.table.Meta(),
			ds.possibleAccessPaths[idx].Index,
			ds.TblCols,
			true,
		)
		if !ok {
			continue
		}

		accessFilters, remainingFilters, _ := collectFilters4MVIndex(ds.SCtx(), filters, idxCols)
		if len(accessFilters) == 0 { // cannot use any filter on this MVIndex
			continue
		}

		partialPaths, isIntersection, ok, err := buildPartialPaths4MVIndex(ds.SCtx(), accessFilters, idxCols, ds.possibleAccessPaths[idx].Index, ds.tableStats.HistColl)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		ds.possibleAccessPaths = append(ds.possibleAccessPaths, ds.buildPartialPathUp4MVIndex(
			partialPaths,
			isIntersection,
			remainingFilters,
			ds.tableStats.HistColl,
		),
		)
	}
	return nil
}

// buildPartialPathUp4MVIndex builds these partial paths up to a complete index merge path.
func (*DataSource) buildPartialPathUp4MVIndex(
	partialPaths []*util.AccessPath,
	isIntersection bool,
	remainingFilters []expression.Expression,
	histColl *statistics.HistColl,
) *util.AccessPath {
	indexMergePath := &util.AccessPath{PartialIndexPaths: partialPaths, IndexMergeAccessMVIndex: true}
	indexMergePath.IndexMergeIsIntersection = isIntersection
	indexMergePath.TableFilters = remainingFilters
	indexMergePath.CountAfterAccess = float64(histColl.RealtimeCount) *
		cardinality.CalcTotalSelectivityForMVIdxPath(histColl, partialPaths, isIntersection)
	return indexMergePath
}

// buildPartialPaths4MVIndex builds partial paths by using these accessFilters upon this MVIndex.
// The accessFilters must be corresponding to these idxCols.
// OK indicates whether it builds successfully. These partial paths should be ignored if ok==false.
func buildPartialPaths4MVIndex(
	sctx context.PlanContext,
	accessFilters []expression.Expression,
	idxCols []*expression.Column,
	mvIndex *model.IndexInfo,
	histColl *statistics.HistColl,
) (
	partialPaths []*util.AccessPath,
	isIntersection bool,
	ok bool,
	err error,
) {
	evalCtx := sctx.GetExprCtx().GetEvalCtx()

	var virColID = -1
	for i := range idxCols {
		// index column may contain other virtual column.
		if idxCols[i].VirtualExpr != nil && idxCols[i].VirtualExpr.GetType(evalCtx).IsArray() {
			virColID = i
			break
		}
	}
	if virColID == -1 { // unexpected, no vir-col on this MVIndex
		return nil, false, false, nil
	}
	if len(accessFilters) <= virColID {
		// No filter related to the vir-col, cannot build a path for multi-valued index. Scanning on a multi-valued
		// index will only produce the rows whose corresponding array is not empty.
		return nil, false, false, nil
	}
	// If the condition is related with the array column, all following condition assumes that the array is not empty:
	// `member of`, `json_contains`, `json_overlaps` all return false when the array is empty, except that
	// `json_contains('[]', '[]')` is true. Therefore, using `json_contains(array, '[]')` is also not allowed here.
	//
	// Only when the condition implies that the array is not empty, it'd be safe to scan on multi-valued index without
	// worrying whether the row with empty array will be lost in the result.

	virCol := idxCols[virColID]
	jsonType := virCol.GetType(evalCtx).ArrayType()
	targetJSONPath, ok := unwrapJSONCast(virCol.VirtualExpr)
	if !ok {
		return nil, false, false, nil
	}

	// extract values related to this vir-col, for example, extract [1, 2] from `json_contains(j, '[1, 2]')`
	var virColVals []expression.Expression
	sf, ok := accessFilters[virColID].(*expression.ScalarFunction)
	if !ok {
		return nil, false, false, nil
	}
	switch sf.FuncName.L {
	case ast.JSONMemberOf: // (1 member of a->'$.zip')
		v, ok := unwrapJSONCast(sf.GetArgs()[0]) // cast(1 as json) --> 1
		if !ok {
			return nil, false, false, nil
		}
		virColVals = append(virColVals, v)
	case ast.JSONContains: // (json_contains(a->'$.zip', '[1, 2, 3]')
		isIntersection = true
		virColVals, ok = jsonArrayExpr2Exprs(
			sctx.GetExprCtx(),
			ast.JSONContains,
			sf.GetArgs()[1],
			jsonType,
			true,
		)
		if !ok || len(virColVals) == 0 {
			// json_contains(JSON, '[]') is TRUE. If the row has an empty array, it'll not exist on multi-valued index,
			// but the `json_contains(array, '[]')` is still true, so also don't try to scan on the index.
			return nil, false, false, nil
		}
	case ast.JSONOverlaps: // (json_overlaps(a->'$.zip', '[1, 2, 3]')
		var jsonPathIdx int
		if sf.GetArgs()[0].Equal(sctx.GetExprCtx().GetEvalCtx(), targetJSONPath) {
			jsonPathIdx = 0 // (json_overlaps(a->'$.zip', '[1, 2, 3]')
		} else if sf.GetArgs()[1].Equal(sctx.GetExprCtx().GetEvalCtx(), targetJSONPath) {
			jsonPathIdx = 1 // (json_overlaps('[1, 2, 3]', a->'$.zip')
		} else {
			return nil, false, false, nil
		}
		var ok bool
		virColVals, ok = jsonArrayExpr2Exprs(
			sctx.GetExprCtx(),
			ast.JSONOverlaps,
			sf.GetArgs()[1-jsonPathIdx],
			jsonType,
			true,
		)
		if !ok || len(virColVals) == 0 { // forbid empty array for safety
			return nil, false, false, nil
		}
	default:
		return nil, false, false, nil
	}

	for _, v := range virColVals {
		if !isSafeTypeConversion4MVIndexRange(v.GetType(evalCtx), virCol.GetType(evalCtx)) {
			return nil, false, false, nil
		}
	}

	for _, v := range virColVals {
		// rewrite json functions to EQ to calculate range, `(1 member of j)` -> `j=1`.
		eq, err := expression.NewFunction(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), virCol, v)
		if err != nil {
			return nil, false, false, err
		}
		newAccessFilters := make([]expression.Expression, len(accessFilters))
		copy(newAccessFilters, accessFilters)
		newAccessFilters[virColID] = eq

		partialPath, ok, err := buildPartialPath4MVIndex(sctx, newAccessFilters, idxCols, mvIndex, histColl)
		if !ok || err != nil {
			return nil, false, ok, err
		}
		partialPaths = append(partialPaths, partialPath)
	}
	return partialPaths, isIntersection, true, nil
}

// isSafeTypeConversion4MVIndexRange checks whether it is safe to convert valType to mvIndexType when building ranges for MVIndexes.
func isSafeTypeConversion4MVIndexRange(valType, mvIndexType *types.FieldType) (safe bool) {
	// for safety, forbid type conversion when building ranges for MVIndexes.
	// TODO: loose this restriction.
	// for example, converting '1' to 1 to access INT MVIndex may cause some wrong result.
	return valType.EvalType() == mvIndexType.EvalType()
}

// buildPartialPath4MVIndex builds a partial path on this MVIndex with these accessFilters.
func buildPartialPath4MVIndex(
	sctx context.PlanContext,
	accessFilters []expression.Expression,
	idxCols []*expression.Column,
	mvIndex *model.IndexInfo,
	histColl *statistics.HistColl,
) (*util.AccessPath, bool, error) {
	partialPath := &util.AccessPath{Index: mvIndex}
	partialPath.Ranges = ranger.FullRange()
	for i := 0; i < len(idxCols); i++ {
		partialPath.IdxCols = append(partialPath.IdxCols, idxCols[i])
		partialPath.IdxColLens = append(partialPath.IdxColLens, mvIndex.Columns[i].Length)
		partialPath.FullIdxCols = append(partialPath.FullIdxCols, idxCols[i])
		partialPath.FullIdxColLens = append(partialPath.FullIdxColLens, mvIndex.Columns[i].Length)
	}
	if err := detachCondAndBuildRangeForPath(sctx, partialPath, accessFilters, histColl); err != nil {
		return nil, false, err
	}
	if len(partialPath.AccessConds) != len(accessFilters) || len(partialPath.TableFilters) > 0 {
		// not all filters are used in this case.
		return nil, false, nil
	}
	return partialPath, true, nil
}

// PrepareIdxColsAndUnwrapArrayType collects columns for an index and returns them as []*expression.Column.
// If any column of them is an array type, we will use it's underlying FieldType in the returned Column.RetType.
// If checkOnly1ArrayTypeCol is true, we will check if this index contains only one array type column. If not, it will
// return (nil, false). This check works as a sanity check for an MV index.
// Though this function is introduced for MV index, it can also be used for normal index if you pass false to
// checkOnly1ArrayTypeCol.
// This function is exported for test.
func PrepareIdxColsAndUnwrapArrayType(
	tableInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	tblCols []*expression.Column,
	checkOnly1ArrayTypeCol bool,
) (idxCols []*expression.Column, ok bool) {
	var virColNum = 0
	for i := range idxInfo.Columns {
		colOffset := idxInfo.Columns[i].Offset
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
		if col.GetStaticType().IsArray() {
			virColNum++
			col = col.Clone().(*expression.Column)
			col.RetType = col.GetStaticType().ArrayType() // use the underlying type directly: JSON-ARRAY(INT) --> INT
			col.RetType.SetCharset(charset.CharsetBin)
			col.RetType.SetCollate(charset.CollationBin)
		}
		idxCols = append(idxCols, col)
	}
	if checkOnly1ArrayTypeCol && virColNum != 1 { // assume only one vir-col in the MVIndex
		return nil, false
	}
	return idxCols, true
}

// collectFilters4MVIndex splits these filters into 2 parts where accessFilters can be used to access this index directly.
// For idx(x, cast(a as array), z), `x=1 and (2 member of a) and z=1 and x+z>0` is split to:
// accessFilters: `x=1 and (2 member of a) and z=1`, remaining: `x+z>0`.
func collectFilters4MVIndex(
	sctx context.PlanContext,
	filters []expression.Expression,
	idxCols []*expression.Column,
) (accessFilters, remainingFilters []expression.Expression, accessTp int) {
	accessTp = unspecifiedFilterTp
	usedAsAccess := make([]bool, len(filters))
	for _, col := range idxCols {
		found := false
		for i, f := range filters {
			if usedAsAccess[i] {
				continue
			}
			if ok, tp := checkAccessFilter4IdxCol(sctx, f, col); ok {
				accessFilters = append(accessFilters, f)
				usedAsAccess[i] = true
				found = true
				// access filter type on mv col overrides normal col for the return value of this function
				if accessTp == unspecifiedFilterTp || accessTp == eqOnNonMVColTp {
					accessTp = tp
				}
				break
			}
		}
		if !found {
			break
		}
	}
	for i := range usedAsAccess {
		if !usedAsAccess[i] {
			remainingFilters = append(remainingFilters, filters[i])
		}
	}
	return accessFilters, remainingFilters, accessTp
}

// CollectFilters4MVIndexMutations exported for unit test.
// For idx(x, cast(a as array), z), `x=1 and (2 member of a) and (1 member of a) and z=1 and x+z>0` is split to:
// accessFilters combination:
// 1: `x=1 and (2 member of a) and z=1`, remaining: `x+z>0`.
// 2: `x=1 and (1 member of a) and z=1`, remaining: `x+z>0`.
//
// Q: case like idx(x, cast(a as array), z), condition like: x=1 and x=2 and ( 2 member of a)? we can derive the x is invalid range?
// A: no way to here, it will derive an empty range in table path by all these conditions, and the heuristic rule will pick the table-dual table path directly.
//
// Theoretically For idx(x, cast(a as array), z), `x=1 and x=2 and (2 member of a) and (1 member of a) and z=1 and x+z>0` here should be split to:
// 1: `x=1 and x=2 and (2 member of a) and z=1`, remaining: `x+z>0`.
// 2: `x=1 and x=2 and (1 member of a) and z=1`, remaining: `x+z>0`.
// Note: x=1 and x=2 will derive an invalid range in ranger detach, for now because of heuristic rule above, we ignore this case here.
//
// just as the 3rd point as we said in generateIndexMerge4ComposedIndex
//
// 3: The predicate of mv index can not converge to a linear interval range at physical phase like EQ and
// GT in normal index. Among the predicates in mv index (member-of/contains/overlap), multi conditions
// about them should be built as self-independent index path, deriving the final intersection/union handles,
// which means a mv index path may be reused for multi related conditions. Here means whether (2 member of a)
// And (1 member of a) is valid composed range or empty range can't be told until runtime intersection/union.
//
// therefore, for multi condition about a single mv index virtual json col here: (2 member of a) and (1 member of a)
// we should build indexMerge above them, and each of them can access to the same mv index. That's why
// we should derive the mutations of virtual json col's access condition, output the accessFilter combination
// for each mutation of it.
//
// In the first case:
// the inputs will be:
//
//	filters:[x=1, (2 member of a), (1 member of a), z=1, x+z>0], idxCols: [x,a,z]
//
// the output will be:
//
//	accessFilters: [x=1, (2 member of a), z=1], remainingFilters: [x+z>0], mvColOffset: 1, mvFilterMutations[(2 member of a), (1 member of a)]
//
// the outer usage will be: accessFilter[mvColOffset] = each element of mvFilterMutations to get the mv access filters mutation combination.
func CollectFilters4MVIndexMutations(sctx base.PlanContext, filters []expression.Expression,
	idxCols []*expression.Column) (accessFilters, remainingFilters []expression.Expression, mvColOffset int, mvFilterMutations []expression.Expression) {
	usedAsAccess := make([]bool, len(filters))
	// accessFilters [x, a<json>, z]
	//                    |
	//                    +----> it may have several substitutions in mvFilterMutations if it's json col.
	mvFilterMutations = make([]expression.Expression, 0, 1)
	mvColOffset = -1
	for z, col := range idxCols {
		found := false
		for i, f := range filters {
			if usedAsAccess[i] {
				continue
			}
			if ok, _ := checkAccessFilter4IdxCol(sctx, f, col); ok {
				if col.VirtualExpr != nil && col.VirtualExpr.GetType(sctx.GetExprCtx().GetEvalCtx()).IsArray() {
					// assert jsonColOffset should always be the same.
					// if the filter is from virtual expression, it means it is about the mv json col.
					mvFilterMutations = append(mvFilterMutations, f)
					if mvColOffset == -1 {
						// means first encountering, recording offset pos, and append it as occupation of access filter.
						mvColOffset = z
						accessFilters = append(accessFilters, f)
					}
					// additional encountering, just map it as used access.
					usedAsAccess[i] = true
					found = true
					continue
				}
				accessFilters = append(accessFilters, f)
				usedAsAccess[i] = true
				found = true
				// shouldn't break once found here, because we want to collect all the mutation mv filters here.
			}
		}
		if !found {
			break
		}
	}
	for i := range usedAsAccess {
		if !usedAsAccess[i] {
			remainingFilters = append(remainingFilters, filters[i])
		}
	}
	return accessFilters, remainingFilters, mvColOffset, mvFilterMutations
}

// cleanAccessPathForMVIndexHint removes all other access path if there is a multi-valued index hint, and this hint
// has a valid path
func (ds *DataSource) cleanAccessPathForMVIndexHint() {
	forcedMultiValuedIndex := make(map[int64]struct{}, len(ds.possibleAccessPaths))
	for _, p := range ds.possibleAccessPaths {
		if !isMVIndexPath(p) || !p.Forced {
			continue
		}
		forcedMultiValuedIndex[p.Index.ID] = struct{}{}
	}
	// no multi-valued index specified, just return
	if len(forcedMultiValuedIndex) == 0 {
		return
	}

	validMVIndexPath := make([]*util.AccessPath, 0, len(ds.possibleAccessPaths))
	for _, p := range ds.possibleAccessPaths {
		if indexMergeContainSpecificIndex(p, forcedMultiValuedIndex) {
			validMVIndexPath = append(validMVIndexPath, p)
		}
	}
	if len(validMVIndexPath) > 0 {
		ds.possibleAccessPaths = validMVIndexPath
	}
}

// indexMergeContainSpecificIndex checks whether the index merge path contains at least one index in the `indexSet`
func indexMergeContainSpecificIndex(path *util.AccessPath, indexSet map[int64]struct{}) bool {
	if path.PartialIndexPaths == nil {
		return false
	}
	for _, p := range path.PartialIndexPaths {
		// NOTE: currently, an index merge access path can only be "a single layer", it's impossible to meet this
		// condition. These codes are just left here for future change.
		if len(p.PartialIndexPaths) > 0 {
			contain := indexMergeContainSpecificIndex(p, indexSet)
			if contain {
				return true
			}
		}

		if p.Index != nil {
			if _, ok := indexSet[p.Index.ID]; ok {
				return true
			}
		}
	}

	return false
}

const (
	unspecifiedFilterTp int = iota
	eqOnNonMVColTp
	multiValuesOROnMVColTp
	multiValuesANDOnMVColTp
	singleValueOnMVColTp
)

// checkAccessFilter4IdxCol checks whether this filter can be used as an accessFilter to access the column of an index,
// and returns which type the access filter is, as defined above.
// Though this function is introduced for MV index, it can also be used for normal index
// If the return value ok is false, the type must be unspecifiedFilterTp.
func checkAccessFilter4IdxCol(
	sctx base.PlanContext,
	filter expression.Expression,
	idxCol *expression.Column,
) (
	ok bool,
	accessFilterTp int,
) {
	sf, ok := filter.(*expression.ScalarFunction)
	if !ok {
		return false, unspecifiedFilterTp
	}
	if idxCol.VirtualExpr != nil { // the virtual column on the MVIndex
		targetJSONPath, ok := unwrapJSONCast(idxCol.VirtualExpr)
		if !ok {
			return false, unspecifiedFilterTp
		}
		var virColVals []expression.Expression
		jsonType := idxCol.GetStaticType().ArrayType()
		var tp int
		switch sf.FuncName.L {
		case ast.JSONMemberOf: // (1 member of a)
			if !targetJSONPath.Equal(sctx.GetExprCtx().GetEvalCtx(), sf.GetArgs()[1]) {
				return false, unspecifiedFilterTp
			}
			v, ok := unwrapJSONCast(sf.GetArgs()[0]) // cast(1 as json) --> 1
			if !ok {
				return false, unspecifiedFilterTp
			}
			virColVals = append(virColVals, v)
			tp = singleValueOnMVColTp
		case ast.JSONContains: // json_contains(a, '1')
			if !targetJSONPath.Equal(sctx.GetExprCtx().GetEvalCtx(), sf.GetArgs()[0]) {
				return false, unspecifiedFilterTp
			}
			virColVals, ok = jsonArrayExpr2Exprs(
				sctx.GetExprCtx(),
				ast.JSONContains,
				sf.GetArgs()[1],
				jsonType,
				false,
			)
			if !ok || len(virColVals) == 0 {
				return false, unspecifiedFilterTp
			}
			tp = multiValuesANDOnMVColTp
		case ast.JSONOverlaps: // json_overlaps(a, '1') or json_overlaps('1', a)
			var jsonPathIdx int
			if sf.GetArgs()[0].Equal(sctx.GetExprCtx().GetEvalCtx(), targetJSONPath) {
				jsonPathIdx = 0 // (json_overlaps(a->'$.zip', '[1, 2, 3]')
			} else if sf.GetArgs()[1].Equal(sctx.GetExprCtx().GetEvalCtx(), targetJSONPath) {
				jsonPathIdx = 1 // (json_overlaps('[1, 2, 3]', a->'$.zip')
			} else {
				return false, unspecifiedFilterTp
			}
			var ok bool
			virColVals, ok = jsonArrayExpr2Exprs(
				sctx.GetExprCtx(),
				ast.JSONOverlaps,
				sf.GetArgs()[1-jsonPathIdx],
				jsonType,
				false,
			)
			if !ok || len(virColVals) == 0 { // forbid empty array for safety
				return false, unspecifiedFilterTp
			}
			tp = multiValuesOROnMVColTp
		default:
			return false, unspecifiedFilterTp
		}
		for _, v := range virColVals {
			if !isSafeTypeConversion4MVIndexRange(v.GetType(sctx.GetExprCtx().GetEvalCtx()), idxCol.GetStaticType()) {
				return false, unspecifiedFilterTp
			}
		}
		// If json_contains or json_overlaps only contains one value, like json_overlaps(a,'[1]') or
		// json_contains(a,'[1]'), we can just ignore the AND/OR semantic, and treat them like 1 member of (a).
		if (tp == multiValuesOROnMVColTp || tp == multiValuesANDOnMVColTp) && len(virColVals) == 1 {
			tp = singleValueOnMVColTp
		}
		return true, tp
	}

	// else: non virtual column
	if sf.FuncName.L != ast.EQ { // only support EQ now
		return false, unspecifiedFilterTp
	}
	args := sf.GetArgs()
	var argCol *expression.Column
	var argConst *expression.Constant
	if c, isCol := args[0].(*expression.Column); isCol {
		if con, isCon := args[1].(*expression.Constant); isCon {
			argCol, argConst = c, con
		}
	} else if c, isCol := args[1].(*expression.Column); isCol {
		if con, isCon := args[0].(*expression.Constant); isCon {
			argCol, argConst = c, con
		}
	}
	if argCol == nil || argConst == nil {
		return false, unspecifiedFilterTp
	}
	if argCol.Equal(sctx.GetExprCtx().GetEvalCtx(), idxCol) {
		return true, eqOnNonMVColTp
	}
	return false, unspecifiedFilterTp
}

// jsonArrayExpr2Exprs converts a JsonArray expression to expression list: cast('[1, 2, 3]' as JSON) --> []expr{1, 2, 3}
func jsonArrayExpr2Exprs(
	sctx expression.BuildContext,
	jsonFuncName string,
	jsonArrayExpr expression.Expression,
	targetType *types.FieldType,
	checkForSkipPlanCache bool,
) ([]expression.Expression, bool) {
	if checkForSkipPlanCache && expression.MaybeOverOptimized4PlanCache(sctx, []expression.Expression{jsonArrayExpr}) {
		// skip plan cache and try to generate the best plan in this case.
		sctx.SetSkipPlanCache(jsonFuncName + " function with immutable parameters can affect index selection")
	}
	if !expression.IsImmutableFunc(jsonArrayExpr) || jsonArrayExpr.GetType(sctx.GetEvalCtx()).EvalType() != types.ETJson {
		return nil, false
	}

	jsonArray, isNull, err := jsonArrayExpr.EvalJSON(sctx.GetEvalCtx(), chunk.Row{})
	if isNull || err != nil {
		return nil, false
	}
	if jsonArray.TypeCode != types.JSONTypeCodeArray {
		single, ok := jsonValue2Expr(jsonArray, targetType) // '1' -> []expr{1}
		if ok {
			return []expression.Expression{single}, true
		}
		return nil, false
	}
	var exprs []expression.Expression
	for i := 0; i < jsonArray.GetElemCount(); i++ { // '[1, 2, 3]' -> []expr{1, 2, 3}
		expr, ok := jsonValue2Expr(jsonArray.ArrayGetElem(i), targetType)
		if !ok {
			return nil, false
		}
		exprs = append(exprs, expr)
	}
	return exprs, true
}

func jsonValue2Expr(v types.BinaryJSON, targetType *types.FieldType) (expression.Expression, bool) {
	datum, err := expression.ConvertJSON2Tp(v, targetType)
	if err != nil {
		return nil, false
	}
	return &expression.Constant{
		Value:   types.NewDatum(datum),
		RetType: targetType,
	}, true
}

func unwrapJSONCast(expr expression.Expression) (expression.Expression, bool) {
	if expr == nil {
		return nil, false
	}
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return nil, false
	}
	if sf == nil || sf.FuncName.L != ast.Cast || sf.GetStaticType().EvalType() != types.ETJson {
		return nil, false
	}
	return sf.GetArgs()[0], true
}

func isMVIndexPath(path *util.AccessPath) bool {
	return !path.IsTablePath() && path.Index != nil && path.Index.MVIndex
}
