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
	"maps"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

// generateIndexMergePath generates IndexMerge AccessPaths on this DataSource.
func generateIndexMergePath(ds *logicalop.DataSource) error {
	var warningMsg string
	stmtCtx := ds.SCtx().GetSessionVars().StmtCtx
	defer func() {
		if len(ds.IndexMergeHints) > 0 && warningMsg != "" {
			ds.IndexMergeHints = nil
			stmtCtx.AppendWarning(errors.NewNoStackError(warningMsg))
			logutil.BgLogger().Debug(warningMsg)
		}
	}()

	// Consider the IndexMergePath. Now, we just generate `IndexMergePath` in DNF case.
	// Use AllConds instread of PushedDownConds,
	// because we want to use IndexMerge even if some expr cannot be pushed to TiKV.
	// We will create new Selection for exprs that cannot be pushed in convertToIndexMergeScan.
	indexMergeConds := make([]expression.Expression, 0, len(ds.AllConds))
	indexMergeConds = append(indexMergeConds, ds.AllConds...)
	sessionAndStmtPermission := (ds.SCtx().GetSessionVars().GetEnableIndexMerge() || len(ds.IndexMergeHints) > 0) && !stmtCtx.NoIndexMergeHint
	if !sessionAndStmtPermission {
		warningMsg = "IndexMerge is inapplicable or disabled. Got no_index_merge hint or tidb_enable_index_merge is off."
		return nil
	}

	if ds.TableInfo.TempTableType == model.TempTableLocal {
		warningMsg = "IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table."
		return nil
	}

	regularPathCount := len(ds.PossibleAccessPaths)

	// Now we have 3 entry functions to generate IndexMerge paths:
	// 1. Generate AND type IndexMerge for non-MV indexes and all OR type IndexMerge.
	var err error
	if warningMsg, err = generateOtherIndexMerge(ds, regularPathCount, indexMergeConds); err != nil {
		return err
	}
	// 2. Generate AND type IndexMerge for MV indexes. Tt can only use one index in an IndexMerge path.
	if err := generateANDIndexMerge4MVIndex(ds, regularPathCount, indexMergeConds); err != nil {
		return err
	}
	oldIndexMergeCount := len(ds.PossibleAccessPaths)
	// 3. Generate AND type IndexMerge for MV indexes. It can use multiple MV and non-MV indexes in an IndexMerge path.
	if err := generateANDIndexMerge4ComposedIndex(ds, regularPathCount, indexMergeConds); err != nil {
		return err
	}

	// Because index merge access paths are built from ds.AllConds, sometimes they can help us consider more filters than
	// the ds.stats, which is calculated from ds.PushedDownConds before this point.
	// So we use a simple and naive method to update ds.stats here using the largest row count from index merge paths.
	// This can help to avoid some cases where the row count of operator above IndexMerge is larger than IndexMerge.
	// TODO: Probably we should directly consider ds.AllConds when calculating ds.stats in the future.
	if len(ds.PossibleAccessPaths) > regularPathCount && len(ds.AllConds) > len(ds.PushedDownConds) {
		var maxRowCount float64
		for i := regularPathCount; i < len(ds.PossibleAccessPaths); i++ {
			maxRowCount = max(maxRowCount, ds.PossibleAccessPaths[i].CountAfterAccess)
		}
		if ds.StatsInfo().RowCount > maxRowCount {
			ds.SetStats(ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), maxRowCount))
		}
	}

	// If without hints, it means that `enableIndexMerge` is true
	if len(ds.IndexMergeHints) != 0 {
		// If len(IndexMergeHints) > 0, then add warnings if index-merge hints cannot work.
		if regularPathCount == len(ds.PossibleAccessPaths) {
			if warningMsg == "" {
				warningMsg = "IndexMerge is inapplicable"
			}
			return nil
		}

		// If len(IndexMergeHints) > 0 and some index-merge paths were added, then prune all other non-index-merge paths.
		// if len(ds.PossibleAccessPaths) > oldIndexMergeCount, it means composed index merge path is generated, prune others.
		if len(ds.PossibleAccessPaths) > oldIndexMergeCount {
			ds.PossibleAccessPaths = ds.PossibleAccessPaths[oldIndexMergeCount:]
		} else {
			ds.PossibleAccessPaths = ds.PossibleAccessPaths[regularPathCount:]
		}
	}

	// If there is a multi-valued index hint, remove all paths which don't use the specified index.
	cleanAccessPathForMVIndexHint(ds)

	return nil
}

func generateNormalIndexPartialPath(
	ds *logicalop.DataSource,
	item expression.Expression,
	candidatePath *util.AccessPath,
) (paths *util.AccessPath, needSelection bool) {
	// Reject partial index first.
	if candidatePath.Index != nil && candidatePath.Index.HasCondition() {
		return nil, false
	}
	pushDownCtx := util.GetPushDownCtx(ds.SCtx())
	cnfItems := expression.SplitCNFItems(item)
	pushedDownCNFItems := make([]expression.Expression, 0, len(cnfItems))
	for _, cnfItem := range cnfItems {
		if expression.CanExprsPushDown(pushDownCtx, []expression.Expression{cnfItem}, kv.TiKV) {
			pushedDownCNFItems = append(pushedDownCNFItems, cnfItem)
		} else {
			needSelection = true
		}
	}
	partialPath := accessPathsForConds(ds, pushedDownCNFItems, candidatePath)
	if partialPath == nil {
		// for this dnf item, we couldn't generate an index merge partial path.
		// (1 member of (a)) or (3 member of (b)) or d=1; if one dnf item like d=1 here could walk index path,
		// the entire index merge is not valid anymore.
		return nil, false
	}

	return partialPath, needSelection
}

// isInIndexMergeHints returns true if the input index name is not excluded by the IndexMerge hints, which means either
// (1) there's no IndexMerge hint, (2) there's IndexMerge hint but no specified index names, or (3) the input index
// name is specified in the IndexMerge hints.
func isInIndexMergeHints(ds *logicalop.DataSource, name string) bool {
	// if no index merge hints, all mv index is accessible
	if len(ds.IndexMergeHints) == 0 {
		return true
	}
	for _, hint := range ds.IndexMergeHints {
		if hint.IndexHint == nil || len(hint.IndexHint.IndexNames) == 0 {
			return true
		}
		for _, hintName := range hint.IndexHint.IndexNames {
			if strings.EqualFold(name, hintName.String()) {
				return true
			}
		}
	}
	return false
}

// indexMergeHintsHasSpecifiedIdx returns true if there's IndexMerge hint, and it has specified index names.
func indexMergeHintsHasSpecifiedIdx(ds *logicalop.DataSource) bool {
	for _, hint := range ds.IndexMergeHints {
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
func isSpecifiedInIndexMergeHints(ds *logicalop.DataSource, name string) bool {
	for _, hint := range ds.IndexMergeHints {
		if hint.IndexHint == nil || len(hint.IndexHint.IndexNames) == 0 {
			continue
		}
		for _, hintName := range hint.IndexHint.IndexNames {
			if strings.EqualFold(name, hintName.String()) {
				return true
			}
		}
	}
	return false
}

// accessPathsForConds generates an AccessPath for given candidate access path and filters.
func accessPathsForConds(
	ds *logicalop.DataSource,
	conditions []expression.Expression,
	path *util.AccessPath,
) *util.AccessPath {
	newPath := &util.AccessPath{}
	if path.IsTablePath() {
		if !isInIndexMergeHints(ds, "primary") {
			return nil
		}
		if ds.TableInfo.IsCommonHandle {
			newPath.IsCommonHandlePath = true
			newPath.Index = path.Index
			newPath.NoncacheableReason = path.NoncacheableReason
		} else {
			newPath.IsIntHandlePath = true
		}
		err := deriveTablePathStats(ds, newPath, conditions, true)
		if err != nil {
			logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
			return nil
		}
		var unsignedIntHandle bool
		if newPath.IsIntHandlePath && ds.TableInfo.PKIsHandle {
			if pkColInfo := ds.TableInfo.GetPkColInfo(); pkColInfo != nil {
				unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			}
		}
		// If the newPath contains a full range, ignore it.
		if ranger.HasFullRange(newPath.Ranges, unsignedIntHandle) {
			return nil
		}
	} else {
		newPath.Index = path.Index
		newPath.NoncacheableReason = path.NoncacheableReason
		if !isInIndexMergeHints(ds, newPath.Index.Name.L) {
			return nil
		}
		err := fillIndexPath(ds, newPath, conditions)
		if err != nil {
			logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
			return nil
		}
		deriveIndexPathStats(ds, newPath, conditions, true)
		// If the newPath contains a full range, ignore it.
		if ranger.HasFullRange(newPath.Ranges, false) {
			return nil
		}
	}
	return newPath
}

func generateNormalIndexPartialPath4And(ds *logicalop.DataSource, normalPathCnt int, usedAccessMap map[string]expression.Expression) []*util.AccessPath {
	if res := generateANDIndexMerge4NormalIndex(ds, normalPathCnt, usedAccessMap); res != nil {
		return res.PartialIndexPaths
	}
	return nil
}

// generateANDIndexMerge4NormalIndex generates IndexMerge paths for `AND` (a.k.a. intersection type IndexMerge)
func generateANDIndexMerge4NormalIndex(ds *logicalop.DataSource, normalPathCnt int, usedAccessMap map[string]expression.Expression) *util.AccessPath {
	// For now, we only consider intersection type IndexMerge when the index names are specified in the hints.
	if !indexMergeHintsHasSpecifiedIdx(ds) {
		return nil
	}
	composedWithMvIndex := len(usedAccessMap) != 0

	// 1. Collect partial paths from normal paths.
	partialPaths := make([]*util.AccessPath, 0, normalPathCnt)
	for i := range normalPathCnt {
		originalPath := ds.PossibleAccessPaths[i]
		// No need to consider table path as a partial path.
		if ds.PossibleAccessPaths[i].IsTablePath() {
			continue
		}
		// since this code path is only for normal index, skip mv index here.
		if ds.PossibleAccessPaths[i].Index.MVIndex {
			continue
		}
		if !isSpecifiedInIndexMergeHints(ds, originalPath.Index.Name.L) {
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
	pushDownCtx := util.GetPushDownCtx(ds.SCtx())
	for _, path := range partialPaths {
		// Classify filters into coveredConds and notCoveredConds.
		coveredConds := make([]expression.Expression, 0, len(path.AccessConds)+len(path.IndexFilters))
		notCoveredConds := make([]expression.Expression, 0, len(path.IndexFilters)+len(path.TableFilters))
		// AccessConds can be covered by partial path.
		coveredConds = append(coveredConds, path.AccessConds...)
		path.IndexFilters = slices.DeleteFunc(path.IndexFilters, func(cond expression.Expression) bool {
			// IndexFilters can be covered by partial path if it can be pushed down to TiKV.
			if !expression.CanExprsPushDown(pushDownCtx, []expression.Expression{cond}, kv.TiKV) {
				notCoveredConds = append(notCoveredConds, cond)
				return true
			}
			coveredConds = append(coveredConds, cond)
			return false
		})
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
	if expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), partialFilters...) {
		dedupedFinalFilters = append(dedupedFinalFilters, partialFilters...)
	}

	// 3. Estimate the row count after partial paths.
	sel, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, partialFilters, nil)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		sel = cost.SelectionFactor
	}

	indexMergePath := &util.AccessPath{
		PartialIndexPaths:        partialPaths,
		IndexMergeIsIntersection: true,
		TableFilters:             dedupedFinalFilters,
		CountAfterAccess:         sel * ds.TableStats.RowCount,
	}
	return indexMergePath
}

// generateMVIndexMergePartialPaths4And try to find mv index merge partial path from a collection of cnf conditions.
func generateMVIndexMergePartialPaths4And(ds *logicalop.DataSource, normalPathCnt int, indexMergeConds []expression.Expression, histColl *statistics.HistColl) ([]*util.AccessPath, map[string]expression.Expression, error) {
	// step1: collect all mv index paths
	possibleMVIndexPaths := make([]*util.AccessPath, 0, len(ds.PossibleAccessPaths))
	for idx := range normalPathCnt {
		if !isMVIndexPath(ds.PossibleAccessPaths[idx]) {
			continue // not a MVIndex path
		}
		if !isInIndexMergeHints(ds, ds.PossibleAccessPaths[idx].Index.Name.L) {
			continue
		}
		possibleMVIndexPaths = append(possibleMVIndexPaths, ds.PossibleAccessPaths[idx])
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
	for idx := range possibleMVIndexPaths {
		idxCols, ok := PrepareIdxColsAndUnwrapArrayType(
			ds.Table.Meta(),
			possibleMVIndexPaths[idx].Index,
			ds.TblColsByID,
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

			partialPaths, isIntersection, ok, err := buildPartialPaths4MVIndexWithPath(ds.SCtx(), accessFilters, idxCols, possibleMVIndexPaths[idx], ds.TableStats.HistColl)
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
	recordsCollection := slices.Collect(maps.Values(mm))
	// according origin offset to stable the partial paths order. (golang map is not order stable)
	slices.SortFunc(recordsCollection, func(a, b *record) int {
		return cmp.Compare(a.originOffset, b.originOffset)
	})
	for _, one := range recordsCollection {
		mvAndPartialPath = append(mvAndPartialPath, one.paths...)
	}
	return mvAndPartialPath, usedAccessCondsMap, nil
}

