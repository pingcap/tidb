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
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

// generateIndexMergePath generates IndexMerge AccessPaths on this DataSource.
func (ds *DataSource) generateIndexMergePath() error {
	var warningMsg string
	stmtCtx := ds.ctx.GetSessionVars().StmtCtx
	defer func() {
		if len(ds.indexMergeHints) > 0 && warningMsg != "" {
			ds.indexMergeHints = nil
			stmtCtx.AppendWarning(errors.Errorf(warningMsg))
			logutil.BgLogger().Debug(warningMsg)
		}
	}()

	// Consider the IndexMergePath. Now, we just generate `IndexMergePath` in DNF case.
	// Use allConds instread of pushedDownConds,
	// because we want to use IndexMerge even if some expr cannot be pushed to TiKV.
	// We will create new Selection for exprs that cannot be pushed in convertToIndexMergeScan.
	indexMergeConds := make([]expression.Expression, 0, len(ds.allConds))
	for _, expr := range ds.allConds {
		indexMergeConds = append(indexMergeConds, expression.PushDownNot(ds.ctx, expr))
	}

	sessionAndStmtPermission := (ds.ctx.GetSessionVars().GetEnableIndexMerge() || len(ds.indexMergeHints) > 0) && !stmtCtx.NoIndexMergeHint
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

	// If without hints, it means that `enableIndexMerge` is true
	if len(ds.indexMergeHints) == 0 {
		return nil
	}
	// If len(indexMergeHints) > 0, then add warnings if index-merge hints cannot work.
	if regularPathCount == len(ds.possibleAccessPaths) {
		if warningMsg == "" {
			warningMsg = "IndexMerge is inapplicable"
		}
		return nil
	}

	// If len(indexMergeHints) > 0 and some index-merge paths were added, then prune all other non-index-merge paths.
	ds.possibleAccessPaths = ds.possibleAccessPaths[regularPathCount:]
	minRowCount := ds.possibleAccessPaths[0].CountAfterAccess
	for _, path := range ds.possibleAccessPaths {
		if minRowCount < path.CountAfterAccess {
			minRowCount = path.CountAfterAccess
		}
	}
	if ds.stats.RowCount > minRowCount {
		ds.stats = ds.tableStats.ScaleByExpectCnt(minRowCount)
	}
	return nil
}

// getIndexMergeOrPath generates all possible IndexMergeOrPaths.
func (ds *DataSource) generateIndexMergeOrPaths(filters []expression.Expression) error {
	usedIndexCount := len(ds.possibleAccessPaths)
	for i, cond := range filters {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.LogicOr {
			continue
		}
		// shouldKeepCurrentFilter means the partial paths can't cover the current filter completely, so we must add
		// the current filter into a Selection after partial paths.
		shouldKeepCurrentFilter := false
		var partialPaths = make([]*util.AccessPath, 0, usedIndexCount)
		dnfItems := expression.FlattenDNFConditions(sf)
		for _, item := range dnfItems {
			cnfItems := expression.SplitCNFItems(item)

			pushedDownCNFItems := make([]expression.Expression, 0, len(cnfItems))
			for _, cnfItem := range cnfItems {
				if expression.CanExprsPushDown(ds.ctx.GetSessionVars().StmtCtx,
					[]expression.Expression{cnfItem},
					ds.ctx.GetClient(),
					kv.TiKV,
				) {
					pushedDownCNFItems = append(pushedDownCNFItems, cnfItem)
				} else {
					shouldKeepCurrentFilter = true
				}
			}

			itemPaths := ds.accessPathsForConds(pushedDownCNFItems, usedIndexCount)
			if len(itemPaths) == 0 {
				partialPaths = nil
				break
			}
			partialPath, err := ds.buildIndexMergePartialPath(itemPaths)
			if err != nil {
				return err
			}
			if partialPath == nil {
				partialPaths = nil
				break
			}
			partialPaths = append(partialPaths, partialPath)
		}
		// If all of the partialPaths use the same index, we will not use the indexMerge.
		singlePath := true
		for i := len(partialPaths) - 1; i >= 1; i-- {
			if partialPaths[i].Index != partialPaths[i-1].Index {
				singlePath = false
				break
			}
		}
		if singlePath {
			continue
		}
		if len(partialPaths) > 1 {
			possiblePath := ds.buildIndexMergeOrPath(filters, partialPaths, i, shouldKeepCurrentFilter)
			if possiblePath == nil {
				return nil
			}

			accessConds := make([]expression.Expression, 0, len(partialPaths))
			for _, p := range partialPaths {
				indexCondsForP := p.AccessConds[:]
				indexCondsForP = append(indexCondsForP, p.IndexFilters...)
				if len(indexCondsForP) > 0 {
					accessConds = append(accessConds, expression.ComposeCNFCondition(ds.ctx, indexCondsForP...))
				}
			}
			accessDNF := expression.ComposeDNFCondition(ds.ctx, accessConds...)
			sel, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, []expression.Expression{accessDNF}, nil)
			if err != nil {
				logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
				sel = SelectionFactor
			}
			possiblePath.CountAfterAccess = sel * ds.tableStats.RowCount
			ds.possibleAccessPaths = append(ds.possibleAccessPaths, possiblePath)
		}
	}
	return nil
}

// isInIndexMergeHints returns true if the input index name is not excluded by the IndexMerge hints, which means either
// (1) there's no IndexMerge hint, (2) there's IndexMerge hint but no specified index names, or (3) the input index
// name is specified in the IndexMerge hints.
func (ds *DataSource) isInIndexMergeHints(name string) bool {
	if len(ds.indexMergeHints) == 0 {
		return true
	}
	for _, hint := range ds.indexMergeHints {
		if hint.indexHint == nil || len(hint.indexHint.IndexNames) == 0 {
			return true
		}
		for _, hintName := range hint.indexHint.IndexNames {
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
		if hint.indexHint == nil || len(hint.indexHint.IndexNames) == 0 {
			continue
		}
		if len(hint.indexHint.IndexNames) > 0 {
			return true
		}
	}
	return false
}

// indexMergeHintsHasSpecifiedIdx return true if the input index name is specified in the IndexMerge hint.
func (ds *DataSource) isSpecifiedInIndexMergeHints(name string) bool {
	for _, hint := range ds.indexMergeHints {
		if hint.indexHint == nil || len(hint.indexHint.IndexNames) == 0 {
			continue
		}
		for _, hintName := range hint.indexHint.IndexNames {
			if strings.EqualFold(strings.ToLower(name), strings.ToLower(hintName.String())) {
				return true
			}
		}
	}
	return false
}

// accessPathsForConds generates all possible index paths for conditions.
func (ds *DataSource) accessPathsForConds(conditions []expression.Expression, usedIndexCount int) []*util.AccessPath {
	var results = make([]*util.AccessPath, 0, usedIndexCount)
	for i := 0; i < usedIndexCount; i++ {
		path := &util.AccessPath{}
		if ds.possibleAccessPaths[i].IsTablePath() {
			if !ds.isInIndexMergeHints("primary") {
				continue
			}
			if ds.tableInfo.IsCommonHandle {
				path.IsCommonHandlePath = true
				path.Index = ds.possibleAccessPaths[i].Index
			} else {
				path.IsIntHandlePath = true
			}
			err := ds.deriveTablePathStats(path, conditions, true)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			var unsignedIntHandle bool
			if path.IsIntHandlePath && ds.tableInfo.PKIsHandle {
				if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
					unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
				}
			}
			// If the path contains a full range, ignore it.
			if ranger.HasFullRange(path.Ranges, unsignedIntHandle) {
				continue
			}
			// If we have point or empty range, just remove other possible paths.
			if len(path.Ranges) == 0 || path.OnlyPointRange(ds.SCtx()) {
				if len(results) == 0 {
					results = append(results, path)
				} else {
					results[0] = path
					results = results[:1]
				}
				break
			}
		} else {
			path.Index = ds.possibleAccessPaths[i].Index
			if !ds.isInIndexMergeHints(path.Index.Name.L) {
				continue
			}
			err := ds.fillIndexPath(path, conditions)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			ds.deriveIndexPathStats(path, conditions, true)
			// If the path contains a full range, ignore it.
			if ranger.HasFullRange(path.Ranges, false) {
				continue
			}
			// If we have empty range, or point range on unique index, just remove other possible paths.
			if len(path.Ranges) == 0 || (path.OnlyPointRange(ds.SCtx()) && path.Index.Unique) {
				if len(results) == 0 {
					results = append(results, path)
				} else {
					results[0] = path
					results = results[:1]
				}
				break
			}
		}
		results = append(results, path)
	}
	return results
}

// buildIndexMergePartialPath chooses the best index path from all possible paths.
// Now we choose the index with minimal estimate row count.
func (ds *DataSource) buildIndexMergePartialPath(indexAccessPaths []*util.AccessPath) (*util.AccessPath, error) {
	if len(indexAccessPaths) == 1 {
		return indexAccessPaths[0], nil
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
	return indexAccessPaths[minEstRowIndex], nil
}

// buildIndexMergeOrPath generates one possible IndexMergePath.
func (ds *DataSource) buildIndexMergeOrPath(
	filters []expression.Expression,
	partialPaths []*util.AccessPath,
	current int,
	shouldKeepCurrentFilter bool,
) *util.AccessPath {
	indexMergePath := &util.AccessPath{PartialIndexPaths: partialPaths}
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, filters[:current]...)
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, filters[current+1:]...)
	for _, path := range partialPaths {
		// If any partial path contains table filters, we need to keep the whole DNF filter in the Selection.
		if len(path.TableFilters) > 0 {
			shouldKeepCurrentFilter = true
		}
		// If any partial path's index filter cannot be pushed to TiKV, we should keep the whole DNF filter.
		if len(path.IndexFilters) != 0 && !expression.CanExprsPushDown(ds.ctx.GetSessionVars().StmtCtx, path.IndexFilters, ds.ctx.GetClient(), kv.TiKV) {
			shouldKeepCurrentFilter = true
			// Clear IndexFilter, the whole filter will be put in indexMergePath.TableFilters.
			path.IndexFilters = nil
		}
		if len(path.TableFilters) != 0 && !expression.CanExprsPushDown(ds.ctx.GetSessionVars().StmtCtx, path.TableFilters, ds.ctx.GetClient(), kv.TiKV) {
			shouldKeepCurrentFilter = true
			path.TableFilters = nil
		}
	}
	if shouldKeepCurrentFilter {
		indexMergePath.TableFilters = append(indexMergePath.TableFilters, filters[current])
	}
	return indexMergePath
}

// generateIndexMergeAndPaths generates IndexMerge paths for `AND` (a.k.a. intersection type IndexMerge)
func (ds *DataSource) generateIndexMergeAndPaths(normalPathCnt int) *util.AccessPath {
	// For now, we only consider intersection type IndexMerge when the index names are specified in the hints.
	if !ds.indexMergeHintsHasSpecifiedIdx() {
		return nil
	}

	// 1. Collect partial paths from normal paths.
	var partialPaths []*util.AccessPath
	for i := 0; i < normalPathCnt; i++ {
		originalPath := ds.possibleAccessPaths[i]
		// No need to consider table path as a partial path.
		if ds.possibleAccessPaths[i].IsTablePath() {
			continue
		}
		if !ds.isSpecifiedInIndexMergeHints(originalPath.Index.Name.L) {
			continue
		}
		// If the path contains a full range, ignore it.
		if ranger.HasFullRange(originalPath.Ranges, false) {
			continue
		}
		newPath := originalPath.Clone()
		partialPaths = append(partialPaths, newPath)
	}
	if len(partialPaths) < 2 {
		return nil
	}

	// 2. Collect filters that can't be covered by the partial paths and deduplicate them.
	finalFilters := make([]expression.Expression, 0)
	partialFilters := make([]expression.Expression, 0, len(partialPaths))
	hashCodeSet := make(map[string]struct{})
	for _, path := range partialPaths {
		// Classify filters into coveredConds and notCoveredConds.
		coveredConds := make([]expression.Expression, 0, len(path.AccessConds)+len(path.IndexFilters))
		notCoveredConds := make([]expression.Expression, 0, len(path.IndexFilters)+len(path.TableFilters))
		// AccessConds can be covered by partial path.
		coveredConds = append(coveredConds, path.AccessConds...)
		for i, cond := range path.IndexFilters {
			// IndexFilters can be covered by partial path if it can be pushed down to TiKV.
			if !expression.CanExprsPushDown(ds.ctx.GetSessionVars().StmtCtx, []expression.Expression{cond}, ds.ctx.GetClient(), kv.TiKV) {
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
			hashCode := string(cond.HashCode(ds.ctx.GetSessionVars().StmtCtx))
			notCoveredHashCodeSet[hashCode] = struct{}{}
		}
		for _, cond := range coveredConds {
			hashCode := string(cond.HashCode(ds.ctx.GetSessionVars().StmtCtx))
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
		hashCode := string(cond.HashCode(ds.ctx.GetSessionVars().StmtCtx))
		if _, ok := hashCodeSet[hashCode]; !ok {
			dedupedFinalFilters = append(dedupedFinalFilters, cond)
			hashCodeSet[hashCode] = struct{}{}
		}
	}

	// 3. Estimate the row count after partial paths.
	sel, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, partialFilters, nil)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		sel = SelectionFactor
	}

	indexMergePath := &util.AccessPath{
		PartialIndexPaths:        partialPaths,
		IndexMergeIsIntersection: true,
		TableFilters:             dedupedFinalFilters,
		CountAfterAccess:         sel * ds.tableStats.RowCount,
	}
	return indexMergePath
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
	stmtCtx := ds.ctx.GetSessionVars().StmtCtx
	needConsiderIndexMerge := true
	if len(ds.indexMergeHints) == 0 {
		for i := 1; i < len(ds.possibleAccessPaths); i++ {
			if len(ds.possibleAccessPaths[i].AccessConds) != 0 {
				needConsiderIndexMerge = false
				break
			}
		}
		if needConsiderIndexMerge {
			// PushDownExprs() will append extra warnings, which is annoying. So we reset warnings here.
			warnings := stmtCtx.GetWarnings()
			extraWarnings := stmtCtx.GetExtraWarnings()
			_, remaining := expression.PushDownExprs(stmtCtx, indexMergeConds, ds.ctx.GetClient(), kv.UnSpecified)
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
	indexMergeAndPath := ds.generateIndexMergeAndPaths(regularPathCount)
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

		idxCols, ok := ds.prepareCols4MVIndex(ds.possibleAccessPaths[idx].Index)
		if !ok {
			continue
		}

		for current, filter := range filters {
			sf, ok := filter.(*expression.ScalarFunction)
			if !ok || sf.FuncName.L != ast.LogicOr {
				continue
			}
			dnfFilters := expression.FlattenDNFConditions(sf) // [(1 member of (a) and b=1), (2 member of (a) and b=2)]

			// build partial paths for each dnf filter
			cannotFit := false
			var partialPaths []*util.AccessPath
			for _, dnfFilter := range dnfFilters {
				mvIndexFilters := []expression.Expression{dnfFilter}
				if sf, ok := dnfFilter.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
					mvIndexFilters = expression.FlattenCNFConditions(sf) // (1 member of (a) and b=1) --> [(1 member of (a)), b=1]
				}

				accessFilters, remainingFilters := ds.collectFilters4MVIndex(mvIndexFilters, idxCols)
				if len(accessFilters) == 0 || len(remainingFilters) > 0 { // limitation 1
					cannotFit = true
					break
				}
				paths, isIntersection, ok, err := ds.buildPartialPaths4MVIndex(accessFilters, idxCols, ds.possibleAccessPaths[idx].Index)
				if err != nil {
					return nil, err
				}
				if isIntersection || !ok { // limitation 2
					cannotFit = true
					break
				}
				partialPaths = append(partialPaths, paths...)
			}
			if cannotFit {
				continue
			}

			var remainingFilters []expression.Expression
			remainingFilters = append(remainingFilters, filters[:current]...)
			remainingFilters = append(remainingFilters, filters[current+1:]...)

			indexMergePath := ds.buildPartialPathUp4MVIndex(partialPaths, false, remainingFilters)
			mvIndexPaths = append(mvIndexPaths, indexMergePath)
		}
	}
	return
}

// generateIndexMergeJSONMVIndexPath generates paths for (json_member_of / json_overlaps / json_contains) on multi-valued index.
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

		idxCols, ok := ds.prepareCols4MVIndex(ds.possibleAccessPaths[idx].Index)
		if !ok {
			continue
		}

		accessFilters, remainingFilters := ds.collectFilters4MVIndex(filters, idxCols)
		if len(accessFilters) == 0 { // cannot use any filter on this MVIndex
			continue
		}

		partialPaths, isIntersection, ok, err := ds.buildPartialPaths4MVIndex(accessFilters, idxCols, ds.possibleAccessPaths[idx].Index)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		ds.possibleAccessPaths = append(ds.possibleAccessPaths, ds.buildPartialPathUp4MVIndex(partialPaths, isIntersection, remainingFilters))
	}
	return nil
}

// buildPartialPathUp4MVIndex builds these partial paths up to a complete index merge path.
func (ds *DataSource) buildPartialPathUp4MVIndex(partialPaths []*util.AccessPath, isIntersection bool, remainingFilters []expression.Expression) *util.AccessPath {
	indexMergePath := &util.AccessPath{PartialIndexPaths: partialPaths, IndexMergeAccessMVIndex: true}
	indexMergePath.IndexMergeIsIntersection = isIntersection
	indexMergePath.TableFilters = remainingFilters

	// TODO: use a naive estimation strategy here now for simplicity, make it more accurate.
	minEstRows, maxEstRows := math.MaxFloat64, -1.0
	for _, p := range indexMergePath.PartialIndexPaths {
		minEstRows = math.Min(minEstRows, p.CountAfterAccess)
		maxEstRows = math.Max(maxEstRows, p.CountAfterAccess)
	}
	if indexMergePath.IndexMergeIsIntersection {
		indexMergePath.CountAfterAccess = minEstRows
	} else {
		indexMergePath.CountAfterAccess = maxEstRows
	}
	return indexMergePath
}

// buildPartialPaths4MVIndex builds partial paths by using these accessFilters upon this MVIndex.
// The accessFilters must be corresponding to these idxCols.
// OK indicates whether it builds successfully. These partial paths should be ignored if ok==false.
func (ds *DataSource) buildPartialPaths4MVIndex(accessFilters []expression.Expression,
	idxCols []*expression.Column, mvIndex *model.IndexInfo) (
	partialPaths []*util.AccessPath, isIntersection bool, ok bool, err error) {
	var virColID = -1
	for i := range idxCols {
		if idxCols[i].VirtualExpr != nil {
			virColID = i
			break
		}
	}
	if virColID == -1 { // unexpected, no vir-col on this MVIndex
		return nil, false, false, nil
	}
	if len(accessFilters) <= virColID { // no filter related to the vir-col, build a partial path directly.
		partialPath, ok, err := ds.buildPartialPath4MVIndex(accessFilters, idxCols, mvIndex)
		return []*util.AccessPath{partialPath}, false, ok, err
	}

	virCol := idxCols[virColID]
	jsonType := virCol.GetType().ArrayType()
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
		virColVals, ok = jsonArrayExpr2Exprs(ds.ctx, sf.GetArgs()[1], jsonType)
		if !ok || len(virColVals) == 0 { // json_contains(JSON, '[]') is TRUE
			return nil, false, false, nil
		}
	case ast.JSONOverlaps: // (json_overlaps(a->'$.zip', '[1, 2, 3]')
		var jsonPathIdx int
		if sf.GetArgs()[0].Equal(ds.ctx, targetJSONPath) {
			jsonPathIdx = 0 // (json_overlaps(a->'$.zip', '[1, 2, 3]')
		} else if sf.GetArgs()[1].Equal(ds.ctx, targetJSONPath) {
			jsonPathIdx = 1 // (json_overlaps('[1, 2, 3]', a->'$.zip')
		} else {
			return nil, false, false, nil
		}
		var ok bool
		virColVals, ok = jsonArrayExpr2Exprs(ds.ctx, sf.GetArgs()[1-jsonPathIdx], jsonType)
		if !ok || len(virColVals) == 0 { // forbid empty array for safety
			return nil, false, false, nil
		}
	default:
		return nil, false, false, nil
	}

	for _, v := range virColVals {
		// rewrite json functions to EQ to calculate range, `(1 member of j)` -> `j=1`.
		eq, err := expression.NewFunction(ds.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), virCol, v)
		if err != nil {
			return nil, false, false, err
		}
		accessFilters[virColID] = eq

		partialPath, ok, err := ds.buildPartialPath4MVIndex(accessFilters, idxCols, mvIndex)
		if !ok || err != nil {
			return nil, false, ok, err
		}
		partialPaths = append(partialPaths, partialPath)
	}
	return partialPaths, isIntersection, true, nil
}

// buildPartialPath4MVIndex builds a partial path on this MVIndex with these accessFilters.
func (ds *DataSource) buildPartialPath4MVIndex(accessFilters []expression.Expression, idxCols []*expression.Column, mvIndex *model.IndexInfo) (*util.AccessPath, bool, error) {
	partialPath := &util.AccessPath{Index: mvIndex}
	partialPath.Ranges = ranger.FullRange()
	for i := 0; i < len(idxCols); i++ {
		partialPath.IdxCols = append(partialPath.IdxCols, idxCols[i])
		partialPath.IdxColLens = append(partialPath.IdxColLens, mvIndex.Columns[i].Length)
		partialPath.FullIdxCols = append(partialPath.FullIdxCols, idxCols[i])
		partialPath.FullIdxColLens = append(partialPath.FullIdxColLens, mvIndex.Columns[i].Length)
	}
	if err := ds.detachCondAndBuildRangeForPath(partialPath, accessFilters); err != nil {
		return nil, false, err
	}
	if len(partialPath.AccessConds) != len(accessFilters) || len(partialPath.TableFilters) > 0 {
		// not all filters are used in this case.
		return nil, false, nil
	}
	return partialPath, true, nil
}

func (ds *DataSource) prepareCols4MVIndex(mvIndex *model.IndexInfo) (idxCols []*expression.Column, ok bool) {
	var virColNum = 0
	for i := range mvIndex.Columns {
		colOffset := mvIndex.Columns[i].Offset
		colMeta := ds.table.Meta().Cols()[colOffset]
		var col *expression.Column
		for _, c := range ds.TblCols {
			if c.ID == colMeta.ID {
				col = c
				break
			}
		}
		if col == nil { // unexpected, no vir-col on this MVIndex
			return nil, false
		}
		if col.GetType().IsArray() {
			virColNum++
			col = col.Clone().(*expression.Column)
			col.RetType = col.GetType().ArrayType() // use the underlying type directly: JSON-ARRAY(INT) --> INT
			col.RetType.SetCharset(charset.CharsetBin)
			col.RetType.SetCollate(charset.CollationBin)
		}
		idxCols = append(idxCols, col)
	}
	if virColNum != 1 { // assume only one vir-col in the MVIndex
		return nil, false
	}
	return idxCols, true
}

// collectFilters4MVIndex splits these filters into 2 parts where accessFilters can be used to access this index directly.
// For idx(x, cast(a as array), z), `x=1 and (2 member of a) and z=1 and x+z>0` is splitted to:
// accessFilters: `x=1 and (2 member of a) and z=1`, remaining: `x+z>0`.
func (ds *DataSource) collectFilters4MVIndex(filters []expression.Expression, idxCols []*expression.Column) (accessFilters, remainingFilters []expression.Expression) {
	usedAsAccess := make([]bool, len(filters))
	for _, col := range idxCols {
		found := false
		for i, f := range filters {
			if usedAsAccess[i] {
				continue
			}
			if ds.checkFilter4MVIndexColumn(f, col) {
				accessFilters = append(accessFilters, f)
				usedAsAccess[i] = true
				found = true
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
	return accessFilters, remainingFilters
}

// checkFilter4MVIndexColumn checks whether this filter can be used as an accessFilter to access the MVIndex column.
func (ds *DataSource) checkFilter4MVIndexColumn(filter expression.Expression, idxCol *expression.Column) bool {
	sf, ok := filter.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if idxCol.VirtualExpr != nil { // the virtual column on the MVIndex
		targetJSONPath, ok := unwrapJSONCast(idxCol.VirtualExpr)
		if !ok {
			return false
		}
		switch sf.FuncName.L {
		case ast.JSONMemberOf: // (1 member of a)
			return targetJSONPath.Equal(ds.ctx, sf.GetArgs()[1])
		case ast.JSONContains: // json_contains(a, '1')
			return targetJSONPath.Equal(ds.ctx, sf.GetArgs()[0])
		case ast.JSONOverlaps: // json_overlaps(a, '1') or json_overlaps('1', a)
			return targetJSONPath.Equal(ds.ctx, sf.GetArgs()[0]) ||
				targetJSONPath.Equal(ds.ctx, sf.GetArgs()[1])
		default:
			return false
		}
	} else {
		if sf.FuncName.L != ast.EQ { // only support EQ now
			return false
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
			return false
		}
		if argCol.Equal(ds.ctx, idxCol) {
			return true
		}
	}
	return false
}

// jsonArrayExpr2Exprs converts a JsonArray expression to expression list: cast('[1, 2, 3]' as JSON) --> []expr{1, 2, 3}
func jsonArrayExpr2Exprs(sctx sessionctx.Context, jsonArrayExpr expression.Expression, targetType *types.FieldType) ([]expression.Expression, bool) {
	if !expression.IsInmutableExpr(jsonArrayExpr) || jsonArrayExpr.GetType().EvalType() != types.ETJson {
		return nil, false
	}

	jsonArray, isNull, err := jsonArrayExpr.EvalJSON(sctx, chunk.Row{})
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
	if sf == nil || sf.FuncName.L != ast.Cast || sf.GetType().EvalType() != types.ETJson {
		return nil, false
	}
	return sf.GetArgs()[0], true
}

func isMVIndexPath(path *util.AccessPath) bool {
	return !path.IsTablePath() && path.Index != nil && path.Index.MVIndex
}
