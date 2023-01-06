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
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
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
	// Consider the IndexMergePath. Now, we just generate `IndexMergePath` in DNF case.
	// Use allConds instread of pushedDownConds,
	// because we want to use IndexMerge even if some expr cannot be pushed to TiKV.
	// We will create new Selection for exprs that cannot be pushed in convertToIndexMergeScan.
	indexMergeConds := make([]expression.Expression, 0, len(ds.allConds))
	for _, expr := range ds.allConds {
		indexMergeConds = append(indexMergeConds, expression.PushDownNot(ds.ctx, expr))
	}

	stmtCtx := ds.ctx.GetSessionVars().StmtCtx
	isPossibleIdxMerge := len(indexMergeConds) > 0 && len(ds.possibleAccessPaths) > 1
	sessionAndStmtPermission := (ds.ctx.GetSessionVars().GetEnableIndexMerge() || len(ds.indexMergeHints) > 0) && !stmtCtx.NoIndexMergeHint
	// We current do not consider `IndexMergePath`:
	// 1. If there is an index path.
	// 2. TODO: If there exists exprs that cannot be pushed down. This is to avoid wrongly estRow of Selection added by rule_predicate_push_down.
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

			remainingExpr := 0
			for _, expr := range remaining {
				// Handle these 3 functions specially since they can be used to access MVIndex.
				if sf, ok := expr.(*expression.ScalarFunction); ok {
					if sf.FuncName.L == ast.JSONMemberOf || sf.FuncName.L == ast.JSONOverlaps ||
						sf.FuncName.L == ast.JSONContains {
						continue
					}
				}
				remainingExpr++
			}
			if remainingExpr > 0 {
				needConsiderIndexMerge = false
			}
		}
	}

	if isPossibleIdxMerge && sessionAndStmtPermission && needConsiderIndexMerge && ds.tableInfo.TempTableType != model.TempTableLocal {
		err := ds.generateAndPruneIndexMergePath(indexMergeConds, ds.indexMergeHints != nil)
		if err != nil {
			return err
		}
	} else if len(ds.indexMergeHints) > 0 {
		ds.indexMergeHints = nil
		var msg string
		if !isPossibleIdxMerge {
			msg = "No available filter or available index."
		} else if !sessionAndStmtPermission {
			msg = "Got no_index_merge hint or tidb_enable_index_merge is off."
		} else if ds.tableInfo.TempTableType == model.TempTableLocal {
			msg = "Cannot use IndexMerge on temporary table."
		}
		msg = fmt.Sprintf("IndexMerge is inapplicable or disabled. %s", msg)
		stmtCtx.AppendWarning(errors.Errorf(msg))
		logutil.BgLogger().Debug(msg)
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
		var partialPaths = make([]*util.AccessPath, 0, usedIndexCount)
		dnfItems := expression.FlattenDNFConditions(sf)
		for _, item := range dnfItems {
			cnfItems := expression.SplitCNFItems(item)
			itemPaths := ds.accessPathsForConds(cnfItems, usedIndexCount)
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
			possiblePath := ds.buildIndexMergeOrPath(filters, partialPaths, i)
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
func (ds *DataSource) buildIndexMergeOrPath(filters []expression.Expression, partialPaths []*util.AccessPath, current int) *util.AccessPath {
	indexMergePath := &util.AccessPath{PartialIndexPaths: partialPaths}
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, filters[:current]...)
	indexMergePath.TableFilters = append(indexMergePath.TableFilters, filters[current+1:]...)
	var addCurrentFilter bool
	for _, path := range partialPaths {
		// If any partial path contains table filters, we need to keep the whole DNF filter in the Selection.
		if len(path.TableFilters) > 0 {
			addCurrentFilter = true
		}
		// If any partial path's index filter cannot be pushed to TiKV, we should keep the whole DNF filter.
		if len(path.IndexFilters) != 0 && !expression.CanExprsPushDown(ds.ctx.GetSessionVars().StmtCtx, path.IndexFilters, ds.ctx.GetClient(), kv.TiKV) {
			addCurrentFilter = true
			// Clear IndexFilter, the whole filter will be put in indexMergePath.TableFilters.
			path.IndexFilters = nil
		}
		if len(path.TableFilters) != 0 && !expression.CanExprsPushDown(ds.ctx.GetSessionVars().StmtCtx, path.TableFilters, ds.ctx.GetClient(), kv.TiKV) {
			addCurrentFilter = true
			path.TableFilters = nil
		}
	}
	if addCurrentFilter {
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

func (ds *DataSource) generateAndPruneIndexMergePath(indexMergeConds []expression.Expression, needPrune bool) error {
	regularPathCount := len(ds.possibleAccessPaths)
	// 1. Generate possible IndexMerge paths for `OR`.
	err := ds.generateIndexMergeOrPaths(indexMergeConds)
	if err != nil {
		return err
	}
	// 2. Generate possible IndexMerge paths for `AND`.
	indexMergeAndPath := ds.generateIndexMergeAndPaths(regularPathCount)
	if indexMergeAndPath != nil {
		ds.possibleAccessPaths = append(ds.possibleAccessPaths, indexMergeAndPath)
	}
	// 3. Generate possible IndexMerge paths for MVIndex.
	mvIndexMergePath, err := ds.generateIndexMergeJSONMVIndexPath(regularPathCount, indexMergeConds)
	if err != nil {
		return err
	}
	if mvIndexMergePath != nil {
		ds.possibleAccessPaths = append(ds.possibleAccessPaths, mvIndexMergePath...)
	}

	// 4. If needed, append a warning if no IndexMerge is generated.

	// If without hints, it means that `enableIndexMerge` is true
	if len(ds.indexMergeHints) == 0 {
		return nil
	}
	// With hints and without generated IndexMerge paths
	if regularPathCount == len(ds.possibleAccessPaths) {
		ds.indexMergeHints = nil
		ds.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("IndexMerge is inapplicable"))
		return nil
	}

	// 4. If needPrune is true, prune non-IndexMerge paths.

	// Do not need to consider the regular paths in find_best_task().
	// So we can use index merge's row count as DataSource's row count.
	if needPrune {
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
	}
	return nil
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
func (ds *DataSource) generateIndexMergeJSONMVIndexPath(normalPathCnt int, filters []expression.Expression) (mvIndexPaths []*util.AccessPath, err error) {
	for idx := 0; idx < normalPathCnt; idx++ {
		if ds.possibleAccessPaths[idx].IsTablePath() || ds.possibleAccessPaths[idx].Index == nil || !ds.possibleAccessPaths[idx].Index.MVIndex {
			continue // not a MVIndex path
		}
		if !ds.isSpecifiedInIndexMergeHints(ds.possibleAccessPaths[idx].Index.Name.L) {
			continue // for safety, only consider using MVIndex when there is a `use_index_merge` hint now.
			// TODO: remove this limitation
		}

		// Step 1. Extract the underlying JSON column from MVIndex Info.
		mvIndex := ds.possibleAccessPaths[idx].Index
		if len(mvIndex.Columns) != 1 {
			// only support single-column MVIndex now: idx((cast(a->'$.zip' as signed array)))
			// TODO: support composite MVIndex idx((x, cast(a->'$.zip' as int array), z))
			continue
		}
		mvVirColOffset := mvIndex.Columns[0].Offset
		mvVirColMeta := ds.table.Meta().Cols()[mvVirColOffset]

		var virCol *expression.Column
		for _, ce := range ds.TblCols {
			if ce.ID == mvVirColMeta.ID {
				virCol = ce.Clone().(*expression.Column)
				virCol.RetType = ce.GetType().ArrayType() // use the underlying type directly: JSON-ARRAY(INT) --> INT
				break
			}
		}
		// unwrap the outside cast: cast(json_extract(test.t.a, $.zip), JSON) --> json_extract(test.t.a, $.zip)
		targetJSONPath, ok := unwrapJSONCast(virCol.VirtualExpr)
		if !ok {
			continue
		}

		// Step 2. Iterate all filters and generate corresponding IndexMerge paths.
		for filterIdx, filter := range filters {
			// Step 2.1. Extract jsonPath and vals from json_member / json_overlaps / json_contains functions.
			sf, ok := filter.(*expression.ScalarFunction)
			if !ok {
				continue
			}

			var jsonPath expression.Expression
			var vals []expression.Expression
			var indexMergeIsIntersection bool
			switch sf.FuncName.L {
			case ast.JSONMemberOf: // (1 member of a->'$.zip')
				jsonPath = sf.GetArgs()[1]
				v, ok := unwrapJSONCast(sf.GetArgs()[0]) // cast(1 as json) --> 1
				if !ok {
					continue
				}
				vals = append(vals, v)
			case ast.JSONContains: // (json_contains(a->'$.zip', '[1, 2, 3]')
				indexMergeIsIntersection = true
				jsonPath = sf.GetArgs()[0]
				var ok bool
				vals, ok = jsonArrayExpr2Exprs(ds.ctx, sf.GetArgs()[1])
				if !ok {
					continue
				}
			case ast.JSONOverlaps: // (json_overlaps(a->'$.zip', '[1, 2, 3]')
				var jsonPathIdx int
				if sf.GetArgs()[0].Equal(ds.ctx, targetJSONPath) {
					jsonPathIdx = 0 // (json_overlaps(a->'$.zip', '[1, 2, 3]')
				} else if sf.GetArgs()[1].Equal(ds.ctx, targetJSONPath) {
					jsonPathIdx = 1 // (json_overlaps('[1, 2, 3]', a->'$.zip')
				} else {
					continue
				}
				jsonPath = sf.GetArgs()[jsonPathIdx]
				var ok bool
				vals, ok = jsonArrayExpr2Exprs(ds.ctx, sf.GetArgs()[1-jsonPathIdx])
				if !ok {
					continue
				}
			default:
				continue
			}

			// Step 2.2. Check some limitations.
			if jsonPath == nil || len(vals) == 0 {
				continue
			}
			if !jsonPath.Equal(ds.ctx, targetJSONPath) {
				continue // not on the same JSON col
			}
			// only support INT now
			// TODO: support more types
			if jsonPath.GetType().EvalType() == types.ETInt {
				continue
			}
			allInt := true
			// TODO: support using IndexLookUp to handle single-value cases.
			for _, v := range vals {
				if v.GetType().EvalType() != types.ETInt {
					allInt = false
				}
			}
			if !allInt {
				continue
			}

			// Step 2.3. Generate a IndexMerge Path of this filter on the current MVIndex.
			var partialPaths []*util.AccessPath
			for _, v := range vals {
				partialPath := &util.AccessPath{Index: mvIndex}
				partialPath.Ranges = ranger.FullRange()
				// TODO: get the actual column length of this virtual column
				partialPath.IdxCols, partialPath.IdxColLens = []*expression.Column{virCol}, []int{types.UnspecifiedLength}
				partialPath.FullIdxCols, partialPath.FullIdxColLens = []*expression.Column{virCol}, []int{types.UnspecifiedLength}

				// calculate the path range with the condition `a->'$.zip' = 1`.
				eq, err := expression.NewFunction(ds.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), virCol, v)
				if err != nil {
					return nil, err
				}
				if err = ds.detachCondAndBuildRangeForPath(partialPath, []expression.Expression{eq}); err != nil {
					return nil, err
				}

				partialPaths = append(partialPaths, partialPath)
			}
			indexMergePath := ds.buildIndexMergeOrPath(filters, partialPaths, filterIdx)
			indexMergePath.IndexMergeIsIntersection = indexMergeIsIntersection
			mvIndexPaths = append(mvIndexPaths, indexMergePath)
		}
	}
	return
}

// jsonArrayExpr2Exprs converts a JsonArray expression to expression list: cast('[1, 2, 3]' as JSON) --> []expr{1, 2, 3}
func jsonArrayExpr2Exprs(sctx sessionctx.Context, jsonArrayExpr expression.Expression) ([]expression.Expression, bool) {
	// only support cast(const as JSON)
	arrayExpr, wrappedByJSONCast := unwrapJSONCast(jsonArrayExpr)
	if !wrappedByJSONCast {
		return nil, false
	}
	if _, isConst := arrayExpr.(*expression.Constant); !isConst {
		return nil, false
	}
	if expression.IsMutableEffectsExpr(arrayExpr) {
		return nil, false
	}

	jsonArray, isNull, err := jsonArrayExpr.EvalJSON(sctx, chunk.Row{})
	if isNull || err != nil {
		return nil, false
	}
	if jsonArray.TypeCode != types.JSONTypeCodeArray {
		single, ok := jsonValue2Expr(jsonArray) // '1' -> []expr{1}
		if ok {
			return []expression.Expression{single}, true
		}
		return nil, false
	}
	var exprs []expression.Expression
	for i := 0; i < jsonArray.GetElemCount(); i++ { // '[1, 2, 3]' -> []expr{1, 2, 3}
		expr, ok := jsonValue2Expr(jsonArray.ArrayGetElem(i))
		if !ok {
			return nil, false
		}
		exprs = append(exprs, expr)
	}
	return exprs, true
}

func jsonValue2Expr(v types.BinaryJSON) (expression.Expression, bool) {
	if v.TypeCode != types.JSONTypeCodeInt64 {
		// only support INT now
		// TODO: support more types
		return nil, false
	}
	val := v.GetInt64()
	return &expression.Constant{
		Value:   types.NewDatum(val),
		RetType: types.NewFieldType(mysql.TypeLonglong),
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
