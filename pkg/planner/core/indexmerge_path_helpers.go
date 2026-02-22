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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
)

// generateOtherIndexMerge is the entry point for generateORIndexMerge() and generateANDIndexMerge4NormalIndex(), plus
// some extra logic to keep some specific behaviors the same as before.
func generateOtherIndexMerge(ds *logicalop.DataSource, regularPathCount int, indexMergeConds []expression.Expression) (string, error) {
	isPossibleIdxMerge := len(indexMergeConds) > 0 && // have corresponding access conditions, and
		len(ds.PossibleAccessPaths) > 1 // have multiple index paths
	if !isPossibleIdxMerge {
		return "IndexMerge is inapplicable or disabled. No available filter or available index.", nil
	}

	// We current do not consider `IndexMergePath`:
	// 1. If there is an index path.
	// 2. TODO: If there exists exprs that cannot be pushed down. This is to avoid wrongly estRow of Selection added by rule_predicate_push_down.
	stmtCtx := ds.SCtx().GetSessionVars().StmtCtx
	needConsiderIndexMerge := true
	// if current index merge hint is nil, once there is a no-access-cond in one of possible access path.
	if len(ds.IndexMergeHints) == 0 {
		skipRangeScanCheck := fixcontrol.GetBoolWithDefault(
			ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(),
			fixcontrol.Fix52869,
			false,
		)
		ds.SCtx().GetSessionVars().RecordRelevantOptFix(fixcontrol.Fix52869)
		if !skipRangeScanCheck {
			for i := 1; i < len(ds.PossibleAccessPaths); i++ {
				if len(ds.PossibleAccessPaths[i].AccessConds) != 0 {
					needConsiderIndexMerge = false
					break
				}
			}
		}
		if needConsiderIndexMerge {
			// PushDownExprs() will append extra warnings, which is annoying. So we reset warnings here.
			warnings := stmtCtx.GetWarnings()
			extraWarnings := stmtCtx.GetExtraWarnings()
			_, remaining := expression.PushDownExprs(util.GetPushDownCtx(ds.SCtx()), indexMergeConds, kv.UnSpecified)
			stmtCtx.SetWarnings(warnings)
			stmtCtx.SetExtraWarnings(extraWarnings)
			if len(remaining) > 0 {
				needConsiderIndexMerge = false
			}
		}
	}

	// 1. Generate possible IndexMerge paths for `OR`.
	err := generateORIndexMerge(ds, indexMergeConds)
	if err != nil {
		return "", err
	}
	// 2. Generate possible IndexMerge paths for `AND`.
	indexMergeAndPath := generateANDIndexMerge4NormalIndex(ds, regularPathCount, nil)
	if indexMergeAndPath != nil {
		ds.PossibleAccessPaths = append(ds.PossibleAccessPaths, indexMergeAndPath)
	}

	if needConsiderIndexMerge {
		return "", nil
	}

	var containMVPath bool
	for i := regularPathCount; i < len(ds.PossibleAccessPaths); i++ {
		path := ds.PossibleAccessPaths[i]
		for _, p := range util.SliceRecursiveFlattenIter[*util.AccessPath](path.PartialAlternativeIndexPaths) {
			if isMVIndexPath(p) {
				containMVPath = true
				break
			}
		}

		if !containMVPath {
			ds.PossibleAccessPaths = slices.Delete(ds.PossibleAccessPaths, i, i+1)
		}
	}
	if len(ds.PossibleAccessPaths) == regularPathCount {
		return "IndexMerge is inapplicable or disabled. ", nil
	}
	return "", nil
}

// generateANDIndexMerge4ComposedIndex tries to generate AND type index merge AccessPath for ( json_member_of /
// json_overlaps / json_contains) on multiple multi-valued or normal indexes.
/*
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
*/
func generateANDIndexMerge4ComposedIndex(ds *logicalop.DataSource, normalPathCnt int, indexMergeConds []expression.Expression) error {
	isPossibleIdxMerge := len(indexMergeConds) > 0 && // have corresponding access conditions, and
		len(ds.PossibleAccessPaths) > 1 // have multiple index paths
	if !isPossibleIdxMerge {
		return nil
	}

	// Collect access paths that satisfy the hints, and make sure there is at least one MV index path.
	var mvIndexPathCnt int
	for idx := range normalPathCnt {
		if (ds.PossibleAccessPaths[idx].IsTablePath() &&
			!isInIndexMergeHints(ds, "primary")) ||
			(!ds.PossibleAccessPaths[idx].IsTablePath() &&
				!isInIndexMergeHints(ds, ds.PossibleAccessPaths[idx].Index.Name.L)) {
			continue
		}
		if isMVIndexPath(ds.PossibleAccessPaths[idx]) {
			mvIndexPathCnt++
		}
	}
	if mvIndexPathCnt == 0 {
		return nil
	}

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
	mvIndexPartialPaths, usedAccessMap, err := generateMVIndexMergePartialPaths4And(ds, normalPathCnt, indexMergeConds, ds.TableStats.HistColl)
	if err != nil {
		return err
	}
	if len(mvIndexPartialPaths) == 0 {
		return nil
	}
	normalIndexPartialPaths := generateNormalIndexPartialPath4And(ds, normalPathCnt, usedAccessMap)
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

	condInIdxFilter := make(map[string]struct{}, len(remainedCNFs))
	// try to derive index filters for each path
	for _, path := range combinedPartialPaths {
		idxFilters, _ := splitIndexFilterConditions(ds, remainedCNFs, path.FullIdxCols, path.FullIdxColLens)
		idxFilters = util.CloneExprs(idxFilters)
		path.IndexFilters = append(path.IndexFilters, idxFilters...)
		for _, idxFilter := range idxFilters {
			condInIdxFilter[string(idxFilter.HashCode())] = struct{}{}
		}
	}

	// Collect the table filters.
	// Since it's the intersection type index merge here, as long as a filter appears in one path, we don't need it in
	// the table filters.
	var tableFilters []expression.Expression
	for _, CNFItem := range remainedCNFs {
		if _, ok := condInIdxFilter[string(CNFItem.HashCode())]; !ok {
			tableFilters = append(tableFilters, CNFItem)
		}
	}

	mvp := buildPartialPathUp4MVIndex(combinedPartialPaths, true, tableFilters, ds.TableStats.HistColl)

	ds.PossibleAccessPaths = append(ds.PossibleAccessPaths, mvp)
	return nil
}

// generateANDIndexMerge4MVIndex tries to generate AND type index merge AccessPath for ( json_member_of /
// json_overlaps / json_contains) on a single multi-valued index.
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
*/
func generateANDIndexMerge4MVIndex(ds *logicalop.DataSource, normalPathCnt int, filters []expression.Expression) error {
	for idx := range normalPathCnt {
		if !isMVIndexPath(ds.PossibleAccessPaths[idx]) {
			continue // not a MVIndex path
		}

		// for single MV index usage, if specified use the specified one, if not, all can be access and chosen by cost model.
		if !isInIndexMergeHints(ds, ds.PossibleAccessPaths[idx].Index.Name.L) {
			continue
		}

		idxCols, ok := PrepareIdxColsAndUnwrapArrayType(
			ds.Table.Meta(),
			ds.PossibleAccessPaths[idx].Index,
			ds.TblColsByID,
			true,
		)
		if !ok {
			continue
		}

		accessFilters, remainingFilters, _ := collectFilters4MVIndex(ds.SCtx(), filters, idxCols)
		if len(accessFilters) == 0 { // cannot use any filter on this MVIndex
			continue
		}

		partialPaths, isIntersection, ok, err := buildPartialPaths4MVIndexWithPath(ds.SCtx(), accessFilters, idxCols, ds.PossibleAccessPaths[idx], ds.TableStats.HistColl)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		// Here, all partial paths are built from the same MV index, so we can directly use the first one to get the
		// metadata.
		// And according to buildPartialPaths4MVIndex, there must be at least one partial path if it returns ok.
		firstPath := partialPaths[0]
		idxFilters, tableFilters := splitIndexFilterConditions(
			ds,
			remainingFilters,
			firstPath.FullIdxCols,
			firstPath.FullIdxColLens,
		)

		// Add the index filters to every partial path.
		// For union type index merge, this is necessary for correctness.
		for _, path := range partialPaths {
			clonedIdxFilters := util.CloneExprs(idxFilters)
			path.IndexFilters = append(path.IndexFilters, clonedIdxFilters...)
		}

		ds.PossibleAccessPaths = append(ds.PossibleAccessPaths, buildPartialPathUp4MVIndex(
			partialPaths,
			isIntersection,
			tableFilters,
			ds.TableStats.HistColl,
		),
		)
	}
	return nil
}

// buildPartialPathUp4MVIndex builds these partial paths up to a complete index merge path.
