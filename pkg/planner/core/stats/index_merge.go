package stats

import (
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

// generateIndexMergePath generates IndexMerge AccessPaths on this DataSource.
func generateIndexMergePath(ds *logicalop.DataSource) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
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

// cleanAccessPathForMVIndexHint removes all other access path if there is a multi-valued index hint, and this hint
// has a valid path
func cleanAccessPathForMVIndexHint(ds *logicalop.DataSource) {
	forcedMultiValuedIndex := make(map[int64]struct{}, len(ds.PossibleAccessPaths))
	for _, p := range ds.PossibleAccessPaths {
		if !plannerutil.IsMVIndexPath(p) || !p.Forced {
			continue
		}
		forcedMultiValuedIndex[p.Index.ID] = struct{}{}
	}
	// no multi-valued index specified, just return
	if len(forcedMultiValuedIndex) == 0 {
		return
	}

	validMVIndexPath := make([]*plannerutil.AccessPath, 0, len(ds.PossibleAccessPaths))
	for _, p := range ds.PossibleAccessPaths {
		if indexMergeContainSpecificIndex(p, forcedMultiValuedIndex) {
			validMVIndexPath = append(validMVIndexPath, p)
		}
	}
	if len(validMVIndexPath) > 0 {
		ds.PossibleAccessPaths = validMVIndexPath
	}
}

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
			_, remaining := expression.PushDownExprs(plannerutil.GetPushDownCtx(ds.SCtx()), indexMergeConds, kv.UnSpecified)
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
		for _, p := range plannerutil.SliceRecursiveFlattenIter[*plannerutil.AccessPath](path.PartialAlternativeIndexPaths) {
			if plannerutil.IsMVIndexPath(p) {
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
		if !plannerutil.IsMVIndexPath(ds.PossibleAccessPaths[idx]) {
			continue // not a MVIndex path
		}

		// for single MV index usage, if specified use the specified one, if not, all can be access and chosen by cost model.
		if !isInIndexMergeHints(ds, ds.PossibleAccessPaths[idx].Index.Name.L) {
			continue
		}

		idxCols, ok := PrepareIdxColsAndUnwrapArrayType(
			ds.Table.Meta(),
			ds.PossibleAccessPaths[idx].Index,
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

		partialPaths, isIntersection, ok, err := buildPartialPaths4MVIndex(ds.SCtx(), accessFilters, idxCols, ds.PossibleAccessPaths[idx].Index, ds.TableStats.HistColl)
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
			clonedIdxFilters := plannerutil.CloneExprs(idxFilters)
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
