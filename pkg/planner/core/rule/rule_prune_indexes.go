// Copyright 2026 PingCAP, Inc.
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

package rule

import (
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
)

const (
	// defaultMaxIndexes is the default maximum number of indexes to keep when pruning.
	// This prevents overly aggressive pruning when the threshold is small.
	// TODO: We should add a second pruning phase around the fillIndexPath step,
	// where we can refine the indexes further based on the actual column statistics.
	// Therefore, if a customer did set tidn_opt_index_prune_threshold < 10, we could
	// a minimum of 10 in the first pruning phase, and prune further in the second phase.
	defaultMaxIndexes = 10
)

// indexWithScore stores an access path along with its coverage scores for ranking.
// Scoring is computed over the index's effective key — the declared columns plus the
// clustered-handle columns TiKV appends to a non-unique secondary index — so a handle
// column bound by an equality predicate extends the usable prefix exactly as a declared
// column would. Handle-prefix columns are counted the same as any other column here;
// their only special treatment is coversNonDiscounted, which drives the redundancy drop.
type indexWithScore struct {
	path                 *util.AccessPath
	interestingCount     int     // Total number of interesting columns covered (handle-prefix columns included)
	consecutiveColumnIDs []int64 // IDs of consecutive columns from the start of the effective key
	coveredColumnIDs     []int64 // IDs of all covered interesting columns in key order; consecutive ones come first
	// coversNonDiscounted is true when the index covers at least one interesting column
	// outside the equality-bound leading handle prefix. When it is false the index's only
	// access is the handle prefix, which the clustered table path already serves directly,
	// so the index is pure redundancy and is dropped during selection (see droppable).
	coversNonDiscounted bool
}

// columnRequirements holds the column maps needed for index pruning.
type columnRequirements struct {
	interestingColIDs map[int64]struct{}
	// discountedColIDs is the equality/IN-bound leading prefix of the clustered key
	// (e.g. a tenant ID that leads every index in a multi-tenant schema). These columns
	// still count toward an index's score like any other covered column; they are tracked
	// only to identify redundant indexes: an index whose entire covered access lies within
	// this prefix (coversNonDiscounted == false) is served by the clustered table path and
	// is dropped. The name is retained for continuity with earlier revisions that instead
	// subtracted these columns from the score.
	discountedColIDs map[int64]struct{}
}

// ShouldPreferIndexMerge returns true if index merge should be preferred, either due to hints or fix control.
func ShouldPreferIndexMerge(ds *logicalop.DataSource) bool {
	return len(ds.IndexMergeHints) > 0 || fixcontrol.GetBoolWithDefault(
		ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(),
		fixcontrol.Fix52869,
		false,
	)
}

// PruneIndexesByWhereAndOrder prunes indexes based on their coverage of interesting columns.
// It keeps the most promising indexes up to the threshold, prioritizing those that:
// 1. Cover more interesting columns
// 2. Have consecutive column matches from the index start (enabling index prefix usage)
// 3. Support single-scan (covering index without table lookups)
// 4. Have different consecutive column orderings (e.g., if interesting columns are A, B, keep both (A,B) and (B,A))
// Indexes are ranked by their effective key (declared columns plus the appended
// clustered handle), so an equality-bound handle column counts toward the score like any
// other. Leading clustered-key columns bound by equality/IN predicates (e.g. a tenant ID
// that prefixes every index in a multi-tenant schema) are not discounted from the score;
// instead, an index whose *only* covered access is that handle prefix is dropped as
// redundant, because the clustered table path already serves the same rows. This keeps
// the relative ranking of useful indexes intact — an index that binds a column beyond the
// handle prefix (whether declared or via the appended handle) outranks one that does not.
// The threshold controls the behavior:
// threshold = -1: disable pruning (handled by caller)
// threshold = 0: only prune indexes with no interesting columns (score == 0)
// threshold > 0: keep at least threshold indexes (but at least defaultMaxIndexes)
// but if there are fewer than threshold indexes, we will still prune zero-score indexes.
func PruneIndexesByWhereAndOrder(ds *logicalop.DataSource, paths []*util.AccessPath, interestingColumns []*expression.Column, threshold int) []*util.AccessPath {
	if len(paths) <= 1 {
		return paths
	}

	totalPathCount := len(paths)

	// If we disabled the prune, return directly.
	if threshold < 0 {
		return paths
	}
	// Now the prune must happen.

	// If threshold is 0 or greater than total paths, we only prune zero-score indexes.
	onlyPruneZeroScore := threshold == 0 || (threshold > totalPathCount)

	// Build column ID maps and calculate totals
	req := buildColumnRequirements(interestingColumns)
	req.discountedColIDs = discountedHandlePrefixCols(ds, req.interestingColIDs)

	preferredIndexes := make([]indexWithScore, 0, totalPathCount)
	tablePaths := make([]*util.AccessPath, 0, 1)
	mvIndexPaths := make([]*util.AccessPath, 0, 1)
	indexMergeIndexPaths := make([]*util.AccessPath, 0, 1)
	preferMerge := ShouldPreferIndexMerge(ds)

	// Check if IndexMerge hints specify specific index names
	// We need to import the function from indexmerge_path.go, but since it's in a different package,
	// we'll implement the check inline here
	hasSpecifiedIndexes := false
	if len(ds.IndexMergeHints) > 0 {
		for _, hint := range ds.IndexMergeHints {
			if hint.IndexHint != nil && len(hint.IndexHint.IndexNames) > 0 {
				hasSpecifiedIndexes = true
				break
			}
		}
	}

	// Categorize each index path
	for _, path := range paths {
		if path.IsTablePath() {
			tablePaths = append(tablePaths, path)
			continue
		}

		// Always keep multi-value indexes (like table paths)
		if path.Index != nil && path.Index.MVIndex {
			mvIndexPaths = append(mvIndexPaths, path)
			continue
		}

		// If we have forced paths, we shouldn't prune any paths
		if path.Forced {
			return paths
		}

		// Skip paths with nil Index
		if path.Index == nil {
			continue
		}

		// Calculate coverage for this index
		// Use TableInfo.Columns (not ds.Columns) because IndexColumn.Offset refers to TableInfo.Columns
		var tableColumns []*model.ColumnInfo
		if ds.TableInfo != nil {
			tableColumns = ds.TableInfo.Columns
		}
		idxScore := scoreIndexPath(ds, path, req, tableColumns)

		if path.FullIdxCols != nil {
			path.IsSingleScan = ds.IsSingleScan(path.FullIdxCols, path.FullIdxColLens)
		}

		// Check if this index is specified in IndexMerge hints
		// Note: Indexes specified in USE_INDEX_MERGE(t, idx1, idx2) are NOT marked as "forced"
		// (only USE_INDEX/FORCE_INDEX mark indexes as forced), so we need to collect them here
		// to ensure they're not pruned. This is only needed when hints specify specific index names.
		if hasSpecifiedIndexes {
			indexName := path.Index.Name.L
			isSpecified := false
			for _, hint := range ds.IndexMergeHints {
				if hint.IndexHint == nil || len(hint.IndexHint.IndexNames) == 0 {
					continue
				}
				for _, hintName := range hint.IndexHint.IndexNames {
					// Use case-insensitive comparison like isSpecifiedInIndexMergeHints does
					if strings.EqualFold(indexName, hintName.String()) {
						isSpecified = true
						break
					}
				}
				if isSpecified {
					break
				}
			}
			if isSpecified {
				// This index is explicitly specified in IndexMerge hints, keep it
				// Add it to indexMergeIndexPaths so it's guaranteed to be included even if it doesn't score well
				indexMergeIndexPaths = append(indexMergeIndexPaths, path)
				continue
			}
		}

		// If index merge is preferred (via general hints without index names, or fix control),
		// keep indexes that have any coverage. Note: When specific indexes are mentioned in hints,
		// those are handled above. Here we handle general IndexMerge hints (no specific index names)
		// or fix control. We still apply some filtering (len(consecutiveColumnIDs) > 0 OR other coverage)
		// to avoid keeping completely useless indexes, but we're more lenient than normal pruning.
		if preferMerge && !hasSpecifiedIndexes {
			// When IndexMerge is preferred without specific index names, keep any index with coverage
			if len(idxScore.consecutiveColumnIDs) > 0 || path.IsSingleScan || idxScore.interestingCount > 0 {
				preferredIndexes = append(preferredIndexes, idxScore)
				continue
			}
		}

		// Add to preferred indexes if it has any coverage or is a covering scan
		// We'll handle ordering diversity in buildFinalResult to ensure we keep
		// different orderings even if they have lower scores
		if path.IsSingleScan || idxScore.interestingCount > 0 {
			preferredIndexes = append(preferredIndexes, idxScore)
		}
	}

	// Build final result by sorting and selecting top indexes
	maxToKeep := max(threshold, defaultMaxIndexes)
	result := buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths, preferredIndexes, maxToKeep, onlyPruneZeroScore, req)

	failpoint.InjectCall("InjectCheckForIndexPrune", result)

	// Safety check: if we ended up with nothing, return the original paths
	if len(result) == 0 {
		return paths
	}

	// Additional safety: if we only have table paths and MVIndex paths and no regular indexes, keep original.
	// Exception: when discounted columns exist, an empty index set is an intentional outcome — every index
	// covered only the equality-bound clustered-key prefix, which the table path already serves directly.
	if len(result) == len(tablePaths)+len(mvIndexPaths) && len(preferredIndexes) == 0 && len(req.discountedColIDs) == 0 {
		return paths
	}

	return result
}

// buildColumnRequirements builds column ID maps for efficient lookup.
func buildColumnRequirements(interestingColumns []*expression.Column) columnRequirements {
	req := columnRequirements{
		interestingColIDs: make(map[int64]struct{}, len(interestingColumns)),
	}

	// Build interesting column IDs
	for _, col := range interestingColumns {
		req.interestingColIDs[col.ID] = struct{}{}
	}

	return req
}

// collectEqOrInBoundColIDs returns the IDs of columns bound by an equality or IN
// predicate against constants in the given conjunctive conditions. Both = and the
// null-safe <=> (ast.NullEQ) count, since the ranger builds equality access ranges
// for either (see detacher.go extractEqAndInCondition).
func collectEqOrInBoundColIDs(conds []expression.Expression) map[int64]struct{} {
	var colIDs map[int64]struct{}
	addCol := func(col *expression.Column) {
		if colIDs == nil {
			colIDs = make(map[int64]struct{})
		}
		colIDs[col.ID] = struct{}{}
	}
	for _, cond := range conds {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		args := sf.GetArgs()
		switch sf.FuncName.L {
		case ast.EQ, ast.NullEQ:
			if col, ok := args[0].(*expression.Column); ok {
				if _, isConst := args[1].(*expression.Constant); isConst {
					addCol(col)
				}
			} else if col, ok := args[1].(*expression.Column); ok {
				if _, isConst := args[0].(*expression.Constant); isConst {
					addCol(col)
				}
			}
		case ast.In:
			col, ok := args[0].(*expression.Column)
			if !ok {
				continue
			}
			allConst := true
			for _, arg := range args[1:] {
				if _, isConst := arg.(*expression.Constant); !isConst {
					allConst = false
					break
				}
			}
			if allConst {
				addCol(col)
			}
		}
	}
	return colIDs
}

// discountedHandlePrefixCols returns the leading clustered-key columns that are bound
// by equality or IN predicates on constants. In multi-tenant schemas every index leads
// with the tenant column, so covering it says nothing about which index is better —
// and the table path (clustered key) already provides the same access. Such columns
// still extend an index's usable prefix, so scoring lets them anchor consecutive-column
// tracking without contributing to the score. Only a bound leading prefix qualifies:
// discounting stops at the first clustered-key column without an equality/IN predicate.
func discountedHandlePrefixCols(ds *logicalop.DataSource, interestingColIDs map[int64]struct{}) map[int64]struct{} {
	if ds.TableInfo == nil {
		return nil
	}
	var pkColIDs []int64
	if ds.TableInfo.PKIsHandle {
		if pkCol := ds.TableInfo.GetPkColInfo(); pkCol != nil {
			pkColIDs = []int64{pkCol.ID}
		}
	} else if ds.TableInfo.IsCommonHandle {
		pk := ds.TableInfo.GetPrimaryKey()
		if pk == nil {
			return nil
		}
		for _, idxCol := range pk.Columns {
			if idxCol.Offset < 0 || idxCol.Offset >= len(ds.TableInfo.Columns) {
				return nil
			}
			if idxCol.Length > 0 {
				// A prefix column cannot fully serve an equality predicate.
				break
			}
			pkColIDs = append(pkColIDs, ds.TableInfo.Columns[idxCol.Offset].ID)
		}
	}
	if len(pkColIDs) == 0 {
		return nil
	}
	eqBound := collectEqOrInBoundColIDs(ds.PushedDownConds)
	if len(eqBound) == 0 {
		return nil
	}
	var discounted map[int64]struct{}
	for _, colID := range pkColIDs {
		if _, ok := eqBound[colID]; !ok {
			break
		}
		if _, ok := interestingColIDs[colID]; !ok {
			break
		}
		if discounted == nil {
			discounted = make(map[int64]struct{}, len(pkColIDs))
		}
		discounted[colID] = struct{}{}
	}
	return discounted
}

// buildOrderingKey creates a string key representing the consecutive column ordering.
// This is used to detect and keep indexes with different orderings.
func buildOrderingKey(columnIDs []int64) string {
	if len(columnIDs) == 0 {
		return ""
	}
	// Create a simple string representation of the column ID sequence
	// Using a format like "1,2,3" for columns with IDs 1, 2, 3
	var builder strings.Builder
	// Pre-allocate capacity: estimate ~4 bytes per ID (for small IDs) + commas
	builder.Grow(len(columnIDs) * 5)
	for i, id := range columnIDs {
		if i > 0 {
			builder.WriteString(",")
		}
		fmt.Fprintf(&builder, "%d", id)
	}
	return builder.String()
}

// buildCoveredSetKey builds an order-insensitive signature of the interesting columns an
// index covers (over its effective key). Indexes sharing this key cover the same set of
// interesting columns; which of them survive is then decided by their consecutive prefix
// (see isDominatedCoverage).
func buildCoveredSetKey(info indexWithScore) string {
	if len(info.coveredColumnIDs) == 0 {
		return ""
	}
	sorted := slices.Clone(info.coveredColumnIDs)
	slices.Sort(sorted)
	return buildOrderingKey(sorted)
}

// isPrefixOf reports whether short is a leading subsequence of long.
func isPrefixOf(short, long []int64) bool {
	if len(short) > len(long) {
		return false
	}
	for i, id := range short {
		if long[i] != id {
			return false
		}
	}
	return true
}

// isDominatedCoverage reports whether an already-selected index dominates the candidate:
// it covers the identical interesting-column set (same coveredKey) through a consecutive
// prefix that begins with the candidate's consecutive prefix. Such an index reaches every
// column the candidate does, but through an equal-or-longer usable prefix, so the
// candidate adds no access the query cannot already get and is pruned. Covering a *larger*
// set does not dominate — a narrower index remains a valid alternative for the cost model.
func isDominatedCoverage(entry scoredIndex, state *indexSelectionState) bool {
	for _, sel := range state.seenConsecutiveByCovered[entry.coveredKey] {
		if isPrefixOf(entry.info.consecutiveColumnIDs, sel) {
			return true
		}
	}
	return false
}

// effectiveIndexColumnIDs returns the column IDs of an index's physical key: the declared
// index columns followed by the clustered-handle columns TiKV appends to a non-unique
// secondary index's key. It derives the appended suffix from ds.HandleColsToAppend — the
// same method fillIndexPath uses to extend the path for range building — so pruning ranks
// an index by the access conditions its real key can satisfy: a trailing handle column
// bound by an equality predicate extends the usable prefix exactly as a declared column
// would. A negative sentinel marks a position whose column is unavailable so prefix
// tracking still breaks there. FullIdxCols carries the declared columns at prune time
// (fillIndexPath, which performs the append on the path itself, runs later).
func effectiveIndexColumnIDs(ds *logicalop.DataSource, path *util.AccessPath) []int64 {
	ids := make([]int64, 0, len(path.FullIdxCols)+len(ds.CommonHandleCols)+1)
	for _, col := range path.FullIdxCols {
		if col == nil {
			ids = append(ids, -1)
			continue
		}
		ids = append(ids, col.ID)
	}
	appendCols, _ := ds.HandleColsToAppend(path, path.FullIdxCols)
	for _, col := range appendCols {
		if col != nil {
			ids = append(ids, col.ID)
		}
	}
	return ids
}

// scoreIndexPath calculates coverage metrics for a single index path over its effective
// key (declared columns plus the appended clustered handle; see effectiveIndexColumnIDs).
// Handle-prefix columns are scored the same as any other covered column; the only handle-
// specific signal is coversNonDiscounted, which records whether the index reaches any
// interesting column beyond the equality-bound leading handle prefix. When FullIdxCols is
// nil (e.g., in static pruning mode) it falls back to path.Index.Columns and cannot
// determine consecutiveColumnIDs, so that field remains empty.
func scoreIndexPath(
	ds *logicalop.DataSource,
	path *util.AccessPath,
	req columnRequirements,
	tableColumns []*model.ColumnInfo,
) indexWithScore {
	score := indexWithScore{path: path}

	if path.Index != nil && path.Index.ConditionExprString != "" {
		for _, col := range path.Index.AffectColumn {
			// Some columns from the constraint is not found. Then the path can not be selected.
			// We mixed the WHERE clause and other clauses like JOIN/ORDER BY to prune the indexes.
			// So this check is not the strictest one.
			// An unresolvable constraint column counts as not found: returning a zero score
			// only prunes the index, which is always safe (the table path is kept).
			if col.Offset < 0 || col.Offset >= len(tableColumns) || tableColumns[col.Offset] == nil {
				return score
			}
			if _, found := req.interestingColIDs[tableColumns[col.Offset].ID]; !found {
				return score
			}
		}
		// Pre check for partial index passed, continue to calculate the score.
	}

	if path.FullIdxCols != nil {
		// Normal path: walk the effective key (declared columns + appended handle).
		// chainPos tracks how far the usable prefix extends: every covered interesting
		// column — including a handle-prefix column — extends it, and consecutive tracking
		// stops at the first key column that is not interesting.
		chainPos := 0
		for i, idxColID := range effectiveIndexColumnIDs(ds, path) {
			if idxColID < 0 {
				continue
			}
			if _, found := req.interestingColIDs[idxColID]; !found {
				continue
			}
			score.interestingCount++
			score.coveredColumnIDs = append(score.coveredColumnIDs, idxColID)
			if _, discounted := req.discountedColIDs[idxColID]; !discounted {
				score.coversNonDiscounted = true
			}
			// Track consecutive columns from the start of the effective key.
			if i == chainPos {
				chainPos++
				score.consecutiveColumnIDs = append(score.consecutiveColumnIDs, idxColID)
			}
		}
	} else if path.Index != nil && tableColumns != nil {
		// Fallback path: use Index.Columns (for static pruning mode when FullIdxCols is nil)
		// Map IndexColumn.Offset to column ID via tableColumns
		for _, idxCol := range path.Index.Columns {
			if idxCol.Offset < 0 || idxCol.Offset >= len(tableColumns) {
				continue
			}
			colInfo := tableColumns[idxCol.Offset]
			if colInfo == nil {
				continue
			}
			idxColID := colInfo.ID

			// Check if this index column matches an interesting column
			if _, found := req.interestingColIDs[idxColID]; found {
				score.interestingCount++
				if _, discounted := req.discountedColIDs[idxColID]; !discounted {
					score.coversNonDiscounted = true
				}
				// Note: We cannot track consecutiveColumnIDs here because we don't have
				// the full column information needed to determine if columns are consecutive
				// in the index. This is acceptable as the user indicated.
			}
		}
	}

	return score
}

// buildFinalResult sorts and selects the top indexes to keep, combining table paths,
// multi-value indexes, index merge indexes, and preferred indexes.
type scoredIndex struct {
	info             indexWithScore
	score            int
	columns          int
	isSingleScan     bool
	totalConsecutive int
	// droppable marks an index whose only covered access is the equality-bound leading
	// handle prefix (coversNonDiscounted is false) and that is not a covering scan. The
	// clustered table path serves the same rows, so such an index is redundant and is
	// skipped during selection.
	droppable bool
	// coveredKey is the order-insensitive signature of the covered interesting-column set
	// (see buildCoveredSetKey); empty when the index has no consecutive interesting columns.
	// Domination among indexes with the same coveredKey is resolved via their consecutive
	// prefixes (isDominatedCoverage).
	coveredKey string
}

func scoreAndSort(indexes []indexWithScore, req columnRequirements) []scoredIndex {
	if len(indexes) == 0 {
		return nil
	}
	scored := make([]scoredIndex, 0, len(indexes))
	// Handle-prefix columns now contribute to per-index counts, so the full-coverage
	// target is the whole interesting set.
	marginalTotal := len(req.interestingColIDs)
	for _, candidate := range indexes {
		score := calculateScoreFromCoverage(candidate, marginalTotal, candidate.path.IsSingleScan)
		// Skip indexes with score == 0 as they don't provide any value
		if score == 0 {
			continue
		}
		cols := len(candidate.path.FullIdxCols)
		coveredKey := ""
		if len(candidate.consecutiveColumnIDs) > 0 {
			coveredKey = buildCoveredSetKey(candidate)
		}
		scored = append(scored, scoredIndex{
			info:             candidate,
			score:            score,
			columns:          cols,
			isSingleScan:     candidate.path.IsSingleScan,
			totalConsecutive: len(candidate.consecutiveColumnIDs),
			droppable:        !candidate.coversNonDiscounted && !candidate.path.IsSingleScan,
			coveredKey:       coveredKey,
		})
	}
	slices.SortFunc(scored, func(a, b scoredIndex) int {
		// Tie-breaker: prefer indexes with higher score
		if a.score != b.score {
			return b.score - a.score
		}
		// Tie-breaker: prefer indexes that reach beyond the handle prefix over redundant
		// (droppable) ones, so a kept index is never ordered behind pure redundancy.
		if a.droppable != b.droppable {
			if a.droppable {
				return 1
			}
			return -1
		}
		// Tie-breaker: prefer indexes with more consecutive columns
		if a.totalConsecutive != b.totalConsecutive {
			return b.totalConsecutive - a.totalConsecutive
		}
		// Tie-breaker: prefer indexes with single-scan
		if a.isSingleScan != b.isSingleScan {
			if a.isSingleScan {
				return -1
			}
			return 1
		}
		// Tie-breaker: prefer indexes with fewer columns — the entries are narrower,
		// and on clustered tables the PK suffix (usable for index-side filtering and
		// ordering) starts earlier in the effective index.
		if a.columns != b.columns {
			return a.columns - b.columns
		}
		// Tie-breaker: use index ID for deterministic ordering when all other criteria are equal
		// This ensures stable sorting for functionally identical indexes (e.g., k1 and k2 with same expressions)
		if a.info.path.Index != nil && b.info.path.Index != nil {
			// Use proper three-way comparison to avoid integer overflow
			// (a.info.path.Index.ID is int64, casting the difference to int can overflow)
			if a.info.path.Index.ID < b.info.path.Index.ID {
				return -1
			}
			if a.info.path.Index.ID > b.info.path.Index.ID {
				return 1
			}
		}
		return 0
	})
	return scored
}

func buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths []*util.AccessPath, preferredIndexes []indexWithScore, maxToKeep int, onlyPruneZeroScore bool, req columnRequirements) []*util.AccessPath {
	result := make([]*util.AccessPath, 0, len(tablePaths)+len(indexMergeIndexPaths)+len(preferredIndexes))

	// CRITICAL: Always include table paths - this is mandatory for correctness
	result = append(result, tablePaths...)
	// CRITICAL: Always include multi-value index paths - we do not have sufficient
	// information to determine if they should be pruned in this function.
	result = append(result, mvIndexPaths...)
	// CRITICAL: Always include indexes specified in IndexMerge hints - index merge needs them to build partial paths
	result = append(result, indexMergeIndexPaths...)

	added := make(map[*util.AccessPath]struct{}, len(tablePaths)+len(mvIndexPaths)+len(indexMergeIndexPaths))
	for _, path := range tablePaths {
		added[path] = struct{}{}
	}
	for _, path := range mvIndexPaths {
		added[path] = struct{}{}
	}
	for _, path := range indexMergeIndexPaths {
		added[path] = struct{}{}
	}

	preferredScored := scoreAndSort(preferredIndexes, req)

	// Prune paths with 0 score and redundant handle-prefix-only paths. The latter are
	// served by the clustered table path (always kept above), so they add no access the
	// query cannot already get.
	if onlyPruneZeroScore {
		for _, entry := range preferredScored {
			if entry.droppable {
				continue
			}
			if _, ok := added[entry.info.path]; !ok {
				result = append(result, entry.info.path)
			}
		}
		return result
	}

	// Apply two-phase selection to limit the number of indexes
	phase1Limit := maxToKeep / 2
	selectionState := newIndexSelectionState(phase1Limit, maxToKeep)

	result = selectIndexes(preferredScored, added, req, selectionState, result)

	return result
}

// indexSelectionState tracks state during two-phase index selection
type indexSelectionState struct {
	phase1Limit              int
	remaining                int
	phase1Count              int
	seenConsecutiveColumnIDs map[int64]struct{}
	// seenConsecutiveByCovered maps a covered-set key to the consecutive prefixes of the
	// already-selected indexes with that coverage, used to detect coverage domination.
	seenConsecutiveByCovered map[string][][]int64
}

func newIndexSelectionState(phase1Limit, maxToKeep int) *indexSelectionState {
	return &indexSelectionState{
		phase1Limit:              phase1Limit,
		remaining:                maxToKeep,
		seenConsecutiveColumnIDs: make(map[int64]struct{}),
		seenConsecutiveByCovered: make(map[string][][]int64),
	}
}

// selectIndexes performs two-phase selection of indexes
func selectIndexes(preferredScored []scoredIndex, added map[*util.AccessPath]struct{}, req columnRequirements, state *indexSelectionState, result []*util.AccessPath) []*util.AccessPath {
	for _, entry := range preferredScored {
		path := entry.info.path
		if _, ok := added[path]; ok {
			continue
		}
		if state.remaining == 0 {
			break
		}
		// Redundant handle-prefix-only indexes are served by the clustered table path;
		// drop them rather than spend a slot on access the query can already get.
		if entry.droppable {
			continue
		}

		shouldAdd := shouldAddIndex(entry, path, req, state)
		if shouldAdd {
			result = append(result, path)
			added[path] = struct{}{}
			state.remaining--
		}
	}
	return result
}

// shouldAddIndex determines if an index should be added based on phase and diversity rules
func shouldAddIndex(entry scoredIndex, path *util.AccessPath, req columnRequirements, state *indexSelectionState) bool {
	hasConsecutive := entry.totalConsecutive > 0
	if state.phase1Count < state.phase1Limit {
		// Phase 1: Keep top threshold/2 based on score, but skip an index whose
		// interesting-column coverage is dominated by an already-selected index: the
		// earlier (higher-ranked) index reaches the same columns through an equal-or-
		// longer usable prefix, so keeping this one wastes a slot, stats loading, and
		// range building downstream.
		if hasConsecutive {
			if isDominatedCoverage(entry, state) {
				return false
			}
		}
		state.phase1Count++
		recordCoverage(entry, state)
		return true
	}

	// Phase 2: Apply diversity rules
	if hasConsecutive {
		return shouldAddIndexWithConsecutive(entry, state)
	}

	return shouldAddIndexWithoutConsecutive(entry, path, req, state)
}

// recordCoverage tracks consecutive column IDs and per-coverage consecutive prefixes.
func recordCoverage(entry scoredIndex, state *indexSelectionState) {
	for _, colID := range entry.info.consecutiveColumnIDs {
		state.seenConsecutiveColumnIDs[colID] = struct{}{}
	}
	if entry.coveredKey != "" {
		state.seenConsecutiveByCovered[entry.coveredKey] = append(
			state.seenConsecutiveByCovered[entry.coveredKey], entry.info.consecutiveColumnIDs)
	}
}

// shouldAddIndexWithConsecutive checks if an index with consecutive columns should be added in phase 2
func shouldAddIndexWithConsecutive(entry scoredIndex, state *indexSelectionState) bool {
	if isDominatedCoverage(entry, state) {
		return false
	}
	recordCoverage(entry, state)
	return true
}

// shouldAddIndexWithoutConsecutive checks if an index without consecutive columns should be added in phase 2
func shouldAddIndexWithoutConsecutive(entry scoredIndex, path *util.AccessPath, req columnRequirements, state *indexSelectionState) bool {
	if entry.info.interestingCount != 1 {
		return true
	}

	// For single-column indexes, check if the column is already covered by a consecutive column
	singleColID := findSingleInterestingColumn(path, req)
	if singleColID < 0 {
		return true
	}

	// Don't add if the column is already in a consecutive column, unless it's a single scan
	_, covered := state.seenConsecutiveColumnIDs[singleColID]
	return !covered || path.IsSingleScan
}

// findSingleInterestingColumn finds the single interesting column ID in an index.
// Returns -1 if FullIdxCols is nil (e.g., in static pruning mode) since we cannot
// determine the specific column without FullIdxCols. This causes the caller to
// keep the index, which is the safe default.
func findSingleInterestingColumn(path *util.AccessPath, req columnRequirements) int64 {
	if path.FullIdxCols == nil {
		return -1
	}
	for _, idxCol := range path.FullIdxCols {
		if idxCol != nil {
			if _, discounted := req.discountedColIDs[idxCol.ID]; discounted {
				continue
			}
			if _, found := req.interestingColIDs[idxCol.ID]; found {
				return idxCol.ID
			}
		}
	}
	return -1
}

// calculateScoreFromCoverage calculates a ranking score using already-computed coverage information.
// This avoids re-iterating through index columns.
func calculateScoreFromCoverage(info indexWithScore, totalColumns int, isSingleScan bool) int {
	score := 0

	// Score for interesting column coverage
	score += info.interestingCount * 10

	// Bonus for consecutive interesting columns from start (critical for index usage)
	// Consecutive columns are much more valuable than scattered matches
	// Index on (a,b,c,d) with interesting columns a, b, c can use first 3 columns
	// But with interesting columns a, d, can only use first 1 column
	score += len(info.consecutiveColumnIDs) * 10

	// Bonus if the index is covering all interesting columns
	if totalColumns > 0 && info.interestingCount == totalColumns {
		score += 10
	}

	// Bonus for single-scan (covering index without table lookups)
	if isSingleScan {
		score += 20
	}

	return score
}
