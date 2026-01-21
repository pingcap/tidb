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
type indexWithScore struct {
	path                 *util.AccessPath
	interestingCount     int     // Total number of interesting columns covered
	consecutiveColumnIDs []int64 // IDs of consecutive columns (for detecting different orderings)
}

// columnRequirements holds the column maps needed for index pruning.
type columnRequirements struct {
	interestingColIDs map[int64]struct{}
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
// The threshold controls the behavior:
// threshold = -1: disable pruning (handled by caller)
// threshold = 0: only prune indexes with no interesting columns (score == 0)
// threshold > 0: keep at least threshold indexes (but at least defaultMaxIndexes)
func PruneIndexesByWhereAndOrder(ds *logicalop.DataSource, paths []*util.AccessPath, interestingColumns []*expression.Column, threshold int) []*util.AccessPath {
	if len(paths) <= 1 {
		return paths
	}

	totalPathCount := len(paths)

	// If totalPathCount <= threshold or we disabled the prune, return directly.
	if threshold < 0 || threshold > totalPathCount {
		return paths
	}

	// Build column ID maps and calculate totals
	req := buildColumnRequirements(interestingColumns)

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
	result := buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths, preferredIndexes, maxToKeep, threshold, req)

	failpoint.InjectCall("InjectCheckForIndexPrune", result)

	// Safety check: if we ended up with nothing, return the original paths
	if len(result) == 0 {
		return paths
	}

	// Additional safety: if we only have table paths and MVIndex paths and no regular indexes, keep original
	if len(result) == len(tablePaths)+len(mvIndexPaths) && len(preferredIndexes) == 0 {
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
		builder.WriteString(fmt.Sprintf("%d", id))
	}
	return builder.String()
}

// scoreIndexPath calculates coverage metrics for a single index path.
// When FullIdxCols is nil (e.g., in static pruning mode), it uses path.Index.Columns
// and tableColumns to determine interesting columns. Note that consecutiveColumnIDs
// cannot be determined when FullIdxCols is nil, so it will remain empty.
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
			if _, found := req.interestingColIDs[ds.TableInfo.Columns[col.Offset].ID]; !found {
				return score
			}
		}
		// Pre check for partial index passed, continue to calculate the score.
	}

	if path.FullIdxCols != nil {
		// Normal path: use FullIdxCols which contains expression.Column with IDs
		for i, idxCol := range path.FullIdxCols {
			if idxCol == nil {
				continue
			}
			idxColID := idxCol.ID

			// Check if this index column matches an interesting column
			if _, found := req.interestingColIDs[idxColID]; found {
				score.interestingCount++
				// Track consecutive columns from the start of the index
				if i == len(score.consecutiveColumnIDs) {
					score.consecutiveColumnIDs = append(score.consecutiveColumnIDs, idxColID)
				}
			}
			// Note: We continue checking all columns to count all interesting columns,
			// even if they're not consecutive from the start. The consecutive tracking
			// will naturally stop once we hit a non-interesting column, since the condition
			// `i == len(score.consecutiveColumnIDs)` will no longer be true.
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
}

func scoreAndSort(indexes []indexWithScore, req columnRequirements) []scoredIndex {
	if len(indexes) == 0 {
		return nil
	}
	scored := make([]scoredIndex, 0, len(indexes))
	for _, candidate := range indexes {
		score := calculateScoreFromCoverage(candidate, len(req.interestingColIDs), candidate.path.IsSingleScan)
		// Skip indexes with score == 0 as they don't provide any value
		if score == 0 {
			continue
		}
		cols := len(candidate.path.FullIdxCols)
		scored = append(scored, scoredIndex{
			info:             candidate,
			score:            score,
			columns:          cols,
			isSingleScan:     candidate.path.IsSingleScan,
			totalConsecutive: len(candidate.consecutiveColumnIDs),
		})
	}
	slices.SortFunc(scored, func(a, b scoredIndex) int {
		// Tie-breaker: prefer indexes with higher score
		if a.score != b.score {
			return b.score - a.score
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
		// Tie-breaker: prefer indexes with fewer columns if
		// they have only 1 consecutive column.
		if a.totalConsecutive == 1 && a.columns != b.columns {
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

func buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths []*util.AccessPath, preferredIndexes []indexWithScore, maxToKeep, threshold int, req columnRequirements) []*util.AccessPath {
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

	// Prune all the path with 0 score.
	if threshold == 0 {
		for _, entry := range preferredScored {
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
	hasNonZeroScore          bool
	phase1Count              int
	seenConsecutiveColumnIDs map[int64]struct{}
	seenOrderingKeys         map[string]struct{}
}

func newIndexSelectionState(phase1Limit, maxToKeep int) *indexSelectionState {
	return &indexSelectionState{
		phase1Limit:              phase1Limit,
		remaining:                maxToKeep,
		seenConsecutiveColumnIDs: make(map[int64]struct{}),
		seenOrderingKeys:         make(map[string]struct{}),
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
		if state.hasNonZeroScore && entry.score == 0 {
			continue
		}

		shouldAdd := shouldAddIndex(entry, path, req, state)
		if shouldAdd {
			result = append(result, path)
			added[path] = struct{}{}
			if entry.score > 0 {
				state.hasNonZeroScore = true
			}
			state.remaining--
		}
	}
	return result
}

// shouldAddIndex determines if an index should be added based on phase and diversity rules
func shouldAddIndex(entry scoredIndex, path *util.AccessPath, req columnRequirements, state *indexSelectionState) bool {
	if state.phase1Count < state.phase1Limit {
		// Phase 1: Keep top threshold/2 based solely on score
		state.phase1Count++
		recordConsecutiveColumns(entry.info.consecutiveColumnIDs, state)
		return true
	}

	// Phase 2: Apply diversity rules
	hasConsecutive := len(entry.info.consecutiveColumnIDs) > 0
	if hasConsecutive {
		return shouldAddIndexWithConsecutive(entry.info.consecutiveColumnIDs, state)
	}

	return shouldAddIndexWithoutConsecutive(entry, path, req, state)
}

// recordConsecutiveColumns tracks consecutive column IDs and ordering keys
func recordConsecutiveColumns(consecutiveColumnIDs []int64, state *indexSelectionState) {
	for _, colID := range consecutiveColumnIDs {
		state.seenConsecutiveColumnIDs[colID] = struct{}{}
	}
	if len(consecutiveColumnIDs) > 0 {
		orderingKey := buildOrderingKey(consecutiveColumnIDs)
		state.seenOrderingKeys[orderingKey] = struct{}{}
	}
}

// shouldAddIndexWithConsecutive checks if an index with consecutive columns should be added in phase 2
func shouldAddIndexWithConsecutive(consecutiveColumnIDs []int64, state *indexSelectionState) bool {
	orderingKey := buildOrderingKey(consecutiveColumnIDs)
	if _, seen := state.seenOrderingKeys[orderingKey]; seen {
		return false
	}
	state.seenOrderingKeys[orderingKey] = struct{}{}
	recordConsecutiveColumns(consecutiveColumnIDs, state)
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
	if info.interestingCount == totalColumns {
		score += 10
	}

	// Bonus for single-scan (covering index without table lookups)
	if isSingleScan {
		score += 20
	}

	return score
}
