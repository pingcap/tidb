// Copyright 2025 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
)

// indexWithScore stores an access path along with its coverage scores for ranking.
type indexWithScore struct {
	path                     *util.AccessPath
	whereCount               int
	joinCount                int
	orderingCount            int
	consecutiveWhereCount    int // Consecutive WHERE columns from start of index
	consecutiveJoinCount     int // Consecutive JOIN columns from start of index
	consecutiveOrderingCount int // Consecutive ordering columns from start of index
}

// columnRequirements holds the column maps and totals needed for index pruning.
type columnRequirements struct {
	whereColIDs            map[int64]struct{}
	joinColIDs             map[int64]struct{}
	orderingColIDs         map[int64]struct{}
	totalWhereColumns      int
	totalJoinColumns       int
	totalOrderingColumns   int
	totalLocalRequiredCols int // WHERE + ordering columns
	totalJoinRequiredCols  int // JOIN + WHERE columns
}

// PruneIndexesByWhereAndOrder prunes indexes based on their coverage of WHERE, join and ordering columns.
// It keeps the most promising indexes up to the threshold, prioritizing those that:
// 1. Cover more required columns (WHERE, JOIN, ORDER BY)
// 2. Have consecutive column matches from the index start (enabling index prefix usage)
// 3. Support single-scan (covering index without table lookups)
func PruneIndexesByWhereAndOrder(ds *logicalop.DataSource, paths []*util.AccessPath, whereColumns, orderingColumns, joinColumns []*expression.Column, threshold int) []*util.AccessPath {
	if len(paths) <= 1 {
		return paths
	}

	// If there are no required columns, don't prune - just return all paths
	if len(whereColumns) == 0 && len(orderingColumns) == 0 && len(joinColumns) == 0 {
		return paths
	}

	// Build column ID maps and calculate totals
	req := buildColumnRequirements(whereColumns, orderingColumns, joinColumns)

	// maxIndexes allows a minimum number of indexes to be kept regardless of threshold.
	// max/minToKeep only calculate index plans to keep in addition to table plans.
	// This avoids extreme pruning when threshold is very low (or even zero).
	const maxIndexes = 10
	maxToKeep := max(maxIndexes, threshold)
	minToKeep := max(1, min(maxIndexes, threshold))

	// Prepare lists to hold indexes with different levels of coverage
	perfectCoveringIndexes := make([]indexWithScore, 0, maxIndexes)
	preferredIndexes := make([]indexWithScore, 0, maxIndexes)
	tablePaths := make([]*util.AccessPath, 0, 1)

	maxConsecutiveWhere, maxConsecutiveJoin := 0, 0
	hasSingleScan := false

	// Categorize each index path
	for _, path := range paths {
		if path.IsTablePath() {
			tablePaths = append(tablePaths, path)
			continue
		}

		// If we have forced paths, we shouldn't prune any paths
		if path.Forced {
			return paths
		}

		// Calculate coverage for this index
		idxScore := scoreIndexPath(path, req)

		// Early skip for indexes that don't match any leading columns
		if len(perfectCoveringIndexes) >= minToKeep && (idxScore.consecutiveWhereCount == 0 && idxScore.consecutiveJoinCount == 0) {
			continue
		}

		// Calculate aggregate metrics
		totalLocalCovered := idxScore.whereCount + idxScore.orderingCount
		totalJoinCovered := idxScore.whereCount + idxScore.joinCount
		totalConsecutive := idxScore.consecutiveWhereCount + idxScore.consecutiveOrderingCount + idxScore.consecutiveJoinCount

		// Check if this is a covering index and set IsSingleScan
		if totalLocalCovered >= req.totalLocalRequiredCols && totalJoinCovered >= req.totalJoinRequiredCols {
			path.IsSingleScan = ds.IsSingleScan(path.FullIdxCols, path.FullIdxColLens)
		}

		// Skip non-single-scan indexes if we already have enough single-scan ones
		maxListLength := max(len(perfectCoveringIndexes), len(preferredIndexes))
		if maxListLength >= minToKeep && hasSingleScan && !path.IsSingleScan &&
			idxScore.consecutiveWhereCount <= maxConsecutiveWhere && idxScore.consecutiveJoinCount <= maxConsecutiveJoin {
			continue
		}

		// Categorize as perfect covering if it covers ALL required columns
		if ((totalLocalCovered == req.totalLocalRequiredCols && req.totalLocalRequiredCols > 0) ||
			(totalJoinCovered == req.totalJoinRequiredCols && req.totalJoinRequiredCols > 0)) &&
			(totalConsecutive > 0 || path.IsSingleScan) {
			perfectCoveringIndexes = append(perfectCoveringIndexes, idxScore)
			maxConsecutiveWhere = max(maxConsecutiveWhere, idxScore.consecutiveWhereCount)
			maxConsecutiveJoin = max(maxConsecutiveJoin, idxScore.consecutiveJoinCount)
			if path.IsSingleScan {
				hasSingleScan = true
			}
			continue
		}

		// Categorize as preferred if it has meaningful coverage
		if shouldKeepAsPreferred(idxScore, maxListLength, maxToKeep, minToKeep, maxConsecutiveWhere, maxConsecutiveJoin, req) {
			preferredIndexes = append(preferredIndexes, idxScore)
		}
	}

	// Build final result by sorting and selecting top indexes
	result := buildFinalResult(tablePaths, perfectCoveringIndexes, preferredIndexes, maxToKeep, req)

	// Safety check: if we ended up with nothing, return the original paths
	if len(result) == 0 {
		return paths
	}

	// Additional safety: if we only have table paths and no indexes, keep original
	if len(result) == len(tablePaths) && len(perfectCoveringIndexes) == 0 && len(preferredIndexes) == 0 {
		return paths
	}

	return result
}

// shouldKeepAsPreferred determines if an index should be kept in the preferred list
// based on its coverage characteristics and current list sizes.
func shouldKeepAsPreferred(idxScore indexWithScore, maxListLength, maxToKeep, minToKeep, maxConsecutiveWhere, maxConsecutiveJoin int, req columnRequirements) bool {
	if maxListLength >= maxToKeep {
		// At capacity - only keep if better than current best
		if (idxScore.consecutiveWhereCount > maxConsecutiveWhere && maxConsecutiveWhere > 0) ||
			(idxScore.consecutiveJoinCount > maxConsecutiveJoin && maxConsecutiveJoin > 0) {
			return true
		}
	} else if maxListLength >= minToKeep {
		// Have minimum - keep if meets or exceeds current best
		if (idxScore.consecutiveWhereCount >= maxConsecutiveWhere && maxConsecutiveWhere > 0) ||
			(idxScore.consecutiveJoinCount >= maxConsecutiveJoin && maxConsecutiveJoin > 0) {
			return true
		}
		if idxScore.consecutiveWhereCount >= req.totalWhereColumns && req.totalWhereColumns > 1 {
			return true
		}
		if idxScore.consecutiveJoinCount >= req.totalJoinColumns && req.totalJoinColumns > 0 {
			return true
		}
		if idxScore.consecutiveOrderingCount >= req.totalOrderingColumns && req.totalOrderingColumns > 0 {
			return true
		}
	} else {
		// Below minimum - more lenient acceptance criteria
		totalConsecutive := idxScore.consecutiveWhereCount + idxScore.consecutiveOrderingCount + idxScore.consecutiveJoinCount
		totalLocalCovered := idxScore.whereCount + idxScore.orderingCount
		totalJoinCovered := idxScore.whereCount + idxScore.joinCount

		if totalConsecutive > 0 && (maxListLength < minToKeep || totalLocalCovered > 1 || totalJoinCovered > 1) {
			return true
		}
		if idxScore.whereCount >= req.totalWhereColumns && req.totalWhereColumns > 0 {
			return true
		}
	}
	return false
}

// buildColumnRequirements builds column ID maps for efficient lookup and calculates totals.
// It handles deduplication to avoid double-counting columns that appear in multiple contexts.
func buildColumnRequirements(whereColumns, orderingColumns, joinColumns []*expression.Column) columnRequirements {
	req := columnRequirements{
		whereColIDs:    make(map[int64]struct{}, len(whereColumns)),
		joinColIDs:     make(map[int64]struct{}, len(joinColumns)),
		orderingColIDs: make(map[int64]struct{}, len(orderingColumns)),
	}

	// Build join column IDs first (highest priority for deduplication)
	for _, col := range joinColumns {
		req.joinColIDs[col.ID] = struct{}{}
		req.totalJoinColumns++
	}

	// Build WHERE column IDs (exclude join columns to avoid double-counting)
	for _, col := range whereColumns {
		if _, exists := req.joinColIDs[col.ID]; !exists {
			req.whereColIDs[col.ID] = struct{}{}
			req.totalWhereColumns++
		}
	}

	// Build ordering column IDs (exclude WHERE columns to avoid double-counting)
	for _, col := range orderingColumns {
		if _, exists := req.whereColIDs[col.ID]; !exists {
			req.orderingColIDs[col.ID] = struct{}{}
			req.totalOrderingColumns++
		}
	}

	// Calculate total coverage requirements
	req.totalLocalRequiredCols = req.totalWhereColumns + req.totalOrderingColumns
	req.totalJoinRequiredCols = req.totalJoinColumns
	if req.totalJoinRequiredCols > 0 {
		req.totalJoinRequiredCols += req.totalWhereColumns
	}

	return req
}

// scoreIndexPath calculates coverage metrics for a single index path.
func scoreIndexPath(path *util.AccessPath, req columnRequirements) indexWithScore {
	score := indexWithScore{path: path}

	for i, idxCol := range path.FullIdxCols {
		idxColID := idxCol.ID

		// Check if this index column matches a WHERE column
		if req.totalWhereColumns > 0 {
			if _, found := req.whereColIDs[idxColID]; found {
				score.whereCount++
				if i == score.consecutiveWhereCount {
					score.consecutiveWhereCount++
				}
				if score.consecutiveJoinCount > 0 && i == score.consecutiveJoinCount+score.consecutiveWhereCount {
					score.consecutiveJoinCount++
				}
				continue
			}
		}

		// Check if this index column matches a join column
		if req.totalJoinColumns > 0 {
			if _, found := req.joinColIDs[idxColID]; found {
				score.joinCount++
				if i == score.consecutiveJoinCount+score.consecutiveWhereCount {
					score.consecutiveJoinCount++
				}
				continue
			}
		}

		// Check if this index column matches an ordering column
		if req.totalOrderingColumns > 0 {
			if _, found := req.orderingColIDs[idxColID]; found {
				score.orderingCount++
				if i == score.consecutiveOrderingCount+score.consecutiveWhereCount {
					score.consecutiveOrderingCount++
				}
				continue
			}
		}
	}

	return score
}

// buildFinalResult sorts and selects the top indexes to keep, combining table paths,
// perfect covering indexes, and preferred indexes.
func buildFinalResult(tablePaths []*util.AccessPath, perfectCoveringIndexes, preferredIndexes []indexWithScore,
	maxToKeep int, req columnRequirements) []*util.AccessPath {
	const maxIndexes = 10
	result := make([]*util.AccessPath, 0, maxIndexes)

	// CRITICAL: Always include table paths - this is mandatory for correctness
	result = append(result, tablePaths...)

	// Sort and add perfect covering indexes
	maxScoreToKeep := 0
	if len(perfectCoveringIndexes) > maxToKeep {
		slices.SortFunc(perfectCoveringIndexes, func(a, b indexWithScore) int {
			return compareIndexScores(a, b, req)
		})
		maxScoreToKeep = calculateScoreFromCoverage(perfectCoveringIndexes[0], req.totalWhereColumns, req.totalJoinColumns, req.totalOrderingColumns, perfectCoveringIndexes[0].path.IsSingleScan)
	}

	remaining := maxToKeep
	if remaining > 0 && len(perfectCoveringIndexes) > 0 {
		toAdd := min(remaining, len(perfectCoveringIndexes))
		for _, idxWithScore := range perfectCoveringIndexes[:toAdd] {
			result = append(result, idxWithScore.path)
			if maxScoreToKeep == 0 {
				score := calculateScoreFromCoverage(idxWithScore, req.totalWhereColumns, req.totalJoinColumns, req.totalOrderingColumns, idxWithScore.path.IsSingleScan)
				maxScoreToKeep = max(maxScoreToKeep, score)
			}
		}
		remaining -= toAdd
	}

	// Filter and add preferred indexes
	if remaining > 0 && len(preferredIndexes) > 0 {
		minScoreThreshold := maxScoreToKeep / 2
		if maxScoreToKeep > 0 {
			filtered := make([]indexWithScore, 0, len(preferredIndexes))
			for _, idxWithScore := range preferredIndexes {
				score := calculateScoreFromCoverage(idxWithScore, req.totalWhereColumns, req.totalJoinColumns, req.totalOrderingColumns, idxWithScore.path.IsSingleScan)
				if score >= minScoreThreshold {
					filtered = append(filtered, idxWithScore)
				}
			}
			preferredIndexes = filtered
		}

		if len(preferredIndexes) > remaining {
			slices.SortFunc(preferredIndexes, func(a, b indexWithScore) int {
				return compareIndexScores(a, b, req)
			})
		}

		toAdd := min(remaining, len(preferredIndexes))
		for _, idxWithScore := range preferredIndexes[:toAdd] {
			result = append(result, idxWithScore.path)
		}
	}

	return result
}

// compareIndexScores compares two index scores for sorting (higher score first).
func compareIndexScores(a, b indexWithScore, req columnRequirements) int {
	scoreA := calculateScoreFromCoverage(a, req.totalWhereColumns, req.totalJoinColumns, req.totalOrderingColumns, a.path.IsSingleScan)
	scoreB := calculateScoreFromCoverage(b, req.totalWhereColumns, req.totalJoinColumns, req.totalOrderingColumns, b.path.IsSingleScan)
	if scoreA != scoreB {
		return scoreB - scoreA // Higher score first
	}
	// Tie-breaker: shorter index first
	return len(a.path.Index.Columns) - len(b.path.Index.Columns)
}

// calculateScoreFromCoverage calculates a ranking score using already-computed coverage information.
// This avoids re-iterating through index columns.
func calculateScoreFromCoverage(info indexWithScore, totalWhereColumns, totalJoinColumns, totalOrderingCols int, isSingleScan bool) int {
	score := 0

	// Score for WHERE column coverage
	score += info.whereCount * 10

	// Bonus for consecutive WHERE columns from start (critical for index usage)
	// Consecutive columns are much more valuable than scattered matches
	// Index on (a,b,c,d) with WHERE a=1 AND b=2 AND c=3 can use first 3 columns
	// But with WHERE a=1 AND d=4, can only use first 1 column
	score += info.consecutiveWhereCount * 10

	// Bonus if the index is covering all WHERE columns
	if info.whereCount == totalWhereColumns {
		score += 10
	}

	// Bonus for consecutive JOIN columns from start (critical for index usage)
	// Consecutive columns are much more valuable than scattered matches
	score += info.consecutiveJoinCount * 10

	// Bonus if the index is covering all WHERE columns
	if info.joinCount > 0 && info.joinCount == totalJoinColumns {
		score += 10
	}

	// NOTE: For ordering, we cannot guarantee that the presence of ordering
	// columns will always lead to a plan that doesn't need sort.
	// So we only give a bonus for consecutive ordering columns
	if info.consecutiveOrderingCount == totalOrderingCols {
		score += 10
	}
	// Bonus for single-scan
	if isSingleScan {
		score += 20
	}

	return score
}
