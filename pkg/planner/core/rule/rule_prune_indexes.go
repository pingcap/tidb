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

	totalWhereColumns := 0
	totalOrderingColumns := 0
	totalJoinColumns := 0

	// Build ID-based lookup maps for column matching
	joinColIDs := make(map[int64]struct{}, len(joinColumns))
	for _, col := range joinColumns {
		joinColIDs[col.ID] = struct{}{}
		totalJoinColumns++
	}
	// NOTE: Where columns may also be included in join columns, so that are excluded here to avoid double-counting
	// TO-DO: Consider refactoring the way columns are extracted from the predicate columns to avoid this risk.
	// If a join column has never appeared in a query as a local where column, then it will be correctly
	// identified as a join column only. But if a join column has also appeared in a local where column,
	// then it will be identified as both a join column and a where column in the current implementation.
	whereColIDs := make(map[int64]struct{}, len(whereColumns))
	for _, col := range whereColumns {
		// Skip if already in joinColIDs to avoid double-counting
		if _, exists := joinColIDs[col.ID]; !exists {
			whereColIDs[col.ID] = struct{}{}
			totalWhereColumns++
		}
	}
	orderingColIDs := make(map[int64]struct{}, len(orderingColumns))
	for _, col := range orderingColumns {
		// Skip if already in whereColIDs to avoid double-counting
		if _, exists := whereColIDs[col.ID]; !exists {
			orderingColIDs[col.ID] = struct{}{}
			totalOrderingColumns++
		}
	}
	// Calculate total coverage as a single table index
	totalLocalRequiredCols := totalWhereColumns + totalOrderingColumns
	// Calculate total coverage as a join table index
	totalJoinRequiredCols := totalJoinColumns
	if totalJoinRequiredCols > 0 {
		totalJoinRequiredCols += totalWhereColumns
	}
	maxConsecutiveWhere, maxConsecutiveJoin := 0, 0

	// maxIndexes allows a minimum number of indexes to be kept regardless of threshold.
	// max/minToKeep only calculate index plans to keep in addition to table plans.
	// This avoids extreme pruning when threshold is very low (or even zero).
	const maxIndexes = 10
	maxToKeep := max(maxIndexes, threshold)
	minToKeep := max(1, min(maxIndexes, threshold))
	// Prepare lists to hold indexes with different levels of coverage
	perfectCoveringIndexes := make([]indexWithScore, 0, maxIndexes) // Indexes covering all required columns
	preferredIndexes := make([]indexWithScore, 0, maxIndexes)       // "Next" best indexes with preferable coveraage
	tablePaths := make([]*util.AccessPath, 0, 1)                    // Usually just one table path

	lenPerfectCoveringIndexes, lenPreferredIndexes := 0, 0
	hasSingleScan := false
	for _, path := range paths {
		if path.IsTablePath() {
			tablePaths = append(tablePaths, path)
			continue
		}

		// If we have forced paths, we shouldn't prune any paths.
		// Hints processing should have already done the pruning.
		// It means we may have entered this function due to a low threshold.
		if path.Forced {
			return paths
		}

		// Count how many WHERE, join and ordering columns are covered by this index
		// Also track consecutive matches from the start (most valuable for index usage)
		coveredWhereCount := 0
		coveredOrderingCount := 0
		coveredJoinCount := 0
		consecutiveWhereCount := 0
		consecutiveOrderingCount := 0
		consecutiveJoinCount := 0

		skipThisIndex := false

		for i, idxCol := range path.FullIdxCols {
			idxColID := idxCol.ID

			// Check if this index column matches a WHERE column
			if totalWhereColumns > 0 {
				if _, found := whereColIDs[idxColID]; found {
					coveredWhereCount++
					// Track consecutive matches from start of index
					if i == consecutiveWhereCount {
						consecutiveWhereCount++
					}
					// Also increment consecutive join count if there has already been at least one
					// consecutive join match, and the current index column position matches the
					// sum of consecutive join and where matches.
					if consecutiveJoinCount > 0 && i == consecutiveJoinCount+consecutiveWhereCount {
						consecutiveJoinCount++
					}
					continue // Move to next index column - since a column can only appear once
				}
			}
			// Check if this index column matches a join column
			if totalJoinColumns > 0 {
				if _, found := joinColIDs[idxColID]; found {
					coveredJoinCount++
					// Track consecutive matches from start of index
					if i == consecutiveJoinCount+consecutiveWhereCount {
						consecutiveJoinCount++
					}
					continue // Move to next index column - since a column can only appear once
				}
			}

			// Check if this index column matches an ordering column (ORDER BY or MIN/MAX)
			if totalOrderingColumns > 0 {
				if _, found := orderingColIDs[idxColID]; found {
					coveredOrderingCount++
					// Track consecutive matches from where column matches
					if i == consecutiveOrderingCount+consecutiveWhereCount {
						consecutiveOrderingCount++
					}
					continue
				}
			}
			// If we have enough perfect covering indexes, and we've reached here then
			// the index columns are not a match. Don't continue with this index.
			if lenPerfectCoveringIndexes >= minToKeep && (i == 0 || hasSingleScan) && i < max(maxConsecutiveWhere, maxConsecutiveJoin) {
				skipThisIndex = true
				break
			}
		}

		if skipThisIndex {
			continue
		}
		// Calculate this plans totals
		totalLocalCovered := coveredWhereCount + coveredOrderingCount
		totalJoinCovered := coveredWhereCount + coveredJoinCount
		totalConsecutive := consecutiveWhereCount + consecutiveOrderingCount + consecutiveJoinCount
		maxListLength := max(lenPerfectCoveringIndexes, lenPreferredIndexes)

		if totalLocalCovered >= totalLocalRequiredCols && totalJoinCovered >= totalJoinRequiredCols {
			path.IsSingleScan = ds.IsSingleScan(path.FullIdxCols, path.FullIdxColLens)
		}
		if maxListLength >= minToKeep && hasSingleScan && !path.IsSingleScan &&
			consecutiveWhereCount <= maxConsecutiveWhere && consecutiveJoinCount <= maxConsecutiveJoin {
			continue
		}
		// Perfect covering: covers ALL required columns AND has consecutive matches from start
		if ((totalLocalCovered == totalLocalRequiredCols && totalLocalRequiredCols > 0) ||
			(totalJoinCovered == totalJoinRequiredCols && totalJoinRequiredCols > 0)) &&
			(totalConsecutive > 0 || path.IsSingleScan) {
			perfectCoveringIndexes = append(perfectCoveringIndexes, indexWithScore{
				path:                     path,
				whereCount:               coveredWhereCount,
				orderingCount:            coveredOrderingCount,
				joinCount:                coveredJoinCount,
				consecutiveWhereCount:    consecutiveWhereCount,
				consecutiveJoinCount:     consecutiveJoinCount,
				consecutiveOrderingCount: consecutiveOrderingCount,
			})
			maxConsecutiveWhere = max(maxConsecutiveWhere, consecutiveWhereCount)
			maxConsecutiveJoin = max(maxConsecutiveJoin, consecutiveJoinCount)
			lenPerfectCoveringIndexes = len(perfectCoveringIndexes)
			if path.IsSingleScan {
				hasSingleScan = true
			}
			continue
		}

		// Preferred: has meaningful coverage
		hasGoodCoverage := false

		if maxListLength >= maxToKeep {
			if (consecutiveWhereCount > maxConsecutiveWhere && maxConsecutiveWhere > 0) || (consecutiveJoinCount > maxConsecutiveJoin && maxConsecutiveJoin > 0) {
				hasGoodCoverage = true
			}
		} else if maxListLength >= minToKeep {
			if (consecutiveWhereCount >= maxConsecutiveWhere && maxConsecutiveWhere > 0) || (consecutiveJoinCount >= maxConsecutiveJoin && maxConsecutiveJoin > 0) {
				hasGoodCoverage = true
			}
			if consecutiveWhereCount >= totalWhereColumns && totalWhereColumns > 1 {
				hasGoodCoverage = true
			}
			// consecutiveJoinCount can be greater than totalJoinColumns because
			// we include consecutive where and join columns - since both are available
			// for index join matching.
			if consecutiveJoinCount >= totalJoinColumns && totalJoinColumns > 0 {
				hasGoodCoverage = true
			}
			if consecutiveOrderingCount >= totalOrderingColumns && totalOrderingColumns > 0 {
				hasGoodCoverage = true
			}
		} else {
			if totalConsecutive > 0 {
				if lenPreferredIndexes < minToKeep ||
					(totalLocalCovered > 1 || totalJoinCovered > 1) {
					hasGoodCoverage = true
				}
			}
			if coveredWhereCount >= totalWhereColumns && totalWhereColumns > 0 {
				hasGoodCoverage = true
			}
		}

		if hasGoodCoverage {
			preferredIndexes = append(preferredIndexes, indexWithScore{
				path:                     path,
				whereCount:               coveredWhereCount,
				orderingCount:            coveredOrderingCount,
				joinCount:                coveredJoinCount,
				consecutiveJoinCount:     consecutiveJoinCount,
				consecutiveWhereCount:    consecutiveWhereCount,
				consecutiveOrderingCount: consecutiveOrderingCount,
			})
			lenPreferredIndexes = len(preferredIndexes)
		}
	}

	// Build result with priority: table paths, perfect covering, then preferred
	result := make([]*util.AccessPath, 0, maxIndexes)

	// CRITICAL: Always include table paths - this is mandatory for correctness
	result = append(result, tablePaths...)

	// Only sort perfect covering indexes if we have more than we can use
	// If we'll keep all of them, no need to sort
	maxScoreToKeep := 0
	if len(perfectCoveringIndexes) > maxToKeep {
		// Score perfectCoveringIndexes the same way as preferred, but include isSingleScan bonus
		slices.SortFunc(perfectCoveringIndexes, func(a, b indexWithScore) int {
			scoreA := calculateScoreFromCoverage(a, totalWhereColumns, totalJoinColumns, totalOrderingColumns, a.path.IsSingleScan)
			scoreB := calculateScoreFromCoverage(b, totalWhereColumns, totalJoinColumns, totalOrderingColumns, b.path.IsSingleScan)
			if scoreA != scoreB {
				return scoreB - scoreA
			}
			// Tie-breaker: shorter index first
			return len(a.path.Index.Columns) - len(b.path.Index.Columns)
		})
		// Track the max score from the best perfect covering index
		maxScoreToKeep = calculateScoreFromCoverage(perfectCoveringIndexes[0], totalWhereColumns, totalJoinColumns, totalOrderingColumns, perfectCoveringIndexes[0].path.IsSingleScan)
	}

	// Add perfect covering indexes (prefer these over preferred)
	// Add as many index paths (to the result containing table paths) as per maxToKeep.
	remaining := maxToKeep
	if remaining > 0 && len(perfectCoveringIndexes) > 0 {
		toAdd := min(remaining, len(perfectCoveringIndexes))
		for _, idxWithScore := range perfectCoveringIndexes[:toAdd] {
			result = append(result, idxWithScore.path)
			// Track max score if not already set
			if maxScoreToKeep == 0 {
				score := calculateScoreFromCoverage(idxWithScore, totalWhereColumns, totalJoinColumns, totalOrderingColumns, idxWithScore.path.IsSingleScan)
				maxScoreToKeep = max(maxScoreToKeep, score)
			}
		}
		remaining -= toAdd
	}

	// Add preferred indexes if we still have room (extract paths from indexWithScore)
	if remaining > 0 && len(preferredIndexes) > 0 {
		// Filter out indexes that are 50% below the max score before sorting
		minScoreThreshold := maxScoreToKeep / 2
		if maxScoreToKeep > 0 {
			filtered := make([]indexWithScore, 0, len(preferredIndexes))
			for _, idxWithScore := range preferredIndexes {
				score := calculateScoreFromCoverage(idxWithScore, totalWhereColumns, totalJoinColumns, totalOrderingColumns, idxWithScore.path.IsSingleScan)
				if score >= minScoreThreshold {
					filtered = append(filtered, idxWithScore)
				}
			}
			preferredIndexes = filtered
		}

		// Only sort if we have more preferred indexes than remaining slots
		// If we'll keep all of them, no need to sort
		if len(preferredIndexes) > remaining {
			slices.SortFunc(preferredIndexes, func(a, b indexWithScore) int {
				// Calculate scores using pre-computed coverage info
				scoreA := calculateScoreFromCoverage(a, totalWhereColumns, totalJoinColumns, totalOrderingColumns, a.path.IsSingleScan)
				scoreB := calculateScoreFromCoverage(b, totalWhereColumns, totalJoinColumns, totalOrderingColumns, b.path.IsSingleScan)
				// Higher score is better, so reverse the comparison
				if scoreA != scoreB {
					return scoreB - scoreA
				}
				// If same score, prefer shorter index
				return len(a.path.Index.Columns) - len(b.path.Index.Columns)
			})
		}

		toAdd := min(remaining, len(preferredIndexes))
		for _, idxWithScore := range preferredIndexes[:toAdd] {
			result = append(result, idxWithScore.path)
		}
	}

	// Safety check: if we ended up with nothing, return the original paths
	// This prevents accidentally pruning everything
	if len(result) == 0 {
		return paths
	}

	// Additional safety: if we only have table paths and no indexes at all, keep original
	// This might happen if no indexes match, but we should still consider all indexes
	if len(result) == len(tablePaths) && len(perfectCoveringIndexes) == 0 && len(preferredIndexes) == 0 {
		// We pruned ALL indexes - this is probably too aggressive, keep original
		return paths
	}

	return result
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
