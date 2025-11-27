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
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
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

// ShouldPreferIndexMerge returns true if index merge should be preferred, either due to hints or fix control.
func ShouldPreferIndexMerge(ds *logicalop.DataSource) bool {
	return len(ds.IndexMergeHints) > 0 || fixcontrol.GetBoolWithDefault(
		ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(),
		fixcontrol.Fix52869,
		false,
	)
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

	// Build column ID maps and calculate totals
	req := buildColumnRequirements(whereColumns, orderingColumns, joinColumns)

	// If there are no required columns, only keep table paths and covering indexes (IsSingleScan)
	// since indexes are only useful for filtering/ordering, or as covering indexes to avoid table lookups
	//noRequiredColumns := len(whereColumns) == 0 && len(orderingColumns) == 0 && len(joinColumns) == 0

	totalPathCount := len(paths)

	// If totalPathCount <= threshold, we should keep all indexes with score > 0
	// Which means only prune with score ==0
	// Only prune with score > 0 when we have more index paths than the threshold
	const defaultMaxIndexes = 10
	var maxToKeep int
	if totalPathCount > threshold {
		// When pruning, use threshold if provided, otherwise use defaultMaxIndexes
		if threshold > 0 {
			maxToKeep = threshold
		} else {
			maxToKeep = defaultMaxIndexes
		}
	} else {
		// When not pruning (len(paths) < threshold), set maxToKeep to keep all indexes with score > 0
		maxToKeep = totalPathCount
	}

	preferredWhereIndexes := make([]indexWithScore, 0, maxToKeep)
	preferredJoinIndexes := make([]indexWithScore, 0, maxToKeep)
	tablePaths := make([]*util.AccessPath, 0, 1)
	mvIndexPaths := make([]*util.AccessPath, 0, 1)
	indexMergeIndexPaths := make([]*util.AccessPath, 0, 1)
	preferMerge := ShouldPreferIndexMerge(ds)

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

		// Skip paths with nil Index or uninitialized FullIdxCols
		if path.Index == nil || path.FullIdxCols == nil {
			continue
		}

		// Calculate coverage for this index
		idxScore := scoreIndexPath(path, req)

		// Calculate aggregate metrics
		//totalLocalCovered := idxScore.whereCount + idxScore.orderingCount
		//totalJoinCovered := idxScore.joinCount + idxScore.whereCount
		totalConsecutive := idxScore.consecutiveWhereCount + idxScore.consecutiveOrderingCount + idxScore.consecutiveJoinCount

		path.IsSingleScan = ds.IsSingleScan(path.FullIdxCols, path.FullIdxColLens)

		// If index merge is preferred, keep all indexes as index merge might need any of them
		if preferMerge && totalConsecutive > 0 {
			// Keep all indexes when index merge is preferred
			preferredWhereIndexes = append(preferredWhereIndexes, idxScore)
			continue
		}

		// Add to preferred indexes if it has any coverage or is a covering scan
		shouldAddWhere := totalConsecutive > 0 || path.IsSingleScan || idxScore.whereCount > 0
		if shouldAddWhere {
			preferredWhereIndexes = append(preferredWhereIndexes, idxScore)
		}

		if idxScore.consecutiveJoinCount > 0 {
			preferredJoinIndexes = append(preferredJoinIndexes, idxScore)
		}
	}

	// Build final result by sorting and selecting top indexes
	result := buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths, preferredWhereIndexes, preferredJoinIndexes, maxToKeep, req)

	// Safety check: if we ended up with nothing, return the original paths
	if len(result) == 0 {
		return paths
	}

	// Additional safety: if we only have table paths and MVIndex paths and no regular indexes, keep original
	if len(result) == len(tablePaths)+len(mvIndexPaths) && len(preferredWhereIndexes) == 0 && len(preferredJoinIndexes) == 0 {
		return paths
	}

	return result
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

	if path.FullIdxCols == nil {
		return score
	}

	for i, idxCol := range path.FullIdxCols {
		if idxCol == nil {
			continue
		}
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
// multi-value indexes, index merge indexes, and preferred indexes.
type scoredIndex struct {
	info                indexWithScore
	score               int
	columns             int
	hasConsecutiveWhere bool
	hasConsecutiveJoin  bool
	isSingleScan        bool
	totalConsecutive    int
}

func scoreAndSort(indexes []indexWithScore, req columnRequirements) []scoredIndex {
	if len(indexes) == 0 {
		return nil
	}
	scored := make([]scoredIndex, 0, len(indexes))
	for _, candidate := range indexes {
		score := calculateScoreFromCoverage(candidate, req.totalWhereColumns, req.totalJoinColumns, req.totalOrderingColumns, candidate.path.IsSingleScan)
		// Skip indexes with score == 0 as they don't provide any value
		if score == 0 {
			continue
		}
		cols := len(candidate.path.FullIdxCols)
		totalConsecutive := candidate.consecutiveWhereCount + candidate.consecutiveJoinCount + candidate.consecutiveOrderingCount
		scored = append(scored, scoredIndex{
			info:                candidate,
			score:               score,
			columns:             cols,
			hasConsecutiveWhere: candidate.consecutiveWhereCount > 0,
			hasConsecutiveJoin:  candidate.consecutiveJoinCount > 0,
			isSingleScan:        candidate.path.IsSingleScan,
			totalConsecutive:    totalConsecutive,
		})
	}
	slices.SortFunc(scored, func(a, b scoredIndex) int {
		if a.score != b.score {
			return b.score - a.score
		}
		if a.totalConsecutive != b.totalConsecutive {
			return b.totalConsecutive - a.totalConsecutive
		}
		if a.isSingleScan != b.isSingleScan {
			if a.isSingleScan {
				return -1
			}
			return 1
		}
		if a.totalConsecutive == 1 && a.columns != b.columns {
			return a.columns - b.columns
		}
		return 0
	})
	return scored
}

func buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths []*util.AccessPath, whereIndexes, joinIndexes []indexWithScore, maxToKeep int, req columnRequirements) []*util.AccessPath {
	const maxIndexes = 10
	result := make([]*util.AccessPath, 0, maxIndexes)

	// CRITICAL: Always include table paths - this is mandatory for correctness
	result = append(result, tablePaths...)
	// CRITICAL: Always include multi-value index paths - we do not have sufficient
	// information to determine if they should be pruned in this function.
	result = append(result, mvIndexPaths...)
	// CRITICAL: Always include indexes specified in IndexMerge hints - index merge needs them to build partial paths
	result = append(result, indexMergeIndexPaths...)

	if maxToKeep <= 0 {
		return result
	}

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

	whereScored := scoreAndSort(whereIndexes, req)
	joinScored := scoreAndSort(joinIndexes, req)

	remaining := maxToKeep
	hasNonZeroScore := false
	if len(whereScored) > 0 && remaining > 0 {
		for _, entry := range whereScored {
			path := entry.info.path
			if _, ok := added[path]; ok {
				continue
			}
			if remaining == 0 {
				break
			}
			// If we have at least 1 index with score > 0, don't append any with score == 0
			if hasNonZeroScore && entry.score == 0 {
				continue
			}
			result = append(result, path)
			added[path] = struct{}{}
			if entry.score > 0 {
				hasNonZeroScore = true
			}
			remaining--
		}
	}

	if len(joinScored) > 0 {
		maxJoinScore := joinScored[0].score
		bestJoinPath := joinScored[0].info.path
		if _, ok := added[bestJoinPath]; !ok {
			result = append(result, bestJoinPath)
			added[bestJoinPath] = struct{}{}
			if maxJoinScore > 0 {
				hasNonZeroScore = true
			}
			if remaining > 0 {
				remaining--
			}
		}
		if remaining > 0 {
			for idx := range joinScored {
				if idx == 0 {
					continue
				}
				entry := joinScored[idx]
				path := entry.info.path
				if _, ok := added[path]; ok {
					continue
				}
				if remaining == 0 {
					break
				}
				// If we have at least 1 index with score > 0, don't append any with score == 0
				if hasNonZeroScore && entry.score == 0 {
					continue
				}
				result = append(result, path)
				added[path] = struct{}{}
				if entry.score > 0 {
					hasNonZeroScore = true
				}
				remaining--
			}
		}
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
