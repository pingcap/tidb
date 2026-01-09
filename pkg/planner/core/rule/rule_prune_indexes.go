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
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
)

const (
	// defaultMaxIndexes is the default maximum number of indexes to keep when pruning.
	// This prevents overly aggressive pruning when the threshold is small.
	defaultMaxIndexes = 10
)

// indexWithScore stores an access path along with its coverage scores for ranking.
type indexWithScore struct {
	path                 *util.AccessPath
	interestingCount     int     // Total number of interesting columns covered
	consecutiveCount     int     // Consecutive interesting columns from start of index
	consecutiveColumnIDs []int64 // IDs of consecutive columns (for detecting different orderings)
}

// columnRequirements holds the column maps and totals needed for index pruning.
type columnRequirements struct {
	interestingColIDs map[int64]struct{}
	totalColumns      int
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
func PruneIndexesByWhereAndOrder(ds *logicalop.DataSource, paths []*util.AccessPath, interestingColumns []*expression.Column, threshold int) []*util.AccessPath {
	if len(paths) <= 1 {
		return paths
	}

	// Build column ID maps and calculate totals
	req := buildColumnRequirements(interestingColumns)

	totalPathCount := len(paths)

	// If totalPathCount <= threshold, we should keep all indexes with score > 0
	// Which means only prune with score ==0
	// Only prune with score > 0 when we have more index paths than the threshold
	var maxToKeep int
	if totalPathCount > threshold && threshold > 0 {
		maxToKeep = max(threshold, defaultMaxIndexes) // Avoid being too aggressive when threshold is small
	} else {
		// When not pruning (len(paths) < threshold), set maxToKeep to keep all indexes with score > 0
		maxToKeep = totalPathCount
	}

	preferredIndexes := make([]indexWithScore, 0, maxToKeep)
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

	// Track consecutive column orderings to ensure we keep indexes with different orderings
	consecutiveOrderings := make(map[string]struct{})

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

		path.IsSingleScan = ds.IsSingleScan(path.FullIdxCols, path.FullIdxColLens)

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
		// or fix control. We still apply some filtering (consecutiveCount > 0 OR other coverage)
		// to avoid keeping completely useless indexes, but we're more lenient than normal pruning.
		if preferMerge && !hasSpecifiedIndexes {
			// When IndexMerge is preferred without specific index names, keep any index with coverage
			if idxScore.consecutiveCount > 0 || path.IsSingleScan || idxScore.interestingCount > 0 {
				preferredIndexes = append(preferredIndexes, idxScore)
				continue
			}
		}

		// Add to preferred indexes if it has any coverage or is a covering scan
		shouldAdd := idxScore.consecutiveCount > 0 || path.IsSingleScan || idxScore.interestingCount > 0
		if shouldAdd {
			// Check if this index has a different consecutive ordering than we've seen
			orderingKey := buildOrderingKey(idxScore.consecutiveColumnIDs)
			if idxScore.consecutiveCount > 0 {
				if _, seen := consecutiveOrderings[orderingKey]; !seen {
					// This is a new ordering, keep it
					preferredIndexes = append(preferredIndexes, idxScore)
					consecutiveOrderings[orderingKey] = struct{}{}
				} else {
					// We've seen this ordering before, but still consider the index
					// if it has good coverage or is a covering scan
					if idxScore.interestingCount > 0 || path.IsSingleScan {
						preferredIndexes = append(preferredIndexes, idxScore)
					}
				}
			} else {
				// No consecutive columns, but still add if it has coverage
				preferredIndexes = append(preferredIndexes, idxScore)
			}
		}
	}

	// Build final result by sorting and selecting top indexes
	result := buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths, preferredIndexes, maxToKeep, req)

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

// buildColumnRequirements builds column ID maps for efficient lookup and calculates totals.
func buildColumnRequirements(interestingColumns []*expression.Column) columnRequirements {
	req := columnRequirements{
		interestingColIDs: make(map[int64]struct{}, len(interestingColumns)),
	}

	// Build interesting column IDs
	for _, col := range interestingColumns {
		req.interestingColIDs[col.ID] = struct{}{}
		req.totalColumns++
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
	key := ""
	for i, id := range columnIDs {
		if i > 0 {
			key += ","
		}
		key += fmt.Sprintf("%d", id)
	}
	return key
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

		// Check if this index column matches an interesting column
		if _, found := req.interestingColIDs[idxColID]; found {
			score.interestingCount++
			// Track consecutive columns from the start of the index
			if i == score.consecutiveCount {
				score.consecutiveCount++
				score.consecutiveColumnIDs = append(score.consecutiveColumnIDs, idxColID)
			}
		} else {
			// Once we hit a non-interesting column, stop tracking consecutive columns
			// (we only care about consecutive matches from the start)
			// If we've already started tracking consecutive columns and hit a non-interesting one, break
			if score.consecutiveCount > 0 && i >= score.consecutiveCount {
				break
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
		score := calculateScoreFromCoverage(candidate, req.totalColumns, candidate.path.IsSingleScan)
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
			totalConsecutive: candidate.consecutiveCount,
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

func buildFinalResult(tablePaths, mvIndexPaths, indexMergeIndexPaths []*util.AccessPath, preferredIndexes []indexWithScore, maxToKeep int, req columnRequirements) []*util.AccessPath {
	result := make([]*util.AccessPath, 0, defaultMaxIndexes)

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

	preferredScored := scoreAndSort(preferredIndexes, req)

	remaining := maxToKeep
	hasNonZeroScore := false

	if len(preferredScored) > 0 && remaining > 0 {
		for _, entry := range preferredScored {
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

	return result
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
	score += info.consecutiveCount * 10

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
