// Copyright 2024 PingCAP, Inc.
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
	"sort"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/stretchr/testify/require"
)

func createTestIndex(name string, columns ...string) *model.IndexInfo {
	idx := &model.IndexInfo{
		Name: ast.NewCIStr(name),
	}
	for _, col := range columns {
		idx.Columns = append(idx.Columns, &model.IndexColumn{
			Name: ast.NewCIStr(col),
		})
	}
	return idx
}

func createTestPath(index *model.IndexInfo, eqOrInCount int, tableFilters, indexFilters []string, isSingleScan, forced bool) *util.AccessPath {
	path := &util.AccessPath{
		Index:           index,
		EqOrInCondCount: eqOrInCount,
		TableFilters:    make([]expression.Expression, len(tableFilters)),
		IndexFilters:    make([]expression.Expression, len(indexFilters)),
		IsSingleScan:    isSingleScan,
		Forced:          forced,
	}
	return path
}

func createTablePath() *util.AccessPath {
	return &util.AccessPath{
		Index:           nil, // Table path has nil index
		IsIntHandlePath: true,
	}
}

func TestPruneIndexesByPrefixAndEqOrInCondCount(t *testing.T) {
	tests := []struct {
		name          string
		paths         []*util.AccessPath
		maxEqOrIn     int
		numTabFilters int
		numIdxFilters int
		expectedCount int
		expectedNames []string
		description   string
	}{
		{
			name: "Perfect covering index optimization",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{}, []string{}, true, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_ac", "a", "c"), 2, []string{"filter1"}, []string{}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 0,
			numIdxFilters: 0,
			expectedCount: 1,
			expectedNames: []string{"idx_abc"},
			description:   "Should keep only the perfect covering index (maxEqOrIn=3, no filters)",
		},
		{
			name: "Perfect covering index with forced index",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{}, []string{}, true, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_forced", "a", "d"), 2, []string{"filter1"}, []string{}, false, true),
			},
			maxEqOrIn:     3,
			numTabFilters: 0,
			numIdxFilters: 0,
			expectedCount: 2,
			expectedNames: []string{"idx_abc", "idx_forced"},
			description:   "Should keep perfect covering index and forced index",
		},
		{
			name: "Perfect covering index with table path",
			paths: []*util.AccessPath{
				createTablePath(),
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{}, []string{}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 0,
			numIdxFilters: 0,
			expectedCount: 2,
			expectedNames: []string{"", "idx_abc"}, // Empty string for table path
			description:   "Should keep table path and perfect covering index",
		},
		{
			name: "No perfect covering index - fallback to normal pruning",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{"filter1"}, []string{}, false, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{"filter1"}, []string{}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 1,
			numIdxFilters: 0,
			expectedCount: 1,
			expectedNames: []string{"idx_abc"},
			description:   "Should prune prefix indexes when no perfect covering index exists",
		},
		{
			name: "Single scan preservation",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{}, []string{}, true, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{}, []string{}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 1,
			numIdxFilters: 0,
			expectedCount: 2,
			expectedNames: []string{"idx_ab", "idx_abc"},
			description:   "Should preserve single scan index even if it's a prefix of another",
		},
		{
			name: "Single scan preservation with eqOrInCount > 1",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{}, []string{}, true, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{}, []string{}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 1,
			numIdxFilters: 0,
			expectedCount: 2,
			expectedNames: []string{"idx_ab", "idx_abc"},
			description:   "Should preserve single scan index when eqOrInCount > 1",
		},
		{
			name: "Single scan not preserved when eqOrInCount = 1",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, true, false),
				createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{}, []string{}, false, false),
			},
			maxEqOrIn:     2,
			numTabFilters: 1,
			numIdxFilters: 0,
			expectedCount: 1,
			expectedNames: []string{"idx_ab"},
			description:   "Should prune single scan index when eqOrInCount = 1",
		},
		{
			name: "Different column sequences",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_ba", "b", "a"), 2, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{}, []string{}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 1,
			numIdxFilters: 0,
			expectedCount: 1,
			expectedNames: []string{"idx_abc"},
			description:   "Should prune indexes with same columns in different sequences",
		},
		{
			name:          "Empty paths",
			paths:         []*util.AccessPath{},
			maxEqOrIn:     0,
			numTabFilters: 0,
			numIdxFilters: 0,
			expectedCount: 0,
			expectedNames: []string{},
			description:   "Should handle empty paths",
		},
		{
			name: "Single path",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
			},
			maxEqOrIn:     1,
			numTabFilters: 0,
			numIdxFilters: 0,
			expectedCount: 1,
			expectedNames: []string{"idx_a"},
			description:   "Should return single path unchanged",
		},
		{
			name: "Multiple perfect covering indexes",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_abd", "a", "b", "d"), 3, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{"filter1"}, []string{}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 0,
			numIdxFilters: 0,
			expectedCount: 2,
			expectedNames: []string{"idx_abc", "idx_abd"},
			description:   "Should keep all perfect covering indexes",
		},
		{
			name: "Perfect covering index with index filters",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{}, []string{"idx_filter"}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 0,
			numIdxFilters: 1,
			expectedCount: 1,
			expectedNames: []string{"idx_abc"},
			description:   "Should keep the index with higher eqOrInCondCount when no perfect covering index exists",
		},
		{
			name: "Perfect covering index with table filters",
			paths: []*util.AccessPath{
				createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
				createTestPath(createTestIndex("idx_abc", "a", "b", "c"), 3, []string{"tab_filter"}, []string{}, false, false),
			},
			maxEqOrIn:     3,
			numTabFilters: 1,
			numIdxFilters: 0,
			expectedCount: 1,
			expectedNames: []string{"idx_abc"},
			description:   "Should keep the index with higher eqOrInCondCount when no perfect covering index exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pruneIndexesByPrefixAndEqOrInCondCount(tt.paths, tt.maxEqOrIn, tt.numTabFilters, tt.numIdxFilters)

			require.Equal(t, tt.expectedCount, len(result), tt.description)

			// Check that the expected indexes are present
			resultNames := make([]string, 0, len(result))
			for _, path := range result {
				if path.Index == nil {
					resultNames = append(resultNames, "") // Table path
				} else {
					resultNames = append(resultNames, path.Index.Name.O)
				}
			}

			// Sort both slices for comparison
			sort.Strings(resultNames)
			sort.Strings(tt.expectedNames)
			require.Equal(t, tt.expectedNames, resultNames, tt.description)
		})
	}
}

func TestPruneIndexesByPrefixAndEqOrInCondCountPerformance(t *testing.T) {
	// Test performance optimization with many indexes
	t.Run("Performance with perfect covering index", func(t *testing.T) {
		// Create many indexes to test performance
		paths := make([]*util.AccessPath, 0, 100)

		// Add a perfect covering index
		paths = append(paths, createTestPath(createTestIndex("idx_perfect", "a", "b", "c"), 3, []string{}, []string{}, false, false))

		// Add many non-optimal indexes
		for i := 0; i < 99; i++ {
			paths = append(paths, createTestPath(
				createTestIndex("idx_"+string(rune('a'+i)), "a", "b"),
				2,
				[]string{"filter"},
				[]string{},
				false,
				false,
			))
		}

		result := pruneIndexesByPrefixAndEqOrInCondCount(paths, 3, 0, 0)

		// Should only keep the perfect covering index
		require.Equal(t, 1, len(result))
		require.Equal(t, "idx_perfect", result[0].Index.Name.O)
	})

	t.Run("Performance without perfect covering index", func(t *testing.T) {
		// Create many indexes without a perfect covering index
		paths := make([]*util.AccessPath, 0, 100)

		// Add many indexes with different eqOrInCondCount to trigger pruning
		for i := 0; i < 50; i++ {
			paths = append(paths, createTestPath(
				createTestIndex("idx_"+string(rune('a'+i)), "a", "b"),
				1,
				[]string{"filter"},
				[]string{},
				false,
				false,
			))
		}
		for i := 50; i < 100; i++ {
			paths = append(paths, createTestPath(
				createTestIndex("idx_"+string(rune('a'+i)), "a", "b", "c"),
				2,
				[]string{"filter"},
				[]string{},
				false,
				false,
			))
		}

		result := pruneIndexesByPrefixAndEqOrInCondCount(paths, 2, 1, 0)

		// Should apply normal pruning logic and remove some indexes
		require.Less(t, len(result), len(paths))
	})
}

func TestPruneIndexesByPrefixAndEqOrInCondCountEdgeCases(t *testing.T) {
	t.Run("Nil index in path", func(t *testing.T) {
		paths := []*util.AccessPath{
			{Index: nil, EqOrInCondCount: 0, IsIntHandlePath: true}, // Table path
			createTestPath(createTestIndex("idx_a", "a"), 1, []string{}, []string{}, false, false),
		}

		result := pruneIndexesByPrefixAndEqOrInCondCount(paths, 1, 0, 0)

		// Should handle nil index gracefully and preserve table path
		require.Equal(t, 2, len(result))
	})

	t.Run("Zero maxEqOrIn", func(t *testing.T) {
		paths := []*util.AccessPath{
			createTestPath(createTestIndex("idx_a", "a"), 0, []string{}, []string{}, false, false),
		}

		result := pruneIndexesByPrefixAndEqOrInCondCount(paths, 0, 0, 0)

		// Should handle zero maxEqOrIn
		require.Equal(t, 1, len(result))
	})

	t.Run("Large number of filters", func(t *testing.T) {
		paths := []*util.AccessPath{
			createTestPath(createTestIndex("idx_a", "a"), 1, []string{"f1", "f2", "f3"}, []string{}, false, false),
			createTestPath(createTestIndex("idx_ab", "a", "b"), 2, []string{"f1"}, []string{}, false, false),
		}

		result := pruneIndexesByPrefixAndEqOrInCondCount(paths, 2, 1, 0)

		// Should prefer index with fewer filters
		require.Equal(t, 1, len(result))
		require.Equal(t, "idx_ab", result[0].Index.Name.O)
	})
}
