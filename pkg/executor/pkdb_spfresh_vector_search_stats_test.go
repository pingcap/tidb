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

package executor

import (
	"testing"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestNewVectorSearchRuntimeStatsWithLegacyStats(t *testing.T) {
	stats := legacyVectorSearchRuntimeStatsForTest()

	require.Equal(t, legacyVectorSearchRuntimeStatsStringForTest(), stats.String())
}

func TestSPFreshVectorSearchRuntimeStatsMergeANN(t *testing.T) {
	stats := newVectorSearchRuntimeStats(&tipb.SPFreshSearchStats{
		Ann: &tipb.SPFreshANNStats{
			SearchMicros:                 1_000,
			PartitionsScanned:            2,
			CentroidsScanned:             3,
			LeafVectorsScanned:           4,
			RoughCandidates:              5,
			BoundPrunedCandidates:        6,
			RerankCandidates:             7,
			ExactEvaluated:               8,
			PartitionCacheHits:           9,
			PartitionCacheMisses:         10,
			PartitionCacheLookupMicros:   11_000,
			PartitionCacheMissLoadMicros: 12_000,
			TableLookupMicros:            13_000,
			RerankMicros:                 14_000,
		},
	})
	stats.Merge(newVectorSearchRuntimeStats(&tipb.SPFreshSearchStats{
		Ann: &tipb.SPFreshANNStats{
			ExactFallback:                true,
			SearchMicros:                 10_000,
			PartitionsScanned:            20,
			CentroidsScanned:             30,
			LeafVectorsScanned:           40,
			RoughCandidates:              50,
			BoundPrunedCandidates:        60,
			RerankCandidates:             70,
			ExactEvaluated:               80,
			PartitionCacheHits:           90,
			PartitionCacheMisses:         100,
			PartitionCacheLookupMicros:   110_000,
			PartitionCacheMissLoadMicros: 120_000,
			TableLookupMicros:            130_000,
			RerankMicros:                 140_000,
		},
	}))

	require.Equal(t, "ann: {exact_fallback: true, spfresh: {search: {time: 11ms, partitions_scanned: 22, centroids_scanned: 33, leaf_vectors_scanned: 44}, candidates: {rough: 55, bound_pruned: 66, rerank: 77, exact_evaluated: 88}, partition_cache: {hit: 99, miss: 110, lookup_time: 121ms, miss_load_time: 132ms}, table_lookup_time: 143ms, rerank_time: 154ms}}", stats.String())
}

func TestSPFreshVectorSearchRuntimeStatsStringWithZeroANN(t *testing.T) {
	stats := newVectorSearchRuntimeStats(&tipb.SPFreshSearchStats{Ann: &tipb.SPFreshANNStats{}})

	require.Equal(t, uint64(1), stats.annTaskCount)
	require.Equal(t, "ann: {exact_fallback: false, spfresh: {search: {time: 0s, partitions_scanned: 0, centroids_scanned: 0, leaf_vectors_scanned: 0}, candidates: {rough: 0, bound_pruned: 0, rerank: 0, exact_evaluated: 0}, partition_cache: {hit: 0, miss: 0, lookup_time: 0s, miss_load_time: 0s}, table_lookup_time: 0s, rerank_time: 0s}}", stats.String())
}

func TestSPFreshVectorSearchRuntimeStatsStringWithMixedANNPresence(t *testing.T) {
	for _, test := range []struct {
		name         string
		missingStats *tipb.SPFreshSearchStats
	}{
		{name: "ann absent", missingStats: &tipb.SPFreshSearchStats{}},
		{name: "stats absent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			newStats := legacySPFreshSearchStatsForTest()
			newStats.Ann = &tipb.SPFreshANNStats{
				ExactFallback: true,
				SearchMicros:  1,
			}
			stats := newVectorSearchRuntimeStats(newStats)
			stats.Merge(newVectorSearchRuntimeStats(test.missingStats))

			require.Equal(t, legacyVectorSearchRuntimeStatsStringForTest(), stats.String())
			require.NotContains(t, stats.String(), "ann: {")
		})
	}
}

func legacyVectorSearchRuntimeStatsForTest() *vectorSearchRuntimeStats {
	return newVectorSearchRuntimeStats(legacySPFreshSearchStatsForTest())
}

func legacySPFreshSearchStatsForTest() *tipb.SPFreshSearchStats {
	return &tipb.SPFreshSearchStats{
		ReadOnly:             true,
		OversampleFactor:     2.5,
		PartitionsScanned:    7,
		VectorsScanned:       123,
		PartitionCacheHits:   17,
		PartitionCacheMisses: 18,
		TableLookupKeys:      45,
		TableLookupBytes:     6789,
		PermitMicros:         11_000,
		ConfigMicros:         12_000,
		IndexOpenMicros:      13_000,
		SearchMicros:         14_000,
		TableLookupMicros:    15_000,
		TikvClientRpcCount:   16,
		TikvClientRpcMicros:  16_000,
	}
}

func legacyVectorSearchRuntimeStatsStringForTest() string {
	return "vector_search: {read_only: true, oversample: 2.5, partitions_scanned: 7, vectors_scanned: 123, partition_cache_hits: 17, partition_cache_misses: 18, table_lookup_keys: 45, table_lookup_bytes: 6789, permit: 11ms, config: 12ms, index_open: 13ms, search: 14ms, table_lookup: 15ms, tikv_client_rpc_count: 16, tikv_client_rpc: 16ms}"
}
