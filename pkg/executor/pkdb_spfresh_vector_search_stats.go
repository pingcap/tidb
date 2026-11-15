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
	"bytes"
	"strconv"

	"github.com/pingcap/tipb/go-tipb"
)

// spfreshANNRuntimeStats keeps a value copy of one or more protobuf reports.
// Durations remain in protocol microseconds until EXPLAIN formatting.
type spfreshANNRuntimeStats tipb.SPFreshANNStats

// newVectorSearchRuntimeStats records one successful SPFresh response. A
// missing ann message leaves annTaskCount unchanged so merged mixed-version
// responses do not expose a partial ANN report.
func newVectorSearchRuntimeStats(stats *tipb.SPFreshSearchStats) *vectorSearchRuntimeStats {
	runtimeStats := &vectorSearchRuntimeStats{taskCount: 1}
	if stats == nil {
		return runtimeStats
	}

	runtimeStats.ReadOnly = stats.ReadOnly
	runtimeStats.OversampleFactor = stats.OversampleFactor
	runtimeStats.PartitionsScanned = stats.PartitionsScanned
	runtimeStats.VectorsScanned = stats.VectorsScanned
	runtimeStats.PartitionCacheHits = stats.PartitionCacheHits
	runtimeStats.PartitionCacheMisses = stats.PartitionCacheMisses
	runtimeStats.TableLookupKeys = stats.TableLookupKeys
	runtimeStats.TableLookupBytes = stats.TableLookupBytes
	runtimeStats.PermitMicros = stats.PermitMicros
	runtimeStats.ConfigMicros = stats.ConfigMicros
	runtimeStats.IndexOpenMicros = stats.IndexOpenMicros
	runtimeStats.SearchMicros = stats.SearchMicros
	runtimeStats.TableLookupMicros = stats.TableLookupMicros
	runtimeStats.TiKVClientRPCCount = stats.TikvClientRpcCount
	runtimeStats.TiKVClientRPCMicros = stats.TikvClientRpcMicros

	ann := stats.GetAnn()
	if ann == nil {
		return runtimeStats
	}
	runtimeStats.annTaskCount = 1
	runtimeStats.ann = spfreshANNRuntimeStats(*ann)
	return runtimeStats
}

func (e *vectorSearchRuntimeStats) hasCompleteANNStats() bool {
	return e.taskCount > 0 && e.annTaskCount == e.taskCount
}

func (e *vectorSearchRuntimeStats) appendANNStats(buf *bytes.Buffer) {
	buf.WriteString("ann: {exact_fallback: ")
	buf.WriteString(strconv.FormatBool(e.ann.ExactFallback))
	buf.WriteString(", spfresh: {search: {time: ")
	buf.WriteString(formatVectorSearchDurationFromMicros(e.ann.SearchMicros))
	buf.WriteString(", partitions_scanned: ")
	buf.WriteString(strconv.FormatUint(e.ann.PartitionsScanned, 10))
	buf.WriteString(", centroids_scanned: ")
	buf.WriteString(strconv.FormatUint(e.ann.CentroidsScanned, 10))
	buf.WriteString(", leaf_vectors_scanned: ")
	buf.WriteString(strconv.FormatUint(e.ann.LeafVectorsScanned, 10))
	buf.WriteString("}, candidates: {rough: ")
	buf.WriteString(strconv.FormatUint(e.ann.RoughCandidates, 10))
	buf.WriteString(", bound_pruned: ")
	buf.WriteString(strconv.FormatUint(e.ann.BoundPrunedCandidates, 10))
	buf.WriteString(", rerank: ")
	buf.WriteString(strconv.FormatUint(e.ann.RerankCandidates, 10))
	buf.WriteString(", exact_evaluated: ")
	buf.WriteString(strconv.FormatUint(e.ann.ExactEvaluated, 10))
	buf.WriteString("}, partition_cache: {hit: ")
	buf.WriteString(strconv.FormatUint(e.ann.PartitionCacheHits, 10))
	buf.WriteString(", miss: ")
	buf.WriteString(strconv.FormatUint(e.ann.PartitionCacheMisses, 10))
	buf.WriteString(", lookup_time: ")
	buf.WriteString(formatVectorSearchDurationFromMicros(e.ann.PartitionCacheLookupMicros))
	buf.WriteString(", miss_load_time: ")
	buf.WriteString(formatVectorSearchDurationFromMicros(e.ann.PartitionCacheMissLoadMicros))
	buf.WriteString("}, table_lookup_time: ")
	buf.WriteString(formatVectorSearchDurationFromMicros(e.ann.TableLookupMicros))
	buf.WriteString(", rerank_time: ")
	buf.WriteString(formatVectorSearchDurationFromMicros(e.ann.RerankMicros))
	buf.WriteString("}}")
}

func (e *spfreshANNRuntimeStats) merge(other spfreshANNRuntimeStats) {
	e.ExactFallback = e.ExactFallback || other.ExactFallback
	e.SearchMicros += other.SearchMicros
	e.PartitionsScanned += other.PartitionsScanned
	e.CentroidsScanned += other.CentroidsScanned
	e.LeafVectorsScanned += other.LeafVectorsScanned
	e.RoughCandidates += other.RoughCandidates
	e.BoundPrunedCandidates += other.BoundPrunedCandidates
	e.RerankCandidates += other.RerankCandidates
	e.ExactEvaluated += other.ExactEvaluated
	e.PartitionCacheHits += other.PartitionCacheHits
	e.PartitionCacheMisses += other.PartitionCacheMisses
	e.PartitionCacheLookupMicros += other.PartitionCacheLookupMicros
	e.PartitionCacheMissLoadMicros += other.PartitionCacheMissLoadMicros
	e.TableLookupMicros += other.TableLookupMicros
	e.RerankMicros += other.RerankMicros
}
