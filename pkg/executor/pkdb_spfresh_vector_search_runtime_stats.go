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
	"math"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/util/execdetails"
)

// vectorSearchRuntimeStats is runtime stats returned by the remote vector search engine.
//
// NOTE: This is used for EXPLAIN ANALYZE, and is independent from the rows returned.
type vectorSearchRuntimeStats struct {
	ReadOnly             bool
	OversampleFactor     float64
	PartitionsScanned    uint64
	VectorsScanned       uint64
	PartitionCacheHits   uint64
	PartitionCacheMisses uint64
	TableLookupKeys      uint64
	TableLookupBytes     uint64
	PermitMicros         uint64
	ConfigMicros         uint64
	IndexOpenMicros      uint64
	SearchMicros         uint64
	TableLookupMicros    uint64
	TiKVClientRPCCount   uint64
	TiKVClientRPCMicros  uint64

	// taskCount counts successful SPFresh responses registered for this plan.
	// annTaskCount counts the responses that explicitly reported ANN stats.
	taskCount    uint64
	annTaskCount uint64
	ann          spfreshANNRuntimeStats
}

// Tp implements the execdetails.RuntimeStats interface.
func (*vectorSearchRuntimeStats) Tp() int {
	return execdetails.TpVectorSearchRuntimeStats
}

// Clone implements the execdetails.RuntimeStats interface.
func (e *vectorSearchRuntimeStats) Clone() execdetails.RuntimeStats {
	if e == nil {
		return &vectorSearchRuntimeStats{}
	}
	clone := *e
	return &clone
}

// Merge implements the execdetails.RuntimeStats interface.
func (e *vectorSearchRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	other, ok := rs.(*vectorSearchRuntimeStats)
	if !ok || other == nil || e == nil {
		return
	}
	e.ReadOnly = e.ReadOnly || other.ReadOnly
	if e.OversampleFactor == 0 {
		e.OversampleFactor = other.OversampleFactor
	}
	e.PartitionsScanned += other.PartitionsScanned
	e.VectorsScanned += other.VectorsScanned
	e.PartitionCacheHits += other.PartitionCacheHits
	e.PartitionCacheMisses += other.PartitionCacheMisses
	e.TableLookupKeys += other.TableLookupKeys
	e.TableLookupBytes += other.TableLookupBytes
	e.PermitMicros += other.PermitMicros
	e.ConfigMicros += other.ConfigMicros
	e.IndexOpenMicros += other.IndexOpenMicros
	e.SearchMicros += other.SearchMicros
	e.TableLookupMicros += other.TableLookupMicros
	e.TiKVClientRPCCount += other.TiKVClientRPCCount
	e.TiKVClientRPCMicros += other.TiKVClientRPCMicros
	e.taskCount += other.taskCount
	e.annTaskCount += other.annTaskCount
	e.ann.merge(other.ann)
}

// String implements the execdetails.RuntimeStats interface.
func (e *vectorSearchRuntimeStats) String() string {
	if e == nil {
		return ""
	}

	hasANNStats := e.hasCompleteANNStats()
	hasLegacyStats := e.hasLegacyStats()
	if !hasANNStats && !hasLegacyStats {
		return ""
	}

	buf := bytes.NewBuffer(make([]byte, 0, 512))
	if hasANNStats {
		e.appendANNStats(buf)
	}
	if hasLegacyStats {
		if hasANNStats {
			buf.WriteString(", ")
		}
		e.appendLegacyStats(buf, hasANNStats)
	}
	return buf.String()
}

func (e *vectorSearchRuntimeStats) hasLegacyStats() bool {
	return e.ReadOnly ||
		e.OversampleFactor != 0 ||
		e.PartitionsScanned != 0 ||
		e.VectorsScanned != 0 ||
		e.PartitionCacheHits != 0 ||
		e.PartitionCacheMisses != 0 ||
		e.TableLookupKeys != 0 ||
		e.TableLookupBytes != 0 ||
		e.PermitMicros != 0 ||
		e.ConfigMicros != 0 ||
		e.IndexOpenMicros != 0 ||
		e.SearchMicros != 0 ||
		e.TableLookupMicros != 0 ||
		e.TiKVClientRPCCount != 0 ||
		e.TiKVClientRPCMicros != 0
}

func (e *vectorSearchRuntimeStats) appendLegacyStats(buf *bytes.Buffer, omitANNOverlap bool) {
	buf.WriteString("vector_search: {")
	buf.WriteString("read_only: ")
	buf.WriteString(strconv.FormatBool(e.ReadOnly))
	buf.WriteString(", ")
	if e.OversampleFactor > 0 {
		buf.WriteString("oversample: ")
		buf.WriteString(strconv.FormatFloat(e.OversampleFactor, 'g', -1, 64))
		buf.WriteString(", ")
	}
	if !omitANNOverlap && e.PartitionsScanned > 0 {
		buf.WriteString("partitions_scanned: ")
		buf.WriteString(strconv.FormatUint(e.PartitionsScanned, 10))
		buf.WriteString(", ")
	}
	if !omitANNOverlap && e.VectorsScanned > 0 {
		buf.WriteString("vectors_scanned: ")
		buf.WriteString(strconv.FormatUint(e.VectorsScanned, 10))
		buf.WriteString(", ")
	}
	if !omitANNOverlap && e.PartitionCacheHits > 0 {
		buf.WriteString("partition_cache_hits: ")
		buf.WriteString(strconv.FormatUint(e.PartitionCacheHits, 10))
		buf.WriteString(", ")
	}
	if !omitANNOverlap && e.PartitionCacheMisses > 0 {
		buf.WriteString("partition_cache_misses: ")
		buf.WriteString(strconv.FormatUint(e.PartitionCacheMisses, 10))
		buf.WriteString(", ")
	}
	if e.TableLookupKeys > 0 {
		buf.WriteString("table_lookup_keys: ")
		buf.WriteString(strconv.FormatUint(e.TableLookupKeys, 10))
		buf.WriteString(", ")
	}
	if e.TableLookupBytes > 0 {
		buf.WriteString("table_lookup_bytes: ")
		buf.WriteString(strconv.FormatUint(e.TableLookupBytes, 10))
		buf.WriteString(", ")
	}
	if e.PermitMicros > 0 {
		buf.WriteString("permit: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.PermitMicros))
		buf.WriteString(", ")
	}
	if e.ConfigMicros > 0 {
		buf.WriteString("config: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.ConfigMicros))
		buf.WriteString(", ")
	}
	if e.IndexOpenMicros > 0 {
		buf.WriteString("index_open: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.IndexOpenMicros))
		buf.WriteString(", ")
	}
	if !omitANNOverlap && e.SearchMicros > 0 {
		buf.WriteString("search: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.SearchMicros))
		buf.WriteString(", ")
	}
	if !omitANNOverlap && e.TableLookupMicros > 0 {
		buf.WriteString("table_lookup: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.TableLookupMicros))
		buf.WriteString(", ")
	}
	if e.TiKVClientRPCCount > 0 {
		buf.WriteString("tikv_client_rpc_count: ")
		buf.WriteString(strconv.FormatUint(e.TiKVClientRPCCount, 10))
		buf.WriteString(", ")
	}
	if e.TiKVClientRPCMicros > 0 {
		buf.WriteString("tikv_client_rpc: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.TiKVClientRPCMicros))
		buf.WriteString(", ")
	}
	if buf.Bytes()[buf.Len()-2] == ',' {
		buf.Truncate(buf.Len() - 2)
	}
	buf.WriteString("}")
}

func formatVectorSearchDurationFromMicros(micros uint64) string {
	maxMicros := uint64(math.MaxInt64 / int64(time.Microsecond))
	if micros > maxMicros {
		micros = maxMicros
	}
	return execdetails.FormatDuration(time.Duration(micros) * time.Microsecond)
}
