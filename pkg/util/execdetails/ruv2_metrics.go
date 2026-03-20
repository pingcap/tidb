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

package execdetails

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type ruv2MetricsKeyType struct{}

// RUV2Weights contains the TiDB-side RU v2 weights needed to calculate scaled
// statement RU values.
type RUV2Weights struct {
	RUScale float64

	ResultChunkCells        float64
	ExecutorL1              float64
	ExecutorL2              float64
	ExecutorL3              float64
	ExecutorL5InsertRows    float64
	PlanCnt                 float64
	PlanDeriveStatsPaths    float64
	ResourceManagerReadCnt  float64
	ResourceManagerWriteCnt float64
	SessionParserTotal      float64
	TxnCnt                  float64
}

// RUV2MetricsCtxKey is used to carry statement-level RUv2 metrics in context.Context.
var RUV2MetricsCtxKey = ruv2MetricsKeyType{}

// RUV2MetricsFromContext returns the RUv2 metrics stored in ctx.
func RUV2MetricsFromContext(ctx context.Context) *RUV2Metrics {
	if ctx == nil {
		return nil
	}
	metrics, _ := ctx.Value(RUV2MetricsCtxKey).(*RUV2Metrics)
	return metrics
}

// RUV2Metrics stores statement-level RUv2 metrics.
type RUV2Metrics struct {
	resultChunkCells int64

	executorL1 sync.Map
	executorL2 sync.Map
	executorL3 sync.Map

	executorL5InsertRows int64
	planCnt              int64
	planDeriveStatsPaths int64
	sessionParserTotal   int64
	txnCnt               int64

	resourceManagerReadCnt  int64
	resourceManagerWriteCnt int64

	tikvKvEngineCacheMiss             int64
	tikvCoprocessorExecutorIterations int64
	tikvCoprocessorResponseBytes      int64
	tikvRaftstoreStoreWriteTriggerWB  int64
	tikvStorageProcessedKeysBatchGet  int64
	tikvStorageProcessedKeysGet       int64
	tikvCoprocessorWorkTotal          sync.Map
}

// NewRUV2Metrics creates a new RUv2 metrics container.
func NewRUV2Metrics() *RUV2Metrics {
	return &RUV2Metrics{}
}

// AddResultChunkCells records result cells written by the current statement.
func (m *RUV2Metrics) AddResultChunkCells(delta int64) {
	atomic.AddInt64(&m.resultChunkCells, delta)
}

// AddExecutorMetric records a statement-level executor metric for the given RUv2 level.
func (m *RUV2Metrics) AddExecutorMetric(level int, label string, delta int64) {
	if delta == 0 || label == "" {
		return
	}
	switch level {
	case 1:
		addRUV2LabelCounter(&m.executorL1, label, delta)
	case 2:
		addRUV2LabelCounter(&m.executorL2, label, delta)
	case 3:
		addRUV2LabelCounter(&m.executorL3, label, delta)
	}
}

// AddExecutorL5InsertRows records affected insert rows for RUv2 accounting.
func (m *RUV2Metrics) AddExecutorL5InsertRows(delta int64) {
	atomic.AddInt64(&m.executorL5InsertRows, delta)
}

// AddPlanCnt records plan builder invocations for the current statement.
func (m *RUV2Metrics) AddPlanCnt(delta int64) {
	atomic.AddInt64(&m.planCnt, delta)
}

// AddPlanDeriveStatsPaths records derived stats paths for the current statement.
func (m *RUV2Metrics) AddPlanDeriveStatsPaths(delta int64) {
	atomic.AddInt64(&m.planDeriveStatsPaths, delta)
}

// AddSessionParserTotal records parser executions for the current statement.
func (m *RUV2Metrics) AddSessionParserTotal(delta int64) {
	atomic.AddInt64(&m.sessionParserTotal, delta)
}

// AddTxnCnt records transaction completions attributed to the current statement.
func (m *RUV2Metrics) AddTxnCnt(delta int64) {
	atomic.AddInt64(&m.txnCnt, delta)
}

// AddResourceManagerReadCnt records TiKV read RPCs charged to resource management.
func (m *RUV2Metrics) AddResourceManagerReadCnt(delta int64) {
	atomic.AddInt64(&m.resourceManagerReadCnt, delta)
}

// AddResourceManagerWriteCnt records TiKV write RPCs charged to resource management.
func (m *RUV2Metrics) AddResourceManagerWriteCnt(delta int64) {
	atomic.AddInt64(&m.resourceManagerWriteCnt, delta)
}

// AddTiKVKVEngineCacheMiss records TiKV kv_engine_cache_miss counters from ExecDetailsV2.
func (m *RUV2Metrics) AddTiKVKVEngineCacheMiss(delta int64) {
	atomic.AddInt64(&m.tikvKvEngineCacheMiss, delta)
}

// AddTiKVCoprocessorExecutorIterations records TiKV coprocessor iteration counters.
func (m *RUV2Metrics) AddTiKVCoprocessorExecutorIterations(delta int64) {
	atomic.AddInt64(&m.tikvCoprocessorExecutorIterations, delta)
}

// AddTiKVCoprocessorResponseBytes records TiKV coprocessor response bytes.
func (m *RUV2Metrics) AddTiKVCoprocessorResponseBytes(delta int64) {
	atomic.AddInt64(&m.tikvCoprocessorResponseBytes, delta)
}

// AddTiKVRaftstoreStoreWriteTriggerWB records TiKV raftstore write trigger bytes.
func (m *RUV2Metrics) AddTiKVRaftstoreStoreWriteTriggerWB(delta int64) {
	atomic.AddInt64(&m.tikvRaftstoreStoreWriteTriggerWB, delta)
}

// AddTiKVStorageProcessedKeysBatchGet records TiKV batch-get processed keys.
func (m *RUV2Metrics) AddTiKVStorageProcessedKeysBatchGet(delta int64) {
	atomic.AddInt64(&m.tikvStorageProcessedKeysBatchGet, delta)
}

// AddTiKVStorageProcessedKeysGet records TiKV get processed keys.
func (m *RUV2Metrics) AddTiKVStorageProcessedKeysGet(delta int64) {
	atomic.AddInt64(&m.tikvStorageProcessedKeysGet, delta)
}

// AddTiKVCoprocessorWorkTotal records TiKV executor input counters by executor type.
func (m *RUV2Metrics) AddTiKVCoprocessorWorkTotal(label string, delta int64) {
	if delta == 0 || label == "" {
		return
	}
	addRUV2LabelCounter(&m.tikvCoprocessorWorkTotal, label, delta)
}

// Snapshot returns a copy of metrics for reporting and freezes TiDBRU using the
// supplied weights.
func (m *RUV2Metrics) Snapshot(weights RUV2Weights) RUV2MetricsSnapshot {
	if m == nil {
		return RUV2MetricsSnapshot{}
	}
	readCnt := atomic.LoadInt64(&m.resourceManagerReadCnt)
	writeCnt := atomic.LoadInt64(&m.resourceManagerWriteCnt)
	snapshot := RUV2MetricsSnapshot{
		ResultChunkCells:                  atomic.LoadInt64(&m.resultChunkCells),
		ExecutorL1:                        snapshotRUV2LabelCounter(&m.executorL1),
		ExecutorL2:                        snapshotRUV2LabelCounter(&m.executorL2),
		ExecutorL3:                        snapshotRUV2LabelCounter(&m.executorL3),
		ExecutorL5InsertRows:              atomic.LoadInt64(&m.executorL5InsertRows),
		PlanCnt:                           atomic.LoadInt64(&m.planCnt),
		PlanDeriveStatsPaths:              atomic.LoadInt64(&m.planDeriveStatsPaths),
		SessionParserTotal:                atomic.LoadInt64(&m.sessionParserTotal),
		TxnCnt:                            atomic.LoadInt64(&m.txnCnt),
		ResourceManagerReadCnt:            readCnt,
		ResourceManagerWriteCnt:           writeCnt,
		TiKVKVEngineCacheMiss:             atomic.LoadInt64(&m.tikvKvEngineCacheMiss),
		TiKVCoprocessorExecutorIterations: atomic.LoadInt64(&m.tikvCoprocessorExecutorIterations),
		TiKVCoprocessorResponseBytes:      atomic.LoadInt64(&m.tikvCoprocessorResponseBytes),
		TiKVRaftstoreStoreWriteTriggerWB:  atomic.LoadInt64(&m.tikvRaftstoreStoreWriteTriggerWB),
		TiKVStorageProcessedKeysBatchGet:  atomic.LoadInt64(&m.tikvStorageProcessedKeysBatchGet),
		TiKVStorageProcessedKeysGet:       atomic.LoadInt64(&m.tikvStorageProcessedKeysGet),
		TiKVCoprocessorExecutorWorkTotal:  snapshotRUV2LabelCounter(&m.tikvCoprocessorWorkTotal),
	}
	snapshot.TiDBRU = snapshot.calculateRUValuesWithWeights(weights)
	snapshot.tiDBRUFrozen = true
	return snapshot
}

type ruv2LabelCounter = sync.Map

func addRUV2LabelCounter(counter *ruv2LabelCounter, label string, delta int64) {
	if counter == nil {
		return
	}
	if current, ok := counter.Load(label); ok {
		atomic.AddInt64(current.(*int64), delta)
		return
	}
	value := new(int64)
	actual, _ := counter.LoadOrStore(label, value)
	atomic.AddInt64(actual.(*int64), delta)
}

func snapshotRUV2LabelCounter(counter *ruv2LabelCounter) map[string]int64 {
	if counter == nil {
		return nil
	}
	var out map[string]int64
	counter.Range(func(key, value any) bool {
		label, ok := key.(string)
		if !ok {
			return true
		}
		val, ok := value.(*int64)
		if !ok {
			return true
		}
		if out == nil {
			out = make(map[string]int64)
		}
		out[label] = atomic.LoadInt64(val)
		return true
	})
	return out
}

// RUV2MetricsSnapshot is a read-only copy of RUv2 metrics.
type RUV2MetricsSnapshot struct {
	ResultChunkCells int64

	ExecutorL1 map[string]int64
	ExecutorL2 map[string]int64
	ExecutorL3 map[string]int64

	ExecutorL5InsertRows int64
	PlanCnt              int64
	PlanDeriveStatsPaths int64
	SessionParserTotal   int64
	TxnCnt               int64

	ResourceManagerReadCnt  int64
	ResourceManagerWriteCnt int64

	TiKVKVEngineCacheMiss             int64
	TiKVCoprocessorExecutorIterations int64
	TiKVCoprocessorResponseBytes      int64
	TiKVRaftstoreStoreWriteTriggerWB  int64
	TiKVStorageProcessedKeysBatchGet  int64
	TiKVStorageProcessedKeysGet       int64
	TiKVCoprocessorExecutorWorkTotal  map[string]int64

	// TiKVRU is the TiKV RU v2 value (scaled integer) calculated in client-go and stored in RUDetails.
	// Callers must populate this from RUDetails.TiKVRUV2() after calling Snapshot(), as it is not set automatically.
	TiKVRU int64
	// TiFlashRU is the TiFlash RU value (scaled integer after truncation) calculated in client-go and stored in RUDetails.
	TiFlashRU int64
	// TiDBRU is the TiDB RU v2 value (scaled integer) frozen at snapshot creation time.
	TiDBRU int64

	tiDBRUFrozen bool
}

// IsZero checks whether all metrics are zero.
func (s RUV2MetricsSnapshot) IsZero() bool {
	return s.ResultChunkCells == 0 &&
		len(s.ExecutorL1) == 0 &&
		len(s.ExecutorL2) == 0 &&
		len(s.ExecutorL3) == 0 &&
		s.ExecutorL5InsertRows == 0 &&
		s.PlanCnt == 0 &&
		s.PlanDeriveStatsPaths == 0 &&
		s.SessionParserTotal == 0 &&
		s.TxnCnt == 0 &&
		s.ResourceManagerReadCnt == 0 &&
		s.ResourceManagerWriteCnt == 0 &&
		s.TiKVKVEngineCacheMiss == 0 &&
		s.TiKVCoprocessorExecutorIterations == 0 &&
		s.TiKVCoprocessorResponseBytes == 0 &&
		s.TiKVRaftstoreStoreWriteTriggerWB == 0 &&
		s.TiKVStorageProcessedKeysBatchGet == 0 &&
		s.TiKVStorageProcessedKeysGet == 0 &&
		len(s.TiKVCoprocessorExecutorWorkTotal) == 0 &&
		s.TiKVRU == 0 &&
		s.TiFlashRU == 0 &&
		s.TiDBRU == 0
}

// Merge merges another snapshot into the receiver.
func (s *RUV2MetricsSnapshot) Merge(other RUV2MetricsSnapshot) {
	if s == nil {
		return
	}
	freezeTiDBRU := s.tiDBRUFrozen && other.tiDBRUFrozen
	mergedTiDBRU := s.TiDBRU + other.TiDBRU
	s.ResultChunkCells += other.ResultChunkCells
	s.ExecutorL5InsertRows += other.ExecutorL5InsertRows
	s.PlanCnt += other.PlanCnt
	s.PlanDeriveStatsPaths += other.PlanDeriveStatsPaths
	s.SessionParserTotal += other.SessionParserTotal
	s.TxnCnt += other.TxnCnt
	s.ResourceManagerReadCnt += other.ResourceManagerReadCnt
	s.ResourceManagerWriteCnt += other.ResourceManagerWriteCnt
	s.TiKVKVEngineCacheMiss += other.TiKVKVEngineCacheMiss
	s.TiKVCoprocessorExecutorIterations += other.TiKVCoprocessorExecutorIterations
	s.TiKVCoprocessorResponseBytes += other.TiKVCoprocessorResponseBytes
	s.TiKVRaftstoreStoreWriteTriggerWB += other.TiKVRaftstoreStoreWriteTriggerWB
	s.TiKVStorageProcessedKeysBatchGet += other.TiKVStorageProcessedKeysBatchGet
	s.TiKVStorageProcessedKeysGet += other.TiKVStorageProcessedKeysGet
	s.TiKVRU += other.TiKVRU
	s.TiFlashRU += other.TiFlashRU
	s.ExecutorL1 = mergeRUV2LabelMap(s.ExecutorL1, other.ExecutorL1)
	s.ExecutorL2 = mergeRUV2LabelMap(s.ExecutorL2, other.ExecutorL2)
	s.ExecutorL3 = mergeRUV2LabelMap(s.ExecutorL3, other.ExecutorL3)
	s.TiKVCoprocessorExecutorWorkTotal = mergeRUV2LabelMap(s.TiKVCoprocessorExecutorWorkTotal, other.TiKVCoprocessorExecutorWorkTotal)
	if freezeTiDBRU {
		s.TiDBRU = mergedTiDBRU
		s.tiDBRUFrozen = true
	} else {
		s.TiDBRU = 0
		s.tiDBRUFrozen = false
	}
}

func mergeRUV2LabelMap(dst, src map[string]int64) map[string]int64 {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(map[string]int64, len(src))
	}
	for k, v := range src {
		dst[k] += v
	}
	return dst
}

// CalculateRUValues calculates the TiDB RU from the snapshot.
// The returned value is a scaled integer.
func (s RUV2MetricsSnapshot) CalculateRUValues(weights RUV2Weights) (tidbRU int64) {
	if s.tiDBRUFrozen {
		return s.TiDBRU
	}
	return s.calculateRUValuesWithWeights(weights)
}

// TotalRU returns the statement RU v2 total as TiDB + TiKV + TiFlash.
func (s RUV2MetricsSnapshot) TotalRU(weights RUV2Weights) int64 {
	return s.CalculateRUValues(weights) + s.TiKVRU + s.TiFlashRU
}

func (s RUV2MetricsSnapshot) calculateRUValuesWithWeights(weights RUV2Weights) (tidbRU int64) {
	tidbRUFloat :=
		float64(s.ResultChunkCells)*weights.ResultChunkCells +
			float64(sumRUV2LabelMap(s.ExecutorL1))*weights.ExecutorL1 +
			float64(sumRUV2LabelMap(s.ExecutorL2))*weights.ExecutorL2 +
			float64(sumRUV2LabelMap(s.ExecutorL3))*weights.ExecutorL3 +
			float64(s.ExecutorL5InsertRows)*weights.ExecutorL5InsertRows +
			float64(s.PlanCnt)*weights.PlanCnt +
			float64(s.PlanDeriveStatsPaths)*weights.PlanDeriveStatsPaths +
			float64(s.ResourceManagerReadCnt)*weights.ResourceManagerReadCnt +
			float64(s.ResourceManagerWriteCnt)*weights.ResourceManagerWriteCnt +
			float64(s.SessionParserTotal)*weights.SessionParserTotal +
			float64(s.TxnCnt)*weights.TxnCnt

	tidbRU = int64(tidbRUFloat * weights.RUScale)
	return
}

func sumRUV2LabelMap(values map[string]int64) int64 {
	if len(values) == 0 {
		return 0
	}
	var total int64
	for _, value := range values {
		total += value
	}
	return total
}

// FormatRUV2Metrics formats RUv2 metrics into a compact string.
func FormatRUV2Metrics(snapshot RUV2MetricsSnapshot, weights RUV2Weights) string {
	if snapshot.IsZero() {
		return ""
	}
	parts := make([]string, 0, 19)
	appendInt := func(key string, value int64) {
		if value != 0 {
			parts = append(parts, fmt.Sprintf("%s:%d", key, value))
		}
	}
	appendIntAlways := func(key string, value int64) {
		parts = append(parts, fmt.Sprintf("%s:%d", key, value))
	}
	appendMap := func(key string, value map[string]int64) {
		if len(value) == 0 {
			return
		}
		formatted := formatRUV2LabelMap(value)
		if formatted != "" {
			parts = append(parts, fmt.Sprintf("%s:%s", key, formatted))
		}
	}

	tidbRU := snapshot.CalculateRUValues(weights)
	tikvRU := snapshot.TiKVRU
	tiflashRU := snapshot.TiFlashRU
	totalRU := snapshot.TotalRU(weights)
	appendIntAlways("total_ru", totalRU)
	appendIntAlways("tidb_ru", tidbRU)
	appendIntAlways("tikv_ru", tikvRU)
	appendIntAlways("tiflash_ru", tiflashRU)

	appendInt("result_chunk_cells", snapshot.ResultChunkCells)
	appendMap("executor_l1", snapshot.ExecutorL1)
	appendMap("executor_l2", snapshot.ExecutorL2)
	appendMap("executor_l3", snapshot.ExecutorL3)
	appendInt("executor_l5_insert_rows", snapshot.ExecutorL5InsertRows)
	appendInt("plan_cnt", snapshot.PlanCnt)
	appendInt("plan_derive_stats_paths", snapshot.PlanDeriveStatsPaths)
	appendInt("session_parser_total", snapshot.SessionParserTotal)
	appendInt("txn_cnt", snapshot.TxnCnt)
	appendInt("resource_manager_read_cnt", snapshot.ResourceManagerReadCnt)
	appendInt("resource_manager_write_cnt", snapshot.ResourceManagerWriteCnt)
	appendInt("tikv_kv_engine_cache_miss", snapshot.TiKVKVEngineCacheMiss)
	appendInt("tikv_coprocessor_executor_iterations", snapshot.TiKVCoprocessorExecutorIterations)
	appendInt("tikv_coprocessor_response_bytes", snapshot.TiKVCoprocessorResponseBytes)
	appendInt("tikv_raftstore_store_write_trigger_wb_bytes", snapshot.TiKVRaftstoreStoreWriteTriggerWB)
	appendInt("tikv_storage_processed_keys_batch_get", snapshot.TiKVStorageProcessedKeysBatchGet)
	appendInt("tikv_storage_processed_keys_get", snapshot.TiKVStorageProcessedKeysGet)
	appendMap("tikv_coprocessor_executor_work_total", snapshot.TiKVCoprocessorExecutorWorkTotal)

	return strings.Join(parts, ", ")
}

func formatRUV2LabelMap(values map[string]int64) string {
	keys := make([]string, 0, len(values))
	for key, value := range values {
		if value != 0 {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(key)
		builder.WriteByte(':')
		builder.WriteString(strconv.FormatInt(values[key], 10))
	}
	builder.WriteByte('}')
	return builder.String()
}
