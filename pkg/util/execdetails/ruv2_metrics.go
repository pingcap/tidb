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

	"github.com/pingcap/tidb/pkg/metrics"
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
	bypass atomic.Bool

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

// SetBypass marks whether statement-level RU accounting should be skipped.
func (m *RUV2Metrics) SetBypass(enabled bool) {
	m.bypass.Store(enabled)
}

// Bypass returns whether statement-level RU accounting should be skipped.
func (m *RUV2Metrics) Bypass() bool {
	return m.bypass.Load()
}

// AddResultChunkCells records result cells written by the current statement.
func (m *RUV2Metrics) AddResultChunkCells(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2ResultChunkCells.Add(float64(delta))
	atomic.AddInt64(&m.resultChunkCells, delta)
}

// AddExecutorMetric records a statement-level executor metric for the given RUv2 level.
func (m *RUV2Metrics) AddExecutorMetric(level int, label string, delta int64) {
	if m.Bypass() || delta == 0 || label == "" {
		return
	}
	switch level {
	case 1:
		metrics.RUV2ExecutorL1.WithLabelValues(label).Add(float64(delta))
	case 2:
		metrics.RUV2ExecutorL2.WithLabelValues(label).Add(float64(delta))
	case 3:
		metrics.RUV2ExecutorL3.WithLabelValues(label).Add(float64(delta))
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
	if m.Bypass() {
		return
	}
	metrics.RUV2ExecutorL5InsertRows.Add(float64(delta))
	atomic.AddInt64(&m.executorL5InsertRows, delta)
}

// AddPlanCnt records plan builder invocations for the current statement.
func (m *RUV2Metrics) AddPlanCnt(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2PlanCnt.Add(float64(delta))
	atomic.AddInt64(&m.planCnt, delta)
}

// AddPlanDeriveStatsPaths records derived stats paths for the current statement.
func (m *RUV2Metrics) AddPlanDeriveStatsPaths(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2PlanDeriveStatsPaths.Add(float64(delta))
	atomic.AddInt64(&m.planDeriveStatsPaths, delta)
}

// AddSessionParserTotal records parser executions for the current statement.
func (m *RUV2Metrics) AddSessionParserTotal(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2SessionParserTotal.Add(float64(delta))
	atomic.AddInt64(&m.sessionParserTotal, delta)
}

// AddTxnCnt records transaction completions attributed to the current statement.
func (m *RUV2Metrics) AddTxnCnt(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TxnCnt.Add(float64(delta))
	atomic.AddInt64(&m.txnCnt, delta)
}

// AddResourceManagerReadCnt records TiKV read RPCs charged to resource management.
func (m *RUV2Metrics) AddResourceManagerReadCnt(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2ResourceManagerReadCnt.Add(float64(delta))
	atomic.AddInt64(&m.resourceManagerReadCnt, delta)
}

// AddResourceManagerWriteCnt records TiKV write RPCs charged to resource management.
func (m *RUV2Metrics) AddResourceManagerWriteCnt(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2ResourceManagerWriteCnt.Add(float64(delta))
	atomic.AddInt64(&m.resourceManagerWriteCnt, delta)
}

// AddTiKVKVEngineCacheMiss records TiKV kv_engine_cache_miss counters from ExecDetailsV2.
func (m *RUV2Metrics) AddTiKVKVEngineCacheMiss(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TiKVKVEngineCacheMiss.Add(float64(delta))
	atomic.AddInt64(&m.tikvKvEngineCacheMiss, delta)
}

// AddTiKVCoprocessorExecutorIterations records TiKV coprocessor iteration counters.
func (m *RUV2Metrics) AddTiKVCoprocessorExecutorIterations(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TiKVCoprocessorExecutorIterations.Add(float64(delta))
	atomic.AddInt64(&m.tikvCoprocessorExecutorIterations, delta)
}

// AddTiKVCoprocessorResponseBytes records TiKV coprocessor response bytes.
func (m *RUV2Metrics) AddTiKVCoprocessorResponseBytes(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TiKVCoprocessorResponseBytes.Add(float64(delta))
	atomic.AddInt64(&m.tikvCoprocessorResponseBytes, delta)
}

// AddTiKVRaftstoreStoreWriteTriggerWB records TiKV raftstore write trigger bytes.
func (m *RUV2Metrics) AddTiKVRaftstoreStoreWriteTriggerWB(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TiKVRaftstoreStoreWriteTriggerWB.Add(float64(delta))
	atomic.AddInt64(&m.tikvRaftstoreStoreWriteTriggerWB, delta)
}

// AddTiKVStorageProcessedKeysBatchGet records TiKV batch-get processed keys.
func (m *RUV2Metrics) AddTiKVStorageProcessedKeysBatchGet(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TiKVStorageProcessedKeysBatchGet.Add(float64(delta))
	atomic.AddInt64(&m.tikvStorageProcessedKeysBatchGet, delta)
}

// AddTiKVStorageProcessedKeysGet records TiKV get processed keys.
func (m *RUV2Metrics) AddTiKVStorageProcessedKeysGet(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TiKVStorageProcessedKeysGet.Add(float64(delta))
	atomic.AddInt64(&m.tikvStorageProcessedKeysGet, delta)
}

// AddTiKVCoprocessorWorkTotal records TiKV executor input counters by executor type.
func (m *RUV2Metrics) AddTiKVCoprocessorWorkTotal(label string, delta int64) {
	if m.Bypass() || delta == 0 || label == "" {
		return
	}
	metrics.RUV2TiKVCoprocessorWorkTotal.WithLabelValues(label).Add(float64(delta))
	addRUV2LabelCounter(&m.tikvCoprocessorWorkTotal, label, delta)
}

// Clone returns a copy of the current metrics for reporting.
func (m *RUV2Metrics) Clone() *RUV2Metrics {
	if m == nil {
		return nil
	}
	cloned := &RUV2Metrics{}
	cloned.bypass.Store(m.Bypass())
	atomic.StoreInt64(&cloned.resultChunkCells, atomic.LoadInt64(&m.resultChunkCells))
	cloneRUV2LabelCounter(&cloned.executorL1, &m.executorL1)
	cloneRUV2LabelCounter(&cloned.executorL2, &m.executorL2)
	cloneRUV2LabelCounter(&cloned.executorL3, &m.executorL3)
	atomic.StoreInt64(&cloned.executorL5InsertRows, atomic.LoadInt64(&m.executorL5InsertRows))
	atomic.StoreInt64(&cloned.planCnt, atomic.LoadInt64(&m.planCnt))
	atomic.StoreInt64(&cloned.planDeriveStatsPaths, atomic.LoadInt64(&m.planDeriveStatsPaths))
	atomic.StoreInt64(&cloned.sessionParserTotal, atomic.LoadInt64(&m.sessionParserTotal))
	atomic.StoreInt64(&cloned.txnCnt, atomic.LoadInt64(&m.txnCnt))
	atomic.StoreInt64(&cloned.resourceManagerReadCnt, atomic.LoadInt64(&m.resourceManagerReadCnt))
	atomic.StoreInt64(&cloned.resourceManagerWriteCnt, atomic.LoadInt64(&m.resourceManagerWriteCnt))
	atomic.StoreInt64(&cloned.tikvKvEngineCacheMiss, atomic.LoadInt64(&m.tikvKvEngineCacheMiss))
	atomic.StoreInt64(&cloned.tikvCoprocessorExecutorIterations, atomic.LoadInt64(&m.tikvCoprocessorExecutorIterations))
	atomic.StoreInt64(&cloned.tikvCoprocessorResponseBytes, atomic.LoadInt64(&m.tikvCoprocessorResponseBytes))
	atomic.StoreInt64(&cloned.tikvRaftstoreStoreWriteTriggerWB, atomic.LoadInt64(&m.tikvRaftstoreStoreWriteTriggerWB))
	atomic.StoreInt64(&cloned.tikvStorageProcessedKeysBatchGet, atomic.LoadInt64(&m.tikvStorageProcessedKeysBatchGet))
	atomic.StoreInt64(&cloned.tikvStorageProcessedKeysGet, atomic.LoadInt64(&m.tikvStorageProcessedKeysGet))
	cloneRUV2LabelCounter(&cloned.tikvCoprocessorWorkTotal, &m.tikvCoprocessorWorkTotal)
	return cloned
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

func cloneRUV2LabelCounter(dst, src *ruv2LabelCounter) {
	if dst == nil || src == nil {
		return
	}
	src.Range(func(key, value any) bool {
		label, ok := key.(string)
		if !ok {
			return true
		}
		val, ok := value.(*int64)
		if !ok {
			return true
		}
		if cloned := atomic.LoadInt64(val); cloned != 0 {
			addRUV2LabelCounter(dst, label, cloned)
		}
		return true
	})
}

// Merge merges another metrics container into the receiver.
func (m *RUV2Metrics) Merge(other *RUV2Metrics) {
	if m == nil || other == nil {
		return
	}
	if m.Bypass() || other.Bypass() {
		return
	}
	atomic.AddInt64(&m.resultChunkCells, other.ResultChunkCells())
	mergeIntoRUV2LabelCounter(&m.executorL1, &other.executorL1)
	mergeIntoRUV2LabelCounter(&m.executorL2, &other.executorL2)
	mergeIntoRUV2LabelCounter(&m.executorL3, &other.executorL3)
	atomic.AddInt64(&m.executorL5InsertRows, other.ExecutorL5InsertRows())
	atomic.AddInt64(&m.planCnt, other.PlanCnt())
	atomic.AddInt64(&m.planDeriveStatsPaths, other.PlanDeriveStatsPaths())
	atomic.AddInt64(&m.sessionParserTotal, other.SessionParserTotal())
	atomic.AddInt64(&m.txnCnt, other.TxnCnt())
	atomic.AddInt64(&m.resourceManagerReadCnt, other.ResourceManagerReadCnt())
	atomic.AddInt64(&m.resourceManagerWriteCnt, other.ResourceManagerWriteCnt())
	atomic.AddInt64(&m.tikvKvEngineCacheMiss, other.TiKVKVEngineCacheMiss())
	atomic.AddInt64(&m.tikvCoprocessorExecutorIterations, other.TiKVCoprocessorExecutorIterations())
	atomic.AddInt64(&m.tikvCoprocessorResponseBytes, other.TiKVCoprocessorResponseBytes())
	atomic.AddInt64(&m.tikvRaftstoreStoreWriteTriggerWB, other.TiKVRaftstoreStoreWriteTriggerWB())
	atomic.AddInt64(&m.tikvStorageProcessedKeysBatchGet, other.TiKVStorageProcessedKeysBatchGet())
	atomic.AddInt64(&m.tikvStorageProcessedKeysGet, other.TiKVStorageProcessedKeysGet())
	mergeIntoRUV2LabelCounter(&m.tikvCoprocessorWorkTotal, &other.tikvCoprocessorWorkTotal)
}

func mergeIntoRUV2LabelCounter(dst, src *ruv2LabelCounter) {
	if dst == nil || src == nil {
		return
	}
	cloneRUV2LabelCounter(dst, src)
}

// ResultChunkCells returns result cells written by the current statement.
func (m *RUV2Metrics) ResultChunkCells() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.resultChunkCells)
}

// ExecutorL5InsertRows returns affected insert rows for RUv2 accounting.
func (m *RUV2Metrics) ExecutorL5InsertRows() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.executorL5InsertRows)
}

// PlanCnt returns plan builder invocations for the current statement.
func (m *RUV2Metrics) PlanCnt() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.planCnt)
}

// PlanDeriveStatsPaths returns derived stats paths for the current statement.
func (m *RUV2Metrics) PlanDeriveStatsPaths() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.planDeriveStatsPaths)
}

// SessionParserTotal returns parser executions for the current statement.
func (m *RUV2Metrics) SessionParserTotal() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.sessionParserTotal)
}

// TxnCnt returns transaction completions attributed to the current statement.
func (m *RUV2Metrics) TxnCnt() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.txnCnt)
}

// ResourceManagerReadCnt returns TiKV read RPCs charged to resource management.
func (m *RUV2Metrics) ResourceManagerReadCnt() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.resourceManagerReadCnt)
}

// ResourceManagerWriteCnt returns TiKV write RPCs charged to resource management.
func (m *RUV2Metrics) ResourceManagerWriteCnt() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.resourceManagerWriteCnt)
}

// TiKVKVEngineCacheMiss returns TiKV kv_engine_cache_miss counters from ExecDetailsV2.
func (m *RUV2Metrics) TiKVKVEngineCacheMiss() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.tikvKvEngineCacheMiss)
}

// TiKVCoprocessorExecutorIterations returns TiKV coprocessor iteration counters.
func (m *RUV2Metrics) TiKVCoprocessorExecutorIterations() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.tikvCoprocessorExecutorIterations)
}

// TiKVCoprocessorResponseBytes returns TiKV coprocessor response bytes.
func (m *RUV2Metrics) TiKVCoprocessorResponseBytes() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.tikvCoprocessorResponseBytes)
}

// TiKVRaftstoreStoreWriteTriggerWB returns TiKV raftstore write trigger bytes.
func (m *RUV2Metrics) TiKVRaftstoreStoreWriteTriggerWB() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.tikvRaftstoreStoreWriteTriggerWB)
}

// TiKVStorageProcessedKeysBatchGet returns TiKV batch-get processed keys.
func (m *RUV2Metrics) TiKVStorageProcessedKeysBatchGet() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.tikvStorageProcessedKeysBatchGet)
}

// TiKVStorageProcessedKeysGet returns TiKV get processed keys.
func (m *RUV2Metrics) TiKVStorageProcessedKeysGet() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.tikvStorageProcessedKeysGet)
}

// IsZero checks whether all metrics are zero.
func (m *RUV2Metrics) IsZero() bool {
	if m == nil || m.Bypass() {
		return true
	}
	return m.ResultChunkCells() == 0 &&
		len(snapshotRUV2LabelCounter(&m.executorL1)) == 0 &&
		len(snapshotRUV2LabelCounter(&m.executorL2)) == 0 &&
		len(snapshotRUV2LabelCounter(&m.executorL3)) == 0 &&
		m.ExecutorL5InsertRows() == 0 &&
		m.PlanCnt() == 0 &&
		m.PlanDeriveStatsPaths() == 0 &&
		m.SessionParserTotal() == 0 &&
		m.TxnCnt() == 0 &&
		m.ResourceManagerReadCnt() == 0 &&
		m.ResourceManagerWriteCnt() == 0 &&
		m.TiKVKVEngineCacheMiss() == 0 &&
		m.TiKVCoprocessorExecutorIterations() == 0 &&
		m.TiKVCoprocessorResponseBytes() == 0 &&
		m.TiKVRaftstoreStoreWriteTriggerWB() == 0 &&
		m.TiKVStorageProcessedKeysBatchGet() == 0 &&
		m.TiKVStorageProcessedKeysGet() == 0 &&
		len(snapshotRUV2LabelCounter(&m.tikvCoprocessorWorkTotal)) == 0
}

// CalculateRUValues calculates the current TiDB RU from the metrics using the
// provided weights. The weights specify how each component is weighted in the
// RU calculation. Returns the calculated TiDB RU as a float64.
func (m *RUV2Metrics) CalculateRUValues(weights RUV2Weights) (tidbRU float64) {
	if m == nil || m.Bypass() {
		return 0
	}
	return m.calculateRUValuesWithWeights(weights)
}

// TotalRU returns the statement RU v2 total as TiDB + TiKV + TiFlash.
func (m *RUV2Metrics) TotalRU(weights RUV2Weights, tiKVRU, tiFlashRU float64) float64 {
	if m == nil {
		return tiKVRU + tiFlashRU
	}
	if m.Bypass() {
		return 0
	}
	return m.CalculateRUValues(weights) + tiKVRU + tiFlashRU
}

func (m *RUV2Metrics) calculateRUValuesWithWeights(weights RUV2Weights) (tidbRU float64) {
	tidbRUFloat :=
		float64(m.ResultChunkCells())*weights.ResultChunkCells +
			float64(sumRUV2LabelMap(snapshotRUV2LabelCounter(&m.executorL1)))*weights.ExecutorL1 +
			float64(sumRUV2LabelMap(snapshotRUV2LabelCounter(&m.executorL2)))*weights.ExecutorL2 +
			float64(sumRUV2LabelMap(snapshotRUV2LabelCounter(&m.executorL3)))*weights.ExecutorL3 +
			float64(m.ExecutorL5InsertRows())*weights.ExecutorL5InsertRows +
			float64(m.PlanCnt())*weights.PlanCnt +
			float64(m.PlanDeriveStatsPaths())*weights.PlanDeriveStatsPaths +
			float64(m.ResourceManagerReadCnt())*weights.ResourceManagerReadCnt +
			float64(m.ResourceManagerWriteCnt())*weights.ResourceManagerWriteCnt +
			float64(m.SessionParserTotal())*weights.SessionParserTotal +
			float64(m.TxnCnt())*weights.TxnCnt

	return tidbRUFloat * weights.RUScale
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

// FormatRUV2Summary formats the RUv2 total and detailed metrics in one pass.
func FormatRUV2Summary(metrics *RUV2Metrics, weights RUV2Weights, tiKVRU, tiFlashRU float64) (total string, detail string) {
	if metrics != nil && metrics.Bypass() {
		return "", ""
	}
	var (
		resultChunkCells                  int64
		executorL1                        map[string]int64
		executorL2                        map[string]int64
		executorL3                        map[string]int64
		executorL5InsertRows              int64
		planCnt                           int64
		planDeriveStatsPaths              int64
		sessionParserTotal                int64
		txnCnt                            int64
		resourceManagerReadCnt            int64
		resourceManagerWriteCnt           int64
		tiKVKVEngineCacheMiss             int64
		tiKVCoprocessorExecutorIterations int64
		tiKVCoprocessorResponseBytes      int64
		tiKVRaftstoreStoreWriteTriggerWB  int64
		tiKVStorageProcessedKeysBatchGet  int64
		tiKVStorageProcessedKeysGet       int64
		tiKVCoprocessorExecutorWorkTotal  map[string]int64
		tidbRU                            float64
	)
	if metrics != nil {
		resultChunkCells = metrics.ResultChunkCells()
		executorL1 = snapshotRUV2LabelCounter(&metrics.executorL1)
		executorL2 = snapshotRUV2LabelCounter(&metrics.executorL2)
		executorL3 = snapshotRUV2LabelCounter(&metrics.executorL3)
		executorL5InsertRows = metrics.ExecutorL5InsertRows()
		planCnt = metrics.PlanCnt()
		planDeriveStatsPaths = metrics.PlanDeriveStatsPaths()
		sessionParserTotal = metrics.SessionParserTotal()
		txnCnt = metrics.TxnCnt()
		resourceManagerReadCnt = metrics.ResourceManagerReadCnt()
		resourceManagerWriteCnt = metrics.ResourceManagerWriteCnt()
		tiKVKVEngineCacheMiss = metrics.TiKVKVEngineCacheMiss()
		tiKVCoprocessorExecutorIterations = metrics.TiKVCoprocessorExecutorIterations()
		tiKVCoprocessorResponseBytes = metrics.TiKVCoprocessorResponseBytes()
		tiKVRaftstoreStoreWriteTriggerWB = metrics.TiKVRaftstoreStoreWriteTriggerWB()
		tiKVStorageProcessedKeysBatchGet = metrics.TiKVStorageProcessedKeysBatchGet()
		tiKVStorageProcessedKeysGet = metrics.TiKVStorageProcessedKeysGet()
		tiKVCoprocessorExecutorWorkTotal = snapshotRUV2LabelCounter(&metrics.tikvCoprocessorWorkTotal)
		tidbRU = metrics.calculateRUValuesWithWeights(weights)
	}
	if resultChunkCells == 0 &&
		len(executorL1) == 0 &&
		len(executorL2) == 0 &&
		len(executorL3) == 0 &&
		executorL5InsertRows == 0 &&
		planCnt == 0 &&
		planDeriveStatsPaths == 0 &&
		sessionParserTotal == 0 &&
		txnCnt == 0 &&
		resourceManagerReadCnt == 0 &&
		resourceManagerWriteCnt == 0 &&
		tiKVKVEngineCacheMiss == 0 &&
		tiKVCoprocessorExecutorIterations == 0 &&
		tiKVCoprocessorResponseBytes == 0 &&
		tiKVRaftstoreStoreWriteTriggerWB == 0 &&
		tiKVStorageProcessedKeysBatchGet == 0 &&
		tiKVStorageProcessedKeysGet == 0 &&
		len(tiKVCoprocessorExecutorWorkTotal) == 0 &&
		tiKVRU == 0 &&
		tiFlashRU == 0 {
		return "", ""
	}
	parts := make([]string, 0, 19)
	appendInt := func(key string, value int64) {
		if value != 0 {
			parts = append(parts, fmt.Sprintf("%s:%d", key, value))
		}
	}
	appendFloat64Always := func(key string, value float64) {
		parts = append(parts, fmt.Sprintf("%s:%.2f", key, value))
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

	totalRU := tidbRU + tiKVRU + tiFlashRU
	total = fmt.Sprintf("%.2f", totalRU)
	appendFloat64Always("total_ru", totalRU)
	appendFloat64Always("tidb_ru", tidbRU)
	appendFloat64Always("tikv_ru", tiKVRU)
	appendFloat64Always("tiflash_ru", tiFlashRU)

	appendInt("result_chunk_cells", resultChunkCells)
	appendMap("executor_l1", executorL1)
	appendMap("executor_l2", executorL2)
	appendMap("executor_l3", executorL3)
	appendInt("executor_l5_insert_rows", executorL5InsertRows)
	appendInt("plan_cnt", planCnt)
	appendInt("plan_derive_stats_paths", planDeriveStatsPaths)
	appendInt("session_parser_total", sessionParserTotal)
	appendInt("txn_cnt", txnCnt)
	appendInt("resource_manager_read_cnt", resourceManagerReadCnt)
	appendInt("resource_manager_write_cnt", resourceManagerWriteCnt)
	appendInt("tikv_kv_engine_cache_miss", tiKVKVEngineCacheMiss)
	appendInt("tikv_coprocessor_executor_iterations", tiKVCoprocessorExecutorIterations)
	appendInt("tikv_coprocessor_response_bytes", tiKVCoprocessorResponseBytes)
	appendInt("tikv_raftstore_store_write_trigger_wb_bytes", tiKVRaftstoreStoreWriteTriggerWB)
	appendInt("tikv_storage_processed_keys_batch_get", tiKVStorageProcessedKeysBatchGet)
	appendInt("tikv_storage_processed_keys_get", tiKVStorageProcessedKeysGet)
	appendMap("tikv_coprocessor_executor_work_total", tiKVCoprocessorExecutorWorkTotal)

	return total, strings.Join(parts, ", ")
}

// FormatRUV2Total formats the RUv2 total into a slow log string.
func FormatRUV2Total(metrics *RUV2Metrics, weights RUV2Weights, tiKVRU, tiFlashRU float64) string {
	total, _ := FormatRUV2Summary(metrics, weights, tiKVRU, tiFlashRU)
	return total
}

// FormatRUV2Metrics formats RUv2 metrics into a compact detail string.
func FormatRUV2Metrics(metrics *RUV2Metrics, weights RUV2Weights, tiKVRU, tiFlashRU float64) string {
	_, detail := FormatRUV2Summary(metrics, weights, tiKVRU, tiFlashRU)
	return detail
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
