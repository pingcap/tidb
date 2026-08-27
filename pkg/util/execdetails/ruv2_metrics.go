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

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	tikvutil "github.com/tikv/client-go/v2/util"
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
	WriteKeys               float64
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
	if stmtDetails, _ := ctx.Value(StmtExecDetailKey).(*StmtExecDetails); stmtDetails != nil {
		if metrics := stmtDetails.getRUV2Metrics(); metrics != nil {
			return metrics
		}
	}
	// Keep the standalone context key as the fallback path for callers that
	// intentionally inherit RUv2 metrics into a context without StmtExecDetails.
	if metrics, _ := ctx.Value(RUV2MetricsCtxKey).(*RUV2Metrics); metrics != nil {
		return metrics
	}
	return nil
}

// UpdateRUV2MetricsFromRUV2 adds raw RUv2 counters into the statement-level metrics snapshot.
func UpdateRUV2MetricsFromRUV2(m *RUV2Metrics, ru *kvrpcpb.RUV2) {
	if m == nil || ru == nil || m.Bypass() {
		return
	}
	m.applyRawCounters(ru)
}

// applyRawCounters writes ru into m. Caller must check Bypass.
func (m *RUV2Metrics) applyRawCounters(ru *kvrpcpb.RUV2) {
	if v := ru.ReadRpcCount; v != 0 {
		metrics.RUV2ResourceManagerReadCnt.Add(float64(v))
		atomic.AddInt64(&m.resourceManagerReadCnt, int64(v))
	}
	if v := ru.KvEngineCacheMiss; v != 0 {
		metrics.RUV2TiKVKVEngineCacheMiss.Add(float64(v))
		atomic.AddInt64(&m.tikvKvEngineCacheMiss, int64(v))
	}
	if v := ru.StorageProcessedKeysBatchGet; v != 0 {
		metrics.RUV2TiKVStorageProcessedKeysBatchGet.Add(float64(v))
		atomic.AddInt64(&m.tikvStorageProcessedKeysBatchGet, int64(v))
	}
	if v := ru.StorageProcessedKeysGet; v != 0 {
		metrics.RUV2TiKVStorageProcessedKeysGet.Add(float64(v))
		atomic.AddInt64(&m.tikvStorageProcessedKeysGet, int64(v))
	}

	var extra *ruv2MetricsExtra
	ensureExtra := func() *ruv2MetricsExtra {
		if extra == nil {
			extra = m.ensureExtra()
		}
		return extra
	}
	if v := ru.WriteRpcCount; v != 0 {
		metrics.RUV2ResourceManagerWriteCnt.Add(float64(v))
		atomic.AddInt64(&ensureExtra().resourceManagerWriteCnt, int64(v))
	}
	if v := ru.CoprocessorExecutorIterations; v != 0 {
		metrics.RUV2TiKVCoprocessorExecutorIterations.Add(float64(v))
		atomic.AddInt64(&ensureExtra().tikvCoprocessorExecutorIterations, int64(v))
	}
	if v := ru.CoprocessorResponseBytes; v != 0 {
		metrics.RUV2TiKVCoprocessorResponseBytes.Add(float64(v))
		atomic.AddInt64(&ensureExtra().tikvCoprocessorResponseBytes, int64(v))
	}
	if v := ru.RaftstoreStoreWriteTriggerWbBytes; v != 0 {
		metrics.RUV2TiKVRaftstoreStoreWriteTriggerWB.Add(float64(v))
		atomic.AddInt64(&ensureExtra().tikvRaftstoreStoreWriteTriggerWB, int64(v))
	}
	if inputs := ru.ExecutorInputs; inputs != nil {
		addWork := func(label string, v uint64) {
			if v == 0 {
				return
			}
			metrics.RUV2TiKVCoprocessorWorkTotalCounter(label).Add(float64(v))
			addRUV2ExtraLabelCounter(&ensureExtra().tikvCoprocessorWorkTotal, label, int64(v))
		}
		addWork("BatchIndexScan", inputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan)
		addWork("BatchTableScan", inputs.TikvCoprocessorExecutorWorkTotalBatchTableScan)
		addWork("BatchSelection", inputs.TikvCoprocessorExecutorWorkTotalBatchSelection)
		addWork("BatchTopN", inputs.TikvCoprocessorExecutorWorkTotalBatchTopN)
		addWork("BatchLimit", inputs.TikvCoprocessorExecutorWorkTotalBatchLimit)
		addWork("BatchSimpleAggr", inputs.TikvCoprocessorExecutorWorkTotalBatchSimpleAggr)
		addWork("BatchFastHashAggr", inputs.TikvCoprocessorExecutorWorkTotalBatchFastHashAggr)
	}
}

// SyncRUV2MetricsFromRUDetails drains the raw RUv2 counters accumulated in
// RUDetails since the last drain and adds them into the statement-level metrics.
// It is safe to call multiple times; each call transfers only the delta.
func SyncRUV2MetricsFromRUDetails(metrics *RUV2Metrics, ruDetails *tikvutil.RUDetails) {
	if metrics == nil || ruDetails == nil || metrics.Bypass() {
		return
	}
	UpdateRUV2MetricsFromRUV2(metrics, ruDetails.DrainRUV2())
}

// UpdateRUV2MetricsFromCommitDetails adds commit write counters into RUv2 metrics.
func UpdateRUV2MetricsFromCommitDetails(metrics *RUV2Metrics, commitDetails *tikvutil.CommitDetails) {
	if metrics == nil || commitDetails == nil || metrics.Bypass() {
		return
	}
	if commitDetails.WriteKeys != 0 {
		metrics.AddWriteKeys(int64(commitDetails.WriteKeys))
	}
	if commitDetails.WriteSize != 0 {
		metrics.AddWriteSize(int64(commitDetails.WriteSize))
	}
}

// RUV2Metrics stores statement-level RUv2 metrics.
type RUV2Metrics struct {
	bypass atomic.Bool

	resultChunkCells int64

	executorL1 ruv2ExecutorL1Counter

	planCnt            int64
	sessionParserTotal int64
	txnCnt             int64

	resourceManagerReadCnt int64

	tikvKvEngineCacheMiss            int64
	tikvStorageProcessedKeysBatchGet int64
	tikvStorageProcessedKeysGet      int64

	extra atomic.Pointer[ruv2MetricsExtra]
}

type ruv2MetricsExtra struct {
	executorL2 ruv2ExtraLabelCounter
	executorL3 ruv2ExtraLabelCounter

	executorL5InsertRows int64
	planDeriveStatsPaths int64

	resourceManagerWriteCnt int64
	writeKeys               int64
	writeSize               int64

	tikvCoprocessorExecutorIterations int64
	tikvCoprocessorResponseBytes      int64
	tikvRaftstoreStoreWriteTriggerWB  int64
	tikvCoprocessorWorkTotal          ruv2ExtraLabelCounter
}

func (m *RUV2Metrics) loadExtra() *ruv2MetricsExtra {
	if m == nil {
		return nil
	}
	return m.extra.Load()
}

func (m *RUV2Metrics) ensureExtra() *ruv2MetricsExtra {
	if m == nil {
		return nil
	}
	if extra := m.extra.Load(); extra != nil {
		return extra
	}
	extra := &ruv2MetricsExtra{}
	if m.extra.CompareAndSwap(nil, extra) {
		return extra
	}
	return m.extra.Load()
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
	if counter := metrics.RUV2ExecutorCounter(level, label); counter != nil {
		counter.Add(float64(delta))
	}
	switch level {
	case 1:
		m.executorL1.add(label, delta)
	case 2:
		addRUV2ExtraLabelCounter(&m.ensureExtra().executorL2, label, delta)
	case 3:
		addRUV2ExtraLabelCounter(&m.ensureExtra().executorL3, label, delta)
	}
}

// execL1Kind selects one of the hot L1 executor counter fields; execL1None means none.
type execL1Kind uint8

const (
	execL1None execL1Kind = iota
	execL1BatchPointGet
	execL1PointGet
	execL1Limit
)

// ExecutorMetricRecorder is a pre-resolved counter for one hot L1 executor metric.
// The zero value records nothing; callers must check Available before Record.
type ExecutorMetricRecorder struct {
	counter prometheus.Counter
	kind    execL1Kind
}

// Available reports whether this recorder was resolved.
func (r ExecutorMetricRecorder) Available() bool { return r.kind != execL1None }

// Record applies delta. Caller must ensure m is non-nil and not bypassed.
func (r ExecutorMetricRecorder) Record(m *RUV2Metrics, delta int64) {
	r.counter.Add(float64(delta))
	atomic.AddInt64(m.executorL1.fieldByKind(r.kind), delta)
}

// ResolveExecutorMetric returns a pre-resolved recorder for hot L1 executor
// labels, or the zero recorder for everything else.
func ResolveExecutorMetric(level int, label string) ExecutorMetricRecorder {
	if level != 1 {
		return ExecutorMetricRecorder{}
	}
	kind := execL1KindForLabel(label)
	if kind == execL1None {
		return ExecutorMetricRecorder{}
	}
	c := metrics.RUV2ExecutorCounter(level, label)
	if c == nil {
		return ExecutorMetricRecorder{}
	}
	return ExecutorMetricRecorder{counter: c, kind: kind}
}

func execL1KindForLabel(label string) execL1Kind {
	switch label {
	case ruv2LabelBatchPointGetExec:
		return execL1BatchPointGet
	case ruv2LabelPointGetExecutor:
		return execL1PointGet
	case ruv2LabelLimitExec:
		return execL1Limit
	default:
		return execL1None
	}
}

// AddExecutorL5InsertRows records insert rows multiplied by inserted column count for RUv2 accounting.
func (m *RUV2Metrics) AddExecutorL5InsertRows(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2ExecutorL5InsertRows.Add(float64(delta))
	atomic.AddInt64(&m.ensureExtra().executorL5InsertRows, delta)
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
	atomic.AddInt64(&m.ensureExtra().planDeriveStatsPaths, delta)
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
	atomic.AddInt64(&m.ensureExtra().resourceManagerWriteCnt, delta)
}

// AddWriteKeys records commit write keys for RUv2 accounting.
func (m *RUV2Metrics) AddWriteKeys(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2WriteKeys.Add(float64(delta))
	atomic.AddInt64(&m.ensureExtra().writeKeys, delta)
}

// AddWriteSize records commit write size for RUv2 shadow accounting.
func (m *RUV2Metrics) AddWriteSize(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2WriteSize.Add(float64(delta))
	atomic.AddInt64(&m.ensureExtra().writeSize, delta)
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
	atomic.AddInt64(&m.ensureExtra().tikvCoprocessorExecutorIterations, delta)
}

// AddTiKVCoprocessorResponseBytes records TiKV coprocessor response bytes.
func (m *RUV2Metrics) AddTiKVCoprocessorResponseBytes(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TiKVCoprocessorResponseBytes.Add(float64(delta))
	atomic.AddInt64(&m.ensureExtra().tikvCoprocessorResponseBytes, delta)
}

// AddTiKVRaftstoreStoreWriteTriggerWB records TiKV raftstore write trigger bytes.
func (m *RUV2Metrics) AddTiKVRaftstoreStoreWriteTriggerWB(delta int64) {
	if m.Bypass() {
		return
	}
	metrics.RUV2TiKVRaftstoreStoreWriteTriggerWB.Add(float64(delta))
	atomic.AddInt64(&m.ensureExtra().tikvRaftstoreStoreWriteTriggerWB, delta)
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
	metrics.RUV2TiKVCoprocessorWorkTotalCounter(label).Add(float64(delta))
	addRUV2ExtraLabelCounter(&m.ensureExtra().tikvCoprocessorWorkTotal, label, delta)
}

// Clone returns a copy of the current metrics for reporting.
func (m *RUV2Metrics) Clone() *RUV2Metrics {
	if m == nil {
		return nil
	}
	cloned := &RUV2Metrics{}
	cloned.bypass.Store(m.Bypass())
	atomic.StoreInt64(&cloned.resultChunkCells, atomic.LoadInt64(&m.resultChunkCells))
	cloneRUV2ExecutorL1Counter(&cloned.executorL1, &m.executorL1)
	atomic.StoreInt64(&cloned.planCnt, atomic.LoadInt64(&m.planCnt))
	atomic.StoreInt64(&cloned.sessionParserTotal, atomic.LoadInt64(&m.sessionParserTotal))
	atomic.StoreInt64(&cloned.txnCnt, atomic.LoadInt64(&m.txnCnt))
	atomic.StoreInt64(&cloned.resourceManagerReadCnt, atomic.LoadInt64(&m.resourceManagerReadCnt))
	atomic.StoreInt64(&cloned.tikvKvEngineCacheMiss, atomic.LoadInt64(&m.tikvKvEngineCacheMiss))
	atomic.StoreInt64(&cloned.tikvStorageProcessedKeysBatchGet, atomic.LoadInt64(&m.tikvStorageProcessedKeysBatchGet))
	atomic.StoreInt64(&cloned.tikvStorageProcessedKeysGet, atomic.LoadInt64(&m.tikvStorageProcessedKeysGet))
	if extra := m.loadExtra(); extra != nil {
		cloneRUV2MetricsExtra(cloned.ensureExtra(), extra)
	}
	return cloned
}

const (
	ruv2LabelBatchPointGetExec = "BatchPointGetExec"
	ruv2LabelPointGetExecutor  = "PointGetExecutor"
	ruv2LabelLimitExec         = "LimitExec"
)

type ruv2ExecutorL1Counter struct {
	batchPointGetExec int64
	pointGetExecutor  int64
	limitExec         int64
	extra             ruv2ExtraLabelCounter
}

type ruv2ExtraLabelCounter struct {
	values atomic.Pointer[sync.Map]
}

func (c *ruv2ExecutorL1Counter) add(label string, delta int64) {
	if p := c.fieldByKind(execL1KindForLabel(label)); p != nil {
		atomic.AddInt64(p, delta)
		return
	}
	addRUV2ExtraLabelCounter(&c.extra, label, delta)
}

func (c *ruv2ExecutorL1Counter) fieldByKind(kind execL1Kind) *int64 {
	switch kind {
	case execL1BatchPointGet:
		return &c.batchPointGetExec
	case execL1PointGet:
		return &c.pointGetExecutor
	case execL1Limit:
		return &c.limitExec
	}
	return nil
}

func (c *ruv2ExecutorL1Counter) snapshot() map[string]int64 {
	var out map[string]int64
	out = addRUV2LabelValue(out, ruv2LabelBatchPointGetExec, atomic.LoadInt64(&c.batchPointGetExec))
	out = addRUV2LabelValue(out, ruv2LabelPointGetExecutor, atomic.LoadInt64(&c.pointGetExecutor))
	out = addRUV2LabelValue(out, ruv2LabelLimitExec, atomic.LoadInt64(&c.limitExec))
	return snapshotRUV2ExtraLabelCounter(&c.extra, out)
}

func (c *ruv2ExecutorL1Counter) sum() int64 {
	return atomic.LoadInt64(&c.batchPointGetExec) +
		atomic.LoadInt64(&c.pointGetExecutor) +
		atomic.LoadInt64(&c.limitExec) +
		sumRUV2ExtraLabelCounter(&c.extra)
}

func (c *ruv2ExecutorL1Counter) isZero() bool {
	return c.sum() == 0
}

func addRUV2LabelValue(out map[string]int64, label string, value int64) map[string]int64 {
	if value == 0 {
		return out
	}
	if out == nil {
		out = make(map[string]int64)
	}
	out[label] = value
	return out
}

func addRUV2FixedCounter(dst *int64, delta int64) {
	if delta != 0 {
		atomic.AddInt64(dst, delta)
	}
}

func (c *ruv2ExtraLabelCounter) load() *sync.Map {
	if c == nil {
		return nil
	}
	return c.values.Load()
}

func (c *ruv2ExtraLabelCounter) loadOrCreate() *sync.Map {
	if c == nil {
		return nil
	}
	if counterMap := c.values.Load(); counterMap != nil {
		return counterMap
	}
	counterMap := &sync.Map{}
	if c.values.CompareAndSwap(nil, counterMap) {
		return counterMap
	}
	return c.values.Load()
}

func addRUV2ExtraLabelCounter(counter *ruv2ExtraLabelCounter, label string, delta int64) {
	counterMap := counter.loadOrCreate()
	if counterMap == nil {
		return
	}
	if current, ok := counterMap.Load(label); ok {
		atomic.AddInt64(current.(*int64), delta)
		return
	}
	value := new(int64)
	actual, _ := counterMap.LoadOrStore(label, value)
	atomic.AddInt64(actual.(*int64), delta)
}

func snapshotRUV2ExtraLabelCounter(counter *ruv2ExtraLabelCounter, out map[string]int64) map[string]int64 {
	counterMap := counter.load()
	if counterMap == nil {
		return out
	}
	counterMap.Range(func(key, value any) bool {
		label, ok := key.(string)
		if !ok {
			return true
		}
		val, ok := value.(*int64)
		if !ok {
			return true
		}
		returnValue := atomic.LoadInt64(val)
		if returnValue == 0 {
			return true
		}
		if out == nil {
			out = make(map[string]int64)
		}
		out[label] = returnValue
		return true
	})
	return out
}

func sumRUV2ExtraLabelCounter(counter *ruv2ExtraLabelCounter) int64 {
	counterMap := counter.load()
	if counterMap == nil {
		return 0
	}
	var total int64
	counterMap.Range(func(_, value any) bool {
		if val, ok := value.(*int64); ok {
			total += atomic.LoadInt64(val)
		}
		return true
	})
	return total
}

func cloneRUV2ExtraLabelCounter(dst, src *ruv2ExtraLabelCounter) {
	if dst == nil || src == nil {
		return
	}
	counterMap := src.load()
	if counterMap == nil {
		return
	}
	counterMap.Range(func(key, value any) bool {
		label, ok := key.(string)
		if !ok {
			return true
		}
		val, ok := value.(*int64)
		if !ok {
			return true
		}
		if cloned := atomic.LoadInt64(val); cloned != 0 {
			addRUV2ExtraLabelCounter(dst, label, cloned)
		}
		return true
	})
}

func cloneRUV2ExecutorL1Counter(dst, src *ruv2ExecutorL1Counter) {
	if dst == nil || src == nil {
		return
	}
	addRUV2FixedCounter(&dst.batchPointGetExec, atomic.LoadInt64(&src.batchPointGetExec))
	addRUV2FixedCounter(&dst.pointGetExecutor, atomic.LoadInt64(&src.pointGetExecutor))
	addRUV2FixedCounter(&dst.limitExec, atomic.LoadInt64(&src.limitExec))
	cloneRUV2ExtraLabelCounter(&dst.extra, &src.extra)
}

func cloneRUV2MetricsExtra(dst, src *ruv2MetricsExtra) {
	if dst == nil || src == nil {
		return
	}
	cloneRUV2ExtraLabelCounter(&dst.executorL2, &src.executorL2)
	cloneRUV2ExtraLabelCounter(&dst.executorL3, &src.executorL3)
	addRUV2FixedCounter(&dst.executorL5InsertRows, atomic.LoadInt64(&src.executorL5InsertRows))
	addRUV2FixedCounter(&dst.planDeriveStatsPaths, atomic.LoadInt64(&src.planDeriveStatsPaths))
	addRUV2FixedCounter(&dst.resourceManagerWriteCnt, atomic.LoadInt64(&src.resourceManagerWriteCnt))
	addRUV2FixedCounter(&dst.writeKeys, atomic.LoadInt64(&src.writeKeys))
	addRUV2FixedCounter(&dst.writeSize, atomic.LoadInt64(&src.writeSize))
	addRUV2FixedCounter(&dst.tikvCoprocessorExecutorIterations, atomic.LoadInt64(&src.tikvCoprocessorExecutorIterations))
	addRUV2FixedCounter(&dst.tikvCoprocessorResponseBytes, atomic.LoadInt64(&src.tikvCoprocessorResponseBytes))
	addRUV2FixedCounter(&dst.tikvRaftstoreStoreWriteTriggerWB, atomic.LoadInt64(&src.tikvRaftstoreStoreWriteTriggerWB))
	cloneRUV2ExtraLabelCounter(&dst.tikvCoprocessorWorkTotal, &src.tikvCoprocessorWorkTotal)
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
	cloneRUV2ExecutorL1Counter(&m.executorL1, &other.executorL1)
	atomic.AddInt64(&m.planCnt, other.PlanCnt())
	atomic.AddInt64(&m.sessionParserTotal, other.SessionParserTotal())
	atomic.AddInt64(&m.txnCnt, other.TxnCnt())
	atomic.AddInt64(&m.resourceManagerReadCnt, other.ResourceManagerReadCnt())
	atomic.AddInt64(&m.tikvKvEngineCacheMiss, other.TiKVKVEngineCacheMiss())
	atomic.AddInt64(&m.tikvStorageProcessedKeysBatchGet, other.TiKVStorageProcessedKeysBatchGet())
	atomic.AddInt64(&m.tikvStorageProcessedKeysGet, other.TiKVStorageProcessedKeysGet())
	if extra := other.loadExtra(); extra != nil {
		cloneRUV2MetricsExtra(m.ensureExtra(), extra)
	}
}

// ResultChunkCells returns result cells written by the current statement.
func (m *RUV2Metrics) ResultChunkCells() int64 {
	if m == nil {
		return 0
	}
	return atomic.LoadInt64(&m.resultChunkCells)
}

// ExecutorL5InsertRows returns insert rows multiplied by inserted column count for RUv2 accounting.
func (m *RUV2Metrics) ExecutorL5InsertRows() int64 {
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.executorL5InsertRows)
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
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.planDeriveStatsPaths)
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
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.resourceManagerWriteCnt)
}

// WriteKeys returns commit write keys for RUv2 accounting.
func (m *RUV2Metrics) WriteKeys() int64 {
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.writeKeys)
}

// WriteSize returns commit write size for RUv2 shadow accounting.
func (m *RUV2Metrics) WriteSize() int64 {
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.writeSize)
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
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.tikvCoprocessorExecutorIterations)
}

// TiKVCoprocessorResponseBytes returns TiKV coprocessor response bytes.
func (m *RUV2Metrics) TiKVCoprocessorResponseBytes() int64 {
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.tikvCoprocessorResponseBytes)
}

// TiKVRaftstoreStoreWriteTriggerWB returns TiKV raftstore write trigger bytes.
func (m *RUV2Metrics) TiKVRaftstoreStoreWriteTriggerWB() int64 {
	extra := m.loadExtra()
	if extra == nil {
		return 0
	}
	return atomic.LoadInt64(&extra.tikvRaftstoreStoreWriteTriggerWB)
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
	if m.ResultChunkCells() != 0 ||
		!m.executorL1.isZero() ||
		m.PlanCnt() != 0 ||
		m.SessionParserTotal() != 0 ||
		m.TxnCnt() != 0 ||
		m.ResourceManagerReadCnt() != 0 ||
		m.TiKVKVEngineCacheMiss() != 0 ||
		m.TiKVStorageProcessedKeysBatchGet() != 0 ||
		m.TiKVStorageProcessedKeysGet() != 0 {
		return false
	}
	extra := m.loadExtra()
	return extra == nil ||
		(sumRUV2ExtraLabelCounter(&extra.executorL2) == 0 &&
			sumRUV2ExtraLabelCounter(&extra.executorL3) == 0 &&
			m.ExecutorL5InsertRows() == 0 &&
			m.PlanDeriveStatsPaths() == 0 &&
			m.ResourceManagerWriteCnt() == 0 &&
			m.WriteKeys() == 0 &&
			m.WriteSize() == 0 &&
			m.TiKVCoprocessorExecutorIterations() == 0 &&
			m.TiKVCoprocessorResponseBytes() == 0 &&
			m.TiKVRaftstoreStoreWriteTriggerWB() == 0 &&
			sumRUV2ExtraLabelCounter(&extra.tikvCoprocessorWorkTotal) == 0)
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
	var (
		executorL2              int64
		executorL3              int64
		executorL5InsertRows    int64
		planDeriveStatsPaths    int64
		resourceManagerWriteCnt int64
		writeKeys               int64
	)
	if extra := m.loadExtra(); extra != nil {
		executorL2 = sumRUV2ExtraLabelCounter(&extra.executorL2)
		executorL3 = sumRUV2ExtraLabelCounter(&extra.executorL3)
		executorL5InsertRows = atomic.LoadInt64(&extra.executorL5InsertRows)
		planDeriveStatsPaths = atomic.LoadInt64(&extra.planDeriveStatsPaths)
		resourceManagerWriteCnt = atomic.LoadInt64(&extra.resourceManagerWriteCnt)
		writeKeys = atomic.LoadInt64(&extra.writeKeys)
	}
	tidbRUFloat :=
		float64(m.ResultChunkCells())*weights.ResultChunkCells +
			float64(m.executorL1.sum())*weights.ExecutorL1 +
			float64(executorL2)*weights.ExecutorL2 +
			float64(executorL3)*weights.ExecutorL3 +
			float64(executorL5InsertRows)*weights.ExecutorL5InsertRows +
			float64(m.PlanCnt())*weights.PlanCnt +
			float64(planDeriveStatsPaths)*weights.PlanDeriveStatsPaths +
			float64(m.ResourceManagerReadCnt())*weights.ResourceManagerReadCnt +
			float64(resourceManagerWriteCnt)*weights.ResourceManagerWriteCnt +
			float64(writeKeys)*weights.WriteKeys +
			float64(m.SessionParserTotal())*weights.SessionParserTotal +
			float64(m.TxnCnt())*weights.TxnCnt

	return tidbRUFloat * weights.RUScale
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
		writeKeys                         int64
		writeSize                         int64
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
		executorL1 = metrics.executorL1.snapshot()
		if extra := metrics.loadExtra(); extra != nil {
			executorL2 = snapshotRUV2ExtraLabelCounter(&extra.executorL2, nil)
			executorL3 = snapshotRUV2ExtraLabelCounter(&extra.executorL3, nil)
			executorL5InsertRows = atomic.LoadInt64(&extra.executorL5InsertRows)
			planDeriveStatsPaths = atomic.LoadInt64(&extra.planDeriveStatsPaths)
			resourceManagerWriteCnt = atomic.LoadInt64(&extra.resourceManagerWriteCnt)
			writeKeys = atomic.LoadInt64(&extra.writeKeys)
			writeSize = atomic.LoadInt64(&extra.writeSize)
			tiKVCoprocessorExecutorIterations = atomic.LoadInt64(&extra.tikvCoprocessorExecutorIterations)
			tiKVCoprocessorResponseBytes = atomic.LoadInt64(&extra.tikvCoprocessorResponseBytes)
			tiKVRaftstoreStoreWriteTriggerWB = atomic.LoadInt64(&extra.tikvRaftstoreStoreWriteTriggerWB)
			tiKVCoprocessorExecutorWorkTotal = snapshotRUV2ExtraLabelCounter(&extra.tikvCoprocessorWorkTotal, nil)
		}
		planCnt = metrics.PlanCnt()
		sessionParserTotal = metrics.SessionParserTotal()
		txnCnt = metrics.TxnCnt()
		resourceManagerReadCnt = metrics.ResourceManagerReadCnt()
		tiKVKVEngineCacheMiss = metrics.TiKVKVEngineCacheMiss()
		tiKVStorageProcessedKeysBatchGet = metrics.TiKVStorageProcessedKeysBatchGet()
		tiKVStorageProcessedKeysGet = metrics.TiKVStorageProcessedKeysGet()
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
		writeKeys == 0 &&
		writeSize == 0 &&
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
	appendInt("write_keys", writeKeys)
	appendInt("write_size", writeSize)
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
