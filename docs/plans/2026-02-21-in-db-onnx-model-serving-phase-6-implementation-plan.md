# In-DB ONNX Model Serving Phase 6 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add Phase 6 observability: EXPLAIN ANALYZE model stats, slow log model_inference payload, and low-cardinality metrics (load latency + batch size).

**Architecture:** Record statement-scoped model inference stats in `StmtCtx`, keyed by plan ID, model/version, and role. Use a lightweight EvalContext wrapper to pass role/plan ID into `MODEL_PREDICT` evaluation. Aggregate per-plan stats into a `ModelRuntimeStats` registered during `EXPLAIN ANALYZE`, and export per-model summaries to slow logs. Metrics stay low-cardinality via histograms and result labels.

**Tech Stack:** Go, TiDB executor/stmtctx/expression, Prometheus client_golang, slow log parser, infoschema slow_query table.

---

### Task 1: Statement-Scoped Model Inference Stats Collector

**Files:**
- Create: `/Users/brian.w/projects/tidb/pkg/sessionctx/stmtctx/model_inference_stats.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/sessionctx/stmtctx/stmtctx.go`
- Test: `/Users/brian.w/projects/tidb/pkg/sessionctx/stmtctx/model_inference_stats_test.go`

**Step 1: Write the failing test**

```go
func TestModelInferenceStatsCollectorAggregates(t *testing.T) {
	sc := stmtctx.NewStmtCtx()
	stats := sc.ModelInferenceStats()
	stats.RecordInference(stmtctx.ModelInferenceRecord{
		PlanID: 1, Role: stmtctx.ModelInferenceRolePredicate, ModelID: 10, Version: 2,
		BatchSize: 3, Duration: 5 * time.Millisecond,
	})
	stats.RecordInference(stmtctx.ModelInferenceRecord{
		PlanID: 1, Role: stmtctx.ModelInferenceRolePredicate, ModelID: 10, Version: 2,
		BatchSize: 2, Duration: 7 * time.Millisecond, Err: errors.New("boom"),
	})
	stats.RecordLoad(stmtctx.ModelInferenceLoadRecord{
		PlanID: 1, Role: stmtctx.ModelInferenceRolePredicate, ModelID: 10, Version: 2,
		Duration: 4 * time.Millisecond,
	})
	summaries := stats.SlowLogSummaries()
	require.Len(t, summaries, 1)
	require.Equal(t, uint64(2), summaries[0].Calls)
	require.Equal(t, uint64(1), summaries[0].Errors)
	require.Equal(t, uint64(3), summaries[0].MaxBatchSize)
	require.Equal(t, uint64(1), summaries[0].LoadErrors)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/sessionctx/stmtctx -run TestModelInferenceStatsCollectorAggregates`  
Expected: FAIL with missing types/methods.

**Step 3: Write minimal implementation**

```go
type ModelInferenceRole string
const (
  ModelInferenceRolePredicate  ModelInferenceRole = "predicate"
  ModelInferenceRoleProjection ModelInferenceRole = "projection"
)

type ModelInferenceRecord struct { PlanID int; Role ModelInferenceRole; ModelID, Version int64; BatchSize int; Duration time.Duration; Err error }
type ModelInferenceLoadRecord struct { PlanID int; Role ModelInferenceRole; ModelID, Version int64; Duration time.Duration; Err error }

type ModelInferenceStatsCollector struct { mu sync.Mutex; stats map[modelInferenceKey]*ModelInferenceStats }
func (sc *StatementContext) ModelInferenceStats() *ModelInferenceStatsCollector { /* GetOrStoreStmtCache */ }
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/sessionctx/stmtctx -run TestModelInferenceStatsCollectorAggregates`  
Expected: PASS.

**Step 5: Commit**

```bash
git add /Users/brian.w/projects/tidb/pkg/sessionctx/stmtctx/model_inference_stats.go \
  /Users/brian.w/projects/tidb/pkg/sessionctx/stmtctx/stmtctx.go \
  /Users/brian.w/projects/tidb/pkg/sessionctx/stmtctx/model_inference_stats_test.go
git commit -m "model: add stmtctx inference stats collector"
```

---

### Task 2: EvalContext Role/Plan Target + Inference/Load Recording

**Files:**
- Create: `/Users/brian.w/projects/tidb/pkg/expression/model_inference_stats.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/expression/builtin_model.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/builder.go`
- Test: `/Users/brian.w/projects/tidb/pkg/executor/model_predict_test.go`

**Step 1: Write the failing test**

```go
func TestExplainAnalyzeModelPredictStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_model_ddl = on")
	tk.MustExec("set global tidb_enable_model_inference = on")
	// create model + table
	// set model predict hook to return value
	rows := tk.MustQuery("explain analyze select model_predict(test.m1, x).score from t").Rows()
	found := false
	for _, row := range rows {
		if strings.Contains(row[4].(string), "model_inference:") {
			found = true
		}
	}
	require.True(t, found)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/executor -run TestExplainAnalyzeModelPredictStats`  
Expected: FAIL (no model_inference stats).

**Step 3: Write minimal implementation**

```go
type modelInferenceStatsTarget interface{ ModelInferenceStatsTarget() (stmtctx.ModelInferenceRole, int) }
func WithModelInferenceStatsTarget(ctx EvalContext, role stmtctx.ModelInferenceRole, planID int) EvalContext { /* wrapper */ }

// In builder.go:
expression.WithModelInferenceStatsTarget(e.evalCtx, stmtctx.ModelInferenceRolePredicate, v.ID())
expression.WithModelInferenceStatsTarget(e.evalCtx, stmtctx.ModelInferenceRoleProjection, v.ID())

// In builtin_model.go:
start := time.Now()
outputs, err := b.meta.predict(inputs)
recordInferenceStats(vars.StmtCtx, ctx, b.meta, batchSize, time.Since(start), err)
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/executor -run TestExplainAnalyzeModelPredictStats`  
Expected: PASS.

**Step 5: Commit**

```bash
git add /Users/brian.w/projects/tidb/pkg/expression/model_inference_stats.go \
  /Users/brian.w/projects/tidb/pkg/expression/builtin_model.go \
  /Users/brian.w/projects/tidb/pkg/executor/builder.go \
  /Users/brian.w/projects/tidb/pkg/executor/model_predict_test.go
git commit -m "model: record inference stats with role/plan target"
```

---

### Task 3: EXPLAIN ANALYZE Runtime Stats Registration

**Files:**
- Create: `/Users/brian.w/projects/tidb/pkg/util/execdetails/model_runtime_stats.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/util/execdetails/runtime_stats.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/explain.go`
- Test: `/Users/brian.w/projects/tidb/pkg/executor/model_predict_test.go`

**Step 1: Write the failing test**

```go
// Extend TestExplainAnalyzeModelPredictStats to assert execution info contains
// "model_inference: time:" and non-zero calls.
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/executor -run TestExplainAnalyzeModelPredictStats`  
Expected: FAIL (no runtime stats string).

**Step 3: Write minimal implementation**

```go
type ModelRuntimeStats struct { Role string; Calls, Errors, MaxBatch uint64; TotalTime time.Duration; TotalBatch uint64 }
func (m *ModelRuntimeStats) String() string { /* model_inference: time:..., calls:..., errors:..., avg_batch:..., max_batch:... */ }
func (*ModelRuntimeStats) Tp() int { return execdetails.TpModelRuntimeStats }

// explain.go: after analyze exec, read stmtCtx.ModelInferenceStats().PlanSummaries()
// and coll.RegisterStats(planID, &ModelRuntimeStats{...})
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/executor -run TestExplainAnalyzeModelPredictStats`  
Expected: PASS.

**Step 5: Commit**

```bash
git add /Users/brian.w/projects/tidb/pkg/util/execdetails/model_runtime_stats.go \
  /Users/brian.w/projects/tidb/pkg/util/execdetails/runtime_stats.go \
  /Users/brian.w/projects/tidb/pkg/executor/explain.go \
  /Users/brian.w/projects/tidb/pkg/executor/model_predict_test.go
git commit -m "model: register explain analyze runtime stats"
```

---

### Task 4: Slow Log Field + Slow Query Table Parsing

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/sessionctx/variable/slow_log.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/adapter_slow_log.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/slow_query.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/infoschema/tables.go`
- Test: `/Users/brian.w/projects/tidb/pkg/executor/model_predict_test.go`

**Step 1: Write the failing test**

```go
func TestModelPredictSlowLogPayload(t *testing.T) {
	// set slow log file + threshold 0
	// run model_predict query
	rows := tk.MustQuery("select model_inference from information_schema.slow_query order by time desc limit 1").Rows()
	require.Contains(t, rows[0][0].(string), "\"model_id\"")
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/executor -run TestModelPredictSlowLogPayload`  
Expected: FAIL (column missing or empty).

**Step 3: Write minimal implementation**

```go
const SlowLogModelInference = "Model_inference"
// SlowQueryLogItems.ModelInference string
// SlowLogFormat: writeSlowLogItem(buf, SlowLogModelInference, logItems.ModelInference)
// SetSlowLogItems: items.ModelInference = stmtCtx.ModelInferenceStats().SlowLogJSON()
// slow_query.go: parse Model_inference as string
// infoschema/tables.go: add column for Model_inference
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/executor -run TestModelPredictSlowLogPayload`  
Expected: PASS.

**Step 5: Commit**

```bash
git add /Users/brian.w/projects/tidb/pkg/sessionctx/variable/slow_log.go \
  /Users/brian.w/projects/tidb/pkg/executor/adapter_slow_log.go \
  /Users/brian.w/projects/tidb/pkg/executor/slow_query.go \
  /Users/brian.w/projects/tidb/pkg/infoschema/tables.go \
  /Users/brian.w/projects/tidb/pkg/executor/model_predict_test.go
git commit -m "model: add slow log model_inference payload"
```

---

### Task 5: Metrics for Load Duration and Batch Size

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/metrics/model.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/metrics/metrics.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/util/modelruntime/infer.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/expression/builtin_model.go`
- Test: `/Users/brian.w/projects/tidb/pkg/util/modelruntime/infer_test.go`
- Test: `/Users/brian.w/projects/tidb/pkg/executor/model_predict_test.go`

**Step 1: Write the failing tests**

```go
func TestModelBatchSizeMetric(t *testing.T) {
	metrics.ModelBatchSize.Reset()
	// stub runInferenceBatchFn to avoid ONNX
	_, _ = RunInferenceBatchWithOptions(nil, "", []byte("dummy"), []string{"a"}, []string{"out"}, [][]float32{{1}, {2}}, InferenceOptions{})
	out, _ := promtestutils.CollectAndFormat(metrics.ModelBatchSize, expfmt.FmtText)
	require.Contains(t, string(out), "tidb_model_batch_size_count{type=\"batch\"} 1")
}
```

```go
func TestModelLoadDurationMetric(t *testing.T) {
	metrics.ModelLoadDuration.Reset()
	// create model, run model_predict once
	out, _ := promtestutils.CollectAndFormat(metrics.ModelLoadDuration, expfmt.FmtText)
	require.Contains(t, string(out), "tidb_model_load_duration_seconds_count{result=\"ok\"} 1")
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./pkg/util/modelruntime -run TestModelBatchSizeMetric`  
Run: `go test ./pkg/executor -run TestModelLoadDurationMetric`  
Expected: FAIL (metrics undefined/not incremented).

**Step 3: Write minimal implementation**

```go
// metrics/model.go
ModelLoadDuration = metricscommon.NewHistogramVec(..., []string{LblResult})
ModelBatchSize = metricscommon.NewHistogramVec(..., []string{LblType})

// modelruntime/infer.go
metrics.ModelBatchSize.WithLabelValues("scalar").Observe(1)
metrics.ModelBatchSize.WithLabelValues("batch").Observe(float64(len(batch)))

// builtin_model.go (loadModelPredictMeta)
start := time.Now()
artifact, err := loader.Load(...)
metrics.ModelLoadDuration.WithLabelValues(metrics.RetLabel(err)).Observe(time.Since(start).Seconds())
```

**Step 4: Run tests to verify they pass**

Run: `go test ./pkg/util/modelruntime -run TestModelBatchSizeMetric`  
Run: `go test ./pkg/executor -run TestModelLoadDurationMetric`  
Expected: PASS.

**Step 5: Commit**

```bash
git add /Users/brian.w/projects/tidb/pkg/metrics/model.go \
  /Users/brian.w/projects/tidb/pkg/metrics/metrics.go \
  /Users/brian.w/projects/tidb/pkg/util/modelruntime/infer.go \
  /Users/brian.w/projects/tidb/pkg/expression/builtin_model.go \
  /Users/brian.w/projects/tidb/pkg/util/modelruntime/infer_test.go \
  /Users/brian.w/projects/tidb/pkg/executor/model_predict_test.go
git commit -m "model: add load and batch size metrics"
```

---

### Task 6: Update Progress Doc

**Files:**
- Modify: `/Users/brian.w/projects/tidb/docs/plans/2026-02-20-in-db-onnx-model-serving-progress.md`

**Step 1: Mark Phase 6 complete**

```markdown
## Phase 6: Observability
- [x] EXPLAIN ANALYZE model inference stats
- [x] Slow log model_inference payload
- [x] Metrics: load latency + batch size
```

**Step 2: Commit**

```bash
git add /Users/brian.w/projects/tidb/docs/plans/2026-02-20-in-db-onnx-model-serving-progress.md
git commit -m "docs: mark phase 6 complete"
```
