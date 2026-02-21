# In-DB ONNX Model Serving Phase 7 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement Phase 7 optimizer cost ordering, RU accounting for inference CPU, and INFORMATION_SCHEMA views for model versions/cache.

**Architecture:** Add model-predicate detection to reorder filters and adjust selectivity defaults; report model inference CPU into existing resource group consumption; expose metadata via INFORMATION_SCHEMA backed by system tables and the process-level session cache.

**Tech Stack:** Go, TiDB planner/cardinality, resource group reporting (PD client), infoschema memtable retrievers, ONNX session cache.

---

### Task 1: Model Predicate Ordering + Filter Cost Weighting

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/planner/core/casetest/rule/rule_predicate_simplification_test.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/expression/util.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/planner/core/rule/rule_predicate_simplification.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/planner/core/plan_cost_ver2.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/planner/core/cost/factors_thresholds.go`

**Step 1: Write the failing test**

Add a test that builds a plan with a cheap predicate and a `tidb_model_predict_output(...)` predicate, runs predicate simplification, and asserts the model predicate is ordered after the cheap predicate.

**Step 2: Run test to verify it fails**

Run (check failpoints first; enable if needed):
```
make failpoint-enable && (
  pushd /Users/brian.w/projects/tidb/pkg/planner/core/rule
  go test -run TestPredicateSimplificationModelPredictOrder --tags=intest
  rc=$?
  popd
  make failpoint-disable
  exit $rc
)
```
Expected: FAIL with ordering assertion.

**Step 3: Write minimal implementation**

- Add a helper in `expression` to detect model predicates (`MODEL_PREDICT` / `tidb_model_predict_output`).
- Add cost factor constant for model predicates.
- Reorder predicates in `applyPredicateSimplificationHelper` using a stable sort keyed by cost/selection heuristic, skipping reorder if mutable/side-effect expressions are present.
- Update `numFunctions` to weight model predicates with the new cost factor.

**Step 4: Run test to verify it passes**

Run the same test command as Step 2.
Expected: PASS.

**Step 5: Commit**

```
git add /Users/brian.w/projects/tidb/pkg/planner/core/casetest/rule/rule_predicate_simplification_test.go \
  /Users/brian.w/projects/tidb/pkg/expression/util.go \
  /Users/brian.w/projects/tidb/pkg/planner/core/rule/rule_predicate_simplification.go \
  /Users/brian.w/projects/tidb/pkg/planner/core/plan_cost_ver2.go \
  /Users/brian.w/projects/tidb/pkg/planner/core/cost/factors_thresholds.go

git commit -m "planner: order model predicates after cheap filters"
```

### Task 2: Default Selectivity for Model Predicates

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/planner/cardinality/selectivity_test.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/planner/cardinality/selectivity.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/planner/cardinality/pseudo.go`

**Step 1: Write the failing test**

Add a selectivity test that includes a `tidb_model_predict_output(...)` predicate and asserts the selectivity falls back to the model default (not the generic factor).

**Step 2: Run test to verify it fails**

Run (check failpoints first; enable if needed):
```
make failpoint-enable && (
  pushd /Users/brian.w/projects/tidb/pkg/planner/cardinality
  go test -run TestSelectivityModelPredictDefault --tags=intest
  rc=$?
  popd
  make failpoint-disable
  exit $rc
)
```
Expected: FAIL with selectivity mismatch.

**Step 3: Write minimal implementation**

- Add a model-predict default selectivity constant.
- In `pseudoSelectivity`, reduce `minFactor` when model predicates appear.
- In `Selectivity`, include model predicates among unestimated expressions to cap `minSelectivity`.

**Step 4: Run test to verify it passes**

Run the same test command as Step 2.
Expected: PASS.

**Step 5: Commit**

```
git add /Users/brian.w/projects/tidb/pkg/planner/cardinality/selectivity_test.go \
  /Users/brian.w/projects/tidb/pkg/planner/cardinality/selectivity.go \
  /Users/brian.w/projects/tidb/pkg/planner/cardinality/pseudo.go

git commit -m "planner: add default selectivity for model predicates"
```

### Task 3: RU Accounting for Model Inference CPU

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/adapter_test.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/adapter.go`

**Step 1: Write the failing test**

Add a test that records model inference stats in `StmtCtx`, invokes the RU reporting helper, and asserts reported `SqlLayerCpuTimeMs` and `TotalCpuTimeMs` include the inference time.

**Step 2: Run test to verify it fails**

Run (check failpoints first; enable if needed):
```
make failpoint-enable && (
  pushd /Users/brian.w/projects/tidb/pkg/executor
  go test -run TestReportModelInferenceRUConsumption --tags=intest
  rc=$?
  popd
  make failpoint-disable
  exit $rc
)
```
Expected: FAIL (no consumption reported).

**Step 3: Write minimal implementation**

- Add an internal helper in `adapter.go` to sum inference time and call `RUConsumptionReporter.ReportConsumption` with `SqlLayerCpuTimeMs` and `TotalCpuTimeMs`.
- Call this helper near the end of `FinishExecuteStmt`.

**Step 4: Run test to verify it passes**

Run the same test command as Step 2.
Expected: PASS.

**Step 5: Commit**

```
git add /Users/brian.w/projects/tidb/pkg/executor/adapter_test.go \
  /Users/brian.w/projects/tidb/pkg/executor/adapter.go

git commit -m "resourcegroup: report model inference CPU consumption"
```

### Task 4: INFORMATION_SCHEMA Views for Model Versions and Cache

**Files:**
- Modify: `/Users/brian.w/projects/tidb/pkg/infoschema/tables.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/builder.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/infoschema_reader.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/executor/infoschema_reader_internal_test.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/util/modelruntime/session_cache.go`
- Modify: `/Users/brian.w/projects/tidb/pkg/util/modelruntime/session_cache_test.go`

**Step 1: Write the failing tests**

- Add `TestSetDataForModelVersions` to build `mysql.tidb_model` and `mysql.tidb_model_version` rows and assert `setDataForModelVersions` returns expected columns.
- Add `TestSetDataForModelCache` to seed the process cache and assert `setDataForModelCache` emits expected rows.
- Add `TestSessionCacheSnapshotEntries` to verify cache snapshot parsing and TTL handling.

**Step 2: Run tests to verify they fail**

Run (check failpoints first; enable if needed):
```
make failpoint-enable && (
  pushd /Users/brian.w/projects/tidb/pkg/executor
  go test -run TestSetDataForModelVersions --tags=intest
  go test -run TestSetDataForModelCache --tags=intest
  popd
  pushd /Users/brian.w/projects/tidb/pkg/util/modelruntime
  go test -run TestSessionCacheSnapshotEntries --tags=intest
  rc=$?
  popd
  make failpoint-disable
  exit $rc
)
```
Expected: FAIL (tables/functions not implemented).

**Step 3: Write minimal implementation**

- Add infoschema constants, table IDs, column definitions, and table map entries for `TIDB_MODEL_VERSIONS` and `TIDB_MODEL_CACHE`.
- Extend `executor/builder.go` memtable switch to route these tables to `memtableRetriever`.
- Implement `setDataForModelVersions` using `ExecRestrictedSQL` over system tables.
- Implement `setDataForModelCache` using a new session-cache snapshot helper.
- Add `SessionCache.SnapshotEntries()` (and any parsing helpers) to `modelruntime`.

**Step 4: Run tests to verify they pass**

Run the same commands as Step 2.
Expected: PASS.

**Step 5: Commit**

```
git add /Users/brian.w/projects/tidb/pkg/infoschema/tables.go \
  /Users/brian.w/projects/tidb/pkg/executor/builder.go \
  /Users/brian.w/projects/tidb/pkg/executor/infoschema_reader.go \
  /Users/brian.w/projects/tidb/pkg/executor/infoschema_reader_internal_test.go \
  /Users/brian.w/projects/tidb/pkg/util/modelruntime/session_cache.go \
  /Users/brian.w/projects/tidb/pkg/util/modelruntime/session_cache_test.go

git commit -m "infoschema: add model versions and cache views"
```

### Task 5: Update Phase Progress Log

**Files:**
- Modify: `/Users/brian.w/projects/tidb/docs/plans/2026-02-20-in-db-onnx-model-serving-progress.md`

**Step 1: Update progress**

Add Phase 7 bullets and mark each sub-scope complete as tasks land.

**Step 2: Commit**

```
git add /Users/brian.w/projects/tidb/docs/plans/2026-02-20-in-db-onnx-model-serving-progress.md

git commit -m "docs: log phase 7 progress"
```

---

Plan complete and saved to `/Users/brian.w/projects/tidb/docs/plans/2026-02-21-in-db-onnx-model-serving-phase-7-implementation-plan.md`. Two execution options:

1. Subagent-Driven (this session)
2. Parallel Session (separate)

Which approach?
