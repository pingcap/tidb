# In-DB ONNX Model Serving Progress

Status log for phased implementation.

## Phase 0: Parser, AST, System Tables
- [x] Parser grammar for `CREATE/ALTER/DROP MODEL` and `MODEL_PREDICT`
- [x] AST nodes and restore/visitor wiring
- [x] System tables and infoschema exposure
- [x] Phase 0 tests green

## Phase 1: DDL, Privileges, Feature Flags
- [x] DDL execution and MVCC version resolution (direct SQL path for create/alter/drop; snapshot-aware SHOW CREATE)
- [x] Privilege checks for model DDL and inference
- [x] Sysvars: `tidb_enable_model_ddl`, `tidb_enable_model_inference`
- [x] Phase 1 tests green (TestModelDDL*)

## Phase 2: Runtime, Expression, Execution
- [x] ONNX runtime integration (always-on build)
- [x] Artifact loader + checksum + cache in util
- [x] `MODEL_PREDICT` expression + planner rewrite + scalar execution
- [x] Executor batching/vectorized evaluation
- [x] ONNX session cache (process-level, LRU)
- [x] Session cache sysvars: `tidb_model_cache_capacity`, `tidb_model_cache_ttl`
- [x] Input validation (type/name/count/shape)
- [x] Phase 2 tests green

## Phase 3: Governance, Safety, Metadata
- [x] Governance sysvars: `tidb_model_max_batch_size`, `tidb_model_timeout`, `tidb_model_allow_nondeterministic`, `tidb_enable_model_custom_ops` (global-only)
- [x] Inference options: per-run timeout + max batch split
- [x] Metadata checks: nondeterministic flag handling
- [x] Custom ops error mapping + gating
- [x] Phase 3 tests green (modelruntime tests + sysvar test + TestModelPredictRejectsNondeterministic)

## Phase 4: NULL Semantics
- [x] Sysvar: `tidb_model_null_behavior` (`ERROR`/`RETURN_NULL`)
- [x] Scalar and vectorized null handling for `MODEL_PREDICT`
- [x] Phase 4 test green (TestModelPredictNullBehaviorReturnNull)

## Phase 5: Metrics
- [x] Metrics: model inference counter + duration histogram
- [x] Metrics: session cache hit/miss/evict counters
- [x] Phase 5 tests green (TestRunInferenceMetrics, TestSessionCacheMetrics)

## Phase 6: Observability
- [x] Statement-scoped model inference stats (role + plan ID)
- [x] EXPLAIN ANALYZE model runtime stats registration
- [x] Slow log Model_inference payload + slow_query table parsing
- [x] Metrics: model load duration + batch size histograms
- [x] Phase 6 tests green (TestModelInferenceStats, TestModelPredictInferenceStatsRecorded, TestExplainAnalyzeIncludesModelInferenceStats, TestSlowLogModelInferencePayload, TestRunInferenceBatchMetrics, TestRecordModelLoadStatsMetrics)

## Notes
- TiFlash model pushdown is future work; v1 only pushes non-model predicates.
- Tests: `go test -run TestModelStatements --tags=intest ./pkg/parser`; `go test -run TestModelSystemTablesBootstrap --tags=intest ./pkg/session`
