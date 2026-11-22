# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-22; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-22.md](structural-audit-2026-11-22.md).

**6 unresolved: 5 open, 1 partial.**

A row states the remaining gap only.

## Unresolved


### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| X01 | partial | `InferType4ControlFuncs` with its `setDecimalFromArgs` / `setFlenFromArgs` has two Rust implementations: `builtin_control.rs` (the function classes, with collation derivation) and `rewriter/control_type.rs` (LEAD/LAG's `typeInfer4LeadLag` and the windows' `set_numeric_len_from_args`, without it, plus per-function tails Go keeps in each `getFunction`). (Done 2026-11-22: the TiDB-store DAG goes through the one `ConstructDAGReq`, so `builder_utils.rs`'s second ToPB family (`AggFuncToPBExpr`, `AggFunctionModeToPB`, `GetTiPBExpr`, the table scan / selection / limit / TopN / aggregation / projection `ToPB`) is gone; one body each for `BoolToOnOff`, `CheckDropTablePartition`, `ContainCorrelatedColumn`, `ContainVirtualColumn`, `IsBinaryLiteral`, `base64NeededEncodedLength`, `newReturnFieldTypeForBaseBuiltinFunc`, `IsUpgradingClusterState`, `insertPlanReplayerStatus`, `isNullToNotNullChange`; the unused `Table::verify_no_overflow_shard_bits` is removed.) Rust: `tidb-expr/src/builtin_control.rs`, `tidb-expr/src/rewriter/control_type.rs`, `tidb-expression-aggregation/src/base_func.rs` (`type_infer_4_lead_lag`) | `pkg/expression/builtin_control.go` (`InferType4ControlFuncs`, `setDecimalFromArgs`, `setFlenFromArgs`) |
| X02 | open | Go's DDL `sess.Session` (`Execute`, `Begin`, `Commit`, `Rollback`) has two Rust ports: `tidb_ddl_session::Session` over a session context, and the `StepSqlSession` trait (with its own `execute`, `begin`, ...) that job steps hold, so code that runs on either is written twice (`getJobsBySQL` in `ddl_history.rs` and in `job_control.rs`). Rust: `tidb-ddl-session/src/lib.rs` (`Session`), `tidb-ddl-build/src/session_pool.rs` (`StepSqlSession`), `tidb-ddl/src/ddl_history.rs`, `tidb-ddl/src/job_control.rs` | `pkg/ddl/session` (`Session`), `pkg/ddl/job_worker.go` (`w.sess`), `pkg/ddl/ddl_history.go` (`getJobsBySQL`) |

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Privileges, wire and administration

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### DDL

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Planner and bindings

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| S01 | open | Cost model version 1 is not transcreated: of `plan_cost_ver1.go`'s 40 functions only `getPlanCostVer1` for a few operators exists, so `SET tidb_cost_model_version = 1` costs and chooses plans differently (probe: `EXPLAIN FORMAT='verbose' SELECT * FROM t1 WHERE b > 1` costs 16279.11 in Go and 73382.22 in Rust; a two-table join on an indexed column is a HashJoin in Go and a MergeJoin in Rust). Rust: `tidb-planner` (`get_plan_cost_ver1` only in `physical_tests.rs` coverage) | `pkg/planner/core/plan_cost_ver1.go` |
| S02 | open | `EXPLAIN EXPLORE <sql>` is answered as a plain EXPLAIN: Go's binding exploration (`ExplorePlansForSQL`, `getBindingPlanInfo`, `runToGetExecInfo`, `fillRecommendation`) and plan generation over hint states (`generatePlanWithSCtx`, `breadthFirstPlanSearch`, `genPlanUnderState`) have no Rust counterpart. Rust: no module; the statement reaches the ordinary EXPLAIN path | `pkg/bindinfo/binding_auto.go`, `pkg/bindinfo/binding_plan_generation.go`, `pkg/executor/explain.go` (explore) |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| S04 | open | Go's `SortExec.Open` always sorts in parallel (`IsUnparallel = false`, `ExecutorConcurrency` workers, multi-way merge, `parallelSortSpillHelper`), and TopN has its own workers; `parallel_sort_worker.go`, `parallel_sort_spill_helper.go` and `topn_worker.go` are not ported (the crate's own doc says so), so Rust sorts only in Go's unparallel mode. Rust: `tidb-executor-sortexec/src/lib.rs`, `topn_chunk_heap.rs` (crate doc: NOT COVERED) | `pkg/executor/sortexec/sort.go`, `parallel_sort_worker.go`, `parallel_sort_spill_helper.go`, `topn_worker.go` |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Statistics

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| S03 | open | ANALYZE is not in Go's shape. One 3,055-line `real_tikv_analyze.rs` holds the planner's `buildAnalyze*` / `getMustAnalyzedColumns`, the executor's `AnalyzeExec` and globalstats' async merge; Go's `AnalyzeExec` worker pool (`analyzeWorker`, `handleResultsError`, `handleGlobalStats`), `AnalyzeColumnsExec`'s sampling build (`buildSamplingStats`, `subMergeWorker`, `subBuildWorker`, `readDataAndSendTask`, `handleNDVForSpecialIndexes`), `analyze_idx.go`'s `fetchAnalyzeResult`, `analyze_global_stats.go` and `analyze_worker.go` have no counterparts. Rust: `tidb-executor/src/real_tikv_analyze.rs`, `analyze.rs`, `cluster_analyze.rs`, `cluster_analyze/sampling.rs`, `analyze/exec.rs` | `pkg/executor/analyze*.go`, `pkg/planner/core/planbuilder.go` (`buildAnalyze*`), `pkg/statistics/handle/globalstats` |
