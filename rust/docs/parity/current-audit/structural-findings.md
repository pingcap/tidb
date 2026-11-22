# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-22; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-22.md](structural-audit-2026-11-22.md).

**2 unresolved: 2 open, 0 partial.**

A row states the remaining gap only.

## Unresolved


### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

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
| R05 | open | Go caches each operator's ver2 cost on its first pricing (`PlanCostInit`) and returns the cached value in later contexts unless `CostFlagRecalculate` is set: an index-lookup cop task compared through `getTaskPlanCost` prices its row-id scan without `isChildOfINL` (range-scan factor, `MinNumRows`/`MinRowSize` clamps), and the IndexLookUp reader built from that task reuses the cached value. This coster prices every context afresh, so the reader's table side uses the row-id formula without the clamps (the costs differ when the row-id scan estimates fewer than one row or a narrow row, or when the row-id and range factors differ). Rust: `tidb-planner/src/find_best_task/coster.rs` (no per-operator cost cache outside EXPLAIN's) | `pkg/planner/core/plan_cost_ver2.go` (`PlanCostInit` checks), `pkg/planner/core/find_best_task.go` (`getTaskPlanCost`) |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Statistics

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| R03 | open | ANALYZE is not in Go's shape. One 3,055-line `real_tikv_analyze.rs` holds the planner's `buildAnalyze*` / `getMustAnalyzedColumns`, the executor's `AnalyzeExec` and globalstats' async merge; Go's `AnalyzeExec` worker pool (`analyzeWorker`, `handleResultsError`, `handleGlobalStats`), `AnalyzeColumnsExec`'s sampling build (`buildSamplingStats`, `subMergeWorker`, `subBuildWorker`, `readDataAndSendTask`, `handleNDVForSpecialIndexes`), `analyze_idx.go`'s `fetchAnalyzeResult`, `analyze_global_stats.go` and `analyze_worker.go` have no counterparts. Rust: `tidb-executor/src/real_tikv_analyze.rs`, `analyze.rs`, `cluster_analyze.rs`, `cluster_analyze/sampling.rs`, `analyze/exec.rs` | `pkg/executor/analyze*.go`, `pkg/planner/core/planbuilder.go` (`buildAnalyze*`), `pkg/statistics/handle/globalstats` |
