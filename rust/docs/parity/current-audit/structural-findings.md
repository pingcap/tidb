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
| R05 | open | Go's `findBestTask` compares data-source cop tasks through `getTaskPlanCost` (an index-lookup cop task's table side priced without `isChildOfINL`, the result cached on each operator by `PlanCostInit`, which the IndexLookUp reader then reuses); this planner converts candidates to root tasks before `compare_task_cost`, so no cop task is ever compared. Plans matched on every probe, but the pricing calls differ: EXPLAIN EXPLORE's recorded variables miss `tidb_opt_table_range_scan_cost_factor` for an index-lookup TopN query, and per-operator cost caching is not reproduced. Rust: `tidb-planner/src/find_best_task/dispatch.rs` (only root tasks reach `compare_task_cost` for the TestRelevantOptVarsAndFixes queries), `find_best_task/coster.rs` (no per-operator cost cache) | `pkg/planner/core/find_best_task.go` (`findBestTask4LogicalDataSource`, `compareTaskCost`, `getTaskPlanCost`), `operator/physicalop` `PlanCostInit` |

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
