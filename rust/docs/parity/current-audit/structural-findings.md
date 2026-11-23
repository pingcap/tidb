# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-22; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-22.md](structural-audit-2026-11-22.md).

**1 unresolved: 0 open, 1 partial.**

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

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Statistics

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| R03 | partial | ANALYZE is not fully in Go's shape. The planner's `buildAnalyze*` / `getMustAnalyzedColumns` / `genV2AnalyzeOptions` still run at execution time in `real_tikv_analyze.rs` (`resolve_cluster_analyze_statement`, over a fresh TiKV catalog snapshot), and `AnalyzeColumnsExec`'s sampling build (`buildSamplingStats` with `readDataAndSendTask` / `subMergeWorker` / `subBuildWorker`, `handleNDVForSpecialIndexes`) and `analyze_idx.go`'s `fetchAnalyzeResult` have no counterparts. Done: globalstats (`tidb-stats-handle-globalstats/src/global_stats.rs`), and `AnalyzeExec.Next` (`analyze_exec_next`: `filterAndCollectTasks`, the `analyzeWorker` pool, `handleResultsError` with `analyze_worker.rs`'s save workers, `analyze_global_stats.rs`). Rust: `tidb-executor/src/real_tikv_analyze.rs`, `analyze.rs`, `cluster_analyze.rs`, `cluster_analyze/sampling.rs`, `analyze/exec.rs` | `pkg/executor/analyze*.go`, `pkg/planner/core/planbuilder.go` (`buildAnalyze*`), `pkg/executor/builder.go` (`buildAnalyze*`) |
