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
| R03 | partial | ANALYZE's planning is not in Go's shape: the planner's `buildAnalyze*` / `getMustAnalyzedColumns` / `genV2AnalyzeOptions` / `getSavedAnalyzeOpts` and the executor builder's `buildAnalyzeSamplingPushdown` (sample rate) still run at execution time in `real_tikv_analyze.rs` (`resolve_cluster_analyze_statement`, over a fresh TiKV catalog snapshot) instead of building `core.Analyze` at compile time. Done: globalstats, `AnalyzeExec.Next` (`analyze_exec_next`, `analyze_worker.rs`, `analyze_global_stats.rs`), `buildSamplingStats`' workers, and `analyze_idx.rs`'s `AnalyzeIndexExec`. Rust: `tidb-executor/src/real_tikv_analyze.rs` (`resolve_cluster_analyze_statement`, `select_index_tasks`, `final_column_choice`, `selected_columns`, `get_saved_analyze_opts`), `analyze.rs` (`lower_analyze_admin`, `resolve_analyze_options`) | `pkg/executor/analyze*.go`, `pkg/planner/core/planbuilder.go` (`buildAnalyze*`), `pkg/executor/builder.go` (`buildAnalyze*`) |
