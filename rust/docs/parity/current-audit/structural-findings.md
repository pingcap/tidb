# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-23; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-23.md](structural-audit-2026-11-23.md).

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
| T10 | partial | Go `pkg/executor`'s `ExecStmt` (`adapter.go`: `Exec`, `FinishExecuteStmt`, `handlePessimisticLockError`, `handleNoDelayExecutor`, the observation's begin and publish, `isSelectForUpdate`) and `ResetContextOfStmt` / `executorBuilder.build`'s dispatch are implemented as `Session` methods in the session crate (`lib.rs`, `dispatch.rs`, `observation.rs`, `statement_lifecycle.rs`), as are `TraceExec` (the node) and `CoprocessorDAGHandler`. Moved to `tidb-executor` already: PlanReplayerExec and PlanReplayerLoadInfo, ImportIntoExec, DDLExec.Next's call, SimpleExec's transaction statements, ShowDDLExec, fetchShowWarnings / fetchShowStatus / fetchShowTableStatus, the account and variable memtable retrievers, and Go master's materialized-view executors (`tidb-executor/src/mview/`). The session's compile routes (queries in `dispatch.rs`'s compile phase, DDL through `identity.rs`'s `preprocess_and_build`, EXPLAIN, privilege collection, MDL table recording) each run Go's preprocess walk (`tidb_planner::preprocess`), where Go's `Compile` runs it once. Rust: `tidb-session/src/lib.rs` (`ExecStmt.Exec`), `dispatch.rs` (`execute_plan`, `handle_cop_plan`, `is_select_for_update`), `observation.rs`, `statement_lifecycle.rs`, `domain/cluster_session_node/mod.rs` (`TraceExec`, `handleNoDelayExecutor`). | `pkg/executor` (`adapter.go`, `select.go`, `plan_replayer.go`, `simple.go`, `ddl.go`, `show.go`, `prepared.go`, ...) |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Statistics

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
