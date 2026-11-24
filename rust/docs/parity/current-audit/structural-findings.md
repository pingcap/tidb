# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-23; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-23.md](structural-audit-2026-11-23.md).

**2 unresolved: 1 open, 1 partial.**

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
| T01 | open | Pipelined DML is not ported: `tidb_dml_type='bulk'` runs a standard transaction; Go's `usePipelinedDmlOrWarn` (fallback warnings for temporary tables, `tidb_constraint_check_in_place=ON`, explicit transactions, ...) and the pipelined transaction are missing. A Go server reports `pipelined:true` in `tidb_last_txn_info` after a bulk INSERT; this one reports `false` and no warnings. The statement's writes land in the implicit transaction's MemDB (T14, resolved); a pipelined transaction flushes that MemDB while the statement runs. Rust: `tidb-session/src/lib.rs` hard-codes `"pipelined":false`; no `use_pipelined_dml_or_warn`. | `pkg/session/session.go` `usePipelinedDmlOrWarn`, `NewTxn` pipelined path; `pkg/store/driver/txn` |
| T10 | partial | Go `pkg/executor`'s `ExecStmt` (`adapter.go`: `Exec`, `FinishExecuteStmt`, `handlePessimisticLockError`, `handleNoDelayExecutor`, the observation's begin and publish, `isSelectForUpdate`) and `ResetContextOfStmt` / `executorBuilder.build`'s dispatch are implemented as `Session` methods in the session crate (`lib.rs`, `dispatch.rs`, `observation.rs`, `statement_lifecycle.rs`), as are `TraceExec` (the node) and `CoprocessorDAGHandler`. Moved to `tidb-executor` already: PlanReplayerExec and PlanReplayerLoadInfo, ImportIntoExec, DDLExec.Next's call, SimpleExec's transaction statements, ShowDDLExec, fetchShowWarnings / fetchShowStatus / fetchShowTableStatus, the account and variable memtable retrievers, and Go master's materialized-view executors (`tidb-executor/src/mview/`). The session's compile routes (queries in `dispatch.rs`'s compile phase, DDL through `identity.rs`'s `preprocess_and_build`, EXPLAIN, privilege collection, MDL table recording) each run Go's preprocess walk (`tidb_planner::preprocess`), where Go's `Compile` runs it once. Rust: `tidb-session/src/lib.rs` (`ExecStmt.Exec`), `dispatch.rs` (`execute_plan`, `handle_cop_plan`, `is_select_for_update`), `observation.rs`, `statement_lifecycle.rs`, `domain/cluster_session_node/mod.rs` (`TraceExec`, `handleNoDelayExecutor`). | `pkg/executor` (`adapter.go`, `select.go`, `plan_replayer.go`, `simple.go`, `ddl.go`, `show.go`, `prepared.go`, ...) |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Statistics

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
