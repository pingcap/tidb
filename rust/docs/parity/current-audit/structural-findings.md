# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-23; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-23.md](structural-audit-2026-11-23.md).

**1 unresolved: 1 open, 0 partial.**

A row states the remaining gap only.

## Unresolved


### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T24 | open | Go `ExecStmt.Exec` holds a statement's pessimistic retry: `handlePessimisticDML` / `handlePessimisticLockError` re-run `buildExecutor` and `openExecutor` over the compiled `Plan` after `OnStmtRetry` (with `StmtCtx.ResetForRetry`), inside one `ExecuteStmt`. The Rust `StatementLifecycle` loop (`HandlePessimisticDml`, driven by `with_bound_statement`'s `run` closure) wraps the whole session statement instead: each attempt re-runs the statement boundary (`begin_statement_execution` / `finish_statement_execution`) and the funnel's compile (hints, binding, privilege check, `planner.Optimize`) before executing again, so no `ExecStmt` value outlives an attempt. `ExecStmt.Exec`'s `SetProcessInfo` and `observeStmtBeginForTopProfiling` are the session's (`statement_phase_observer`, `begin_statement_observation`). Rust: `tidb-session/src/domain/cluster_session_node/mod.rs` (`run_ordinary_write`'s `exec_statement_parsed` closure), `statement_lifecycle.rs` (`with_bound_statement`, `attempt_statement_inner`), `lib.rs` (`run_with_columns_using`), `dispatch.rs` (`prepare_parsed_statement_with_optional_physical_plan`), `observation.rs` (`statement_phase_observer`). | `pkg/executor/adapter.go` (`ExecStmt.Exec`, `handlePessimisticDML`, `handlePessimisticLockError`), `pkg/session/session.go` (`ExecuteStmt`, `runStmt`) |

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
