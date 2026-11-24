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
| T23 | open | Go splits a statement into `session.ExecuteStmt` (txn context, `ResetContextOfStmt`), `executor.Compile` (preprocess, `planner.Optimize` with its hints, binding and plan cache, `CheckPrivilege`) returning an `ExecStmt`, and `ExecStmt.Exec` (`SetProcessInfo`, `observeStmtBeginForTopProfiling`, `buildExecutor`, `handleNoDelay` or the record set). The session's funnel interleaves the three in one function per statement kind: only non-query, non-DML statements compile to a plan the executor builder runs; a query or DML applies its SET_VAR hints, binding and privilege check in the session and opens or runs through `tidb_executor::driver` / `run_*_stmt_with_physical`, with no `ExecStmt` value between compile and execution. The statement observation's begin (`begin_statement_observation`) is likewise the session's. Rust: `tidb-session/src/dispatch.rs` (`execute_parsed_statement_inner`, `prepare_parsed_statement_compile_phase`), `observation.rs` (`begin_statement_observation`, `statement_phase_observer`). | `pkg/session/session.go` (`ExecuteStmt`, `runStmt`), `pkg/executor/compiler.go` (`Compile`), `pkg/executor/adapter.go` (`ExecStmt.Exec`) |

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
