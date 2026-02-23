# Materialized View Log Purge (Implementation and Design Notes)

This document describes the current implementation and behavior of:

```sql
PURGE MATERIALIZED VIEW LOG ON <base_table>
```

It also records the design rationale and follow-up items.

## Current status

- `PURGE MATERIALIZED VIEW LOG` is implemented.
- The statement runs as a **utility statement** (not a DDL statement).
- Execution is handled by a dedicated utility executor (`PurgeMaterializedViewLogExec`).
- No DDL job is submitted for purge execution.

## Why utility execution (not DDL statement execution)

`PURGE MATERIALIZED VIEW LOG` has maintenance semantics similar to `REFRESH MATERIALIZED VIEW`:

- it performs internal SQL and state updates;
- it needs statement-level orchestration and mutex control;
- it should not be treated as schema-changing DDL.

So purge is implemented on the same utility-style path as refresh:

1. Parser/AST statement node (`stmtNode`).
2. Planner utility plan node.
3. Utility executor implementation.

As a result, this statement does not have DDL-statement side effects such as setting `sessionctx.LastExecuteDDL`.

## SQL syntax

```sql
PURGE MATERIALIZED VIEW LOG ON <base_table>
```

- `<base_table>` can be schema-qualified.
- If schema is omitted, current database is used.
- If current database is empty and schema is omitted, `ErrNoDB` is returned.

## Privilege model

The statement requires `ALTER` privilege on the base table (same privilege level as other MV maintenance entry points in this branch).

## Transaction policy

`PURGE MATERIALIZED VIEW LOG` is rejected inside explicit user transactions (`BEGIN` / `START TRANSACTION`) with:

- `cannot run PURGE MATERIALIZED VIEW LOG in explicit transaction`

The statement must run as a standalone statement.

## Execution model

## High-level flow

1. Resolve base table and corresponding MLog table (`$mlog$<base_table>`).
2. Create/get an internal system session.
3. Execute purge in a batch loop, each batch in a separate pessimistic transaction.
4. Use a row lock in `mysql.tidb_mlog_purge` as the cross-node mutex.
5. Compute safe purge tso once.
6. Delete eligible MLog rows in batches.
7. Update latest purge status in `mysql.tidb_mlog_purge`.

## Internal context

- Execution context keeps the statement context and is tagged with:
  - `kv.InternalTxnMVMaintenance`
- Internal SQL runs through `GetSysSession` / `ReleaseSysSession`.

## Per-batch transaction

Each batch does:

1. `BEGIN PESSIMISTIC`
2. Acquire lock row with `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mlog_purge`
3. (First successful lock only) calculate safe purge tso
4. `DELETE ... WHERE _tidb_commit_ts <= safe_purge_tso LIMIT batch_size`
5. Update `mysql.tidb_mlog_purge` latest state
6. `COMMIT`

Loop stops when deleted rows `< batch_size`.

## Batch size variable

- Variable: `tidb_mlog_purge_batch_size`
- Scope: `GLOBAL | SESSION`
- Default: `100000`
- Min: `1`
- Max: `1000000`

If session value is invalid/non-positive at runtime, executor falls back to the default.

## Safe purge tso semantics

`safe_purge_tso` is the upper boundary for deletions (`_tidb_commit_ts <= safe_purge_tso`).

Inputs:

1. Public dependent MVs:
   - from `baseTableInfo.MaterializedViewBase.MViewIDs`
2. In-building `CREATE MATERIALIZED VIEW` jobs:
   - from `mysql.tidb_ddl_job`
   - filtered by `type = ActionCreateMaterializedView` and `FIND_IN_SET(mlog_id, table_ids)`
   - decode `job_meta` and use `job.TableID` as in-building MV id

Rule:

- Use `MIN(COALESCE(LAST_SUCCESSFUL_REFRESH_READ_TSO, 0))` across all dependent MV ids.
- Cap by current purge transaction start tso.
- If no dependent MV exists, `safe_purge_tso` is purge transaction start tso.

Consistency guard for public MVs:

- For public dependent MVs, all of them must have rows in `mysql.tidb_mview_refresh`.
- Missing rows are treated as metadata inconsistency and purge fails.

## Concurrency semantics

Mutex granularity is per `MLOG_ID` via row lock in `mysql.tidb_mlog_purge`.

- Lock SQL:
  - `SELECT 1 FROM mysql.tidb_mlog_purge WHERE MLOG_ID = ? FOR UPDATE NOWAIT`

Outcomes:

1. Lock acquired: continue.
2. NOWAIT conflict:
   - if no rows were deleted yet in this statement: return error.
   - if some rows were already deleted in previous batches: return success with warning:
     - purge stopped before deleting all eligible rows due to lock conflict; retry later.
3. Lock row missing: return error (`mlog purge lock row does not exist for mlog id ...`).

Different `MLOG_ID`s can be purged concurrently.

## State tables

Bootstrap creates:

- `mysql.tidb_mlog_purge` (latest state table)
- `mysql.tidb_mlog_purge_hist` (history table)

Current implementation updates only:

- `mysql.tidb_mlog_purge`
  - `LAST_PURGE_TIME`
  - `LAST_PURGE_ROWS`
  - `LAST_PURGE_DURATION`

History table writes (`mysql.tidb_mlog_purge_hist`) are not implemented yet.

## Dependency on MLog creation path

Purge requires an existing lock row in `mysql.tidb_mlog_purge`.

`CREATE MATERIALIZED VIEW LOG` initializes this row in DDL worker:

- `INSERT IGNORE INTO mysql.tidb_mlog_purge (MLOG_ID) VALUES (...)`

If this system table is missing, create MLog fails with explicit error.

## Error handling notes

- Missing database / table / MLog metadata returns explicit errors.
- Missing system tables (`mysql.tidb_mlog_purge`, `mysql.tidb_mview_refresh`, `mysql.tidb_ddl_job`) return explicit errors.
- Delete execution errors do not leak partial delete writes from failed batch transaction.
- Purge state update is kept even when the current batch fails after partial statement progress bookkeeping.

## TiFlash behavior for purge delete

Delete SQL includes:

- `/*+ read_from_storage(tiflash[mlog]) */`

During purge delete, internal session sets:

- `SessionVars.InMaterializedViewMaintenance = true`

This matches MV maintenance behavior and allows intended optimizer behavior for maintenance SQL.

## Code map

- AST statement:
  - `pkg/parser/ast/misc.go` (`PurgeMaterializedViewLogStmt`)
- Parser grammar:
  - `pkg/parser/parser.y`
- Statement label:
  - `pkg/parser/ast/ast.go` (`PurgeMaterializedViewLog`)
- Planner:
  - `pkg/planner/core/planbuilder.go` (`buildPurgeMaterializedViewLog`)
  - `pkg/planner/core/common_plans.go` (`PurgeMaterializedViewLog` plan)
  - `pkg/planner/core/preprocess.go` (statement type handling)
- Executor:
  - `pkg/executor/builder.go` (`buildPurgeMaterializedViewLog`)
  - `pkg/executor/materialized_view.go` (`PurgeMaterializedViewLogExec`)
- Explicit transaction guard:
  - `pkg/session/session.go` (`validateStatementInTxn`)
- MLog create-time lock-row initialization:
  - `pkg/ddl/create_table.go` (`onCreateMaterializedViewLog`)
- Sysvar:
  - `pkg/sessionctx/variable/tidb_vars.go`
  - `pkg/sessionctx/variable/sysvar.go`
  - `pkg/sessionctx/variable/session.go`

## Known gaps and follow-ups

1. `mysql.tidb_mlog_purge_hist` write path is not implemented yet.
2. No background/asynchronous purge scheduler; purge is statement-driven.
3. No resumable task/checkpoint framework (retry by rerunning statement).
4. Additional observability (batch-level metrics / progress) can be improved.

