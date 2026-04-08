# Materialized View Log Purge (Implementation and Design Notes)

This document describes the current implementation and behavior of:

```sql
PURGE MATERIALIZED VIEW LOG ON <base_table>
```

It also records the design rationale and follow-up items for the refined MV/MLog system-table schema.

## Current status

- `PURGE MATERIALIZED VIEW LOG` is implemented.
- The statement runs as a **utility statement** (not a DDL statement).
- Execution is handled by a dedicated utility executor (`PurgeMaterializedViewLogExec`).
- No DDL job is submitted for purge execution.
- Purge metadata is split into:
  - `mysql.tidb_mlog_purge_info`: metadata + lock-row carrier (`NEXT_TIME`, `LAST_PURGED_TSO`).
  - `mysql.tidb_mlog_purge_hist`: per-purge lifecycle/result records.
- After lock acquisition, purge inserts one `running` history row; when purge finishes, it updates the same history row to final status.
- `LAST_PURGED_TSO` is used as a persisted purge checkpoint to skip redundant purge runs.

## Runtime `NEXT_TIME` Update (Internal SQL Success Path)

- For **internal SQL** triggered purge (identified by `SessionVars.InRestrictedSQL`), after successful purge completion, `mysql.tidb_mlog_purge_info.NEXT_TIME` should be updated together with purge success state.
- Runtime `NEXT_TIME` derivation in this path is intentionally different from create-time derivation:
  - evaluate and use only `PurgeNext` expression;
  - do not apply create-time `START WITH` priority / near-now rules;
  - if `PurgeStartWith` is non-empty and `PurgeNext` is empty, explicitly set `NEXT_TIME = NULL`;
  - if both are empty, keep `NEXT_TIME` unchanged.
- For non-internal (user) SQL purge, keep existing behavior (do not update `NEXT_TIME` on success path).

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
4. Use a row lock in `mysql.tidb_mlog_purge_info` as the cross-node mutex and read `LAST_PURGED_TSO`.
5. Compute `safe_purge_tso` once.
6. After first successful lock acquisition, insert one `running` row into `mysql.tidb_mlog_purge_hist`.
7. If `LAST_PURGED_TSO` is not null and `LAST_PURGED_TSO >= safe_purge_tso`, end this purge directly (no delete work in this run).
8. Delete eligible MLog rows in batches.
9. If a batch deletes `< batch_size` rows, update `LAST_PURGED_TSO = safe_purge_tso` in the same transaction before `COMMIT`.
   - runtime internal-SQL rule: update `NEXT_TIME` by evaluating only `PurgeNext`; if `PurgeStartWith != ''` and `PurgeNext == ''`, set `NEXT_TIME = NULL`.
10. At statement end, update that history row to final status (`success` / `failed`) and fill completion fields.

## Internal context

- Execution context keeps the statement context and is tagged with:
  - `kv.InternalTxnMVMaintenance`
- Internal SQL runs through `GetSysSession` / `ReleaseSysSession`.

## Per-batch transaction

Each batch does:

1. `BEGIN PESSIMISTIC`
2. Acquire lock row and read `LAST_PURGED_TSO` with `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mlog_purge_info`
3. (First successful lock only) calculate `safe_purge_tso` and create purge history (`running`) with a statement-unique `PURGE_JOB_ID`
4. If `LAST_PURGED_TSO` is not null and `LAST_PURGED_TSO >= safe_purge_tso`, stop this purge directly.
5. `DELETE ... WHERE _tidb_commit_ts <= safe_purge_tso LIMIT batch_size`
6. If deleted rows `< batch_size`, update `LAST_PURGED_TSO = safe_purge_tso` before commit
7. `COMMIT`

Loop stops when deleted rows `< batch_size`, or by the checkpoint short-circuit in step 4.

After the loop exits (or exits with error), purge finalizes the `mysql.tidb_mlog_purge_hist` row for this `PURGE_JOB_ID`.

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

- Use `MIN(COALESCE(LAST_SUCCESS_READ_TSO, 0))` across all dependent MV ids.
- Cap by current purge transaction start tso.
- If no dependent MV exists, `safe_purge_tso` is purge transaction start tso.

Checkpoint short-circuit:

- At lock time, purge reads `LAST_PURGED_TSO` from `mysql.tidb_mlog_purge_info`.
- If `LAST_PURGED_TSO` is not null and `LAST_PURGED_TSO >= safe_purge_tso`, purge can return without delete work.
- `LAST_PURGED_TSO` advances only when a batch proves all rows with `_tidb_commit_ts <= safe_purge_tso` are deleted (`deleted_rows < batch_size`).

Consistency guard for public MVs:

- For public dependent MVs, all of them must have rows in `mysql.tidb_mview_refresh_info`.
- Missing rows are treated as metadata inconsistency and purge fails.

## Concurrency semantics

Mutex granularity is per `MLOG_ID` via row lock in `mysql.tidb_mlog_purge_info`.

- Lock SQL:
  - `SELECT LAST_PURGED_TSO FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = ? FOR UPDATE NOWAIT`

Outcomes:

1. Lock acquired: continue.
2. NOWAIT conflict:
   - if no rows were deleted yet in this statement: return error.
   - if some rows were already deleted in previous batches: return success with warning:
     - purge stopped before deleting all eligible rows due to lock conflict; retry later.
3. Lock row missing in `tidb_mlog_purge_info`: return error (`mlog purge lock row does not exist for mlog id ...`).

Different `MLOG_ID`s can be purged concurrently.

## State tables

Bootstrap creates:

- `mysql.tidb_mlog_purge_info`
  - `MLOG_ID` (PK, lock row carrier)
  - `NEXT_TIME` (scheduling metadata)
  - `LAST_PURGED_TSO` (last completed purge boundary checkpoint)
- `mysql.tidb_mlog_purge_hist`
  - `PURGE_JOB_ID` (PK, one row per purge statement)
  - `MLOG_ID`
  - `PURGE_METHOD`
  - `PURGE_TIME` / `PURGE_ENDTIME`
  - `PURGE_ROWS`
  - `PURGE_STATUS` (`running` / `success` / `failed`)

Write responsibilities:

- `tidb_mlog_purge_info`:
  - lock-row/scheduling metadata;
  - update `LAST_PURGED_TSO` when one purge batch confirms all rows `<= safe_purge_tso` are deleted (`deleted_rows < batch_size`), and do it before committing that batch.
  - in internal SQL success path, also update `NEXT_TIME` with runtime rule (`PurgeNext`-only evaluation; explicit `NULL` when `PurgeStartWith` exists but `PurgeNext` is empty).
- `tidb_mlog_purge_hist`:
  - insert one `running` row after lock acquisition;
  - update the same row when statement finishes.

## Create-time `NEXT_TIME` Initialization (`CREATE MATERIALIZED VIEW LOG`)

Purge requires an existing lock row in `mysql.tidb_mlog_purge_info`.

`CREATE MATERIALIZED VIEW LOG` initializes (or upserts) this row in DDL worker:

- `MLOG_ID` is always inserted/ensured.
- `NEXT_TIME` is written according to create-time purge schedule derivation.

Create-time `NEXT_TIME` derivation rules (for `PurgeStartWith` / `PurgeNext`) are:

1. If both are empty, do not update `NEXT_TIME` (row keeps default `NULL`).
2. Evaluate expressions in prepared eval session (`UTC` timezone + DDL job SQL mode).
3. `START WITH` has higher priority, unless it is near-now (`START WITH < now + 10s`) and `NEXT` exists; in that case use `NEXT`.
4. If the chosen expression evaluates to `NULL`, explicitly write `NEXT_TIME = NULL`.

Purge runtime reschedule rule (internal SQL success path) is intentionally different:

- runtime purge uses `PurgeNext` only;
- runtime purge does not apply create-time `START WITH`/near-now priority.

If this system table is missing, create MLog fails with explicit error.

Related dependency from `CREATE MATERIALIZED VIEW` side:

- purge computes `safe_purge_tso` from `mysql.tidb_mview_refresh_info` of dependent MVs;
- `CREATE MATERIALIZED VIEW` creates/upserts those refresh-info rows and also initializes their `NEXT_TIME` with the same create-time schedule rule style (create-time `START WITH`/`NEXT` derivation).

## Error handling notes

- Missing database / table / MLog metadata returns explicit errors.
- Missing system tables (`mysql.tidb_mlog_purge_info`, `mysql.tidb_mlog_purge_hist`, `mysql.tidb_mview_refresh_info`, `mysql.tidb_ddl_job`) return explicit errors.
- Delete execution errors do not leak partial delete writes from failed batch transaction.
- For statement failures, purge finalizes the history row with `failed` status before returning the error.

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

1. No background/asynchronous purge scheduler; purge is statement-driven.
2. No resumable task framework beyond the `LAST_PURGED_TSO` boundary checkpoint (retry by rerunning statement).
3. Additional observability (batch-level metrics / progress) can be improved.
