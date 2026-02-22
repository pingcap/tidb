# Materialized View Refresh (Implementation and Design Notes)

This document describes the current implementation and the next evolution steps for TiDB `REFRESH MATERIALIZED VIEW` (`COMPLETE` / `FAST`).

At the moment:

- `COMPLETE` refresh is implemented with transactional semantics.
- `FAST` refresh already has the internal statement framework path (it can go through compile/optimize/executor), but the real incremental refresh logic is not implemented yet (planner still returns a placeholder "not supported" error).

> Note: the current system table used to persist refresh status is `mysql.tidb_mview_refresh` (not `mv_refresh_info`).
> The key field is `LAST_SUCCESSFUL_REFRESH_READ_TSO`.
> See `pkg/session/bootstrap.go` for the system table definition.

## Goals (scope of the current implementation)

1. **Transactional all-or-nothing**: one refresh must commit or roll back data replacement and refresh metadata in the same transaction.
2. **Concurrency mutex**: for one MV, when multiple sessions refresh concurrently, only one can enter the execution path; others fail immediately with a locking error.
3. **Refresh metadata update**: before success commit, update the MV row in `mysql.tidb_mview_refresh`, especially:
   - `LAST_SUCCESSFUL_REFRESH_READ_TSO = <for_update_ts of this COMPLETE refresh transaction>`
4. **Persist failure metadata too**: if refresh fails, still write `LAST_REFRESH_RESULT='failed'` and failure reason back to `mysql.tidb_mview_refresh` (and guarantee no partial MV table writes).
5. **Usable COMPLETE refresh**: do full data replacement with transactional `DELETE + INSERT`.
6. **FAST framework first**: build an internal-only AST and run it via `ExecuteInternalStmt`, so real incremental execution can be plugged in later (currently planner/build still returns "not supported").
7. **Privilege semantics scoped to MVP**: for outer SQL semantics, only check `ALTER` on MV; run refresh with internal session so system-table privileges on `mysql.tidb_mview_refresh` do not leak to business users.

## Non-goals (not included yet)

- Real incremental execution for `FAST` refresh (including MLOG consumption, merge/upsert, etc.).
- Separate semantics for `WITH SYNC MODE`.
  Refresh is synchronous today, so `WITH SYNC MODE` is parsed/executed but behaves the same as without it.
  If async refresh is introduced later, semantics can be redefined.
- Performance optimization for large MVs (for example large-transaction mitigation, delete cost reduction, swap table strategies).
- History-table (`mysql.tidb_mview_refresh_hist`) write and retention policy.
  MVP updates current status table only.

## Data and metadata sources

- MV physical storage is a normal table marked by `TableInfo.MaterializedView != nil`.
- MV definition SQL is stored in `TableInfo.MaterializedView.SQLContent`, canonical `SELECT ...`.
  See `pkg/meta/model/table.go` and `pkg/ddl/materialized_view.go`.
- Refresh status table:
  - `mysql.tidb_mview_refresh` (PK `MVIEW_ID`, fields include `LAST_REFRESH_RESULT / LAST_REFRESH_TYPE / LAST_REFRESH_TIME / LAST_SUCCESSFUL_REFRESH_READ_TSO / LAST_REFRESH_FAILED_REASON`).

`MVIEW_ID` directly uses MV physical table `TableInfo.ID`.

## SQL behavior (user view)

Supported syntax (all use one common transactional framework today; `FAST` execution is still placeholder):

```sql
REFRESH MATERIALIZED VIEW db.mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE; -- uses current DB
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE COMPLETE; -- same behavior today (refresh is already synchronous)

REFRESH MATERIALIZED VIEW mv FAST;
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE FAST; -- same behavior today (refresh is already synchronous)
```

Current limitation: `FAST` returns "not supported" at planner/build stage (placeholder), but internal statement + `ExecuteInternalStmt` execution path is already wired.

Current privilege semantics (MVP):

- `REFRESH MATERIALIZED VIEW` requires `ALTER` privilege on target MV (outer semantic privilege).
- Internal `DELETE/INSERT` and updates to `mysql.tidb_mview_refresh` run on internal session, so caller does not need direct DML privilege on `mysql.tidb_mview_refresh`.
- If finer-grained privilege semantics are introduced later (for example base-table `SELECT` checks), extend from this MVP baseline.

## Core execution flow (transactional refresh framework)

The most direct implementation is: transaction + row-lock mutex + savepoint + data refresh + metadata update.

`COMPLETE` and `FAST` share the same outer framework; only the "refresh implementation" step differs.

1. Get an internal session from session pool and start a transaction on it (recommended **pessimistic**, so `FOR UPDATE NOWAIT` works immediately).
2. In transaction, lock refresh-info row by `SELECT ... FOR UPDATE NOWAIT` (used as refresh mutex).
3. Read `LAST_SUCCESSFUL_REFRESH_READ_TSO` once again by plain `SELECT` and compare with step 2:
   - if mismatch, treat as refresh failure: update failure metadata and `COMMIT`, then return error to abort refresh.
   - this avoids using inconsistent refresh metadata snapshots inside one refresh flow.
4. Set a savepoint (used to roll back MV data changes while still committing failure metadata).
5. Run refresh implementation by refresh type:
   - `COMPLETE`: `DELETE FROM <mv_table>` + `INSERT INTO <mv_table> <mv_select_sql>`.
   - `FAST`: construct internal statement and run via `ExecuteInternalStmt` (currently planner/build returns "not supported" placeholder).
6. Before commit, update `mysql.tidb_mview_refresh` row (success case):
   - `LAST_REFRESH_RESULT='success'`
   - `LAST_REFRESH_TYPE = <'complete' | 'fast'>`
   - `LAST_REFRESH_TIME=NOW(6)`
   - `LAST_SUCCESSFUL_REFRESH_READ_TSO = <COMPLETE refresh: txn for_update_ts>`
   - `LAST_REFRESH_FAILED_REASON = NULL`
7. Commit transaction.

Failure path (for example `INSERT INTO ... SELECT ...` fails):

1. `ROLLBACK TO SAVEPOINT` to roll back MV data changes (no partial MV data update).
2. Update `mysql.tidb_mview_refresh` row (failure case):
   - `LAST_REFRESH_RESULT='failed'`
   - `LAST_REFRESH_TYPE = <'complete' | 'fast'>`
   - `LAST_REFRESH_TIME=NOW(6)`
   - `LAST_REFRESH_FAILED_REASON = <error string>`
   - `LAST_SUCCESSFUL_REFRESH_READ_TSO` is **not updated** (keeps last successful value).
3. `COMMIT` failure metadata, then return original error to user.

Pseudo SQL (key points only):

```sql
-- all SQL below runs in an internal session
BEGIN PESSIMISTIC;

-- (A) mutex: lock row; if NOWAIT fails, fail immediately
SELECT MVIEW_ID, LAST_SUCCESSFUL_REFRESH_READ_TSO
  FROM mysql.tidb_mview_refresh
 WHERE MVIEW_ID = <mview_id>
 FOR UPDATE NOWAIT;

-- (A2) consistency check: plain read must match step (A)
SELECT LAST_SUCCESSFUL_REFRESH_READ_TSO
  FROM mysql.tidb_mview_refresh
 WHERE MVIEW_ID = <mview_id>;

SAVEPOINT tidb_mview_refresh_sp;

-- (B) full replacement
DELETE FROM <db>.<mv>;
-- note: in strict mode TiDB normally blocks TiFlash/MPP on the SELECT part of a write statement.
-- for internal MV maintenance, internal session can set a dedicated flag
-- (for example `SessionVars.InMaterializedViewMaintenance`) so optimizer bypasses that strict-mode guard,
-- allowing the SELECT side of INSERT ... SELECT to use TiFlash/MPP.
INSERT INTO <db>.<mv> <SQLContent>;
  -- SQLContent is MV definition SELECT (rollback to savepoint on failure)
  -- so COMPLETE refresh can leverage TiFlash for heavy scans.

-- (C) update refresh metadata in the same transaction
UPDATE mysql.tidb_mview_refresh
   SET LAST_REFRESH_RESULT = 'success',
       LAST_REFRESH_TYPE = <refresh_type>,
       LAST_REFRESH_TIME = NOW(6),
       LAST_SUCCESSFUL_REFRESH_READ_TSO = <for_update_ts>,
       LAST_REFRESH_FAILED_REASON = NULL
 WHERE MVIEW_ID = <mview_id>;

COMMIT;
```

### Lock behavior and error semantics

For `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh`, there are 3 outcomes:

1. **Returns 1 row**: lock acquired, refresh can continue.
2. **Returns lock-conflict error**: another session is refreshing (or at least holding this row lock).
   - Typical TiDB/MySQL error code is `3572` (`ErrLockAcquireFailAndNoWaitSet`).
   - MVP can pass through this error directly; a friendlier wrapper is also acceptable
     (for example "another session is refreshing this materialized view").
3. **No error, 0 rows**: missing `MVIEW_ID` row in system table.
   - This is metadata inconsistency and should fail the refresh.

### Why pessimistic transaction

`FOR UPDATE NOWAIT` is meaningful only inside a transaction and should fail immediately on conflict.
Explicit `BEGIN PESSIMISTIC` ensures lock acquisition and conflict behavior match mutex semantics.

### COMPLETE refresh read tso (`for_update_ts`)

Requirement: on successful COMPLETE refresh, `LAST_SUCCESSFUL_REFRESH_READ_TSO` must store the transaction `for_update_ts` used for refresh read.

Reason: in `BEGIN PESSIMISTIC`, DML reads (such as `INSERT INTO ... SELECT ...`) use `for_update_ts`.
So MV data snapshot corresponds to `for_update_ts`.
If only `start_ts` is stored, users may observe that MV data includes rows newer than `LAST_SUCCESSFUL_REFRESH_READ_TSO`,
which also misleads later incremental-refresh/check logic.

`FAST` real incremental logic is not implemented yet (planner still returns placeholder),
so only COMPLETE read-tso semantics are defined for now.

Implementation options:

1. **Read from TxnCtx**: after refresh DML (`DELETE` / `INSERT ... SELECT ...`), read `sctx.GetSessionVars().TxnCtx.GetForUpdateTS()`.
2. **Reuse last_query_info**: after key DML in transaction, read `for_update_ts` from `@@tidb_last_query_info` JSON.

Option (1) is preferred (direct and no extra SQL), and guarantees transaction-context `for_update_ts`.

Current code uses option (1): after COMPLETE DML finishes, read `for_update_ts` from `TxnCtx` and persist it to `LAST_SUCCESSFUL_REFRESH_READ_TSO`.

## Code placement (current implementation)

`REFRESH MATERIALIZED VIEW` is a utility/maintenance statement and does not enter DDL job queue.
Execution path:

1. Parser/AST:
   - `RefreshMaterializedViewStmt` and `RefreshMaterializedViewImplementStmt` are defined in `pkg/parser/ast/misc.go`.
   - parser grammar parses `REFRESH MATERIALIZED VIEW` under generic `Statement` branch in `pkg/parser/parser.y`.
2. Planner:
   - `PlanBuilder.buildRefreshMaterializedView` builds plan and enforces outer privilege check (MVP: `ALTER` on MV).
3. Executor:
   - executor builder maps plan to `RefreshMaterializedViewExec`.
   - `RefreshMaterializedViewExec` runs refresh service directly (`Validate + Lock + Savepoint + DataChanges + RefreshInfo Persist + Commit`).

Core execution semantics:

- Refresh uses internal session, not caller session transaction/variables.
- Refresh path uses dedicated internal source type (`kv.InternalTxnMVMaintenance`).
- Uses `BEGIN PESSIMISTIC` + `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh` for mutex.
- Uses `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` so data changes can be rolled back while failure metadata still commits.
- `COMPLETE` rebuilds data with `DELETE FROM mv` + `INSERT INTO mv SELECT ...`.
- `FAST` uses internal-only statement `RefreshMaterializedViewImplementStmt` as framework entry
  (planner still returns placeholder "not supported").
- `RefreshMaterializedViewStmt` is a normal `StmtNode` with no DDL-statement semantics
  (for example it does not set `LastExecuteDDL` flag).
- Statement is forbidden inside explicit user transactions (`BEGIN` / `START TRANSACTION`),
  and must run as standalone autocommit statement.

## Next phases

### Phase-3: Implement real FAST refresh execution (incremental path)

Phase-3 turns current FAST framework (internal statement + planner placeholder) into a real executable incremental path:

1. Build incremental input set from MLOG and `LAST_SUCCESSFUL_REFRESH_READ_TSO`.
2. Generate executable plan tree / executor for `RefreshMaterializedViewImplementStmt` instead of returning "not supported".
3. Define success/failure metadata updates and replay semantics for FAST, with observability/recovery parity with COMPLETE.

### Phase-4: Support out-of-place COMPLETE refresh (decouple build and cutover)

For out-of-place COMPLETE refresh, the recommended model is "utility main flow + DDL sub-steps":

1. Utility stage: build shadow data (new table or temporary physical object).
2. Cutover stage: run dedicated DDL sub-step for atomic switch (metadata/schema-level operation).
3. Utility stage: clean old objects and update refresh metadata.

Considerations:

- Reusing generic `RENAME TABLE` for MV swap is not recommended.
  TiDB already has MV-specific restrictions (for example rename on MV tables is blocked;
  rename on base tables with MV dependencies is also blocked), so cutover semantics should be designed explicitly.
- MV metadata is table-ID bound (for example `mysql.tidb_mview_refresh.MVIEW_ID`,
  `MaterializedViewBase.MViewIDs` on base table), so cutover must define ID-binding preservation/migration explicitly.

## Test suggestions (for future implementation)

Add executor UT coverage in `pkg/executor/test/executor/` (refresh-focused) and `pkg/executor/test/ddl/` (MV DDL-related):

1. **Basic correctness**:
   - create base table + mlog + mv
   - insert base data
   - execute `REFRESH MATERIALIZED VIEW mv COMPLETE`
   - verify MV content equals `SELECT ... GROUP BY ...`
   - verify `mysql.tidb_mview_refresh.LAST_SUCCESSFUL_REFRESH_READ_TSO > 0` and `LAST_REFRESH_RESULT='success'`
2. **Concurrency mutex**:
   - session A starts refresh and pauses after lock acquisition (`FOR UPDATE`) via failpoint or manual lock hold
   - session B executes refresh and should get NOWAIT lock conflict
3. **Missing metadata row**:
   - delete row from `mysql.tidb_mview_refresh`
   - execute refresh and expect "refresh info row missing" error

## Known limitations and future direction

- FAST refresh currently has framework path only; real incremental logic is not implemented:
  - Introduced internal-only AST `RefreshMaterializedViewImplementStmt`.
    It is constructed inside executor and is not parsed from SQL text.
  - This AST carries:
    1. original `RefreshMaterializedViewStmt` (must be `Type=FAST`)
    2. `LAST_SUCCESSFUL_REFRESH_READ_TSO` value
       (FAST assumes this must be non-NULL `int64`, else fail immediately)
  - Execution uses `ExecuteInternalStmt(ctx, stmtNode)` to enable future integration with regular
    compile/optimize/executor pipeline, instead of simple SQL-text execution path.
  - Planner already routes `RefreshMaterializedViewImplementStmt` through a dedicated build branch,
    but currently returns "not supported" as placeholder.
  - `RefreshMaterializedViewImplementStmt.Restore()` renders
    `IMPLEMENT FOR <RefreshStmt> USING TIMESTAMP <LAST_SUCCESSFUL_REFRESH_READ_TSO>` for log/toString.
    Since parser grammar does not expose this syntax, restore output is not required to be parse-roundtrippable.
  - If `ExecuteInternalStmt` returns non-nil `RecordSet`, call `Next()` to drain before `Close()`;
    this guarantees full executor-tree execution for future plans (for example `INSERT ... SELECT ...`).
- `DELETE FROM mv` + `INSERT INTO mv SELECT ...` in one transaction can create very large transactions for big MVs
  (txn size limits, write amplification, GC pressure).
  A future "build new object + atomic cutover" strategy is possible but needs careful atomicity-boundary design,
  because it introduces DDL semantics.
- For future advanced refresh flows (FAST/incremental, MLOG merge/upsert, or operators not easily represented by SQL text),
  dependency on "build SQL text + ExecuteInternal" should be reduced gradually:
  - current `RefreshMaterializedViewImplementStmt` is step 1: use structured internal AST to carry
    executor-only parameters (for example `LAST_SUCCESSFUL_REFRESH_READ_TSO`) through compile/optimize/executor.
  - next, planner should generate dedicated plan tree / executor for this statement
    (possibly `INSERT INTO mv SELECT ...` or more advanced merge/upsert plans),
    so SQL-text execution can be fully replaced.
- Persisting failure reason (`LAST_REFRESH_FAILED_REASON`) without polluting MV data relies on
  `SAVEPOINT`/`ROLLBACK TO SAVEPOINT`.
  If future execution environments cannot support savepoint (or need stronger consistency semantics),
  metadata persistence may need to be split into a separate transaction, with concurrency and visibility semantics redesigned.
