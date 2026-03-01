# Materialized View Refresh (Implementation and Design Notes)

This document describes the current implementation and the next evolution steps for TiDB `REFRESH MATERIALIZED VIEW` (`COMPLETE` / `FAST`).

At the moment:

- `COMPLETE` refresh is implemented with transactional semantics.
- `FAST` refresh is implemented and uses the same transactional framework as `COMPLETE`.

## Runtime `NEXT_TIME` Update (Internal SQL Success Path)

- For **internal SQL** triggered refresh (identified by `SessionVars.InRestrictedSQL`), after a successful refresh commit, `mysql.tidb_mview_refresh_info.NEXT_TIME` should be updated together with success metadata.
- Runtime `NEXT_TIME` derivation in this path is intentionally different from create-time derivation:
  - evaluate and use only `RefreshNext` expression;
  - do not apply create-time `START WITH` priority / near-now rules;
  - if `RefreshStartWith` is non-empty and `RefreshNext` is empty, explicitly set `NEXT_TIME = NULL`;
  - if both are empty, keep `NEXT_TIME` unchanged.
- For non-internal (user) SQL refresh, keep existing behavior (do not update `NEXT_TIME` on success path).

> Note: refresh metadata and refresh history are now split:
> - `mysql.tidb_mview_refresh_info` stores per-MV metadata for next refresh (for example `LAST_SUCCESS_READ_TSO`).
> - `mysql.tidb_mview_refresh_hist` stores per-refresh lifecycle and results (`running/success/failed`).
> See `pkg/session/bootstrap.go` for system table definitions.

## Goals (scope of the current implementation)

1. **Transactional all-or-nothing for MV data**: one refresh must commit or roll back data replacement atomically.
2. **Concurrency mutex**: for one MV, when multiple sessions refresh concurrently, only one can enter the execution path; others fail immediately with a locking error.
3. **Success-only refresh metadata update with CAS + double check**: only when refresh succeeds, update the MV row in `mysql.tidb_mview_refresh_info`, especially:
   - read `refresh_read_tso` from `TxnCtx.GetForUpdateTS()`
   - update `LAST_SUCCESS_READ_TSO` by CAS-style condition
     (`WHERE MVIEW_ID = <mview_id> AND LAST_SUCCESS_READ_TSO <=> <locked_tso>`)
   - re-read `LAST_SUCCESS_READ_TSO` and require it equals `refresh_read_tso`, otherwise fail refresh as inconsistent
4. **Refresh lifecycle history**: after lock acquisition, insert a `running` row into `mysql.tidb_mview_refresh_hist` using an independent session.
   - `REFRESH_JOB_ID` uses this refresh's `start_tso`.
5. **Finalize history after refresh commit**: after refresh transaction commit outcome is known, update the same history row to `success` or `failed`.
6. **Usable COMPLETE refresh**: do full data replacement with transactional `DELETE + INSERT`.
7. **Usable FAST refresh**: run fast refresh through an internal statement path and incremental merge execution.
8. **Privilege semantics scoped to MVP**: for outer SQL semantics, only check `ALTER` on MV; run refresh with internal sessions so system-table privileges on `mysql.tidb_mview_refresh_info` / `mysql.tidb_mview_refresh_hist` do not leak to business users.

## Non-goals (not included yet)

- Separate semantics for `WITH SYNC MODE`.
  Refresh is synchronous today, so `WITH SYNC MODE` is parsed/executed but behaves the same as without it.
  If async refresh is introduced later, semantics can be redefined.
- Performance optimization for large MVs (for example large-transaction mitigation, delete cost reduction, swap table strategies).
- Long-term retention/cleanup strategy for `mysql.tidb_mview_refresh_hist` (TTL/archival policy).

## Data and metadata sources

- MV physical storage is a normal table marked by `TableInfo.MaterializedView != nil`.
- MV definition SQL is stored in `TableInfo.MaterializedView.SQLContent`, canonical `SELECT ...`.
  See `pkg/meta/model/table.go` and `pkg/ddl/materialized_view.go`.
- Refresh metadata table:
  - `mysql.tidb_mview_refresh_info` (PK `MVIEW_ID`, fields include success metadata used by next refresh, for example `LAST_SUCCESS_READ_TSO`).
- Refresh history table:
  - `mysql.tidb_mview_refresh_hist` (per-job lifecycle/status, primary key is `REFRESH_JOB_ID`; each row also stores `MVIEW_ID`).

`MVIEW_ID` directly uses MV physical table `TableInfo.ID`.

## Create-time `NEXT_TIME` Initialization (`CREATE MATERIALIZED VIEW`)

`REFRESH MATERIALIZED VIEW` relies on an existing row in `mysql.tidb_mview_refresh_info`.

When `CREATE MATERIALIZED VIEW` succeeds, DDL worker initializes (or upserts) that row with:

- `MVIEW_ID`
- initial `LAST_SUCCESS_READ_TSO` (from create-time initial build read tso)
- `NEXT_TIME` (derived from create-time schedule expressions)

Create-time `NEXT_TIME` derivation rules (for `RefreshStartWith` / `RefreshNext`) are:

1. If both are empty, do not update `NEXT_TIME` (row keeps default `NULL`).
2. Evaluate expressions in prepared eval session (`UTC` timezone + DDL job SQL mode).
3. `START WITH` has higher priority, unless it is near-now (`START WITH < now + 10s`) and `NEXT` exists; in that case use `NEXT`.
4. If the chosen expression evaluates to `NULL`, explicitly write `NEXT_TIME = NULL`.

This create-time rule set is intentionally different from runtime internal-refresh reschedule rule:

- runtime internal refresh uses `RefreshNext` only;
- runtime internal refresh does not apply create-time `START WITH`/near-now priority.

## SQL behavior (user view)

Supported syntax (all use one common transactional framework today):

```sql
REFRESH MATERIALIZED VIEW db.mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE; -- uses current DB
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE COMPLETE; -- same behavior today (refresh is already synchronous)

REFRESH MATERIALIZED VIEW mv FAST;
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE FAST; -- same behavior today (refresh is already synchronous)
```

Current note: `FAST` requires `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` to be non-`NULL`; otherwise refresh fails.

Current privilege semantics (MVP):

- `REFRESH MATERIALIZED VIEW` requires `ALTER` privilege on target MV (outer semantic privilege).
- Internal `DELETE/INSERT`, `mysql.tidb_mview_refresh_info` updates, and `mysql.tidb_mview_refresh_hist` writes run on internal sessions, so caller does not need direct DML privilege on those system tables.
- If finer-grained privilege semantics are introduced later (for example base-table `SELECT` checks), extend from this MVP baseline.

## Core execution flow (transactional refresh framework)

The most direct implementation is: transaction + row-lock mutex + history lifecycle + data refresh + success-metadata update.

`COMPLETE` and `FAST` share the same outer framework; only the "refresh implementation" step differs.

1. Get an internal session from session pool and start a transaction on it (recommended **pessimistic**, so `FOR UPDATE NOWAIT` works immediately).
2. In transaction, lock refresh-info row by `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh_info` (used as refresh mutex), and remember the locked row's `LAST_SUCCESS_READ_TSO` value (nullable).
3. Record refresh `start_tso` as `REFRESH_JOB_ID`.
4. Use an independent session to insert one `mysql.tidb_mview_refresh_hist` row with `REFRESH_STATUS='running'` and `REFRESH_JOB_ID=<start_tso>`.
5. Run refresh implementation by refresh type:
   - `COMPLETE`: `DELETE FROM <mv_table>` + `INSERT INTO <mv_table> <mv_select_sql>`.
   - `FAST`: construct internal statement and run via `ExecuteInternalStmt` to apply incremental changes.
6. Success path: read `refresh_read_tso` from transaction context (`TxnCtx.GetForUpdateTS()`).
7. Before commit, persist success metadata with CAS-style SQL:
   - `UPDATE ... SET LAST_SUCCESS_READ_TSO = <refresh_read_tso> WHERE MVIEW_ID = <mview_id> AND LAST_SUCCESS_READ_TSO <=> <locked_tso>`.
   - runtime internal-SQL rule: update `NEXT_TIME` by evaluating only `RefreshNext`; if `RefreshStartWith != ''` and `RefreshNext == ''`, set `NEXT_TIME = NULL`.
8. Do double check by reading back `LAST_SUCCESS_READ_TSO` from `mysql.tidb_mview_refresh_info`:
   - if value is `NULL` or not equal to `<refresh_read_tso>`, treat as unknown inconsistency and fail refresh.
9. Commit refresh transaction.
10. After commit returns success, use independent session to update `mysql.tidb_mview_refresh_hist` for this `REFRESH_JOB_ID` to `REFRESH_STATUS='success'` and fill completion fields.

Failure path (for example `INSERT INTO ... SELECT ...` fails):

1. `ROLLBACK` the refresh transaction to roll back MV data changes (no partial MV data update).
2. Do **not** update `mysql.tidb_mview_refresh_info` (failure does not change success watermark).
3. After refresh transaction finishes and failure is known, use independent session to update `mysql.tidb_mview_refresh_hist` for this `REFRESH_JOB_ID`:
   - `REFRESH_STATUS='failed'`
   - failure reason / error message
   - completion timestamp.
4. Return original error to user.

Pseudo SQL (key points only):

```sql
-- refresh transaction SQL runs on one internal session
BEGIN PESSIMISTIC;

-- (A) mutex: lock row; if NOWAIT fails, fail immediately
SELECT MVIEW_ID, LAST_SUCCESS_READ_TSO
  FROM mysql.tidb_mview_refresh_info
 WHERE MVIEW_ID = <mview_id>
 FOR UPDATE NOWAIT;
-- locked_last_success_read_tso := row.LAST_SUCCESS_READ_TSO (nullable)

-- (A2) use transaction start_tso as refresh job id
-- refresh_job_id := <start_tso>;

-- (A3) independent internal session (not this transaction) inserts running history
INSERT INTO mysql.tidb_mview_refresh_hist (
    MVIEW_ID, REFRESH_JOB_ID, REFRESH_METHOD, REFRESH_TIME, REFRESH_STATUS
) VALUES (
    <mview_id>, <refresh_job_id>, <refresh_method>, NOW(6), 'running'
);

-- (B) full replacement
DELETE FROM <db>.<mv>;
-- note: in strict mode TiDB normally blocks TiFlash/MPP on the SELECT part of a write statement.
-- for internal MV maintenance, internal session can set a dedicated flag
-- (for example `SessionVars.InMaterializedViewMaintenance`) so optimizer bypasses that strict-mode guard,
-- allowing the SELECT side of INSERT ... SELECT to use TiFlash/MPP.
INSERT INTO <db>.<mv> <SQLContent>;
  -- SQLContent is MV definition SELECT (rollback whole refresh txn on failure)
  -- so COMPLETE refresh can leverage TiFlash for heavy scans.

-- (C1) read refresh tso from transaction context
-- refresh_read_tso := <TxnCtx.GetForUpdateTS()>;

-- (C2) success-only metadata update in the same refresh transaction (CAS style)
UPDATE mysql.tidb_mview_refresh_info
   SET LAST_SUCCESS_READ_TSO = <refresh_read_tso>
   -- internal SQL path only:
   --   1) if RefreshNext is non-empty: NEXT_TIME = eval(RefreshNext)
   --   2) else if RefreshStartWith is non-empty: NEXT_TIME = NULL
   --   3) else: NEXT_TIME unchanged
   NEXT_TIME = <runtime_derived_or_unchanged>
 WHERE MVIEW_ID = <mview_id>
   AND LAST_SUCCESS_READ_TSO <=> <locked_last_success_read_tso>;

-- (C3) double check after UPDATE
SELECT LAST_SUCCESS_READ_TSO
  FROM mysql.tidb_mview_refresh_info
 WHERE MVIEW_ID = <mview_id>;
-- if result is NULL or != <refresh_read_tso>, fail refresh as inconsistent

COMMIT;

-- (D) independent internal session finalizes history AFTER refresh commit
UPDATE mysql.tidb_mview_refresh_hist
   SET REFRESH_STATUS = 'success',
       REFRESH_ENDTIME = NOW(6),
       REFRESH_READ_TSO = <refresh_read_tso>,
       REFRESH_FAILED_REASON = NULL
 WHERE MVIEW_ID = <mview_id>
   AND REFRESH_JOB_ID = <refresh_job_id>;

-- (D-failed) if refresh transaction ends as failure, finalize the same row as failed
UPDATE mysql.tidb_mview_refresh_hist
   SET REFRESH_STATUS = 'failed',
       REFRESH_ENDTIME = NOW(6),
       REFRESH_READ_TSO = NULL,
       REFRESH_FAILED_REASON = <refresh_error>
 WHERE MVIEW_ID = <mview_id>
   AND REFRESH_JOB_ID = <refresh_job_id>;
```

### Lock behavior and error semantics

For `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh_info`, there are 3 outcomes:

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

### Refresh read tso (`for_update_ts`)

Requirement: on successful COMPLETE refresh, `LAST_SUCCESS_READ_TSO` must store the transaction `for_update_ts` used for refresh read.

Reason: in `BEGIN PESSIMISTIC`, DML reads (such as `INSERT INTO ... SELECT ...`) use `for_update_ts`.
So MV data snapshot corresponds to `for_update_ts`.
If only `start_ts` is stored, users may observe that MV data includes rows newer than `LAST_SUCCESS_READ_TSO`,
which also misleads later incremental-refresh/check logic.

The same success-path read-tso persistence rule is used for both `COMPLETE` and `FAST`.

Current code path always reads refresh success tso from transaction context:

1. after refresh data changes, call `sctx.GetSessionVars().TxnCtx.GetForUpdateTS()`
2. if the value is `0`, fail refresh
3. persist it to `LAST_SUCCESS_READ_TSO` through CAS update + post-update readback check.

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
   - `RefreshMaterializedViewExec` runs refresh service directly (`Validate + Lock + HistRunning Persist + DataChanges + SuccessInfo Persist + Commit + HistFinalize`).

Core execution semantics:

- Refresh uses internal session, not caller session transaction/variables.
- Refresh path uses dedicated internal source type (`kv.InternalTxnMVMaintenance`).
- Uses `BEGIN PESSIMISTIC` + `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh_info` for mutex.
- On success path, updates `LAST_SUCCESS_READ_TSO` with CAS condition (`LAST_SUCCESS_READ_TSO <=> <locked_tso>`) and verifies readback equals `TxnCtx.GetForUpdateTS()`.
- On refresh execution failure, rolls back the whole refresh transaction to guarantee all-or-nothing MV data replacement.
- `COMPLETE` rebuilds data with `DELETE FROM mv` + `INSERT INTO mv SELECT ...`.
- `FAST` uses internal-only statement `RefreshMaterializedViewImplementStmt` and a dedicated incremental merge plan.
- For `FAST`, executor constructs `RefreshMaterializedViewImplementStmt` with:
  - original `RefreshMaterializedViewStmt` (must be `Type=FAST`)
  - `LAST_SUCCESS_READ_TSO` value (must be non-`NULL` int64)
- `FAST` execution goes through `ExecuteInternalStmt(ctx, stmtNode)`.
- If `ExecuteInternalStmt` returns non-nil `RecordSet`, refresh drains it before `Close()` to guarantee full executor-tree execution.
- `RefreshMaterializedViewStmt` is a normal `StmtNode` with no DDL-statement semantics
  (for example it does not set `LastExecuteDDL` flag).
- Statement is forbidden inside explicit user transactions (`BEGIN` / `START TRANSACTION`),
  and must run as standalone autocommit statement.

## Next phases

### Support out-of-place COMPLETE refresh (decouple build and cutover)

For out-of-place COMPLETE refresh, the recommended model is "utility main flow + DDL sub-steps":

1. Utility stage: build shadow data (new table or temporary physical object).
2. Cutover stage: run dedicated DDL sub-step for atomic switch (metadata/schema-level operation).
3. Utility stage: clean old objects and update refresh metadata.

Considerations:

- Reusing generic `RENAME TABLE` for MV swap is not recommended.
  TiDB already has MV-specific restrictions (for example rename on MV tables is blocked;
  rename on base tables with MV dependencies is also blocked), so cutover semantics should be designed explicitly.
- MV metadata is table-ID bound (for example `mysql.tidb_mview_refresh_info.MVIEW_ID` and `mysql.tidb_mview_refresh_hist.MVIEW_ID`,
  `MaterializedViewBase.MViewIDs` on base table), so cutover must define ID-binding preservation/migration explicitly.

## Test suggestions (for future implementation)

Add executor UT coverage in `pkg/executor/test/executor/` (refresh-focused) and `pkg/executor/test/ddl/` (MV DDL-related):

1. **Basic correctness**:
   - create base table + mlog + mv
   - insert base data
   - execute `REFRESH MATERIALIZED VIEW mv COMPLETE`
   - verify MV content equals `SELECT ... GROUP BY ...`
   - verify `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO > 0`
   - verify one row in `mysql.tidb_mview_refresh_hist` has `REFRESH_STATUS='success'` and `REFRESH_JOB_ID=<start_tso>`
2. **Concurrency mutex**:
   - session A starts refresh and pauses after lock acquisition (`FOR UPDATE`) via failpoint or manual lock hold
   - session B executes refresh and should get NOWAIT lock conflict
3. **Missing metadata row**:
   - delete row from `mysql.tidb_mview_refresh_info`
   - execute refresh and expect "refresh info row missing" error
4. **Failure semantics**:
   - force COMPLETE refresh failure (for example injected `INSERT ... SELECT` error)
   - verify `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` is unchanged
   - verify corresponding `mysql.tidb_mview_refresh_hist` row is finalized to `REFRESH_STATUS='failed'` with error reason

## Known limitations and future direction

- `DELETE FROM mv` + `INSERT INTO mv SELECT ...` in one transaction can create very large transactions for big MVs
  (txn size limits, write amplification, GC pressure).
  A future "build new object + atomic cutover" strategy is possible but needs careful atomicity-boundary design,
  because it introduces DDL semantics.
- History finalization is intentionally after refresh transaction commit, because only then final status is definitive.
  If process crash happens between refresh commit and history finalize update, recovery/reconciliation for
  stale `running` rows is still a future enhancement.
