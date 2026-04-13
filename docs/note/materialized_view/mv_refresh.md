# Materialized View Refresh (Implementation and Design Notes)

This document describes the current implementation and the next evolution steps for TiDB
`REFRESH MATERIALIZED VIEW`
(`COMPLETE IN PLACE` / `COMPLETE OUT OF PLACE` / `COMPLETE DELTA APPLY` / `FAST`).

At the moment:

- `COMPLETE IN PLACE` refresh is implemented with transactional `DELETE + INSERT`.
- `COMPLETE OUT OF PLACE` refresh is implemented with shadow-table build plus DDL cutover.
- `COMPLETE DELTA APPLY` refresh is implemented with a `FULL OUTER JOIN` diff source plus a dedicated apply sink executor.
- `FAST` refresh is implemented and uses the same outer transactional refresh framework as
  `COMPLETE IN PLACE` / `COMPLETE DELTA APPLY`.

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
6. **Usable COMPLETE IN PLACE refresh**: do full data replacement with transactional `DELETE + INSERT`.
7. **Usable COMPLETE OUT OF PLACE refresh**: build shadow data outside the refresh transaction and cut over atomically in DDL.
8. **Usable COMPLETE DELTA APPLY refresh**: compute one full diff stream and apply only changed rows inside the refresh transaction.
9. **Usable FAST refresh**: run fast refresh through an internal statement path and incremental merge execution.
10. **Privilege semantics scoped to MVP**: for outer SQL semantics, only check `ALTER` on MV; run refresh with internal sessions so system-table privileges on `mysql.tidb_mview_refresh_info` / `mysql.tidb_mview_refresh_hist` do not leak to business users.

## Non-goals (not included yet)

- Async execution semantics for `WITH ASYNC MODE`.
  The syntax is reserved by spec, but async refresh is not implemented yet.
  `REFRESH MATERIALIZED VIEW ... WITH ASYNC MODE ...` is parsed and rejected by executor.
- Further performance optimization for large MVs (for example path selection, large-diff memory tuning, TiFlash/MPP validation, and additional spill/perf work).
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

Supported syntax (current implemented modes):

```sql
REFRESH MATERIALIZED VIEW db.mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE; -- uses current DB
REFRESH MATERIALIZED VIEW mv WITH ASYNC MODE COMPLETE; -- parsed, but rejected: async refresh is not supported yet
REFRESH MATERIALIZED VIEW mv COMPLETE OUT OF PLACE;
REFRESH MATERIALIZED VIEW mv COMPLETE DELTA APPLY;

REFRESH MATERIALIZED VIEW mv FAST;
REFRESH MATERIALIZED VIEW mv WITH ASYNC MODE FAST; -- parsed, but rejected: async refresh is not supported yet
```

Current note: `FAST` requires `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` to be non-`NULL`; otherwise refresh fails.

Current note: `COMPLETE DELTA APPLY` currently requires a grouped MV definition and at least one visible
`NOT NULL` MV column that can be used as the side-missing marker.

Current privilege semantics (MVP):

- `REFRESH MATERIALIZED VIEW` requires `ALTER` privilege on target MV (outer semantic privilege).
- Internal `DELETE/INSERT`, `mysql.tidb_mview_refresh_info` updates, and `mysql.tidb_mview_refresh_hist` writes run on internal sessions, so caller does not need direct DML privilege on those system tables.
- If finer-grained privilege semantics are introduced later (for example base-table `SELECT` checks), extend from this MVP baseline.

## Core execution flow (transactional refresh framework)

The most direct implementation is: transaction + row-lock mutex + history lifecycle + data refresh + success-metadata update.

`COMPLETE IN PLACE`, `COMPLETE DELTA APPLY`, and `FAST` share the same outer transactional framework;
only the data-change implementation differs.
`COMPLETE OUT OF PLACE` reuses the outer advisory lock / history lifecycle but has a dedicated build + cutover path.

1. Get an internal session from session pool and start a transaction on it (recommended **pessimistic**, so `FOR UPDATE NOWAIT` works immediately).
2. In transaction, lock refresh-info row by `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh_info` (used as refresh mutex), and remember the locked row's `LAST_SUCCESS_READ_TSO` value (nullable).
3. Record refresh `start_tso` as `REFRESH_JOB_ID`.
4. Use an independent session to insert one `mysql.tidb_mview_refresh_hist` row with `REFRESH_STATUS='running'` and `REFRESH_JOB_ID=<start_tso>`.
5. Run refresh implementation by refresh type:
   - `COMPLETE IN PLACE`: `DELETE FROM <mv_table>` + `INSERT INTO <mv_table> <mv_select_sql>`.
   - `COMPLETE DELTA APPLY`: construct internal implementation statement and run one diff-source + apply-sink plan via `ExecuteInternalStmt`.
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

Requirement: on successful transactional refresh
(`COMPLETE IN PLACE` / `COMPLETE DELTA APPLY` / `FAST`),
`LAST_SUCCESS_READ_TSO` must store the transaction `for_update_ts` used for refresh read.

Reason: in `BEGIN PESSIMISTIC`, DML reads (such as `INSERT INTO ... SELECT ...`) use `for_update_ts`.
So MV data snapshot corresponds to `for_update_ts`.
If only `start_ts` is stored, users may observe that MV data includes rows newer than `LAST_SUCCESS_READ_TSO`,
which also misleads later incremental-refresh/check logic.

The same success-path read-tso persistence rule is used for
`COMPLETE IN PLACE` / `COMPLETE DELTA APPLY` / `FAST`.

Current code path (`COMPLETE IN PLACE` / `COMPLETE DELTA APPLY` / `FAST`) reads refresh success tso from transaction context:

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

- Refresh data changes and refresh-metadata writes run on internal sessions; the outer statement remains a standalone autocommit utility statement and does not join caller explicit transactions.
- Refresh path uses dedicated internal source type (`kv.InternalTxnMVMaintenance`).
- For `COMPLETE IN PLACE` / `COMPLETE DELTA APPLY` / `FAST`, uses `BEGIN PESSIMISTIC` + `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh_info` for mutex.
- For `COMPLETE IN PLACE` / `COMPLETE DELTA APPLY` / `FAST` success path, updates `LAST_SUCCESS_READ_TSO` with CAS condition (`LAST_SUCCESS_READ_TSO <=> <locked_tso>`) and verifies readback equals `TxnCtx.GetForUpdateTS()`.
- For `COMPLETE IN PLACE` / `COMPLETE DELTA APPLY` / `FAST` execution failure, rolls back the whole refresh transaction to guarantee all-or-nothing MV data replacement.
- `COMPLETE IN PLACE` rebuilds data with `DELETE FROM mv` + `INSERT INTO mv SELECT ...`.
- `COMPLETE DELTA APPLY` uses internal-only statement `RefreshMaterializedViewImplementStmt`, a `FULL OUTER JOIN` diff-source plan, and a dedicated `MVCompleteDeltaApply` sink plan.
- `COMPLETE OUT OF PLACE` uses a dedicated execution path (not the above refresh transaction):
  - build stage runs in independent internal session(s) outside explicit refresh transaction;
  - cutover and `mysql.tidb_mview_refresh_info` migration/update are done atomically in DDL worker transaction.
- For `FAST` / `COMPLETE DELTA APPLY`, executor constructs `RefreshMaterializedViewImplementStmt` and executes it through `ExecuteInternalStmt(ctx, stmtNode)`.
- For `FAST`, `RefreshMaterializedViewImplementStmt` carries `LAST_SUCCESS_READ_TSO` (must be non-`NULL` uint64 / `BIGINT UNSIGNED`).
- If `ExecuteInternalStmt` returns non-nil `RecordSet`, refresh drains it before `Close()` to guarantee full executor-tree execution.
- `RefreshMaterializedViewStmt` is a normal `StmtNode` with no DDL-statement semantics
  (for example it does not set `LastExecuteDDL` flag).
- Statement is forbidden inside explicit user transactions (`BEGIN` / `START TRANSACTION`),
  and must run as standalone autocommit statement.

## Implemented extensions and next phases

### COMPLETE OUT OF PLACE (implemented)

Current execution model:

1. Outer refresh path acquires the MV advisory lock and inserts a `running` history row before heavy work.
2. Build stage runs in a dedicated internal autocommit session:
   - create a deterministic shadow table via `CREATE TABLE <shadow> LIKE <mv>`;
   - keep the shadow as a normal table during build (`MaterializedView == nil`);
   - load shadow data with `IMPORT INTO ... FROM (<mv_select_sql>) WITH disable_precheck` on TiKV storage,
     otherwise fallback to `INSERT INTO <shadow> <mv_select_sql>`;
   - capture build read tso from build session `@@tidb_last_query_info.start_ts`.
3. Executor submits a dedicated DDL cutover action carrying at least:
   - old MV id;
   - shadow table id;
   - build read tso;
   - expected pre-cutover refresh-info watermark / nullness for CAS-style migration.
4. DDL worker atomically:
   - transfers the logical MV name to the shadow table;
   - moves MV definition metadata to the new physical table;
   - rewrites base-table reverse references (`MaterializedViewBase.MViewIDs`);
   - migrates `mysql.tidb_mview_refresh_info` ownership from old `MVIEW_ID` to shadow ID;
   - sets `LAST_SUCCESS_READ_TSO = build_read_tso` and preserves `NEXT_TIME`.
5. On success, finalize history as `success`; on failure, keep the old MV serving path unchanged and do best-effort shadow cleanup.

#### Next design evolution: protected shadow table

The current model leaves one known gap: the shadow table is born as a normal physical table,
so there is an intermediate window where external DML/DDL can observe or modify it before cutover.

The next hardening step for `COMPLETE OUT OF PLACE` refresh is to make the shadow table
"born protected" and globally visible before data loading starts.

Key requirements:

1. The shadow table must be protected from user DML/DDL from the moment it is created.
2. TiFlash must be able to observe the new table before `IMPORT INTO` starts, so PD rule delivery
   and snapshot application do not race with late schema visibility.
3. This should be implemented as internal metadata / DDL evolution, not by introducing new
   user-facing SQL syntax.

Recommended implementation shape:

1. Introduce dedicated shadow-table metadata in `TableInfo`.
   - Do not reuse `MaterializedView` metadata directly for the shadow table.
   - Instead, add one internal-only marker describing that the table is an out-of-place refresh
     shadow for a specific MV.
   - This keeps shadow semantics distinct from a real MV while still allowing planner/DDL
     interception to identify the table immediately after creation.

2. Add a new internal DDL action for creating the protected shadow table.
   - Replace the executor-side `CREATE TABLE ... LIKE ...` SQL path with an internal DDL job
     such as `ActionCreateMaterializedViewShadow`.
   - The job should clone the physical schema and TiFlash replica metadata from the existing MV,
     assign a fresh table ID, and persist the shadow marker at create time.
   - This does not add an extra schema-publish step compared with the current design:
     today the shadow is already created by a normal `CREATE TABLE ... LIKE ...` DDL, which also
     allocates a new table ID and publishes schema before build starts. The change is that the
     published object becomes a protected shadow table instead of a normal physical table.
   - No new parser syntax is needed because this action is only submitted internally.

3. Publish the shadow table before build starts.
   - The create-shadow DDL should finish schema version update and global schema synchronization
     before the refresh build phase continues.
   - After this publish point, TiDB and TiFlash can both observe the table ID and table metadata.
   - Only after that should the refresh path start `IMPORT INTO` or `INSERT INTO` for the shadow.

4. Intercept user DML and DDL on the shadow table.
   - Planner/executor should reject user `INSERT`, `REPLACE`, `UPDATE`, `DELETE`, and `IMPORT INTO`
     against a protected shadow table.
   - DDL executor should reject user `ALTER TABLE`, `TRUNCATE TABLE`, `RENAME TABLE`, and similar
     operations on the shadow table.
   - Internal MV maintenance sessions may bypass these checks through an explicit internal flag,
     similar to other MV-maintenance-only paths.

5. Keep the existing out-of-place cutover DDL, but allow protected shadow tables as inputs.
   - Today cutover requires the shadow to be a normal public physical table.
   - After the hardening change, cutover should accept a protected shadow table, rename it to the
     logical MV name, move MV metadata onto it, rewrite base-table reverse references, and then
     clear the shadow marker as part of the same atomic DDL transaction.
   - The existing cutover notifier should continue to be emitted so `mv_service` can observe the
     completed switch.

6. Adapt TiFlash schema sync for the new internal action.
   - TiFlash schema diff handling is action-type based, so the new create-shadow action must be
     recognized there as a create-table-like action.
   - This step is required so TiFlash updates its table-ID mapping and can accept snapshots for
     the shadow table before build traffic starts.
   - Relying only on snapshot-triggered fallback schema sync is not sufficient for a brand-new
     table ID because that path first depends on existing table-ID mapping.

7. Validate the hardened flow with focused coverage.
   - Verify user DML/DDL against the shadow table is rejected.
   - Verify internal refresh can still create, build, and cut over the shadow table successfully.
   - Verify TiFlash-replica scenarios observe the shadow table before `IMPORT INTO` starts.
   - Verify cutover keeps `mysql.tidb_mview_refresh_info`, `LAST_SUCCESS_READ_TSO`, `NEXT_TIME`,
     and refresh history behavior unchanged from the current out-of-place contract.

### Support FAST refresh upper bound with `AS OF TIMESTAMP`

Another planned evolution for `FAST` refresh is to allow users to specify a refresh upper bound
with `AS OF TIMESTAMP`, so one large backlog can be applied in smaller windows instead of forcing
one refresh to catch up all the way to "now".

This section records the intended semantics and implementation constraints before code changes.

#### Scope

1. Only `FAST` refresh should support this feature.
2. `COMPLETE IN PLACE` / `COMPLETE OUT OF PLACE` / `COMPLETE DELTA APPLY` do not need this option.
3. `AS OF TIMESTAMP` here should be treated as the refresh apply upper bound, not as generic
   statement stale-read semantics for the whole refresh statement.

#### Problem statement

When `FAST` refresh falls behind for a long time, applying all changes in one refresh may create
an overly large transaction and fail.

Desired user-facing behavior:

1. lower bound remains the previous successful watermark:
   `FROM_TS = LAST_SUCCESS_READ_TSO`
2. user specifies one target upper bound:
   `TARGET_TS = tso(parsed from AS OF TIMESTAMP)`
3. this refresh applies only changes in `(FROM_TS, TARGET_TS]`
4. after success, the persisted refresh watermark advances to `TARGET_TS`

This allows users to manually move the watermark forward in multiple smaller steps.

#### Timestamp model

With this feature, one `FAST` refresh must distinguish three timestamps:

1. `fromTS`:
   - previous successful refresh watermark
   - loaded from `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO`
2. `targetTSO`:
   - user-specified apply upper bound from `AS OF TIMESTAMP`
   - this is the logical "refresh up to here" watermark
3. `writeTxnTSO`:
   - the refresh transaction's current `for_update_tso`
   - used by the outer refresh statement and MV/MV-log readers in the existing refresh txn

Required invariants:

1. `targetTSO >= fromTS`
   - if `targetTSO < fromTS`, refresh should return an error directly
   - if `targetTSO == fromTS`, refresh should return no-op success
2. `targetTSO <= writeTxnTSO`
   - refresh must not claim to apply beyond the current transaction read horizon
3. `targetTSO` must pass normal snapshot/gc-safe-point validation before execution starts

Most importantly, after success the persisted watermark must become `targetTSO`, not `writeTxnTSO`.

Reason:

- `writeTxnTSO` and `targetTSO` diverge with this feature;
- if success still persists `writeTxnTSO`, then `(targetTSO, writeTxnTSO]` would be skipped forever
  by the next `FAST` refresh.

#### Read snapshot split inside one refresh

The intended read-snapshot split is:

1. MV table:
   - read at current refresh transaction `writeTxnTSO`
2. MV log table:
   - read at current refresh transaction `writeTxnTSO`
   - but delta extraction must explicitly filter `_tidb_commit_ts > fromTS AND _tidb_commit_ts <= targetTSO`
3. base table:
   - full-update / min-max recompute paths must read at `targetTSO`

Why this split is correct:

1. MV table:
   - MV does not contain rows beyond the previous successful watermark
   - refresh execution is serialized by existing refresh mutex semantics
   - so reading MV at current `writeTxnTSO` is acceptable
2. MV log:
   - log visibility can use current transaction snapshot, as long as logical delta window is
     restricted by explicit commit-ts predicates
3. base table:
   - recomputation logic must see the base-table state exactly at `targetTSO`
   - otherwise rows committed in `(targetTSO, writeTxnTSO]` may leak into group recomputation and
     make refresh results "too new"

This mixed-snapshot model is intentional and specific to MV refresh maintenance.

#### Why not use generic statement/table stale read directly

Current TiDB stale-read processing rejects `AS OF TIMESTAMP` inside an already opened transaction.

That means this feature should not be implemented by turning the entire refresh statement into one
generic stale-read statement, nor by directly attaching SQL-layer `AS OF TIMESTAMP` to base-table
references inside the existing refresh write transaction.

Instead, the design should keep:

1. the outer refresh statement and transaction model unchanged
2. `targetTSO` as a dedicated MV-refresh internal concept
3. special handling only for the base-table reader(s) inside `mvmerge`

In other words, this feature is "bounded fast refresh", not "refresh statement stale read".

#### Base-table read path

The cleanest execution model is:

1. `mvmerge` as a whole still runs under the current refresh transaction and uses `writeTxnTSO`
   for its normal child plan
2. the base-table full-update / min-max recompute sub-plan uses a dedicated inner reader that reads
   at `targetTSO`
3. this snapshot override should happen below SQL syntax level, inside planner/executor wiring,
   rather than by injecting generic SQL `AS OF TIMESTAMP`

For `MIN/MAX` full-update lookup, changing only the read ts is not sufficient.

That lookup path must use the schema snapshot at `targetTSO` as well:

1. build the full-update lookup template AST as usual
2. preprocess / resolve / optimize that template with the snapshot InfoSchema at `targetTSO`
3. build the full-update inner reader executor with the same snapshot InfoSchema
4. execute that inner reader at `targetTSO`

Reason:

1. base tables with MV dependencies already block many incompatible DDL operations
2. but index / physical-layout changes may still happen after `targetTSO`
3. planning the full-update lookup with current InfoSchema and only forcing read-ts to `targetTSO`
   may let the optimizer depend on indexes or physical layout that did not exist at `targetTSO`
4. therefore the full-update lookup path must keep schema snapshot and data snapshot aligned at
   `targetTSO`

Implementation notes:

1. planner/executor contract should carry `targetTSO` explicitly for `FAST` refresh
2. `mvmerge.BuildOptions` should grow from only `FromTS` to `FromTS + ToTS`
3. mlog delta SQL generation must really emit both bounds:
   `_tidb_commit_ts > FromTS AND _tidb_commit_ts <= ToTS`
4. full-update/min-max recompute readers should get a dedicated read-ts override equal to `targetTSO`
5. that inner reader should also be treated as stale read at request level, even if the outer refresh
   statement itself is not a generic stale-read statement
6. full-update/min-max lookup planning should use `targetTSO` snapshot InfoSchema rather than current
   InfoSchema
7. full-update/min-max executor build should also use the same `targetTSO` snapshot InfoSchema
8. if full-update/min-max lookup cannot be planned/extracted under the `targetTSO` snapshot schema,
   refresh should fail rather than silently fall back to current-schema planning

#### Metadata and schema assumptions

This design relies on two existing MV constraints:

1. refresh execution for one MV is serialized by the existing mutex path
2. base tables with MV dependencies already block relevant DDL operations

However, those DDL constraints are not sufficient to justify planning `MIN/MAX` full-update lookup
with the current InfoSchema.

Therefore the intended assumption split is:

1. MV table / MV log planning can continue to use current InfoSchema inside the current refresh
   framework
2. base-table `MIN/MAX` full-update lookup must use the snapshot InfoSchema at `targetTSO`
3. this is required even if many incompatible base-table DDLs are already blocked, because later
   index / physical-layout changes can still affect lookup planning correctness

#### GC-safe-point protection

This feature should not introduce any persistent MV-level GC blocking policy.

In particular:

1. do not add an MV option such as `block gc`
2. do not keep cluster GC pinned to `MIN(LAST_SUCCESS_READ_TSO)` across MVs
3. if users leave a backlog unrefreshed for too long and GC advances past the desired historical
   target, later bounded refresh should fail and require a newer target timestamp

What is still required is defining per-execution semantics for one bounded fast refresh.

##### Per-refresh execution semantics

Bounded `FAST ... AS OF TIMESTAMP ...` should follow ordinary stale-read GC semantics rather than
introducing MV-specific GC blocking.

Semantics:

1. every bounded fast refresh must still validate that `targetTSO` itself is a legal stale-read
   timestamp
2. if this refresh path needs `targetTSO` to read base-table snapshot schema/data, and current GC
   safe point is already newer than `targetTSO`, refresh must fail
3. refresh must not acquire a temporary service safe point or otherwise block GC advancement
4. if GC advances beyond `targetTSO` during execution, refresh may fail and roll back
5. users can retry with a newer `targetTSO`

Recommended ordering:

1. parse/evaluate `AS OF TIMESTAMP` into `targetTSO`
2. if `targetTSO < fromTS`, return an error directly
3. if `targetTSO == fromTS`, return no-op success directly
4. run normal stale-read timestamp legality validation before execution
5. only paths that need `targetTSO` base-table snapshot reads perform GC-safe-point validation
6. execute bounded fast refresh without blocking GC
7. rely on the normal snapshot read path to fail if `targetTSO` becomes invalid during execution

Rationale:

1. this matches TiDB's existing stale-read behavior better
2. correctness is preserved because refresh is transactional and can roll back on mid-execution
   stale-read failure
3. long-term backlog retention remains an operational concern and should not be encoded as MV-level
   GC blocking behavior

#### Recommended implementation shape

1. Extend parser/AST for `FAST` refresh to carry an optional `AS OF TIMESTAMP` expression.
2. Parse/evaluate that expression into `targetTSO` during refresh preparation.
3. If `targetTSO == fromTS`, return no-op success without entering `mvmerge`.
4. Keep current outer refresh transaction framework unchanged.
5. For every bounded fast refresh execution, validate `targetTSO` as a legal stale-read
   timestamp, but only paths that need `targetTSO` base-table snapshot reads should enforce the
   GC-safe-point check.
6. Carry both `fromTS` and `targetTSO` into `mvmerge` build/execution.
7. Read MV at `writeTxnTSO`.
8. Read MV log at `writeTxnTSO`, but explicitly filter `_tidb_commit_ts` into `(fromTS, targetTSO]`.
9. For `MIN/MAX` full-update lookup, preprocess / optimize the lookup template with the snapshot
   InfoSchema at `targetTSO`.
10. Build the `MIN/MAX` full-update inner reader with the same snapshot InfoSchema and execute it at
    `targetTSO` as a stale-read request.
11. If that `MIN/MAX` full-update lookup cannot be planned/extracted under the snapshot schema at
    `targetTSO`, fail refresh directly.
12. Persist `LAST_SUCCESS_READ_TSO = targetTSO` on success.

This feature should be implemented as an MV-refresh-specific extension of `FAST` refresh,
not as generic in-transaction stale-read SQL.

#### Recommended development stages

To keep implementation debuggable, the recommended development order is:

##### Stage 1: parser and outer refresh semantics

Goal: add the user-visible `FAST ... AS OF TIMESTAMP ...` entry and finalize outer refresh
statement semantics before touching `mvmerge`.

Scope:

1. extend parser/AST so `REFRESH MATERIALIZED VIEW ... FAST AS OF TIMESTAMP <expr>` is accepted
2. store the optional `AS OF TIMESTAMP` clause on `RefreshMaterializedViewStmt`
3. reject `AS OF TIMESTAMP` for non-`FAST` refresh modes
4. evaluate `AS OF TIMESTAMP` into `targetTSO` during outer refresh preparation
5. define the three outer semantic branches:
   - `targetTSO < fromTS` => return error directly
   - `targetTSO == fromTS` => return no-op success directly
   - `targetTSO > fromTS` => continue bounded fast refresh
6. pass numeric `targetTSO` into the internal refresh-implement statement rather than re-evaluating
   the expression later

Completion criteria:

1. parser/AST tests cover the new syntax
2. outer executor semantics for `<`, `==`, `>` against `fromTS` are fixed
3. no bounded merge logic is required yet

##### Stage 2: bounded-path GC validation semantics

Goal: align bounded fast refresh with ordinary stale-read GC semantics before implementing the
bounded merge window.

Scope:

1. reuse normal stale-read style timestamp legality validation for `targetTSO`
2. keep no-op path (`targetTSO == fromTS`) outside this validation flow
3. only bounded paths that need `targetTSO` base-table snapshot reads should enforce the
   GC-safe-point check
4. do not acquire a temporary service safe point or otherwise block GC
5. document that bounded refresh may fail and roll back if GC advances beyond `targetTSO` during
   execution

Completion criteria:

1. `gcSafePoint > targetTSO` fails refresh only for bounded paths that need `targetTSO`
   base-table snapshot reads
2. bounded paths that only need MV log windowing remain executable without GC-safe-point gating
3. no temporary or persistent MV-level GC blocking behavior is introduced
4. bounded execution semantics are explicitly documented as best-effort with rollback on stale-read
   failure

##### Stage 3: bounded FAST merge window

Goal: make the incremental merge logic respect `(fromTS, targetTSO]`.

Scope:

1. extend internal refresh-implement statement to carry `targetTSO`
2. extend `mvmerge.BuildOptions` from `FromTS` to `FromTS + ToTS`
3. generate MV log delta predicates using both bounds:
   `_tidb_commit_ts > fromTS AND _tidb_commit_ts <= targetTSO`
4. keep MV reads at `writeTxnTSO`
5. after successful bounded fast refresh, persist `LAST_SUCCESS_READ_TSO = targetTSO`
6. assert bounded refresh does not persist a watermark newer than the actual statement read horizon

Completion criteria:

1. bounded fast refresh without `MIN/MAX` can advance watermark in smaller windows
2. repeated bounded runs do not skip any interval

##### Stage 4: `MIN/MAX` full-update lookup with snapshot schema + snapshot data

Goal: make `MIN/MAX` recomputation correct even when current schema/index layout differs from the
schema at `targetTSO`.

Scope:

1. obtain snapshot InfoSchema at `targetTSO`
2. preprocess / resolve / optimize full-update lookup template under that snapshot InfoSchema
3. extract full-update lookup metadata from the snapshot-schema physical plan
4. extend `MVDeltaMerge` plan metadata so executor build can see `targetTSO` for the full-update
   path
5. build the full-update inner reader with:
   - snapshot InfoSchema at `targetTSO`
   - read ts = `targetTSO`
   - stale-read request flag enabled
6. if full-update lookup cannot be planned/extracted under snapshot schema at `targetTSO`, fail
   refresh directly instead of falling back to current-schema planning

Completion criteria:

1. `MIN/MAX` recomputation reads both schema and data at `targetTSO`
2. later index / physical-layout changes do not affect bounded fast refresh correctness

##### Stage 5: end-to-end validation

Goal: add focused regression coverage for the new bounded fast refresh behavior.

Minimum validation scope:

1. parser tests for `FAST ... AS OF TIMESTAMP ...`
2. executor tests for:
   - `targetTSO < fromTS`
   - `targetTSO == fromTS`
   - successful bounded watermark advance
3. GC-related tests for start-time rejection when `targetTSO` is older than GC safe point
4. `MIN/MAX` tests proving recomputation uses `targetTSO`
5. schema/index change tests proving snapshot-schema planning is honored for full-update lookup

Recommended execution order:

1. Stage 1
2. Stage 2
3. Stage 3
4. Stage 4
5. Stage 5

### COMPLETE DELTA APPLY (implemented)

`COMPLETE DELTA APPLY` computes the full MV definition result, shapes one diff stream, and applies only
changed rows to the target MV inside the existing refresh transaction framework.

#### Syntax contract (current implementation)

Refresh mode matrix is:

- `REFRESH MATERIALIZED VIEW ... COMPLETE`
- `REFRESH MATERIALIZED VIEW ... COMPLETE OUT OF PLACE`
- `REFRESH MATERIALIZED VIEW ... COMPLETE DELTA APPLY`
- `REFRESH MATERIALIZED VIEW ... FAST`

and reject these combinations:

- `FAST OUT OF PLACE`
- `FAST DELTA APPLY`
- `COMPLETE OUT OF PLACE DELTA APPLY`

`OUT OF PLACE` and `DELTA APPLY` should be treated as `COMPLETE`-only options.
Parser enforces that they can only appear after `COMPLETE`.

#### Current scope and assumptions

Current implementation is correctness-first and keeps scope tight:

1. Implementation targets grouped MVs; for diff computation, the logical row identity is the `GROUP BY` key.
2. MV definition must have `GROUP BY`; each `GROUP BY` item must be a column name and must appear in the `SELECT` list.
3. Diff join uses `=` for `NOT NULL` group-key columns and `<=>` for nullable group-key columns.
4. Physical row locators used later by `UPDATE` / `DELETE` are a separate concern from diff-join identity:
   - preferred locators are table handles (`PRIMARY KEY` / common handle);
   - `_tidb_rowid` may still be carried from the current MV side as a physical locator,
     but it is not used as the diff-join key.
5. Side-missing detection requires one visible `NOT NULL` MV column as marker; current code picks the first visible `NOT NULL` column from MV schema.
6. If these requirements are not met, reject `COMPLETE DELTA APPLY` directly
   (do not silently fallback to `COMPLETE IN PLACE`).
7. Keep existing outer advisory lock for refresh mutex semantics.
8. Keep existing in-place refresh transaction framework (`BEGIN PESSIMISTIC`, history lifecycle, success-only refresh-info persistence).

#### Why not split into three independent re-compute SQLs in one txn

A naive split (`INSERT diff`, `DELETE diff`, `UPDATE diff`) where each statement re-reads MV/query data has two issues:

1. Later statements can read earlier uncommitted writes in the same transaction.
2. Statement-level read ts can drift across statements, so all diffs may not be computed from one stable snapshot.

Also, stale-read SQL (`... AS OF TIMESTAMP ...`) is not a practical fix here because:

- it is rejected when used inside an explicit transaction;
- `tidb_snapshot` mode blocks write statements.

So current implementation avoids "recompute-per-DML-step" design.

#### Diff computation approach

Use one `FULL OUTER JOIN`-based diff source query, then let one dedicated sink operator apply row changes.

High-level algorithm:

1. Build query-side full result (`Q`) from MV definition SQL.
2. Full-outer-join `Q` with current MV table (`M`) by the `GROUP BY` key
   (which is the logical row identity in this refresh mode).
3. Keep only changed rows:
   - `Q-only` => `INSERT`
   - `M-only` => `DELETE`
   - both exist but payload differs => `UPDATE`
4. Output one diff stream (`FOJ + Selection`) and feed it directly into a dedicated MV-apply sink operator.
   - this operator executes per-row `INSERT` / `UPDATE` / `DELETE` on target MV table in the same transaction.
   - avoid splitting into three standalone write SQL statements.
5. On success, persist refresh watermark (`LAST_SUCCESS_READ_TSO`) with existing CAS + readback validation.

Example diff-shaping SQL (simplified):

```sql
WITH q AS (
    -- Full MV definition result; map selected marker column to q_marker
    SELECT k1, k2, <mv_marker_col> AS q_marker, v1, v2
    FROM (<mv_definition_sql>) q0
),
m AS (
    -- Current MV data; map same marker column to m_marker
    SELECT k1, k2, <mv_marker_col> AS m_marker, v1, v2
    FROM <mv_table>
)
SELECT
    CASE
        WHEN m.m_marker IS NULL THEN 'I'
        WHEN q.q_marker IS NULL THEN 'D'
        ELSE 'U'
    END AS diff_op,
    COALESCE(q.k1, m.k1) AS k1,
    COALESCE(q.k2, m.k2) AS k2,
    q.v1 AS new_v1, q.v2 AS new_v2,
    m.v1 AS old_v1, m.v2 AS old_v2
FROM q
FULL OUTER JOIN m
  ON q.k1 = m.k1
 AND q.k2 = m.k2
WHERE
      q.q_marker IS NULL
   OR m.m_marker IS NULL
   OR NOT (q.v1 <=> m.v1 AND q.v2 <=> m.v2);
```

In the SQL sketch above, `q_marker` / `m_marker` are logical aliases used to express
side-missing detection and `diff_op` derivation. They do not have to remain as standalone
output columns in the final planner-executor layout; the chosen marker can be read from
the `Q` / `M` row image via explicit metadata.

#### Join and diff rules

1. Join predicate:
   - use the `GROUP BY` key columns as the diff-join key;
   - use `=` for `NOT NULL` key columns and `<=>` for nullable key columns;
   - physical locators such as `PRIMARY KEY` handle columns or `_tidb_rowid` are not suitable
     diff-join keys; they are carried only for later `UPDATE` / `DELETE` locate.
2. Payload equality check should use null-safe comparison (`<=>`) per column.
3. Side-missing detection should use one deterministic marker column from MV schema:
   - pick the first visible `NOT NULL` column from MV `TableInfo.Columns` (stable column order);
   - map this column as logical aliases `q_marker` / `m_marker` in diff SQL;
   - `q_marker IS NULL` => row missing on query side (`DELETE`);
   - `m_marker IS NULL` => row missing on MV side (`INSERT`).

This avoids relying on key-column `IS NULL` checks and does not bind design
to any specific aggregate output column.

#### Write-path architecture (align with FAST refresh)

`COMPLETE DELTA APPLY` write stage should follow `FAST` refresh architecture:

1. Use an internal implementation statement path, not ad-hoc SQL text concatenation for write phase.
2. Let optimizer build one physical diff-source plan (`FOJ + Selection`) first.
3. Add one dedicated sink physical operator on top (similar role to `MVDeltaMerge` in FAST path).
4. Executor reads diff rows chunk-by-chunk and applies row operations to MV table directly.

Expected end-to-end shape:

```text
RefreshMaterializedViewExec
  -> executeRefreshMaterializedViewDataChanges(...)
    -> ExecuteInternalStmt(RefreshMaterializedViewImplementStmt for COMPLETE DELTA APPLY)
      -> PlanBuilder.buildRefreshMaterializedViewImplement(...)
        -> optimize diff-source SELECT (FOJ + Selection)
        -> wrap by new sink plan node (for example MVCompleteDiffApply)
      -> executorBuilder.build<NewSink>(...)
        -> new sink exec consumes child rows and writes target table (insert/update/delete)
```

This preserves the same key properties as FAST path:

- one statement-level read snapshot for diff computation;
- write/apply is in the same refresh transaction;
- no "statement A writes, statement B reads uncommitted write" drift from split DMLs.

For operator input layout, keep it explicit and stable (planner-executor contract):

1. row-op column (`diff_op`);
2. optional extra handle column (`_tidb_rowid`) only when MV uses extra row-id handle;
3. old row image (`M`) columns for delete/update old values;
4. new row image (`Q`) columns for insert/update new values.

Additional layout metadata stays explicit even when columns are reused:

1. marker selection is tracked by MV-column offset, so side-missing diagnostics can read the chosen
   marker from the `M` / `Q` row image instead of projecting `q_marker` / `m_marker` twice;
2. `MHandleCols` may either point to old-row-image columns (for PK/common handle) or to the optional
   extra `_tidb_rowid` column.

`diff_op` should be generated in diff-source projection (instead of re-evaluating marker logic in sink):

```sql
CASE
  WHEN m_marker IS NULL THEN 1  -- INSERT
  WHEN q_marker IS NULL THEN 2  -- DELETE
  ELSE 3                        -- UPDATE
END AS diff_op
```

Recommended encoding:

- `1` = `INSERT`
- `2` = `DELETE`
- `3` = `UPDATE`

Use integer op code (for example `TINYINT`) instead of string op code to keep executor branch cost low.

Note on diff filtering:

1. Keep existing diff filter (`q_marker IS NULL OR m_marker IS NULL OR payload_changed`) in `WHERE`.
2. Do not rely on select-field alias visibility in the same query block `WHERE`.
3. If filtering by `diff_op` is needed, wrap one extra projection/query layer.

Write mapping contract for sink executor should be explicit:

Recommended root sink-plan contract (`MVCompleteDeltaApply` style):

1. `OpColID`: child column index of `diff_op`.
2. `MarkerMVOffset`: which MV column is used as the side-missing marker.
3. `GroupKeyMVOffsets`: GROUP BY key offsets in MV column order; sink uses them to skip
   redundant update comparisons on join-equal key columns.
4. `MHandleCols`: physical locator columns built from `M` side (used by `DELETE` and `UPDATE`,
   and intentionally separate from the diff-join key).
5. `MRowInputColIDs` / `QRowInputColIDs`: full old/new row-image mappings in MV column order.

Writable input mappings should be derived in executor from `TargetTable.WritableCols()` plus
`MRowInputColIDs` / `QRowInputColIDs`, instead of being persisted in planner contract. This keeps
complete delta apply aligned with fast-refresh writer ownership.

Per-row operation behavior in sink executor:

1. `diff_op = 1` (`INSERT`): write `Q` row image via `AddRecord`.
2. `diff_op = 2` (`DELETE`): build handle from `MHandleCols`, remove `M` old row via `RemoveRecord`.
3. `diff_op = 3` (`UPDATE`): build handle from `MHandleCols`, update from `M` old row to `Q` new row via `UpdateRecord`.

Current write strategy:

1. Prioritize correctness first: keep sink writer simple and deterministic.
2. `UPDATE` derives precise touched-column sets for non-group-key writable columns in chunk batches.
3. Remaining performance work is about larger-diff memory/spill behavior and broader execution-path validation, not basic writer correctness.

#### Implementation status

Implemented today:

1. Parser/AST support for `COMPLETE DELTA APPLY`, including reject cases for invalid mode combinations.
2. Planner diff-source build with `FULL OUTER JOIN`, diff filter, stable output layout, and explicit sink metadata (`MVCompleteDeltaApply`).
3. Executor builder/runtime that derives writable mappings from target table metadata and applies diff rows via `AddRecord` / `RemoveRecord` / `UpdateRecord`.
4. Refresh framework integration, including advisory lock, history lifecycle, CAS watermark update, rollback-on-error semantics, and observability for `DRY RUN` / `WITH PROFILE`.

Still future work:

1. Larger-diff memory/performance tuning (projection trimming, spill validation, and similar work).
2. TiFlash/MPP path validation when feature switches and pushdown capability allow it.
3. Possible path-selection or cost-based choice among `COMPLETE IN PLACE` / `COMPLETE OUT OF PLACE` / `COMPLETE DELTA APPLY`.

#### Performance notes

- `FULL OUTER JOIN` is chosen because it keeps one-pass diff semantics and a simple correctness model.
- Filtering unchanged rows reduces output/write volume, but does not remove full-join compute cost itself.
- For large tables, memory/spill pressure is expected; keep projection minimal in diff query and rely on spill path correctness.
- If future TiFlash full-join pushdown/MPP is available, this diff-query shape can reuse that capability without changing SQL semantics.

## Test coverage (current implementation)

Current targeted coverage lives mainly in `pkg/executor/test/executor/`, `pkg/planner/core/casetest/mview/`,
and `pkg/planner/mview/`, including:
1. Parser/AST acceptance and rejection for `COMPLETE OUT OF PLACE` / `COMPLETE DELTA APPLY`.
2. Planner contract tests for:
   - `FULL OUTER JOIN` diff-source layout;
   - nullable group-key handling;
   - PK handle / common handle / extra-rowid handle cases.
3. Executor refresh tests for `COMPLETE DELTA APPLY`:
   - manual and internal refresh;
   - insert-only / delete-only / update-only / mixed / no-op cases;
   - `for_update_ts` semantics;
   - rollback-on-error;
   - refresh-history and watermark updates.
4. Out-of-place refresh tests for shadow build / cutover success, CAS mismatch, cleanup, and observability.

Remaining useful additions are mostly around larger-diff performance validation and broader execution-path coverage.

## Known limitations and future direction

- `COMPLETE IN PLACE` (`DELETE FROM mv` + `INSERT INTO mv SELECT ...`) can still create very large transactions for big MVs
  (txn size limits, write amplification, GC pressure).
  `COMPLETE OUT OF PLACE` avoids this path, but further path selection / cost-based choice is still future work.
- History finalization is intentionally after refresh transaction commit, because only then final status is definitive.
  If process crash happens between refresh commit and history finalize update, recovery/reconciliation for
  stale `running` rows is still a future enhancement.
