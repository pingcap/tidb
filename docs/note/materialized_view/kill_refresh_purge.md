# Materialized View Refresh / Purge Kill (Design Notes)

This document describes the design direction for adding kill/cancel capability to
background and manual execution of:

```sql
REFRESH MATERIALIZED VIEW ...
PURGE MATERIALIZED VIEW LOG ON ...
```

The current scope is intentionally narrow:

- support `CANCEL ... JOB <job_id>` first
- use history tables as the cross-node control plane
- make the running task cancel itself locally after observing the cancel request
- propagate an explicit `manual canceled` outcome back to the caller / `mvservice`
- for auto-triggered tasks only, apply a hard-coded 2-minute backoff after manual cancel
- persist the auto-path backoff into `NEXT_TIME`

## Background

Both refresh and purge already have a per-run history row:

- `mysql.tidb_mview_refresh_hist`
- `mysql.tidb_mlog_purge_hist`

Each execution inserts one `running` row, identified by a unique job id:

- refresh: `REFRESH_JOB_ID`
- purge: `PURGE_JOB_ID`

This job id is the natural identifier for "one concrete execution attempt".
It is more precise than table name or MV name, and avoids ambiguity when multiple
runs happen over time.

Another important detail is that automatic MV maintenance is not a single-session
execution model:

1. `mvservice` uses one internal session to issue the internal refresh/purge SQL.
2. The executor then creates additional internal sessions to perform real work
   (for example refresh/purge execution, history update, schedule evaluation, OOP build).

Because of this nested internal-session structure, a pure session-level kill model
is not a natural fit for MV maintenance. A task-level cancel model is a better fit.

## Goals

1. Support canceling one specific running refresh/purge attempt by `job_id`.
2. Support both:
   - auto-triggered refresh/purge from `mvservice`
   - manually executed refresh/purge SQL from user sessions
3. Avoid relying on `SHOW PROCESSLIST` / sys-process registration as the primary
   control mechanism.
4. Avoid introducing cross-node RPC as the first implementation.
5. Preserve existing consistency/finalize behavior:
   - main execution path should stop
   - rollback / finalize / cleanup logic may continue as needed

## Non-goals for the first version

1. No cancel by table name or MV name yet.
2. No configurable backoff after manual cancel yet.
3. No pause/resume job semantics.
4. No attempt to remove ordinary user `KILL QUERY` behavior for manually submitted SQL.

## Why not rely on `SHOW PROCESSLIST` / `KillSysProcess`

TiDB already has a sys-process framework, and `SHOW PROCESSLIST` includes those sys processes.
That works well for some background tasks such as auto analyze.

However, for MV refresh/purge this approach has two drawbacks:

1. It exposes MV maintenance jobs in `SHOW PROCESSLIST`, which is not desirable for this feature.
2. MV maintenance is not naturally "one process == one session", because the real execution path
   nests multiple internal sessions.

For MV refresh/purge, the real cancel target is one logical job attempt, not one internal session.

## Chosen direction

Use the history row of the running job as the cancel control plane.

The high-level idea is:

1. `CANCEL ... JOB <job_id>` updates the matching history row.
2. The running refresh/purge observes the change on its own history row.
3. The running task then cancels itself locally by triggering its own cancel function.
4. The execution path exits with a dedicated `manual canceled` outcome.
5. `mvservice` can recognize that outcome and apply a dedicated auto-path backoff instead of
   ordinary failure retry handling.

This design avoids any direct cross-node routing requirement in the first version:
the history row is already globally visible to all TiDB nodes.

## Why use a dedicated cancel-request field instead of `STATUS='killing'`

Do not overload the lifecycle status column as a control flag.

Using `STATUS='killing'` is problematic because:

1. `running/success/failed` already describes lifecycle state, while "cancel requested"
   is a control signal, not a final state.
2. Finalization logic will eventually overwrite the row with its terminal result anyway.
3. A crash during the middle of cancellation could leave a confusing transient state in history.

Instead, add dedicated cancel-request fields to history tables.

Recommended fields for v1:

- `CANCEL_REQUESTED_AT datetime(6) default null`
- `CANCEL_REQUESTED_BY varchar(...) default null`

Potential future optional fields:

- `CANCEL_REQUEST_REASON text`

`CANCEL_REQUESTED_BY` is needed in v1 because the cancel request may come from another session
or another TiDB node. The running task must be able to reconstruct the requester identity from
the history row when it finalizes the terminal failure reason, for example
`cancelled manually by 'root'@'%'`.

## First-version syntax

The first version only supports cancel by job id.

Suggested syntax:

```sql
CANCEL MATERIALIZED VIEW REFRESH JOB <job_id>;
CANCEL MATERIALIZED VIEW LOG PURGE JOB <job_id>;
```

Possible future extension:

```sql
CANCEL MATERIALIZED VIEW REFRESH <db>.<mv>;
CANCEL MATERIALIZED VIEW LOG PURGE ON <db>.<base_table>;
```

But table-name based cancel is explicitly out of scope for v1.

## Data model changes

Add nullable cancel-request columns to each history table.

### Refresh history

`mysql.tidb_mview_refresh_hist`

- existing key identifier: `REFRESH_JOB_ID`
- new field:
  - `CANCEL_REQUESTED_AT datetime(6) default null`
  - `CANCEL_REQUESTED_BY varchar(...) default null`

### Purge history

`mysql.tidb_mlog_purge_hist`

- existing key identifier: `PURGE_JOB_ID`
- new field:
  - `CANCEL_REQUESTED_AT datetime(6) default null`
  - `CANCEL_REQUESTED_BY varchar(...) default null`

## Cancel request semantics

`CANCEL ... JOB <job_id>` should only mark a running job.

Recommended SQL semantics:

- update only when:
  - target row exists
  - status is `running`
  - `CANCEL_REQUESTED_AT is null`

This makes the operation naturally idempotent.

Pseudo SQL:

```sql
UPDATE mysql.tidb_mview_refresh_hist
   SET CANCEL_REQUESTED_AT = NOW(6),
       CANCEL_REQUESTED_BY = <current_user>
 WHERE REFRESH_JOB_ID = <job_id>
   AND REFRESH_STATUS = 'running'
   AND CANCEL_REQUESTED_AT IS NULL;

UPDATE mysql.tidb_mlog_purge_hist
   SET CANCEL_REQUESTED_AT = NOW(6),
       CANCEL_REQUESTED_BY = <current_user>
 WHERE PURGE_JOB_ID = <job_id>
   AND PURGE_STATUS = 'running'
   AND CANCEL_REQUESTED_AT IS NULL;
```

The statement should not force success if the target job does not exist or is no longer running.
That is a user-visible behavior decision for executor later, but the design intent is:

- "cancel request accepted" only for a live running job
- otherwise return a meaningful "job not running / not found" style error

## Where the watcher should live

The watcher should be implemented in the refresh/purge executor path,
not in `mvservice`.

Reason:

- auto refresh/purge goes through executor
- manual refresh/purge also goes through executor
- history `running` rows are created in executor
- the executor owns the actual long-running execution context that must be canceled

Therefore, placing the watcher in executor naturally covers both auto and manual execution.

Relevant file:

- `pkg/executor/materialized_view.go`

## Local cancel model

The watcher should not try to directly kill one internal session.

Instead:

1. The running refresh/purge owns one local cancelable context.
2. A watcher goroutine polls the corresponding history row by `job_id`.
3. Once `CANCEL_REQUESTED_AT is not null`, the watcher:
   - records local cancel reason as `manual`
   - triggers the local cancel function
4. The main execution path observes context cancellation and exits.

This works better than session-level kill because refresh/purge may use multiple nested internal sessions.
Those sessions are all participating in the same logical job, and the top-level task context is the correct
unit of cancellation.

## Why polling is still needed

It is not enough for the main execution path to check the history row only between steps.

Refresh/purge may spend a long time inside one SQL statement or one large internal phase
(for example large reads, writes, shadow-table build, or checksum-like work).

So v1 needs an asynchronous watcher goroutine:

- it polls independently
- when cancel is requested, it calls local `cancel()`
- the in-flight SQL can then be interrupted through the normal context propagation path

## Execution result semantics

The first version should introduce an explicit "manual canceled" outcome.

Recommended internal representation:

- a dedicated sentinel error, for example `ErrMVTaskCanceledManually`

This sentinel error should be returned from executor when:

1. local cancel reason is known to be `manual`
2. the running job exits due to that cancellation

This is better than letting upper layers guess from plain `context canceled`
or from failure-reason strings written into history.

## Manual SQL behavior

Manual execution should also respond to this cancel mechanism.

That means:

- user submits `REFRESH MATERIALIZED VIEW ...`
- executor inserts running history row
- watcher starts for that `job_id`
- `CANCEL ... JOB <job_id>` marks history row
- watcher sees the request and triggers local cancel
- executor returns `ErrMVTaskCanceledManually`

For manual SQL there is no `mvservice` wrapper around the execution.
So the dedicated result should be returned directly to the user session.

This does not remove ordinary user-driven `KILL QUERY` support.
It only adds a second cancellation path based on job id.

## Auto `mvservice` behavior in v1

For auto-triggered refresh/purge, `mvservice` should recognize the dedicated
`manual canceled` result and treat it specially.

First-version behavior:

- return / propagate `manual canceled` explicitly
- do not treat it as ordinary execution failure for the purpose of retry classification
- apply a hard-coded 2-minute backoff after manual cancel
- persist that backoff into:
  - `mysql.tidb_mview_refresh_info.NEXT_TIME` for refresh
  - `mysql.tidb_mlog_purge_info.NEXT_TIME` for purge
- reschedule the local in-memory task to the same backoff target time

Important implication:

- persisting `NEXT_TIME` is required for correctness of this backoff
- local in-memory reschedule alone is not enough, because a later metadata fetch would otherwise
  rebuild the task from old system-table state and re-make it due too early
- whether the object still has background auto refresh/purge should be judged from the current
  persisted info-row state, using `NEXT_TIME IS NOT NULL` as the v1 signal
- if the current info row already has `NEXT_TIME IS NULL`, `mvservice` must not synthesize a new
  `NEXT_TIME = now + 2 minutes`; in that case the manual-cancel result should be propagated
  without recreating background scheduling state
- the 2-minute delay only applies to the auto path; manual SQL still only returns the
  dedicated `manual canceled` error to the caller

Future versions may add:

- configurable backoff after manual cancel
- `NEXT_TIME` rewrite
- pause semantics

## History final status in v1

This document does not force one final-history representation yet, but the implementation should choose one
of the following explicit models:

1. Preferred long-term model:
   - terminal status `cancelled`
2. Minimal-diff model:
   - terminal status `failed`
   - failure reason clearly indicates `cancelled manually by '<user>'@'<host>'`

Whichever model is chosen, the important part for v1 is:

- a running row must not remain stuck forever after cancel
- final history must clearly show that the run did not complete successfully

## End-to-end flow

### Refresh / purge execution

1. Executor inserts one `running` history row and obtains `job_id`.
2. Executor creates a local cancelable context for this run.
3. Executor starts one watcher goroutine bound to `job_id`.
4. Main execution runs normally.
5. If watcher sees `CANCEL_REQUESTED_AT is not null`:
   - record local cancel reason `manual`
   - call local `cancel()`
6. Main execution exits through normal cancel propagation.
7. Finalize history row with terminal state.
8. Return `ErrMVTaskCanceledManually` if cancel reason is `manual`.

### `CANCEL ... JOB <job_id>`

1. Resolve statement kind:
   - refresh job id
   - purge job id
2. Update matching history row:
   - set `CANCEL_REQUESTED_AT = NOW(6)`
   - only when current row is `running`
3. Return success/failure according to whether a live running row was found.

## Concurrency / race considerations

### Cancel arrives after job already finished

If the job already finalized to `success` or `failed`, the cancel update should not match.
This is fine. The request should simply fail as "job is not running".

### Cancel arrives just before finalize

This is allowed. The final outcome depends on which side wins:

- if main execution has already successfully completed past the effective cancel point,
  the job may still finalize as success
- if cancel is observed in time, the job finalizes as canceled/failed

This is acceptable because the feature is best-effort cancellation of a running attempt,
not a distributed two-phase stop protocol.

### Cancel request remains after local task exits

This is harmless because job id is per-run.
Future runs get a different job id and will not be affected.

## Implementation steps

### Recommended development order (detailed)

The steps below are already ordered by recommended implementation sequence.
This subsection records the intended scope boundary of each stage so development
can proceed incrementally without repeatedly re-deciding cross-layer behavior.

#### Stage A: system-table groundwork

1. Extend both history tables with `CANCEL_REQUESTED_AT` and `CANCEL_REQUESTED_BY`.
2. Cover both fresh bootstrap and upgrade.
3. Do not mix parser/executor behavior changes into this step.

Recommended v1 boundary:

- only schema/bootstrap work
- no watcher
- no new SQL entry yet

#### Stage B: executor cancel primitive

1. Introduce one explicit internal manual-cancel sentinel error.
2. Introduce one local cancel-reason state owned by the running refresh/purge task.
3. Keep the first implementation localized to MV maintenance code instead of trying
   to generalize cancellation infrastructure globally.

Recommended v1 choice:

- use the minimal-diff terminal history model first:
  - keep terminal status as `failed`
  - write a failure reason that clearly indicates `cancelled manually by '<user>'@'<host>'`
- defer any new terminal status such as `cancelled` until the end-to-end path is proven

Reason:

- this keeps the first version focused on kill semantics rather than expanding
  history status contracts across more code paths and tests

#### Stage C: refresh watcher integration

1. Hook the watcher immediately after the refresh `running` history row is inserted.
2. Cover both refresh execution families:
   - `COMPLETE OUT OF PLACE`
   - txn-based refresh path (`COMPLETE IN PLACE` / `COMPLETE DELTA APPLY` / `FAST`)
3. Bind the watcher to the top-level refresh task context, not to one individual
   nested internal session.

Reason:

- refresh may create additional internal sessions, especially in out-of-place mode
- the logical cancel target is one refresh attempt, so the task context is the correct boundary

#### Stage D: purge watcher integration

1. Mirror the same watcher model for purge after the `running` history row is inserted.
2. Reuse as much watcher helper logic as possible with refresh.
3. Keep purge-specific behavior limited to:
   - history table/query shape
   - job id column
   - terminal finalize helper

Reason:

- refresh and purge should not drift into two incompatible cancel semantics

#### Stage E: `mvservice` manual-cancel handling

1. Teach auto refresh/purge helpers to propagate the dedicated manual-cancel result.
2. Teach scheduler result handling to distinguish manual cancel from ordinary execution failure.
3. Apply the hard-coded `2m` backoff only for auto-triggered tasks.
4. Persist that backoff into the corresponding info table only when the object still
   has background scheduling state.

Important v1 rule:

- do not infer "auto scheduling exists" by re-evaluating DDL definition text
- instead, use the current persisted info-row state:
  - refresh: `mysql.tidb_mview_refresh_info.NEXT_TIME IS NOT NULL`
  - purge: `mysql.tidb_mlog_purge_info.NEXT_TIME IS NOT NULL`
- if `NEXT_TIME IS NULL`, manual cancel must not synthesize a new
  `NEXT_TIME = now + 2 minutes`

Reason:

- `NEXT_TIME` is the state that the service already loads to rebuild in-memory tasks
- recreating a non-NULL `NEXT_TIME` when it is already `NULL` would accidentally
  recreate background scheduling for an object that currently has none

#### Stage F: user-facing `CANCEL ... JOB <job_id>` entry

1. Add parser / AST support only for cancel-by-job-id.
2. Route to a utility-style executor path that updates the matching history row.
3. Accept only live running jobs.

Recommended SQL semantics:

- update target row only when:
  - matching `JOB_ID` exists
  - status is `running`
  - `CANCEL_REQUESTED_AT IS NULL`
- if no row matches, return a clear user-visible error rather than silent success

#### Stage G: focused validation

1. Add manual refresh cancel coverage.
2. Add manual purge cancel coverage.
3. Add auto refresh cancel coverage.
4. Add auto purge cancel coverage.
5. Verify final history rows do not remain `running`.
6. Verify `mvservice` does not recreate background scheduling state when the current
   info row already has `NEXT_TIME IS NULL`.

Suggested execution style:

- use existing failpoints right after `running` history insertion
- issue `CANCEL ... JOB <job_id>`
- assert both task exit and final persisted metadata behavior

### Step 1: extend history tables

Goal:

1. Add cancel-request column to refresh/purge history tables.
2. Add bootstrap / upgrade coverage.

Main files:

- `pkg/session/bootstrap.go`
- `pkg/session/bootstraptest/...`

Acceptance criteria:

1. Both history tables contain `CANCEL_REQUESTED_AT` and `CANCEL_REQUESTED_BY`.
2. Fresh bootstrap and upgrade path are covered.

### Step 2: define cancel sentinel / local cancel reason

Goal:

1. Introduce an explicit internal result for manual cancel.
2. Avoid string-based detection.

Main files:

- `pkg/executor/materialized_view.go`
- possibly shared error/helper file if needed

Acceptance criteria:

1. Executor can distinguish manual cancel from ordinary context cancellation.

### Step 3: add refresh watcher

Goal:

1. For each refresh job, start a watcher after the running history row is inserted.
2. Watcher polls the refresh-history row by `REFRESH_JOB_ID`.
3. On cancel request, watcher triggers local cancel.

Main files:

- `pkg/executor/materialized_view.go`

Acceptance criteria:

1. Manual refresh can be canceled by `job_id`.
2. Auto refresh can be canceled by `job_id`.

### Step 4: add purge watcher

Goal:

1. Mirror the same watcher model for purge.

Main files:

- `pkg/executor/materialized_view.go`

Acceptance criteria:

1. Manual purge can be canceled by `job_id`.
2. Auto purge can be canceled by `job_id`.

### Step 5: propagate `manual canceled` to `mvservice`

Goal:

1. Make auto execution return a dedicated manual-cancel result.
2. Let `mvservice` recognize it separately from ordinary failure.
3. Apply a dedicated 2-minute backoff for the auto path.
4. Persist the backoff into `NEXT_TIME` so that future metadata fetch does not override it.

Main files:

- `pkg/mvservice/task_handler.go`
- `pkg/mvservice/service_helper.go`
- `pkg/mvservice/service.go`

Acceptance criteria:

1. `mvservice` can identify manually canceled refresh/purge.
2. Auto path writes `NEXT_TIME = now + 2 minutes` after manual cancel only when the current info row
   still has `NEXT_TIME IS NOT NULL`.
3. If the current info row already has `NEXT_TIME IS NULL`, manual cancel does not recreate
   background scheduling state.
4. Local in-memory reschedule matches the persisted `NEXT_TIME` when that backoff is written.

### Step 6: add `CANCEL ... JOB <job_id>` statement

Goal:

1. Introduce user-facing SQL entry for cancel-by-job-id.
2. Update the matching history table row.

Main files:

- parser / AST files
- planner utility path
- executor utility path

Acceptance criteria:

1. `CANCEL MATERIALIZED VIEW REFRESH JOB <job_id>` works.
2. `CANCEL MATERIALIZED VIEW LOG PURGE JOB <job_id>` works.
3. Statement only accepts live running jobs.

### Step 7: targeted tests

Goal:

1. Cover manual refresh cancel.
2. Cover manual purge cancel.
3. Cover auto refresh cancel.
4. Cover auto purge cancel.
5. Verify final history state is not left as `running`.

Suggested test style:

- use failpoints to pause execution after history `running` row is inserted
- issue `CANCEL ... JOB <job_id>`
- assert:
  - executor / statement exits
  - final history row is terminal, not `running`
  - auto path returns dedicated manual-cancel outcome to `mvservice`

## Future work

1. Support cancel by table name / MV name.
2. Make the backoff after manual cancel configurable.
3. Tune or redesign `NEXT_TIME` handling around manual cancel if needed.
4. Consider terminal status `cancelled` if v1 starts with `failed + reason`.
5. Improve observability around cancel requester / cancel timestamp.
