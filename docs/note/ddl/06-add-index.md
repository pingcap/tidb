# Add Index Deep Dive (online DDL + reorg/backfill)

This note focuses on **`ActionAddIndex`** (and closely related `ActionAddPrimaryKey`). Add index is special because it’s a **reorg/backfill** DDL: it needs a schema state machine + background data backfill while DML continues.

## Entry points (start here)

SQL → DDL module:

- SQL dispatch: `pkg/executor/ddl.go` (`type DDLExec`, `(*DDLExec).Next`)
- Statement → job: `pkg/ddl/executor.go:CreateIndex`, `pkg/ddl/executor.go:createIndex`

Job creation (what’s persisted):

- Job skeleton: `pkg/ddl/executor.go:buildAddIndexJobWithoutTypeAndArgs`
- Job version: `job.Version = model.GetJobVerInUse()` (inside `createIndex`)
- Job type: `job.Type = model.ActionAddIndex` (inside `createIndex`)
- Reorg meta init: `pkg/ddl/reorg_util.go:initJobReorgMetaFromVariables`
- Args (v2 typed job args): `model.ModifyIndexArgs` with `OpType = model.OpAddIndex` (inside `createIndex`)

## Job args and features (global/partial/expression/presplit)

Add index is a “normal” DDL action in terms of SQL surface, but the job args carry a lot of complexity:

- Global index validation: `pkg/ddl/executor.go:checkCreateGlobalIndex`
- Partial index predicate:
  - Build: `pkg/ddl/executor.go:CheckAndBuildIndexConditionString`
  - Apply: `pkg/ddl/index.go:onCreateIndex` (restores `ConditionExprString` on the built `IndexInfo`)
- Expression / generated-key indexes may introduce hidden columns:
  - Build: `pkg/ddl/executor.go:checkIndexNameAndColumns`
  - Publish: `pkg/ddl/index.go:moveAndUpdateHiddenColumnsToPublic` (done when moving `StateNone` → `StateDeleteOnly`)
- Pre-split index regions (optional `SPLIT` clause on index option):
  - Parse: `pkg/ddl/executor.go:buildIndexPresplitOpt`
  - Execute: `pkg/ddl/index_presplit.go:preSplitIndexRegions`

Owner scheduling + worker:

- Reorg worker pool: `pkg/ddl/job_scheduler.go` (routes `mysql.tidb_ddl_job.reorg=1` jobs to `reorgWorkerPool`)
- Worker type: `pkg/ddl/job_worker.go` (`addIdxWorker`)
- Add-index handler: `pkg/ddl/index.go:onCreateIndex`

The `reorg` routing flag is decided at submission time:

- Insert path: `pkg/ddl/job_submitter.go:insertDDLJobs2Table` (`jobW.MayNeedReorg()` → `mysql.tidb_ddl_job.reorg`)

## State machine: index visibility (online DDL)

The add-index job drives an **index state** and a corresponding **job schema state**. The core transitions happen in `pkg/ddl/index.go:onCreateIndex`:

- `StateNone` → `StateDeleteOnly`
- `StateDeleteOnly` → `StateWriteOnly`
- `StateWriteOnly` → `StateWriteReorganization` (reorg/backfill)
- `StateWriteReorganization` → `StatePublic`

The job records the corresponding “schema state” on `job.SchemaState` (e.g. `StateDeleteOnly`, `StateWriteOnly`, `StateWriteReorganization`) so other parts of the framework can reason about compatibility windows.

At each boundary, the worker updates table metadata and schema version so all nodes observe the same compatibility window before the next step.

## Reorg stage = backfill + optional ANALYZE

`StateWriteReorganization` is not “just backfill”. In `pkg/ddl/index.go:onCreateIndex`, the worker:

1. Runs reorg/backfill until done: `pkg/ddl/index.go:doReorgWorkForCreateIndex`
2. Decides whether to run `ANALYZE` (based on job vars and job context):
   - State machine: `job.ReorgMeta.AnalyzeState` (see `pkg/ddl/index.go:onCreateIndex`)
   - Execution: `pkg/ddl/index.go:startAnalyzeAndWait`

Only after analyze is **done/skipped/timeout/failed** does the worker finalize the index to `StatePublic` and finish the job.

## Reorg meta: what controls backfill mode

Before the job is submitted, add index initializes `job.ReorgMeta` from global/session variables:

- Implementation: `pkg/ddl/reorg_util.go:initJobReorgMetaFromVariables`
- Key toggles:
  - `tidb_ddl_enable_fast_reorg` (`vardef.EnableFastReorg`)
  - `tidb_enable_dist_task` (`vardef.EnableDistTask`) — **requires** fast reorg; otherwise the job rejects (`ErrUnsupportedDistTask`)

`job.ReorgMeta` also carries persisted reorg parameters (concurrency, batch size, max write speed) and (for dist-task) target scope / max node count.

## Choosing a backfill process (txn vs txn-merge vs ingest)

The reorg/backfill implementation picks a reorg type once and persists it in `job.ReorgMeta.ReorgTp`:

- Selection logic: `pkg/ddl/index.go:pickBackfillType`

High level:

- If fast reorg is **disabled** → `ReorgTypeTxn` (traditional transactional backfill)
- If fast reorg is **enabled**:
  - If ingest/Lightning environment is available → `ReorgTypeIngest`
  - Else → fallback to `ReorgTypeTxnMerge` (still a fast-reorg pipeline, without Lightning)

The reorg loop itself is coordinated from `pkg/ddl/index.go:doReorgWorkForCreateIndex`.

## Backfill-merge process (temporary index + BackfillState)

Fast-reorg pipelines use a **backfill-merge** state machine persisted on the index (`IndexInfo.BackfillState`):

- Definition and semantics: `pkg/meta/model/reorg.go:BackfillState`
- Driver: `pkg/ddl/index.go:doReorgWorkForCreateIndex`

States:

- `BackfillStateRunning`: backfill is running; writes/deletes are redirected to (or maintained in) a *temporary index*.
- `BackfillStateReadyToMerge`: temp index is ready; **publish this state** so all TiDB nodes start copying writes/deletes for merge safety.
- `BackfillStateMerging`: merge temp index records back to the origin index.
- `BackfillStateInapplicable`: exit the backfill-merge process (and prevent double-write on the temp path).

This state machine exists to keep correctness under: cross-node schema propagation, online writes during backfill, retries, and owner transfer.

## Persistence: where “progress” lives

For resumability (owner transfer/retry), progress must be persisted. Add index uses:

- Job record: `mysql.tidb_ddl_job` (`job_meta`, `reorg`, `processing`)
- Reorg handle table: `mysql.tidb_ddl_reorg` (range + element + reorg meta)
  - Access helpers: `pkg/ddl/job_scheduler.go:getDDLReorgHandle`, `pkg/ddl/job_scheduler.go:initDDLReorgHandle`, `pkg/ddl/job_scheduler.go:deleteDDLReorgHandle`

If you change what’s stored for reorg/backfill, ensure it’s:

- encoded/decoded compatibly across job versions,
- written transactionally with the step’s meta updates,
- sufficient to continue without re-scanning from zero.

## Failure, cancellation, rollback (what to keep invariant)

Add index must be safe under partial failure and retries:

- A job step may be re-run (owner transfer, retryable errors). Treat each step as **idempotent**.
- If backfill fails with non-retryable error (including duplicate key in ingest), the code may convert to rollback:
  - See rollback conversion usage in `pkg/ddl/index.go` (e.g. `convertAddIdxJob2RollbackJob` call sites).

When adding new fast-reorg/ingest behavior, also verify rollback paths don’t leak temp index artifacts and that `BackfillState` transitions remain monotonic and persisted.

## Practical debugging anchors

SQL:

- `ADMIN SHOW DDL JOBS`
- `ADMIN SHOW DDL JOB QUERIES`
- `ADMIN CANCEL DDL JOBS <job_id>`

Tables:

- `mysql.tidb_ddl_job` (queue)
- `mysql.tidb_ddl_history` (history)
- `mysql.tidb_ddl_reorg` (reorg handle/progress)

Code:

- Job creation and args: `pkg/ddl/executor.go:createIndex`
- State transitions: `pkg/ddl/index.go:onCreateIndex`
- Reorg/backfill loop: `pkg/ddl/index.go:doReorgWorkForCreateIndex`
- Ingest backend: `pkg/ddl/ingest/*` (see also `docs/design/2022-06-07-adding-index-acceleration.md`)

## Common pitfalls checklist (before you send a PR)

- Backfill progress or `BackfillState` not persisted → reorg restarts or double-writes after retry.
- Missed schema sync boundary when transitioning index states → nodes observe incompatible DML rules.
- Job args incompatibility across versions (v1/v2) → existing jobs can’t be decoded after upgrade/downgrade.
- Fast reorg + partial index: code rejects partial indexes without fast reorg; keep the invariant (`pickBackfillType` + checks in `onCreateIndex` / `initForReorgIndexes`).
