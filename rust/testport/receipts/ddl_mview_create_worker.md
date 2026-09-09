# `pkg/ddl` MV view-create worker phase-1 receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope:
`onCreateMaterializedView`'s `StateNone` phase and the rollback path over
the batch-16 submitted view job, with the `mysql.tidb_mview_refresh_info`
prewrite storage.

## Drift content and Rust alignment

- New `tidb-exec::mview_refresh_info_table` module: the storage half of
  `prewriteCreateMaterializedViewRefreshInfo` /
  `upsertCreateMaterializedViewRefreshInfo` /
  `deleteCreateMaterializedViewRefreshInfo` over the clustered `MVIEW_ID`
  primary key. The `should_update = false` shape writes the read TSO and
  leaves the deadlines SQL NULL (its `ON DUPLICATE KEY UPDATE` arm rewrites
  the read TSO), the full shape records every column, and deletion is a
  no-op when the row is absent.
- `plan_persisted_materialized_view_create_job_step` ports the worker over
  the persisted active row:
  - Go's argument validation cancels the job on nil/empty/zero/duplicate
    base table IDs (`create materialized view: invalid job args / invalid
    base table id / duplicate base table id`);
  - `onCreateMaterializedViewBaseCheck` per base re-runs at execution time —
    missing base, view/sequence/temporary shape (`ErrWrongObject`),
    partitioned base, non-public state (`ErrInvalidDDLState`), missing
    `MaterializedViewBase.MLogID` and invalid log metadata (both
    `ErrInvalidDDLJob`), non-public log — each cancelling exactly as Go's
    `job.State = Cancelled` returns do;
  - the `StateNone` arm lands the submitted view `TableInfo` PUBLIC at the
    transaction timestamp through `createTable`, records the view ID in
    every base's `MaterializedViewBase.MViewIDs`
    (`updateMaterializedViewBaseInfoOnCreate`, duplicates skipped), bumps
    the schema version with the `ACTION_CREATE_MATERIALIZED_VIEW` diff,
    appends the create-table change event, and prewrites the refresh-info
    row `(view, read_ts = start_ts, NULL, NULL)`;
  - the phase transitions the persisted job to
    `Running`/`StateWriteReorganization` as a NON-terminal step — Go hands
    the job to the reorg build here, and so does this plan.
- `StateWriteReorganization` is the recorded seam: Go's initial build moves
  the base rows in through import-into or insert-select at the build read
  TS (`buildCreateMaterializedViewData`) inside `runReorgJob`; that
  data-movement engine is not ported, so the tick refuses with a retryable
  error and leaves the queued job exactly where Go's own
  `ErrWaitReorgTimeout` tick would — `Running` at
  `StateWriteReorganization`, untouched and resumable.
- `rollbackCreateMaterializedView` ports over the persisted row: the
  created view (if the phase committed) drops with its three auto-ID
  allocators, every base's `MViewIDs` loses the view with the now-empty
  metadata removed (Go's `updateMaterializedViewBaseInfoOnDrop` view arm),
  the refresh-info row is deleted, and the job ends
  `RollbackDone`/`StateNone`.

## Known gaps recorded (not fixed in this batch)

- The `StateWriteReorganization` data build is the standing gap: it needs
  the insert-select / import-into execution engine at the build read TS
  plus `upsertCreateMaterializedViewRefreshInfo`'s post-build derivation
  (read TS, refresh end, next deadline) and the `InitBuildState = Ready`
  update, before the phase can finish the job Go-style.
- The session-side schedule evaluator (`MlogPurgeDerived` parameter) and the
  twelve MV-execution session vars remain the batch-15/16 recorded seams.

## Regression tests

`cluster_ddl_source::persisted_materialized_view_create_step_runs_phase_one_and_rolls_back`
drives the loop end to end over real submissions: the log create executes
to terminal first (batch 15), then the view submission; phase 1 is
non-terminal, creates the view PUBLIC with its metadata, stamps the base's
`MViewIDs` alongside its `MLogID`, prewrites the refresh row at the phase
TS, and leaves the job `Running`/`WriteReorganization`; the phase-2 tick
refuses at the recorded seam with the job untouched; persisting Go's
`Rollingback` transition makes the step drop the view, clear the base's
view reference (keeping the log), delete the refresh row, and land
`ROLLBACK_DONE` history.

Fail-before evidence: before this batch the submitted view job sat in the
queue forever with no catalog effect — no step planner, no phase
transition, and no refresh-info row storage existed.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1161 run, 1154 passed, 7 failed — the same seven as the batch-15/16 base.
# Zero new failures.
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-model -p tidb-ddl-session -p tidb-metadef -p tidb-executor --no-fail-fast
# 1732 run: 166 failed = the executor's base-identical 165 plus the
# pre-existing metadef string-constant baseline mismatch. Zero new.
```

No Go source changed in this batch.
