# `pkg/ddl` MV log-create worker-step receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
worker half of `CreateMaterializedViewLog` — `onCreateMaterializedViewLog`
plus its rollback — over the batch-14 submitted job, with the materialized-view
bootstrap system tables the worker writes.

## Drift content and Rust alignment

- Go master's fresh-cluster bootstrap creates the masking-policy and
  materialized-view `mysql.*` groups
  (`systemTablesOfMaskingPolicyNextGenVersion`,
  `systemTablesOfMaterializedViewNextGenVersion`). `BOOTSTRAP_TABLES` now
  carries all six missing tables — `tidb_masking_policy`,
  `tidb_mview_refresh_info`, `tidb_mlog_purge_info`,
  `tidb_mview_refresh_hist`, `tidb_mview_refresh_alert`,
  `tidb_mlog_purge_hist` — at Go's own reserved IDs, so the purge-schedule
  storage the worker writes exists exactly where a real cluster has it.
- New `tidb-exec::mlog_purge_info_table` module: the storage half of
  `upsertCreateMaterializedViewLogPurgeInfo` /
  `deleteMaterializedViewLogPurgeInfo` over the clustered `MLOG_ID` primary
  key — `should_update` writes (or rewrites) `NEXT_PURGE_UNIX_SECONDS`, the
  `INSERT IGNORE` form records only the log ID and leaves an existing row
  untouched, and deletion is a no-op when the row is absent.
- `plan_persisted_materialized_view_log_job_step` ports
  `onCreateMaterializedViewLog` as one owner transaction over the persisted
  active row (the CHECK-step contract): re-decode the typed arguments
  (nil args/metadata cancel the job), re-run Go's execution-time base checks
  (shape, partition, non-public state, an existing `MLogID` refusing with
  `ErrTableExists`), land the submitted `TableInfo` PUBLIC at the
  transaction timestamp exactly as Go's `createTable` does, set the base
  table's `MaterializedViewBase.MLogID` (Go's
  `updateMaterializedViewBaseInfoOnCreate` log arm, including the
  "already has a materialized view log" refusal), upsert the purge row
  (Go's `(None, true)` derivation for a log without a schedule, with no
  session touched), bump the schema version with the
  `ACTION_CREATE_MATERIALIZED_VIEW_LOG` diff, append the create-table
  change event, and finish the job with Go's
  `FinishMultipleTableJob(Done, Public, [base, mlog])` before moving it to
  both history stores. Execution-time check failures cancel the job and
  move only the terminal rows, exactly as Go's `job.State = Cancelled`
  returns do.
- `plan_rollback_materialized_view_log_step` ports
  `rollbackCreateMaterializedViewLog`: the created table (if the phase
  committed) drops with its three auto-ID allocators, the base's `MLogID`
  clears and the now-empty base metadata is removed (Go's
  `updateMaterializedViewBaseInfoOnDrop` log arm), the purge row is
  deleted, and the job ends `ROLLBACK_DONE`/`StateNone`.

## Known gaps recorded (not fixed in this batch)

- A log that DOES name a purge schedule needs
  `deriveCreateMaterializedViewLogNextUnixSeconds`'s SQL evaluation of the
  schedule expression on the owner's session. The step planner takes that
  derivation as the `MlogPurgeDerived` parameter and refuses with a
  retryable plan error when it is missing; the session-side evaluator is the
  remaining seam (batch 8 moved its first half to `tidb-ddl-session`).
- `onCreateMaterializedView` (the view create's two-phase worker with the
  reorg build), `rollbackCreateMaterializedView`, and
  `prewriteCreateMaterializedViewRefreshInfo` remain the DDL-worker batch,
  behind the view create's restricted-SQL derivation seam.

## Regression tests

`cluster_ddl_source::persisted_materialized_view_log_step_creates_the_log_and_rolls_back`
drives the full worker loop over the batch-14 submission: the owner step is
terminal in one phase, creates the `$mlog$` table PUBLIC with its log
metadata, stamps the base's `MLogID`, records the NULL-deadline purge row,
bumps the schema version, empties the active queue, and lands the DONE
history row whose `multiple_table_infos` carries `[base, $mlog$]`; the
second half submits another log create, persists the `Rollingback`
transition, and asserts the rollback leaves the catalog untouched and ends
`ROLLBACK_DONE`.

Fail-before evidence: before this batch the submitted job row sat in
`mysql.tidb_ddl_job` forever — no step planner, no purge storage, and the
bootstrap did not even create `mysql.tidb_mlog_purge_info`. Every assertion
above binds to behavior absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1160 run, 1153 passed, 7 failed — the base failure set MINUS
# mysql_bootstrap_source::a_bootstrap_spends_exactly_one_schema_version_and_
# describes_it, which this batch genuinely repairs (its stale table-count
# assertion predates both the notifier batch and these tables; the test now
# counts both bootstrap lists). Zero new failures.
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-executor --no-fail-fast
# 165 failed — byte-identical to the stashed-base set (verified this session).
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-session --no-fail-fast
# 690 failed — the extracted failure-name set is identical to the base run
# (verified this session).
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-model -p tidb-ddl-session -p tidb-metadef
# model + ddl-session 326/326; metadef 16/17 with the pre-existing
# every_public_string_constant baseline mismatch (fails identically on the
# base tree — verified this session).
```

No Go source changed in this batch.
