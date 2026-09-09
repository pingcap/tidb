# `pkg/ddl` MV log-create submission receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
`CreateMaterializedViewLog` submission body and the portable prefix of
`CreateMaterializedView` (master `94a9cbedab`), landed on top of batches
9–13.

## Drift content and Rust alignment

- `CreateMaterializedViewLog` now submits through a durable job plan exactly
  like Go's `DoDDLJobWrapper` route:
  `prepare_materialized_view_job_submission` mirrors
  `prepare_check_constraint_job_submission` — the statement runs Go's
  admission order (`isValidMaterializedViewLogBaseTable`'s shape refusals,
  the partitioned-base refusal, the derived `$mlog$` name collision with
  `ErrTableExists`), builds the log through
  `BuildMaterializedViewLogTableInfo`, assembles the job envelope (job
  version, schema/table names, `ACTION_CREATE_MATERIALIZED_VIEW_LOG`,
  binlog history, query text, SQL mode, CDC write source, the two
  involving-schema entries, the `tidb_scatter_region` system var at its
  default scope, and the typed `CreateMaterializedViewLogArgs`), and runs
  the shared `prepare_submit_batch` preflight (queueing state, BDR role,
  upgrading pause). `plan_insert_attempt` then allocates the global IDs —
  Go's `getRequiredGIDCount`/`assignGIDsForJobs` materialized-view-log arms
  (this batch completes the arms batch 10 recorded as a gap) assign the
  table ID inside the args' `TableInfo` and stamp `Job.TableID` from it —
  and lands the active job row atomically.
- `BuildMaterializedViewLogTableInfo` ported in full:
  `FieldTypeForMaterializedViewLogColumn` (key/auto-increment/on-update flag
  deletion, the 65535 `TypeBlob` length normalized back to unspecified),
  `CheckMaterializedViewLogColumnSupported` (JSON and binary-BLOB 1105
  refusals), the column loop (duplicate 1060, reserved `_MLOG$_DML_TYPE` /
  `_MLOG$_OLD_NEW` names 1060, unknown base column 1054 quoting the written
  base-table name), the two NOT NULL physical columns
  (`_MLOG$_DML_TYPE` VARCHAR(1), `_MLOG$_OLD_NEW` TINYINT(4), charsets left
  to the build exactly as Go's `setCharsetCollationFlenDecimal` fills
  them), the ordinary create-table build path
  (`BuildTableInfoWithStmt` equivalent with the schema charset/collation
  and Go's `ClusteredIndexDefMode` from the build context), and the exact
  `*col.Tp` copy semantics — the build's conversion output is re-stamped
  with the computed field types. `checkTooLongTable` (64 runes) refuses the
  derived identifier with `ErrTooLongIdent` (1059).
- The purge schedule: `PURGE IMMEDIATE` refuses (1105), `DEFERRED` is the
  recorded method, and START WITH / NEXT restore through batch 9's
  `BuildAndValidateMViewScheduleExpr` (a non-temporal NEXT refuses with the
  batch-9 `DATETIME/TIMESTAMP` message; the grammar requires NEXT, matching
  Go's `MLogPurgeClause` production). `BuildMLogAccumulationAlertRows`
  ports verbatim (negative `ALERT ROWS` refuses, `None` when absent).
  `MaterializedViewLogInfo` records the declared columns, purge meta, alert
  threshold, definition SQL mode and the `GetTimeZone` name/offset pair.
- `CreateMaterializedView` advances through Go's portable submission
  prefix: `normalizeMVDefinitionHintDBNames` (every optimizer-hint table
  reference without a schema qualifier is pinned to the view's schema,
  across every nested SELECT), `restoreNodeToCanonicalSQL`
  (DefaultRestoreFlags | RestoreStringWithoutCharset), `buildMViewRefreshMeta`
  (FAST + validated START WITH/NEXT), `parseMViewAttributes` (the
  `mview_alert_*` key grammar, duplicate-key and warning/overdue ordering
  refusals), and `GetTimeZone`. The analysis now returns Go's
  `mviewQueryAnalysis` (per-GROUP-BY select indices, NOT-NULL flags,
  MIN/MAX marker) carried in `MviewCreateJobPrefix`, and the SELECT-coverage
  error now quotes the WRITTEN GROUP BY name like Go's `errors.Errorf`.
- The view create still refuses with
  `materialized view job execution is not wired in this tier` at exactly
  Go's `ExecRestrictedSQL("SELECT * FROM (...) LIMIT 0")` derivation point:
  deriving the view column types needs a SQL-execution seam over a meta
  snapshot that this tier does not have, and a valid statement must not be
  pretended into success. The statement context now rides the
  `DdlStatement` (like the CHECK variants) as the envelope carrier.
- `plan_ddl` refuses log creates with
  `materialized view log DDL must execute through mysql.tidb_ddl_job`,
  matching the CHECK routing: the statement's execution is the job route.

## Known gaps recorded (not fixed in this batch)

- The mlog worker phase (`onCreateMaterializedViewLog`: create the table,
  set the base's `MaterializedViewBase.MLogID`, the
  `mysql.tidb_mlog_purge_info` upsert, schema-version bump and the
  create-table event) and the whole `mview_worker.go` remain the
  DDL-worker-infra batch (reorg build, import-into, session pools).
- `AddMViewExecutionSessionVarsToJob`'s twelve MV-execution session vars and
  `initMaterializedViewReorgMetaFromVariables` have no Rust owner on the
  statement context yet; the job carries only the scatter-region system
  var. They belong to the worker batch they serve.
- The view create's column-type derivation, view `TableInfo` build,
  group-key constraints and `MaterializedViewInfo` metadata wait on the
  same execution seam (see the prefix struct's doc).

## Regression tests

`cluster_ddl_source::materialized_view_log_lowering_follows_go_admission_order`
now drives the submission planner end to end: the plan_ddl routing guard,
the missing-base and derived-name-collision refusals, the
`BuildMaterializedViewLogTableInfo` refusal set (duplicate column, reserved
physical name, unknown column, PURGE IMMEDIATE, non-temporal PURGE NEXT,
negative ALERT ROWS, JSON base column), and the valid statement's full
submission — job type/state/schema/table names, involving schemas, the
scatter-region var, the args' TableInfo (log metadata pointing at the base,
purge method/NEXT, alert rows, declared columns, the flag-deleted `id`
copy, the two typed `_MLOG$_*` columns with the utf8mb4 fill) — followed by
`plan_insert_attempt` assigning job and table IDs and landing the active
job row. The view-create test's seam assertions are unchanged and pass.

Fail-before evidence: before this batch the log create ended at the
planning-tier seam refusal and no submission surface existed; the job-spec
assertions, the args' TableInfo shape and the active-row check bind to
symbols and behavior absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1159 run, 1151 passed, 8 failed — the failure set is byte-identical to the
# pre-batch base (re-verified by running the same eight tests on the stashed
# base tree: identical set). Zero new failures.
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-model
# 321/321 passed (session_vars map key-shape change covered by the job tests).
```

No Go source changed in this batch.
