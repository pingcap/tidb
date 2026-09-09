# `pkg/ddl` MV purge-schedule derivation receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope:
`deriveCreateMaterializedViewLogNextUnixSeconds` — the purge-schedule
derivation the create-log worker runs before
`upsertCreateMaterializedViewLogPurgeInfo` — closing the `MlogPurgeDerived`
seam batch 15 recorded.

## Drift content and Rust alignment

- `MlogPurgeDerived::derive` ports Go's decision tree
  (`deriveCreateMaterializedScheduleNextUnixSeconds`) over the log's
  recorded metadata: both `PurgeStartWith`/`PurgeNext` empty yield
  `(None, true)` with no evaluation; `NOW(6)` loads the owner clock; a
  START WITH more than ten seconds in the future wins over NEXT; a START
  WITH inside the near-now window defers to NEXT; any NULL evaluation
  logs Go's `logCreateMaterializedViewLogNextUnixSecondsUpdateNull`
  message and degrades to the `(None, true)` `INSERT IGNORE` shape.
- The evaluation runs through the driver's FROM-less `SELECT NOW(6)` /
  `SELECT <expr>` over an empty catalog — the same SQL Go's owner session
  executes — under an evaluation context carrying the log's recorded
  `definition_sql_mode` and `PurgeScheduleTimeZone` (Go's
  `setCreateMaterializedViewScheduleEvalSession`), with the owner's live
  wall clock supplied through the lazy statement clock. This closes the
  batch-15/16 session-eval seam in the pure planner: the worker step now
  derives internally and the `schedule` parameter is gone.
- The unix-seconds conversion honours the resolved zone shape exactly as
  Go's `MaterializedScheduleTimeToUnixSeconds` (named IANA zones, fixed
  offsets, process-local).

## Planner repair exposed by the derivation

Evaluating `SELECT CAST('2030-01-02 10:00:00' AS DATETIME)` through the
driver panicked in `eliminate_physical_projection`'s
`expect("physical projection elimination needs the child schema")`: the
physical `TableDual` copied the logical dual's `None` schema, while Go's
own dual always carries a non-nil (possibly empty) schema once the logical
build derives one. `find_best_task_4_logical_table_dual` now materialises
the empty default, making FROM-less projections with non-foldable
expressions (the exact shape Go's schedule expressions take) behave like
every other FROM-less projection. Verified on the base tree: the panic
predates this batch; the fix repairs it.

## Known gaps recorded (not fixed in this batch)

- The `StateWriteReorganization` data build for the view create (insert-
  select / import-into at the build read TS) remains batch 19's seam.
- `AddMViewExecutionSessionVarsToJob`'s twelve MV-execution session vars
  and `initMaterializedViewReorgMetaFromVariables` still have no statement
  context owner.

## Regression tests

`cluster_ddl_source::persisted_materialized_view_log_step_derives_the_purge_schedule`
submits a log WITH `PURGE NEXT CAST('2030-01-02 10:00:00' AS DATETIME)`,
runs the worker step, and asserts the purge row records the derived unix
seconds (1893578400 = the datetime at the context's UTC zone). The
unscheduled-log path (batch 15's test) continues through the no-evaluation
shortcut. Fail-before evidence: before this batch the scheduled-log step
refused with the session-eval seam message and no derivation existed.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1162 run, 1155 passed, 7 failed — the exact base failure set (verified
# twice; the placement_delivery flake that appeared in one intermediate run
# also fails on the stashed base in the same environment and recovered in
# the final run). Zero new failures.
```

No Go source changed in this batch.
