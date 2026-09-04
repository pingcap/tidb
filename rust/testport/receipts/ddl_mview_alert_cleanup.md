# `pkg/ddl` MV refresh-alert rollback cleanup receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
create-rollback's alert-row cleanup — `deleteCreateMaterializedViewRefreshAlert`
via `buildDeleteMViewRefreshAlertSQL`.

## Drift content and Rust alignment

- New `tidb-exec::mview_alert_table` storage over
  `mysql.tidb_mview_refresh_alert`'s clustered `MVIEW_ID` primary key
  (locate / find / delete), mirroring the purge-info and refresh-info
  modules.
- `plan_rollback_materialized_view_create_step` now removes the view's
  alert row in the rollback transaction, exactly as Go's
  `deleteCreateMaterializedViewRefreshAlert` does after the refresh-info
  delete. A missing row appends nothing (Go's SQL DELETE affects nothing);
  a missing system table is tolerated, matching Go's
  `ErrTableNotExists` swallowing.

## Known gaps recorded (not fixed in this batch)

- Alert rows are only WRITTEN by refresh workers (not ported); on the
  create path the delete is the faithful no-op it is in Go.

## Regression tests

`persisted_materialized_view_create_step_runs_phase_one_and_rolls_back`
exercises the rollback end to end; the alert delete rides the same
transaction and is a verified no-op on the create path (no alert row
exists).

## Validation

Profile: **WIP** for this slice (the full Ready gate ran on batch 19's
identical failure surface).

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast -E 'test(persisted_materialized_view_create) \
  + test(persisted_materialized_view_log) + test(materialized_view)'
# 10 run, 10 passed.
```

No Go source changed in this batch.
