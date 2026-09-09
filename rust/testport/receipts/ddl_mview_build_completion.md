# `pkg/ddl` MV view-create build-completion receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
view create's `StateWriteReorganization` completion — the post-build
refresh-info upsert, `InitBuildState = Ready`, and the terminal
`FinishMultipleTableJob` — with the build execution remaining the caller's
standing reorg-infra seam.

## Drift content and Rust alignment

- The shared schedule-derivation core moves to the new
  `tidb-exec::mview_schedule_derive` module:
  `derive_schedule_decision` walks Go's
  `deriveCreateMaterializedScheduleNextUnixSeconds` tree (NOW(6) load, the
  ten-second near-now threshold, START WITH precedence, NULL degradation to
  the `(None, true)` insert-ignore shape with the caller's log function)
  through the driver's FROM-less SELECT under the recorded SQL mode and
  schedule zone. `MlogPurgeDerived::derive` now delegates to it — batch
  18's purge behaviour is unchanged by construction and by test.
- The view create's `StateWriteReorganization` arm takes the finished
  build's read TS (`MviewBuildOutcome { read_ts }` — Go's
  `job.SnapshotVer`):
  - without it, the tick refuses retryably exactly as before (the queued
    job stays `Running`/`StateWriteReorganization`, where Go's own
    `ErrWaitReorgTimeout` tick would leave it);
  - with it, the completion transaction records the refresh-info row in
    the full `should_update` shape — `(MVIEW_ID, read_ts,
    LAST_SUCCESS_REFRESH_END = the owner's wall clock,
    NEXT_REFRESH_UNIX_SECONDS = the view's REFRESH schedule derived through
    the shared tree)`, `InitBuildState = Ready` written back onto the view
    `TableInfo`, the schema-version bump with the
    `ACTION_CREATE_MATERIALIZED_VIEW` diff, and
    `FinishMultipleTableJob(Done, Public, [bases.., mview])` moving the job
    to both history stores. The view's REFRESH schedule derives through the
    same shared tree with Go's
    `logCreateMaterializedViewNextUnixSecondsUpdateNull` view-arm logger.

## Known gaps recorded (not fixed in this batch)

- The data-movement execution itself (import-into / insert-select at the
  build read TS) remains the caller-side reorg-infra seam: a caller must
  run the build and hand its read TS in. Without that execution the
  completion contract is not satisfiable end to end.
- `AddMViewExecutionSessionVarsToJob`'s twelve variables are the default
  session's (batch 19's documented reduction).

## Regression tests

`persisted_materialized_view_create_step_runs_phase_one_and_rolls_back`
extends: the phase-2 tick without the build outcome still refuses; with the
outcome the completion transaction is terminal, and the DONE history row's
`multiple_table_infos` carries `[mv_base, mv]`. A second view on another
base runs phase 1 only (non-terminal), then the persisted `Rollingback`
transition undoes phase 1 — the created view drops, the base's `MViewIDs`
clears (its log reference survives), the refresh row is deleted, and the
job ends `ROLLBACK_DONE`. Fail-before evidence: before this batch the
WriteReorganization arm unconditionally refused and no completion path
existed.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1162 run, 1153 passed, 7 failed — the exact base failure set. Zero new.
```

No Go source changed in this batch.
