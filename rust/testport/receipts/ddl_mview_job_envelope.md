# `pkg/ddl` MV job-envelope metadata receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
view-create job envelope's remaining Go fields —
`initMaterializedViewReorgMetaFromVariables` and
`AddMViewExecutionSessionVarsToJob` — closing the batch-14/16/17 recorded
envelope gap.

## Drift content and Rust alignment

- `init_materialized_view_reorg_meta` ports Go's
  `NewDDLReorgMeta` + `initMaterializedViewReorgMetaFromVariables`: the
  empty warning maps Go allocates eagerly, the recorded SQL mode, the
  `GetTimeZone` location, the statement's resource group, the current
  metadata version, the `use_new_collate` flag, and
  `SetConcurrency(DefTiDBDDLReorgWorkerCount)` /
  `SetBatchSize(DefTiDBDDLReorgBatchSize)` /
  `SetMaxWriteSpeed(DefTiDBDDLReorgMaxWriteSpeed)`. `tidb-model` gains the
  matching `DDLReorgMeta::new` constructor (the model's atomics are
  private to the crate). The view job carries the metadata exactly where
  Go attaches it; the log job does not.
- `add_mview_execution_session_vars_to_job` ports Go's
  `AddMViewExecutionSessionVarsToJob`: all twelve MV-execution session
  variables land in the job envelope with Go's exact formatting
  (`strconv.FormatInt` for the integers, `FormatFloat('f', -1, 64)` for
  the spill ratio). The values are the default session's — the statement
  context carries no session-variable image, the same documented
  reduction as the scatter-region var; a session with non-default
  maintenance variables remains the standing gap.
- The derived view spec test now pins the whole envelope: the reorg
  concurrency/batch-size/SQL-mode/zone fields and the thirteen job system
  variables (the scatter region plus the twelve MV-execution vars),
  including `tidb_mview_maintain_mem_quota = 2147483648`,
  `tidb_max_tiflash_threads = -1`, and `tiflash_query_spill_ratio = 0.7`.

## Known gaps recorded (not fixed in this batch)

- The `StateWriteReorganization` data build remains batch 19's standing
  seam (import-into / insert-select at the build read TS, the post-build
  refresh-info upsert with `InitBuildState = Ready`, and the terminal
  `FinishMultipleTableJob`).
- A session with non-default maintenance/reorg variables: the envelope
  records the default session's values until the statement context grows a
  session-variable image.

## Regression tests

`materialized_view_lowering_follows_go_admission_order`'s submitted-spec
assertions extend to the new envelope fields: the reorg metadata's
concurrency, batch size, SQL mode and UTC location, and the job system
variable count with spot-checked MV-execution values.

Fail-before evidence: before this batch the view job carried no
`ReorgMeta` and only the scatter-region system variable; both assertions
bound to behavior absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1162 run, 1154 passed, 8 failed — the base seven plus the
# placement_delivery POST fixture flake, which fails and passes across runs
# on this environment and fails identically on the stashed base tree
# (verified this session). Zero new failures.
```

No Go source changed in this batch.
