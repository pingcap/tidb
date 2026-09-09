# `pkg/ddl` MV remaining pure-surface receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
last unported pure helpers of `pkg/ddl/materialized_view.go` and
`pkg/sessionctx` — `MViewExecutionSessionVarsFromJob`,
`BuildMViewImportIntoOptions`, and `buildMLogPurgeMeta` — completing the
pure surface of the MV port.

## Drift content and Rust alignment

- `m_view_execution_session_vars_from_job` (tidb-session, Go
  `MViewExecutionSessionVarsFromJob` at `materialized_view.go:285`):
  reconstructs the twelve MV-execution variables from a job's
  system-variable envelope, falling back per field to the default session's
  captured values (`capture_applied_m_view_execution_session_vars` over the
  caller's `SessionVars`); a nil job yields the captured defaults
  untouched. Each numeric read parses with Go's `TidbOptInt64`-style
  fallback (a malformed value keeps the default), the spill ratio parses as
  f64, and the two string fields copy verbatim.
- `build_m_view_import_into_options` (tidb-executor::ddl::mview_helpers,
  Go `BuildMViewImportIntoOptions` at `materialized_view.go:335`):
  `disable_precheck` always leads; a positive thread count and a non-empty
  disk quota follow in Go's order, with the quota single-quote doubled as
  Go's `sqlescape.MustEscapeSQL("disk_quota=%?", ..)` produces.
- `build_m_log_purge_meta` (tidb-executor::ddl::mview_helpers, Go
  `buildMLogPurgeMeta` at `materialized_view.go:692`): the ALTER
  MATERIALIZED VIEW LOG purge-clause validation — empty clause yields empty
  meta, `PURGE IMMEDIATE` refuses with Go's ALTER-path wording (distinct
  from the create path's), START WITH / NEXT validate through batch 9's
  canonical schedule-expression builder.

## Known gaps recorded (not fixed in this batch)

- The `StateWriteReorganization` data-build execution (import-into /
  insert-select at the build read TS) remains the standing reorg-infra
  seam; the completion transaction accepts its outcome via
  `MviewBuildOutcome { read_ts }` (batch 20).
- The session-variable image (12 MV vars + reorg vars at real session
  values) remains the batch-19 documented reduction.

## Regression tests

- `mview_execution_vars_reconstruct_from_job_envelope` (tidb-session): a
  job envelope with three of the twelve variables restores exactly those
  and keeps the defaults for the rest; a nil job yields the captured
  defaults.
- `import_into_options_match_gos_order_and_escaping` +
  `purge_meta_validation_matches_go` (tidb-executor): the option order,
  escaping, and the ALTER-path IMMEDIATE refusal.

Fail-before evidence: before this batch none of the three helpers existed;
each test binds to symbols absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1162 run, 1155 passed, 7 failed — the exact base failure set.
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-session --no-fail-fast
# 276 -> then 275 failed on the final run: the extracted name set is
# byte-identical to the base set (the one-off statement_index_usage flake
# passes in isolation and in the final full run). Zero new failures.
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-executor -E 'test(import_into_options) + test(purge_meta_validation)'
# 2/2 passed. Plus m_view_execution_vars_reconstruct 1/1 in tidb-session.
```

No Go source changed in this batch.
