# `pkg/ddl` MV build-SQL generation receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
build-SQL generation contracts the caller executes for the
`StateWriteReorganization` data build — `buildCreateMaterializedViewInsertSQL`,
`buildCreateMaterializedViewImportSQL`, and the residual-row probe from
`hasCreateMaterializedViewBuildRows`.

## Drift content and Rust alignment

- `build_create_materialized_view_insert_sql` (Go
  `mview_worker.go:493`): produces `REPLACE INTO `schema`.`view`
  <SQLContent>` from the view's recorded canonical definition. Errors with
  Go's `create materialized view: invalid select sql` when the view
  metadata is nil or the SQL content is empty.
- `build_create_materialized_view_import_sql` (Go `mview_worker.go:456`):
  produces `IMPORT INTO `schema`.`view` FROM (<SQLContent>) WITH
  <options>` — the options come from batch 22's
  `build_m_view_import_into_options` (Go's `BuildMViewImportIntoOptions`),
  so the disable_precheck/thread/disk_quota triple matches Go's format.
- `build_create_materialized_view_build_rows_check_sql`: the
  `SELECT 1 FROM `schema`.`mview` LIMIT 1` probe that
  `hasCreateMaterializedViewBuildRows` runs to detect residual build rows
  from a crashed prior attempt (the phase-2 rollback trigger).
- All three use Go's `%n` back-quote wrapping (`name` → `` `name` ``).

These are the caller-facing contracts for the data-build execution: the
caller runs the REPLACE INTO (or IMPORT INTO) statement through a real
store session, captures the read TS from `@@tidb_last_query_info`, and
feeds `MviewBuildOutcome { read_ts }` to the completion transaction (batch
20). The residual-row probe detects a crashed prior build so the rollback
path fires.

## Known gaps recorded (not fixed in this batch)

- The SQL execution itself (running the REPLACE INTO or IMPORT INTO
  against a real store) remains the caller-side reorg-infra seam; the
  completion transaction (batch 20) accepts its outcome via
  `MviewBuildOutcome { read_ts }`.

## Regression tests

- `insert_sql_matches_gos_replace_into_shape`: pins the REPLACE INTO form.
- `import_sql_matches_gos_import_into_shape`: pins the IMPORT INTO form
  with the option triple.
- `missing_sql_content_refuses`: pins Go's `ErrInvalidDDLJob` wording.
- `build_rows_check_sql_matches_go`: pins the residual-row probe.

Fail-before evidence: before this batch none of these SQL-generation
functions existed; each test binds to symbols absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1162 run, 1154 passed, 8 failed — the same 7 base failures plus the
# placement_delivery POST fixture flake (verified failing on base).
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-session --no-fail-fast
# 276 failed — the known base 275 plus the statement_index_usage ordering
# flake (passes in isolation and in clean full runs).
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-executor -E 'test(insert_sql) + test(import_sql) + test(missing_sql) \
  + test(build_rows_check)'
# 4/4 passed.
```

No Go source changed in this batch.
