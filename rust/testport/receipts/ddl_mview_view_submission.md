# `pkg/ddl` MV view-create submission receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
view-create submission body of `CreateMaterializedView` — the restricted-SQL
column-type derivation, the view `TableInfo` build, the
`MaterializedViewInfo` metadata and the typed job — routed through the
batch-14 submission planner.

## Drift content and Rust alignment

- Go's `ExecRestrictedSQL("SELECT * FROM (<selectSQL>) AS `tidb_mv_query`
  LIMIT 0")` derivation ports as
  `derive_materialized_view_query_columns`: the definition is single-table
  by admission, so a driver catalog bridge registering just the base table
  (the same `KvTable` construction the partition-routing planner uses) is
  the whole world the query sees, and `LIMIT 0` keeps the execution a
  schema-only read. The output pairs feed the view column build.
- `len(resultFields) != len(s.Cols)` refuses with Go's exact
  `materialized view column count %d does not match query output %d` — an
  undeclared column list therefore refuses too, matching Go's body.
- The view `TableInfo` builds through the ordinary create-table path with
  the one-row-per-group constraint Go derives: a PRIMARY KEY over the
  declared GROUP BY columns when every group key is NOT NULL, UNIQUE
  otherwise (the `mviewQueryAnalysis` NOT-NULL flags batch 13 preserved).
  Each built column is re-stamped with the planner's result field type
  after Go's `DelFlag(PriKeyFlag | UniqueKeyFlag | MultipleKeyFlag |
  AutoIncrementFlag | OnUpdateNowFlag)`, exactly Go's
  `*rf.Column.FieldType` copy. `mvTableInfo.Comment = s.Comment`.
- `MaterializedViewInfo` carries Go's full shape: the single-element
  `BaseTableIDs`, `InitBuildState = Building`, the canonical `SQLContent`,
  the FAST refresh method with its START WITH/NEXT clauses (batch 9
  validated), the ATTRIBUTES alert fields (batch 16's batch-16 predecessor
  `parseMViewAttributes`), the definition SQL mode and division precision
  increment, and both time zones from `GetTimeZone`.
- The job envelope: `ACTION_CREATE_MATERIALIZED_VIEW`, three involving
  schemas (view, base, log), the scatter-region system var, and the typed
  `CreateMaterializedViewArgs { TableInfo, MLogTableIDs: [mlog] }` — then
  the shared `prepare_submit_batch` preflight. The job reports
  `may_need_reorg()` like Go's reorg submission.
- Both MV statements now route through the durable-job planner:
  `plan_ddl` refuses them with the CHECK-style
  `materialized view DDL must execute through mysql.tidb_ddl_job` guard.
- `CreateMaterializedView`'s two portable submission prefix steps
  (`normalizeMVDefinitionHintDBNames`, `restoreNodeToCanonicalSQL`) were
  already landed in batch 16's predecessor; the prefix struct now feeds
  this body directly.

## Known gaps recorded (not fixed in this batch)

- The view create's initial-build reorg phase (`onCreateMaterializedView`'s
  `StateWriteReorganization` arm: import-into / insert-select at the build
  read TS, the refresh-info upsert, `InitBuildState = Ready`) is not wired,
  so a submitted view job stays QUEUED until that batch lands. The log
  create's job remains fully executable end to end.
- A log WITH a purge schedule still needs the session-side schedule
  evaluator (`MlogPurgeDerived` parameter, batch 15's recorded seam).
- `AddMViewExecutionSessionVarsToJob`'s twelve MV-execution session vars and
  `initMaterializedViewReorgMetaFromVariables` still have no statement
  context owner; the envelope carries the scatter-region system var only.

## Regression tests

`materialized_view_lowering_follows_go_admission_order` now routes every
view refusal through the submission planner (identical messages, identical
order) and drives a valid statement through the derivation: the column-count
mismatch refusal (3 vs 2, Go's exact message), then the submitted spec —
`ACTION_CREATE_MATERIALIZED_VIEW` queueing with three involving schemas and
`may_need_reorg`, the derived view columns (`id` INT with the key flags
deleted, `c` COUNT's bigint output type), the PRIMARY KEY shape, the
`MaterializedViewInfo` base back-reference — and the job staying queued for
its (still unwired) initial-build worker batch.
`materialized_view_query_clause_refusals_follow_go` routes identically.

Fail-before evidence: before this batch a valid view create refused with
`materialized view job execution is not wired in this tier` at the
derivation point; the derivation, view TableInfo build, metadata assembly
and typed args did not exist.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# 1160 run, 1153 passed, 7 failed — the same seven as the batch-15 base
# (base's 8 minus the batch-15-repaired stale bootstrap count). Zero new.
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-session --no-fail-fast
# 690 failed on base vs 689 here — the extracted name set is a strict subset
# of the base set (one unrelated flake turned pass); zero new failures.
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-model -p tidb-ddl-session -p tidb-metadef
# 343 run, 342 passed — the one failure is the pre-existing
# every_public_string_constant baseline mismatch (verified on base).
```

No Go source changed in this batch.
