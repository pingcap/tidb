# `pkg/ddl` MV lowering — admission-checks slice receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
lowering/admission slice of `CreateMaterializedView` and
`CreateMaterializedViewLog` (`pkg/ddl/materialized_view.go:356-530`,
`pkg/ddl/mview_schedule_expr.go`, `pkg/ddl/create_table.go` LIKE clearing)
from the materialized-view DDL commit `94a9cbedab`.

## Drift content and Rust alignment

- `DdlStatement` gains `CreateMaterializedView` /
  `CreateMaterializedViewLog` carrying the parsed statement and its
  resolved target names.
- Lowering (`lower_ddl`): Go's `checkMaterializedViewEnabled` (the session
  `EnableMView` switch, default `OFF`, surfaced as
  `StmtContext::with_enable_mview` / `enable_mview`) and the
  `ErrNoDB`/name resolution through `split_name` — the refusals carry Go's
  exact texts (8200 `Unsupported Materialized View is disabled, please set
  \`tidb_mview_enable\` to \`ON\` to enable it`; 1046 `No database selected`).
- Planning (`plan_create_materialized_view`): Go's source-order checks —
  unknown database; `validateCommentLength`'s 1024-byte cap
  (`ErrTooLongTableComment` 8020); SELECT-only; single base table; the
  same-schema requirement; `TableNotExists`; `ErrWrongObject` (1347, `is
  not BASE TABLE`) for view/sequence/temporary bases; the partitioned-base
  refusal (8200 `Unsupported CREATE MATERIALIZED VIEW on partition
  table`); the derived `$mlog$` name existence (1105 `materialized view
  log does not exist for base table …`); the mlog identity check (1105
  `table … is not a materialized view log for base table …`); Go's
  `validateCreateMaterializedViewQuery` through batch 4's
  `mviewutil::check_materialized_view_select` (8200 with Go's message);
  GROUP BY required and WITH-ROLLUP refusals (8200). Valid statements stop
  at the documented job-execution seam (the materialized-view worker
  sub-batch wires submission).
- Planning (`plan_create_materialized_view_log`): unknown database; base
  `TableNotExists`; `isValidMaterializedViewLogBaseTable` (not a view,
  sequence, temporary table, or already an MV/log of one — 1347); the
  partitioned refusal (8200); the derived `$mlog$` name collision
  (`ErrTableExists` 1050, `Table '…' already exists`); then the job seam.
- `BuildTableInfoWithLike` clears the three materialized-view metadata
  fields in the LIKE copy (`tidb-exec/src/cluster_ddl.rs`), in Go's reset
  order after the foreign-keys reset.

The unqualified-base schema fill follows Go's `TableName.Schema` emptiness
(a one-element path is schema-less and inherits the view schema), and
3+-element paths fall to the single-base-table refusal.

## Known gaps recorded (not fixed in this batch)

- Job submission/persistence for the MV statements (the DDL worker, the
  `mview_worker.go` refresh/purge machinery, `SetSchemaDiffForCreateTable`
  arms, GID-allocation arms, delete-range/rolling-back/sanity arms) — next
  batches.
- `BuildAndValidateMViewScheduleExpr` (batch 9) is wired for the refresh
  schedule but its session-eval half waits for the job-execution seam.

## Regression tests

`crates/tidb-exec/tests/cluster_ddl_source.rs`:

- `materialized_view_lowering_follows_go_admission_order` — disabled flag
  (8200 exact text), `ErrNoDB`, unknown database, over-long comment (8020),
  set-operation query, comma join, cross-schema base, missing base
  (1146-class `TableNotExists`), missing `$mlog$` (1105), GROUP BY required,
  WITH ROLLUP, locking clauses through batch 4's checker, and the valid
  statement's job seam;
- `materialized_view_log_lowering_follows_go_admission_order` — valid log
  create at the seam, the derived `$mlog$` name collision (1050), and the
  missing-base refusal;
- `create_table_like_clears_materialized_view_metadata` — a LIKE copy of an
  MV-log source drops all three metadata fields while the source keeps them.

Fail-before evidence: the lowering arms, statement variants, and the
admission checks do not exist in the pre-batch tree; the tests bind to
symbols absent before the batch.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline \
  -p tidb-exec -E 'test(materialized_view)'
# 6/6 passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec --no-fail-fast
# full-suite failure set identical to the pre-batch base
# (`/tmp/exec-base2.txt`-style baseline: 8 pre-existing failures, the flaky
# worker-pool test excluded). Zero new failures.
```

No Go source changed in this batch.
