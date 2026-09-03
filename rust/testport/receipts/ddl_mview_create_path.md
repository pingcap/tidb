# `pkg/ddl` create-path — schedule validation + LIKE metadata slice receipt (Go-master drift)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
create-path hunks of the materialized-view DDL commit `94a9cbedab` that are
carryable in the current Rust layering:
`restoreNodeToCanonicalSQL`, `BuildAndValidateMViewScheduleExpr`
(`pkg/ddl/mview_schedule_expr.go`), and the
`BuildTableInfoWithLike` metadata-clearing arm (`pkg/ddl/create_table.go`).

## Drift content and Rust alignment

- `restoreNodeToCanonicalSQL(node ast.Node)` (`DefaultRestoreFlags |
  RestoreStringWithoutCharset`) → `Expr::restore_with_flags(
  RestoreFlags::DEFAULT | RestoreFlags::STRING_WITHOUT_CHARSET)` on the
  `tidb-ast` public API.
- `BuildAndValidateMViewScheduleExpr(sctx, expr, clause)` →
  `build_and_validate_m_view_schedule_expr(&Expr, clause) ->
  Result<String, Error>` in
  `tidb-executor/src/ddl/mview_schedule_expr.rs`: canonical restore, build
  through `tidb_expr::simple_expr::build_simple_expr` over an empty column
  scope (`NoColumns` — a schedule expression references no table columns, so
  a column reference fails resolution exactly as Go's session expression
  context reports it), type inference through `Expression::static_type`,
  `None` → Go's `"failed to infer expression type for {clause}"`, and the
  non-DATETIME/TIMESTAMP refusal as Go's `ErrGeneralUnsupportedDDL` (8200)
  message with `types.TypeStr` (LongLong → `bigint`).
- `BuildTableInfoWithLike` clears `MaterializedViewBase`/`MaterializedView`/
  `MaterializedViewLog` after the foreign-keys reset → the Rust
  `CREATE TABLE LIKE` copy (`tidb-exec/src/cluster_ddl.rs`) clears the three
  fields at the same position in Go's reset order.

## Known gaps recorded (not fixed in this batch)

- `CreateMaterializedView`/`CreateMaterializedViewLog` on Go's DDL
  `Executor` interface and the full `materialized_view.go` build path
  (1181 lines) land with the job-worker sub-batches; this batch owns the
  validation helper they call and the LIKE metadata behavior.
- Go's `handleAutoIncID` signature refactor is internal shape, not
  observable behavior; `getJobCheckInterval`'s new arm belongs to the
  unported check-interval worker infrastructure.

## Regression tests

- `tidb-executor/src/ddl/mview_schedule_expr.rs::tests::build_and_validate_accepts_datetime_and_refuses_other_types`
  — `NOW() + INTERVAL 1 DAY` passes with canonical restore text; `1 + 1` is
  refused with Go's exact 8200 message and `bigint` type name (Go
  `TypeStr(LongLong)`); a column reference fails the build like Go's
  unresolvable scope.
- `tidb-exec/tests/cluster_ddl_source.rs::create_table_like_clears_materialized_view_metadata`
  — an MV-log source table copies without any of the three metadata fields,
  while the source keeps its own.

Fail-before evidence: the build-and-validate function and the LIKE clearing
do not exist in the pre-batch tree — the refusal test binds to the new
function (absent symbols), and the LIKE-clearing arm's absence routes the
metadata through, which the new assertion detects.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline \
  -p tidb-executor -E 'test(mview_schedule) or test(build_and_validate)'
# 10/10 passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline \
  -p tidb-exec -E 'test(create_table_like_clears)'
# 1/1 passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-executor --no-fail-fast
# full-suite delta vs the pre-batch base `fa74e961db`: the candidate-new
# failures were reproduced at the base commit in a clean worktree
# (driver::select_clauses, join::spill/parallel, tests_table_part2 —
# pre-existing environmental/baseline failures; the one LIKE-path test
# `serial_create_table_like_source` fails identically at base with the
# planner projection panic). Zero new failures attributable to this batch.
```

No Go source changed in this batch.
