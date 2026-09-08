# `pkg/planner/core` empty-range parity receipt

Go comparison commit: `a0cdff369bd4c7060a840e3943049a79470e8af4` (`master`).

## Complete inventory

The complete `pkg/planner/core` tree was inventoried before editing: 559
artifacts in 86 directories and 241,065 lines (356 Go sources, 59 Bazel
build files, 143 JSON casetest fixtures, and one archive fixture). The direct
package contains 107 artifacts: 106 Go production/test files and its root
`BUILD.bazel` (72,951 and 340 lines respectively). Every production file,
test, fixture, generated input/output, platform/build variant, and support
artifact in the tree was included in the file-by-file walk. The relevant Go
owners are `find_best_task.go` (the empty `path.Ranges` short-circuit) and
`expression_rewriter.go` (construction-time evaluation of constant operands);
no Go file was modified.

The companion `pkg/util/ranger` inventory is complete in
[`util_ranger.md`](util_ranger.md); its 13 artifacts include the
`points.go` range builder and all nested context/test/build files.

## Restored behavior

Go's `findBestTask4LogicalDataSource` returns a root `PhysicalTableDual` as
soon as the chosen path's ranger result has zero ranges. Rust's active
dispatcher now applies the same early result for table and index paths,
before point-get, lookup, or residual-selection construction.

Go's `buildFromBinOp` evaluates the non-column operand with an empty row. The
Rust plan-scope resolver now folds strict literal subtrees through row-
dependent parents, and the ranger point builder evaluates strict constant
wrappers, so `CAST(-1 AS DECIMAL)` reaches unsigned-domain fixups. This fixes
`a < -1`, `a <= -1`, and `a = -1` on an unsigned DECIMAL index without changing
the valid `a > -1` range.

## Regression coverage

- `tidb-planner::ranger::points::tests::bin_op_points_evaluate_wrapped_strict_constants`
  is the focused ranger regression: the valid greater-than case clamps to
  `[0,+inf]`, and the less-than case has no points.
- `tidb-session::tests_explain::an_empty_index_range_is_a_table_dual_not_a_scan`
  is the source-derived end-to-end regression: all three impossible unsigned
  predicates explain as `TableDual rows:0`, return no rows, and the control
  predicate remains an `IndexRangeScan` over `[0,+inf]`.

## Validation

Ready profile passed for this batch:

```text
cargo test --offline --locked -p tidb-planner bin_op_points_evaluate_wrapped_strict_constants -- --nocapture
cargo test --offline --locked -p tidb-planner --lib ranger::points -- --nocapture
cargo test --offline --locked -p tidb-planner --lib plan_builder::tests -- --nocapture
cargo test --offline --locked -p tidb-session an_empty_index_range_is_a_table_dual_not_a_scan -- --nocapture
git diff --check
cargo fmt --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
  TMPDIR=/tmp/tidb-codex make lint
```

The Go reference remains read-only; this batch changes Rust planner/ranger
owners and does not alter Go, Bazel, generated, or platform source files.

## Follow-up: two NULL-bound test expectations matched Go (2026-09-09)

`Conds2TableDual` (`operator/logicalop/expression_util.go:24`) replaces a plan
with a `TableDual` when any condition is `expression.IsConstNull`
(`pkg/expression/util.go:2356`) — a bare `lt/le/gt/ge/eq/ne` whose RIGHT
argument is a non-deferred NULL constant. Two executor tests encoded the
opposite of that oracle:

- `driver::tests::index_ranges::index_ranges_are_built_the_way_go_builds_them`
  expected `SELECT id FROM q WHERE score > NULL` to leave an index path with
  an empty range list. Go collapses `score > NULL` to a `TableDual`, so there
  is no index path; the assertion is now `None` and the Rust already agreed.
- `remote_scan::tests::an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request`
  expected `a BETWEEN NULL AND NULL` to read nothing. Go's
  `betweenToExpression` (`expression_rewriter.go:2788`) rewrites BETWEEN into
  ONE `and(ge, le)` condition, so `IsConstNull` sees `and`, misses, and the
  relation is read before the filter drops every row. The empty-range
  assertion now follows only the `a > 97 AND a < 97` query; the NULL bound
  asserts 100 rows crossed the wire, and the control's counter is reset.

Validation: both tests pass; `tidb-executor` lib serialized 1198 passed / 49
failed, the two above and no additions; `rustfmt --edition 2021 --check` clean
on both files; `git diff --check -- rust`.

## Follow-up: the DataSource runs `Conds2TableDual` before the push-down split (2026-09-09)

Go `DataSource.PredicatePushDown` (`logical_datasource.go:185`) simplifies the
incoming predicates, records ALL of them in `AllConds`, and only then splits
them with `expression.PushDownExprs`. The `Conds2TableDual(ds, ds.AllConds)`
call sits between those two steps, so a constant-NULL predicate collapses the
source to an empty `TableDual` EVEN WHEN the predicate is pushable.

The Rust driver partitioned first and never ran `Conds2TableDual` at the
DataSource, so the check only happened for the predicates left above the
source. That was invisible while `gt(cast_double(col), NULL)` was rejected by
the TiKV whitelist: it stayed in the Selection and collapsed there. Once the
dedicated `cast_*` family became pushable under Go's `cast` name
(`cast_hybrid_push.md`), the predicate reached the DataSource and built a
`[NULL,+inf]` index range instead of the empty relation, breaking
`driver::tests::index_ranges::index_ranges_are_built_the_way_go_builds_them`.

`rewrite::predicate_push_down`'s DataSource arm now mirrors Go's order —
simplify, record `AllConds`, `Conds2TableDual`, then split — and
`DataSource::predicate_push_down_local` takes the full predicate list and does
the split itself. The operator test that pinned the old two-list signature now
uses a genuinely non-pushable `round(col, 1)` for the remainder.

Regression: the existing
`driver::tests::index_ranges::index_ranges_are_built_the_way_go_builds_them`
fails with the cast admission fix alone (`Some((1, [[NULL,+inf]]))` instead of
`None`) and passes with this DataSource arm restored. Validation:
`tidb-planner` lib 995 passed / 0 failed; `tidb-executor` lib serialized 1213
passed / 39 failed, byte-identical to the pre-change baseline list.
