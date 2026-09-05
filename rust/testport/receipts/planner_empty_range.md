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
