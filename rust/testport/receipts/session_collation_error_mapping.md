# Rust session collation/charset error mapping receipt

Status: bounded Rust-only alignment batch; this receipt covers the planner
error seam exercised by `COLLATE`, not the entire session or collation
surface.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the Go planner package was
enumerated and read from the fetched tree: 568 artifacts under
`pkg/planner/core` (196 production Go files, 166 Go tests, plus generated
sources, fixtures, platform variants, `BUILD.bazel`, and ownership/build
metadata). The related Go expression package was also rechecked in full: 208
artifacts (117 production Go files, 78 tests, generated/build inputs, and
package metadata). The relevant nested planner and executor test/fixture
directories were included in those manifests; no Go, generated, fixture,
platform, or Bazel file changed.

The Rust owners were inventoried before editing: `tidb-planner` has 345
tracked files and `tidb-session` has 222, including every production source,
inline and standalone test, generated test harness input, fixture, platform
variant, Cargo/build artifact, and package metadata. The changed Rust files
are `tidb-planner/src/plan_base.rs`,
`tidb-planner/src/plan_builder.rs`,
`tidb-planner/src/plan_builder/tests.rs`, and
`tidb-executor/src/driver.rs`.

## Alignment

Go's `pkg/planner/core/expression_rewriter.go` handles
`*ast.SetCollationExpr` by returning
`charset.ErrCollationCharsetMismatch.GenWithStackByArgs` (1253) when a
collation does not belong to the expression's character set. The Rust
rewriter already created the typed `EvalError::CollationCharsetMismatch`, and
the executor already rendered it as the same 1253 message. The missing
behavior was the planner boundary: `PlanError::from(EvalError)` flattened the
typed error into `PlanErrorKind::Internal`, and the driver converted that
class to generic 1105 with Rust's debug text.

Rust now carries `EvalError` in a typed `PlanErrorKind::Eval` variant and the
driver maps that variant back to `DriverError::Exec(ExecError::Eval(...))`.
Other rewrite errors remain message-only planner failures, while all existing
planner error classes keep their previous mappings.

Focused regressions:

- `tidb-planner::plan_builder::tests::an_eval_error_keeps_its_typed_plan_kind`
  pins the new typed plan boundary.
- `tidb-session::tests_collation::collate_clause_must_match_the_charset`
  exercises parse, plan, executor, and wire rendering and now returns
  `(1253, "COLLATION 'latin1_bin' is not valid for CHARACTER SET 'utf8mb4'")`
  instead of `(1105, "CollationCharsetMismatch { ... }")`.

## Validation

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

All four completed successfully. The focused planner test and the exact
serial session collation mismatch test passed. The neighboring full
`tests_collation` module has one unrelated pre-existing failure in
`set_names_reaches_literal_and_folded_expression_collations` (the fixture
expects `utf8mb4_general_ci`, while this tree reports `utf8mb4_bin`); it does
not exercise the planner error conversion changed here.

## Risks and boundaries

Correctness risk is limited to expression errors raised while building a
logical plan: they now preserve their existing typed MySQL mapping instead of
being relabeled as generic 1105. Message-only `RewriteError` and planner
internal failures remain unchanged. No Go source, generated output, fixture,
platform variant, or build artifact was modified.
