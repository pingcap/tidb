# Rust `istrue_with_null` truthiness receipt

Status: bounded Rust-only alignment batch; this receipt covers the planner's
keep-NULL predicate wrapper used by `NOT` and pushed-down boolean conditions.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the Go expression package was
enumerated and read from the fetched tree: 208 artifacts under
`pkg/expression` (117 production Go files, 78 Go tests, generated sources,
fixtures, platform/build inputs, and package metadata). The planner package
was rechecked in full: 568 artifacts under `pkg/planner/core` (196 production
Go files, 166 tests, plus generated sources, fixtures, platform variants,
`BUILD.bazel`, and ownership/build metadata). The session package was also
rechecked in full: 92 artifacts (25 production Go files, 45 Go tests, and its
fixture, generated, platform, build, and metadata files). No Go, generated,
fixture, platform, or Bazel file changed.

The Rust owners were inventoried before editing: `tidb-expr` has 176 tracked
files and `tidb-session` has 222, including every production source, inline and
standalone test, generated test harness input, fixture, platform variant,
Cargo/build artifact, and package metadata. The changed Rust files are
`tidb-expr/src/func.rs`, `tidb-expr/src/scalar_function.rs`,
`tidb-expr/src/tests/go_control_op_math_values.rs`, and
`tidb-session/src/tests_eval_bool.rs`.

## Alignment

Go's `isTrueOrFalseFunctionClass` (`pkg/expression/builtin_op.go`) selects an
ETReal/ETDecimal/ETInt signature and carries a `keepNull` bit. The
`istrue_with_null` form is the `keepNull=true` variant: NULL remains NULL,
while all other values use the same `Datum.ToBool` truthiness conversion.
Go's `pushNotAcrossExpr` (`pkg/expression/util.go`) deliberately wraps a
non-logical predicate in this function so pushing `NOT` does not collapse SQL's
three-valued logic.

Rust registered the name for planning and null-rejection analysis but omitted
both runtime dispatch arms. A pushed-down `WHERE NOT v` therefore reached the
generic `this scalar function is not yet ported` error, even though the
ordinary `not` unary evaluator already implemented the underlying truth test.

The values-only evaluator now maps `istrue_with_null` to `truthy_of`, preserving
NULL as `Datum::Null`. The chunk signature path uses the same behavior beside
the existing `istrue`/`isfalse` cases, so AST and chunk execution agree.

Focused regressions:

- `tidb-expr::tests::go_control_op_math_values::go_test_is_true_or_false`
  checks zero, non-zero, and NULL values for the new keep-NULL arm.
- `tidb-session::tests_eval_bool::eval_bool_matches_tidb_per_eval_type`
  exercises Go-captured string truthiness and `NOT v` filtering across NULL,
  numeric-prefix, real, decimal, temporal, JSON, ENUM, SET, BLOB, and CNF
  cases.
- `tidb-session::tests_eval_bool::not_string_column_preserves_truthiness_and_null`
  is a focused end-to-end regression proving `NOT` returns 0/1 for string
  numeric prefixes and NULL for a NULL source row.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr tests::go_control_op_math_values::go_test_is_true_or_false -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session tests_eval_bool -- --test-threads=1`

All focused tests passed (one expression test and three session tests).

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

The new arm intentionally preserves NULL only for the internal
`istrue_with_null` name; public `IS TRUE`/`IS FALSE` behavior remains
non-NULL, and unary `NOT NULL` remains NULL. Truthiness and warning ownership
continue to flow through `truthy_of`, so no planner-time warning is introduced
for a runtime predicate. No Go source, generated output, fixture, platform
variant, or build artifact was modified.
