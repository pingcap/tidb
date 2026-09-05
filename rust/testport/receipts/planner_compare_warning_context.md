# Rust planner comparison warning-context receipt

Status: bounded Rust-only alignment batch; this receipt covers integer-column
comparisons against non-integer string constants.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the relevant Go packages was
enumerated and read from the fetched tree: 208 artifacts under
`pkg/expression` (195 Go files, including 117 production files and 78 tests,
plus generated/build inputs and metadata), 92 under `pkg/session` (70 Go
files, including 25 production files and 45 tests, plus fixture, generated,
platform, build, and metadata), and 568 under `pkg/planner/core` (196
production Go files and 166 Go tests, plus generated/fixture/platform/build
artifacts and metadata). No Go, generated, fixture, platform, or Bazel file
changed.

The Rust owners were inventoried before editing: `tidb-expr` has 176 tracked
files, `tidb-session` has 222, and `tidb-planner` has 345, including every
production source, inline and standalone test, generated test harness input,
fixture, platform variant, Cargo/build artifact, and package metadata. The
changed Rust files are `tidb-expr/src/builtin_compare.rs`,
`tidb-expr/src/rewriter.rs`, `tidb-expr/src/rewriter/fold_mode.rs`,
`tidb-planner/src/plan_builder.rs`, and
`tidb-planner/src/plan_builder/tests.rs`.

## Alignment

Go's `compareFunctionClass.refineArgs` calls `RefineComparedConstant` while
building `gt(a, '10ab')` against an INT column. The first conversion to the
column type emits 1292, and the comparison of the converted integer with the
original string emits a second 1292. The surviving plan is `gt(a, 10)`, so
the warning count is two per statement and does not grow with scanned rows or
partition layout.

Rust's context-free AST rewriter previously performed the same constant
substitution with `NoColumns`, which intentionally discarded both diagnostics.
The resulting plan shape was correct but session warning state was empty. A
new optional `ColumnResolver::comparison_context` seam is forwarded through
fold-mode child resolvers and populated by `PlanScopeResolver` with the live
statement context. Integer comparison refinement now uses that context when
available, while context-free and fixture resolvers retain their prior
warning-free structural behavior.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib plan_builder::tests::plan_scope_resolver_refines_integer_string_with_live_warning_context -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_compare_refinement::int_column_gt_string_constant_warns_twice_regardless_of_table -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_compare_refinement::the_warning_count_does_not_grow_with_the_scanned_rows -- --exact --nocapture --test-threads=1`

All three focused tests passed. The broader compare-refinement module still
has its pre-existing failures for datetime numeric comparison, invalid
duration NULL-safe comparison, and YEAR two-digit adjustment; none is changed
by this warning-context seam.

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

Only planner AST rewriting with a `PlanScopeResolver` gains warning ownership;
the comparison value and access-path shape are unchanged. Context-free
rewrites, direct expression tests, pure numeric comparisons, and all Go and
generated sources remain untouched. Warnings are emitted once during plan
construction and are not reintroduced per row.
