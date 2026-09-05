# Rust planner `NO_UNSIGNED_SUBTRACTION` receipt

Status: bounded Rust-only alignment batch; this receipt covers statement SQL
mode propagation into integer subtraction result typing.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the Go planner package was
enumerated and read from the fetched tree: 568 artifacts under
`pkg/planner/core` (196 production Go files, 166 tests, plus generated
sources, fixtures, platform variants, `BUILD.bazel`, and ownership/build
metadata). The related Go expression package was rechecked in full: 208
artifacts (117 production Go files, 78 tests, generated/build inputs, and
package metadata). The session package was also rechecked in full: 92
artifacts (25 production Go files, 45 Go tests, and its fixture, generated,
platform, build, and metadata files). No Go, generated, fixture, platform, or
Bazel file changed.

The Rust owners were inventoried before editing: `tidb-planner` has 345
tracked files, `tidb-expr` has 176, and `tidb-session` has 222, including every
production source, inline and standalone test, generated test harness input,
fixture, platform variant, Cargo/build artifact, and package metadata. The
changed Rust files are `tidb-planner/src/plan_builder.rs` and
`tidb-planner/src/plan_builder/tests.rs`.

## Alignment

Go's `arithmeticMinusFunctionClass.getFunction` (`pkg/expression/
builtin_arithmetic.go`) adds the UNSIGNED result flag only when either operand
is unsigned *and* the statement SQL mode does not contain
`NO_UNSIGNED_SUBTRACTION`. The runtime `builtinArithmeticMinusIntSig` reads the
same mode to select signed overflow/value semantics.

Rust's arithmetic inference already accepted a `no_unsigned_subtraction` flag,
but `PlanScopeResolver` left the `ColumnResolver` default (`false`) in every
statement. `SET sql_mode='NO_UNSIGNED_SUBTRACTION'` therefore reached runtime
with a signed arithmetic implementation but an unsigned planned return type;
`SELECT CAST(0 AS UNSIGNED) - 1` was rendered as
`18446744073709551615` instead of Go's `-1`.

The resolver now carries the statement mode, exposes
`with_no_unsigned_subtraction`, implements the resolver trait override, and
receives `StmtContext::no_unsigned_subtraction()` from `PlanBuilder::rewrite_scalar`.
Context-free resolver callers retain the default mode.

Focused regressions:

- `tidb-planner::plan_builder::tests::plan_scope_resolver_uses_no_unsigned_subtraction_mode`
  checks that a rewritten unsigned-minus-signed expression has a signed return
  type under the mode.
- `tidb-session::tests_sql_mode_scanner::no_unsigned_subtraction_changes_the_result_domain_and_value`
  exercises the error before the mode, signed `-1` after `SET`, and the same
  signed value through INSERT/storage.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner plan_builder::tests::plan_scope_resolver_uses_no_unsigned_subtraction_mode -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session tests_sql_mode_scanner -- --test-threads=1`

All focused tests passed (one planner regression and all 15 SQL-mode scanner
tests).

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

The mode is attached only to plan-aware resolvers; no-column/unit-test
resolvers keep Go's default SQL mode. The change affects integer subtraction
metadata and its corresponding runtime overflow domain; other arithmetic,
session SQL modes, and explicit casts remain unchanged. No Go source,
generated output, fixture, platform variant, or build artifact was modified.
