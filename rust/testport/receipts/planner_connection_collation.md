# Rust planner connection-collation receipt

Status: bounded Rust-only alignment batch; this receipt covers the
statement-selected charset/collation seam used by planner expression
rewriting.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the Go planner package was
enumerated and read from the fetched tree: 568 artifacts under
`pkg/planner/core` (196 production Go files, 166 Go tests, plus generated
sources, fixtures, platform variants, `BUILD.bazel`, and ownership/build
metadata). The related Go expression package was rechecked in full: 208
artifacts (117 production Go files, 78 tests, generated/build inputs, and
package metadata). The session package was also inventoried in full: 92
artifacts (25 production Go files, 45 Go tests, and its fixture, generated,
platform, build, and metadata files). No Go, generated, fixture, platform, or
Bazel file changed.

The Rust owners were inventoried before editing: `tidb-planner` has 345
tracked files and `tidb-session` has 222, including every production source,
inline and standalone test, generated test harness input, fixture, platform
variant, Cargo/build artifact, and package metadata. The changed Rust files
are `tidb-planner/src/plan_builder.rs` and
`tidb-planner/src/plan_builder/tests.rs`.

## Alignment

Go's `session.SetCollation` updates every `SetNamesVariables` value and then
`CollationConnection`. Go's expression rewriter obtains that pair from the
statement build context: ordinary literals, string-producing casts, and
derived string builtins use `GetCharsetInfo`, while system constants such as
`VERSION()` intentionally keep the server default. After
`SET NAMES utf8mb4 COLLATE utf8mb4_general_ci`, the capture therefore reports
`utf8mb4_general_ci` for literals and folded string expressions.

Rust's `PlanScopeResolver` previously inherited the hardcoded
`utf8mb4`/`utf8mb4_bin` pair from `tidb_expr::collation_derive`, even when the
session statement context had selected another connection collation. Planner
rewrites consequently returned `utf8mb4_bin` for `COLLATION('a')`,
`CONCAT`, string casts, and nested folded expressions.

The resolver now owns the connection pair, exposes an explicit
`with_connection_charset_info` builder, and receives
`StmtContext::connection_charset_info()` from `PlanBuilder::rewrite_scalar`.
Context-free callers retain the server-default pair. This preserves the
connection setting through planning without changing stronger column,
explicit-collation, or system-constant derivation.

Focused regressions:

- `tidb-planner::plan_builder::tests::plan_scope_resolver_uses_the_statement_connection_collation`
  proves a resolver stamps a coercible literal with the supplied connection
  collation.
- `tidb-session::tests_collation::set_names_reaches_literal_and_folded_expression_collations`
  exercises parse, plan, fold, executor, and wire output for five connection
  expressions and keeps `VERSION()` at `utf8mb4_bin`.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner plan_builder::tests::plan_scope_resolver_uses_the_statement_connection_collation -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session tests_collation::set_names_reaches_literal_and_folded_expression_collations -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session tests_collation -- --nocapture --test-threads=1`

All focused tests passed; the full `tests_collation` module reports 13
passed, 0 failed.

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

The owned pair adds allocation only while constructing a planner resolver;
the default remains the same for callers that do not attach a statement
context. The change affects coercible string derivation only. Column,
explicit `COLLATE`, and system-constant rules remain unchanged. No Go source,
generated output, fixture, platform variant, or build artifact was modified.
