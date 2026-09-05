# Rust LAST_INSERT_ID session-state receipt

Status: bounded Rust-only alignment batch; this receipt covers constant-fold
ownership for `LAST_INSERT_ID()` and `LAST_INSERT_ID(expr)`, including the
construction-time side effect of the one-argument form.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the Go expression package was
enumerated and read from the fetched tree: 208 artifacts under
`pkg/expression` (117 production Go files, 78 tests, generated/build inputs,
and package metadata). The session package was also rechecked in full: 92
artifacts (25 production Go files, 45 tests, and fixture, generated, platform,
build, and metadata files). The planner package was rechecked for its folding
callers: 568 artifacts (196 production Go files, 166 tests, plus generated
sources, fixtures, platform variants, `BUILD.bazel`, and ownership/build
metadata). No Go, generated, fixture, platform, or Bazel file changed.

The Rust owners were inventoried before editing: `tidb-expr` has 176 tracked
files and `tidb-session` has 222, including every production source, inline and
standalone test, generated test harness input, fixture, platform variant,
Cargo/build artifact, and package metadata. The changed Rust files are
`tidb-expr/src/scalar_function.rs`, `tidb-expr/src/constant_fold.rs`,
`tidb-expr/src/expression.rs`, `tidb-expr/src/lib.rs`,
`tidb-expr/src/builtin_ext/json/report.rs`, and
`tidb-planner/src/plan_builder.rs`.

## Alignment

Go's `lastInsertIDFunctionClass` attaches `SessionVarsPropReader` to both
signatures (`pkg/expression/builtin_info.go`). `FoldConstant` may inspect a
zero-argument function, but evaluation without those session properties
returns an error, so the expression remains runtime-bound. The one-argument
form must likewise retain its `SetLastInsertID` side effect.

Rust's scalar evaluator represented a missing `Columns::last_insert_id` as
`Datum::Null` and treated `set_last_insert_id` as a no-op for `NoColumns`. Since
`last_insert_id` was absent from the Rust unfoldable tables, planner folding
replaced `LAST_INSERT_ID()` with `NULL` and could erase
`LAST_INSERT_ID(expr)`'s publication. SQL auto-increment tests consequently
read `NULL` after successful inserts instead of the generated id.

Go's `unFoldableFunctions` deliberately does *not* include this name. The
zero-argument reader must stay runtime-bound because it reads the previous
statement, while `LAST_INSERT_ID(expr)` is folded by `NewFunction` and its
construction-time evaluation publishes through `SessionVars`. Rust now keeps
only the zero-argument call runtime-bound at the arity-aware fold gate. The
plan resolver also evaluates one-argument calls immediately with the live
statement context, so a later sibling-resolution error (including in HAVING)
cannot erase the publication. The scalar-function unfoldable table now
matches Go's source table as well.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib constant_fold::deferred_function_tests::last_insert_id_requires_runtime_session_state -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib constant_fold::deferred_function_tests::last_insert_id_argument_folds_and_publishes_during_construction -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_core::status_values::a_folded_last_insert_id_publishes_even_when_a_later_expression_fails_to_resolve -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_auto_increment:: -- --nocapture --test-threads=1`

The direct zero-argument and construction-side-effect regressions, the
left-to-right sibling/HAVING failure regression, and all 33
auto-increment/session publication tests passed.

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

Only constant-fold eligibility changes for the session-state function name.
Runtime evaluation and the statement-boundary publication channel are
unchanged; other session builtins remain governed by their existing tables.
No Go source, generated output, fixture, platform variant, or build artifact
was modified.
