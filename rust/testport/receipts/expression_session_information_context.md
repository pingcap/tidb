# Rust expression session-information context receipt

Status: bounded Rust-only alignment batch; this receipt covers preserving
session-dependent information functions through planner constant folding.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the relevant Go packages was
enumerated and read from the fetched tree: 208 artifacts under
`pkg/expression` (195 Go files, including 117 production files and 78 tests,
plus generated/build inputs and metadata), 92 under `pkg/session` (70 Go
files, including 25 production files and 45 tests, plus fixture, generated,
platform, build, and metadata files), and 293 under `pkg/ddl` (262 Go files,
including 124 production files and 138 tests, plus fixture, generated,
platform, build, and metadata files). No Go, generated, fixture, platform, or
Bazel file changed.

The Rust owners were inventoried before editing: `tidb-expr` has 176 tracked
files and `tidb-session` has 222, including every production source, inline
and standalone test, generated test harness input, fixture, platform variant,
Cargo/build artifact, and package metadata. The changed Rust files are
`tidb-expr/src/scalar_function.rs`, `tidb-expr/src/constant_fold.rs`, and
`tidb-session/src/tests_column_defaults.rs`.

## Alignment

Go's `databaseFunctionClass`, `currentUserFunctionClass`, and
`userFunctionClass` signatures read `CurrentDB` or
`CurrentUserPropReader`; `CONNECTION_ID`, `VERSION`, `CURRENT_ROLE`,
`CURRENT_RESOURCE_GROUP`, `ROW_COUNT`, and `TIDB_VERSION` similarly read
`SessionVarsPropReader` or session state. These functions are listed in
`UnCacheableFunctions`, and their Go evaluators return a missing-property
error when the build-time context has no session, so `FoldConstant` leaves
them executable for the live session.

Rust's scalar evaluator intentionally returned `NULL` for these names when
given `NoColumns`, and the planner fold treated them as ordinary foldable
functions. A wrapper such as `UPPER(USER())` was therefore frozen to `NULL`
before execution, even after the session authenticated a user; the same
would happen to `DATABASE()` and the other session-information calls.

Both constant-fold registries now retain these information names as
runtime-bound. The live `Columns` context still evaluates them exactly as
before, while no-session planning can no longer replace a missing property
with a constant. Focused expression coverage pins all twelve names, and the
session computed-default suite verifies `UPPER(USER())` and
`UPPER(DATABASE())` return `BOB@10.0.0.1` and `TEST` after `set_user`.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib constant_fold::deferred_function_tests::session_information_functions_require_runtime_context -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_column_defaults::computed_default_whitelist_evaluates_the_allowed_function_shapes -- --exact --nocapture --test-threads=1`

Both focused tests passed. The broader `tests_core::builtins` sweep remains
at its known baseline of five unrelated failures (AES mode, temporal and
numeric warning propagation); none involves these session-information names.

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

Only planner folding of functions that require live session information
changes. Runtime evaluation with a real session, no-session `NULL` behavior
when the function is executed directly, function arguments, and all
non-information folds remain unchanged. No Go source, generated output,
fixture, platform variant, or build artifact was modified.
