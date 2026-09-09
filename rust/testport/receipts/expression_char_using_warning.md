# Rust `CHAR(... USING charset)` warning receipt

Status: bounded Rust-only alignment batch; this receipt covers runtime
warning ownership for constant `CHAR` charset conversions.

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
rechecked in full: 92 artifacts (25 production Go files, 45 tests, and its
fixture, generated, platform, build, and metadata files). No Go, generated,
fixture, platform, or Bazel file changed.

The Rust owners were inventoried before editing: `tidb-expr` has 176 tracked
files, `tidb-planner` has 345, and `tidb-session` has 222, including every
production source, inline and standalone test, generated test harness input,
fixture, platform variant, Cargo/build artifact, and package metadata. The
changed Rust files are `tidb-expr/src/constant_fold.rs` and
`tidb-planner/src/plan_builder/tests.rs`.

## Alignment

Go's `builtinCharSig.evalString` builds the byte sequence from the numeric
arguments, decodes it with the requested `USING` charset, and appends
`ErrInvalidCharacterString` (1300) to the live statement context when the
decode reports invalid bytes. The error is a warning in the default strict
mode; the value becomes `NULL` while retaining the warning. A no-`USING`
`CHAR` uses the binary signature and has no charset-decode warning.

Rust's planner constant folder evaluated every closed scalar function through
`NoColumns`. `CHAR(65, -1, 67.5 USING utf8)` was therefore folded before the
session context existed, and its invalid-byte warning was lost: the result was
correct but `SHOW WARNINGS`/the wire warning count was zero instead of one.

The warning-preserving planner guard now treats `char_func` with a non-NULL
charset sentinel like the existing integer-cast carriers: it remains an
executable scalar function until the live statement context evaluates it.
Context-free expression callers retain the ordinary Go construction-time fold
when they provide a real warning context.

Focused regressions:

- `tidb-planner::plan_builder::tests::plan_scope_resolver_keeps_char_using_for_runtime_warnings`
  proves a constant `CHAR(... USING utf8)` survives the executable-plan fold.
- `tidb-session::tests_charset_introducer::char_using_decodes_and_tags_the_requested_charset`
  exercises result bytes, charset metadata, strict-mode `NULL`, and the exact
  single 1300 warning (including the relaxed-mode follow-up).

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner plan_builder::tests::plan_scope_resolver_keeps_char_using_for_runtime_warnings -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session tests_charset_introducer -- --nocapture --test-threads=1`

Both charset-introducer tests passed.

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

The guard intentionally keeps all `CHAR(... USING charset)` constant nodes
runtime-evaluable, even when a particular input would decode without warning;
this avoids duplicating Go's charset error classification in a no-column
planner context. The no-`USING` binary form, non-constant expressions, and
normal expression-unit folds remain unchanged. No Go source, generated output,
fixture, platform variant, or build artifact was modified.
