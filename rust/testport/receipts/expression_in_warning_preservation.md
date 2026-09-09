# Rust expression mixed-domain `IN` warning receipt

Status: bounded Rust-only alignment batch; this receipt covers preserving
runtime conversion warnings for numeric `IN` lists during planner folding.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the relevant Go packages was
enumerated and read from the fetched tree: 208 artifacts under
`pkg/expression` (195 Go files, including 117 production files and 78 tests,
plus generated/build inputs and metadata), 92 under `pkg/session` (70 Go
files, including 25 production files and 45 tests, plus fixture, generated,
platform, build, and metadata files), and 568 under `pkg/planner/core` (196
production Go files and 166 Go tests, plus generated/fixture/platform/build
artifacts and metadata). No Go, generated, fixture, platform, or Bazel file
changed.

The Rust owners were inventoried before editing: `tidb-expr` has 176 tracked
files, `tidb-session` has 222, and `tidb-planner` has 345, including every
production source, inline and standalone test, generated test harness input,
fixture, platform variant, Cargo/build artifact, and package metadata. The
changed Rust file is `tidb-expr/src/constant_fold.rs`; the session files are
regression evidence only and remain unchanged by this batch.

## Alignment

Go's `builtinInRealSig`/`builtinInIntSig` (`pkg/expression/builtin_other.go`)
coerce every list member to the first argument's numeric evaluation type.
Those conversions run with the live statement context even after an earlier
candidate matches, so `0 IN ('0', 'abc')` returns true and still appends Go's
1292 `Truncated incorrect DOUBLE value: 'abc'` warning. The same behavior is
observable for `NOT IN`.

Rust's runtime `in` evaluator already visits all candidates, but the planner
constant fold replaced an all-literal list with a boolean under `NoColumns`.
That erased the conversion-warning ownership before a statement context was
available. The fold now recognizes a numeric first argument plus string/byte
literal candidates and retains the `IN` function for runtime evaluation. Pure
numeric lists and non-numeric first arguments keep their previous fold path.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-expr --lib constant_fold::deferred_function_tests::mixed_numeric_in_literals_stay_runtime_bound_for_warning_preservation -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_in_list_full_evaluation::literal_in_list_coerces_values_after_the_match -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_in_list_full_evaluation::literal_not_in_list_coerces_values_after_the_match -- --exact --nocapture --test-threads=1`

All three focused tests passed; the session regressions verify both the
returned boolean and the retained 1292 warning after an earlier list match.

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

Only planner-side folding of closed, mixed numeric/string-or-byte `IN` lists
changes. Runtime evaluation, pure numeric folds, dynamic candidates, and all
Go sources and generated artifacts remain unchanged. The retained function is
value-equivalent; it deliberately defers ownership of possible 1292 warnings
to the live statement context.
