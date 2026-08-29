# `pkg/util/israce` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full: `israce.go`,
`norace.go`, and `BUILD.bazel`. There is no package doc, test, test harness,
benchmark, fixture, generated input/output, or ownership file. The local Go
package is byte-identical to the pin.

The `race` build tag selects the constant `RaceEnabled = true`; its `!race`
complement selects `false`.

## Rust ownership and audit result

`rust/crates/tidb-util/src/israce/mod.rs` owns the constant and the crate's
empty `race` feature selects the same compile-time true/false variants. The
ordinary printer consumes the value just as Go's printer does.

The audit removed two Rust-only unit tests, the retired semantic-gate manifest,
and a completed standalone audit plan pinned to an older source commit. These
artifacts had no Go counterparts and duplicated compile-time selection that is
validated by compiling both feature variants.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/israce` — passed.
- `go list -f 'default GoFiles={{.GoFiles}} IgnoredGoFiles={{.IgnoredGoFiles}} TestGoFiles={{.TestGoFiles}} XTestGoFiles={{.XTestGoFiles}}' ./pkg/util/israce` — selected `norace.go`, ignored `israce.go`, and found no tests.
- `go list -race -f 'race GoFiles={{.GoFiles}} IgnoredGoFiles={{.IgnoredGoFiles}} TestGoFiles={{.TestGoFiles}} XTestGoFiles={{.XTestGoFiles}}' ./pkg/util/israce` — selected `israce.go`, ignored `norace.go`, and found no tests.
- `go test ./pkg/util/israce -count=1` — passed (`[no test files]`).
- `go test -race ./pkg/util/israce -count=1` — passed (`[no test files]`).
- `cargo check --offline --locked -p tidb-util` — passed.
- `cargo check --offline --locked -p tidb-util --features race` — passed.
- `rustfmt --edition 2021 --check crates/tidb-util/src/israce/mod.rs` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged production constant and build selection.
- Compatibility: removes only Rust-only tests and retired audit machinery.
- Performance: unchanged compile-time constant.
