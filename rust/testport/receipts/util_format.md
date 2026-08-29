# `pkg/util/format` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full:

- `format.go` — formatter interface, indentation/flattening state machine, and
  SQL display escaping;
- `format_test.go` — `TestFormat`;
- `main_test.go` — common test setup and leak checking;
- `BUILD.bazel` — one library and one flaky short test target.

There is no `doc.go`, generated/platform source, fixture, testdata, benchmark,
fuzz target, example, or additional harness.

## Rust ownership and audit result

The formatter state machine is shared with the already-transcreated
`pkg/parser/format` owner in `rust/crates/tidb-datatype/src/format.rs`.
`rust/crates/tidb-util/src/format.rs` reexports that owner and supplies util's
additional backslash escape.

The audit removed `IndentFormatter::into_inner` and
`FlatFormatter::into_inner`. Neither Go constructor exposes an equivalent
operation through its returned `Formatter` interface, and no production Rust
consumer used it. Tests now pass borrowed writers and inspect them after the
formatter is dropped. Unused `Clone`, `PartialEq`, and `Eq` implementations on
the Rust-only typed fragment boundary were removed as well.

The stale semantic manifest and historical audit plan were deleted. Retained
tests cover the source state machine, cross-call state, flat behavior, writer
counts/errors, opaque formatted values, Go string bytes, and the complete util
escape set.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/format`
- `cargo test -p tidb-datatype --test all 'parser_format_package_source::' --locked`
- `cargo test -p tidb-util --test format_contract --locked`
- focused executor/session output-format consumers
- `cargo test -q -p tidb-util --locked -- --test-threads=1`
- `cargo fmt --all --check`
- `git diff --check`

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; the Go and Rust formatter suites pass with borrowed
  writers and retain exact output/state behavior.
- Compatibility: intentionally removes unconsumed Rust-only convenience and
  trait implementations absent from Go.
- Performance: unchanged; rendering and the single underlying writer call are
  untouched.
