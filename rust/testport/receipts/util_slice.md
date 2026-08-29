# `pkg/util/slice` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `slice.go`,
`slice_test.go`, `main_test.go`, and `BUILD.bazel`. They define `AllOf`,
`Int64sToStrings`, `DeepClone`, one table-driven unit test, and the common test
harness. There is no package doc, README, fixture, benchmark,
generated/platform variant, or ownership file. The checkout is byte-identical
to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/slice.rs` is the sole owner. All three production
functions were already present: `all_of` preserves empty truth and
short-circuiting, `int64s_to_strings` uses signed base-ten formatting, and
`deep_clone` keeps Go nil distinct from a present empty slice while invoking
the element clone operation.

The audit removed four supplementary Rust tests absent from the pinned package
and retained the single table-driven `TestSlice` translation with all four
source rows. The only production consumer, statistics bootstrap SQL, continues
to use the package conversion function.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `cargo test -p tidb-util --locked slice::tests::` — passed.
- `cargo check -p tidb-stats --lib --locked` — passed.
- `cargo test -p tidb-util --locked` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.
- `go test ./pkg/util/slice` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; production behavior was already aligned.
- Compatibility: only non-source test code is removed.
- Performance: unchanged.
