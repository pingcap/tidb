# `pkg/util/size` — complete package transcreation

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
byte-for-byte unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full: `size.go` and
`BUILD.bazel`. They define five binary size units and fifteen commonly used
Go ABI sizes for memory tracing. There is no package doc, test, benchmark,
fixture, generated/platform variant, or ownership file. The checkout is
byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/size/mod.rs` is the sole owner. Its five unit
constants and fifteen ABI constants were already complete. Architecture-width
values derive from the target word size; Go slice, string, interface,
function, and map values retain Go header sizes rather than substituting Rust
container layouts. The audit removed the supplementary Rust constant-table
test because the pinned Go package has no test artifact.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/size` — passed (`[no test files]`).
- `cargo check -p tidb-util --lib --locked` — passed.
- `cargo test -p tidb-util --locked` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; all production constants were already aligned.
- Compatibility: only a Rust-only test is removed.
- Performance: unchanged; all values remain compile-time constants.
