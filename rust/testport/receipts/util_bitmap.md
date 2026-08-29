# `pkg/util/bitmap` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `concurrent.go`,
`concurrent_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package
doc, README, fixture, generated or platform variant, benchmark, fuzz target,
example, or ownership file. The local Go package is unchanged from the pin.

Production behavior is a fixed-length, 32-bit-segment bitmap whose concurrent
`Set` uses atomic load/CAS and reports one winner per zero-to-one transition.
Clone, Reset, single-owner access, most-significant-bit-first numbering, and
capacity-based memory accounting complete the package.

## Rust ownership and audit result

`rust/crates/tidb-util/src/bitmap.rs` owns the complete package. Go `int`
lengths and indexes are represented as `isize`; segment rounding uses the same
wrapping signed addition and arithmetic shift. This restores the pinned Go
outcomes for negative and maximum lengths, including Reset's malformed
`MaxInt` state, instead of applying a Rust-only validity policy.

The audit removed deterministic oversized-length rejection, Rust-only
`must_use` diagnostics, supplemental tests, and the retired semantic manifest.
Exactly the three Go test identities remain.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/bitmap` — passed.
- `cargo test -q -p tidb-util bitmap::tests --lib --locked --
  --test-threads=1` — passed; three tests ran.
- `cargo check -p tidb-util --all-targets --locked`, `cargo fmt --all --check`,
  and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: ordinary bitmap behavior is unchanged; signed invalid-input
  behavior now matches Go rather than a safer Rust policy.
- Compatibility: unused public length/index parameters change from `usize` or
  `i64` to Go-width `isize`; repository search found no production consumer.
- Performance: unchanged for valid production inputs.
