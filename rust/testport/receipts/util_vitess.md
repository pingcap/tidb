# `pkg/util/vitess` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All four pinned artifacts were read in full: `vitess_hash.go`,
`vitess_hash_test.go`, `main_test.go`, and `BUILD.bazel`. The package has one
production function, one unit test, one common-test/goleak harness, and one
Bazel library/test pair. It has no package doc, README, fixture, benchmark,
generated file, platform variant, or ownership file. The checkout is
byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/vitess.rs` is the sole owner. `hash_uint64` performs
one DES block encryption over the big-endian input with an all-zero key and
decodes the ciphertext as big-endian, matching Go. Rust returns the value
directly because fixed-width DES block encryption cannot fail; Go's returned
error is always nil after its package initializer successfully creates the
fixed-width cipher.

The audit removed the Rust-only expanded package narrative, `must_use` API
policy, named null-key constant, supplemental boundary-vector test, and its
four non-source cases. Only the minimal module-export documentation required
by the Rust crate lint remains. The remaining test is the exact five-row
`TestVitessHash` translation.

## Validation

Profile: WIP; this is one package checkpoint in the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/vitess` — passed.
- `go test ./pkg/util/vitess -count=1` — passed (one source test).
- `cargo test -p tidb-util --lib --locked vitess::tests::test_vitess_hash -- --exact` — passed (one source test).
- `cargo check -p tidb-util --lib --locked` — passed without warnings.
- `cargo check -p tidb-expr --lib --locked` — passed.
- `rustfmt --edition 2021 --check crates/tidb-util/src/vitess.rs` and
  `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the source's five known ciphertexts cover the exact algorithm,
  byte order, key, and maximum input.
- Compatibility: removes only Rust-only test/documentation policy; the public
  function and all production consumers are unchanged.
- Performance: production encryption and one-time cipher initialization are
  unchanged.
