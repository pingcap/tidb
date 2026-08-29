# `pkg/util/fastrand` — complete package transcreation

Pinned TiDB source: `e2788410d8d696605e8cb002585877a063ccc909`.
Runtime boundary: Go `go1.25.10`, as declared by the pinned repository.

## Complete inventory

The package has exactly five artifacts, all read in full: `random.go`,
`runtime.go`, `random_test.go`, `main_test.go`, and `BUILD.bazel`. There is no
package doc, fixture, generated input/output, platform file, README, or
ownership file. The local TiDB package is byte-identical to the pin.

Production behavior comprises the private 64-bit `wyrand`, ASCII buffer
generation excluding NUL and `$`, multiply-high `Uint32N`, power-of-two-aware
`Uint64N`, and `Uint32` linked to `runtime.cheaprand`. The package has one unit
test and four parallel benchmarks. `TestMain` only installs common Go test
state and goleak exclusions; it contains no package behavior.

Because `runtime.go` links outside TiDB, the official Go 1.25.10
`src/runtime/rand.go` implementation of `cheaprand` was also read before the
Rust runtime boundary was changed. It uses 64-bit `wyrand` on native
64-bit-multiply targets and the source xorshift64+ step on 32-bit targets.

## Rust ownership and audit result

`rust/crates/tidb-util/src/fastrand/` is the sole owner. `random.rs` preserves
the package's exact wrapping arithmetic and reduction formulas. `runtime.rs`
uses thread-local state as the native per-runtime-thread equivalent of Go's
per-M `cheaprand` state and keeps initialization infallible.

The audit retained the existing correct 64-bit runtime algorithm and added
the missing 32-bit xorshift branch, including native-endian state-word order.
The source has no deterministic runtime-seed API, so Rust does not expose one.

The inline suite now contains only the exact `TestRand` translation. Four
supplemental deterministic-vector, alphabet, zero-bound, and thread-local
tests absent from Go were removed. `benches/fastrand.rs` retains executable
translations of all four source benchmarks and no additional cases. Existing
statistics, password-salt, trace-event, selection, memory, server, and
statement-context consumers continue to use the canonical package owner.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/fastrand` — passed.
- `go test ./pkg/util/fastrand -count=1` — passed, 1 test.
- `cargo test --offline --locked -p tidb-util --lib fastrand::random::tests::test_rand -- --exact` — passed, 1 test.
- `cargo bench --offline --locked -p tidb-util --bench fastrand --no-run` — passed.
- `cargo fmt -p tidb-util -- --check` and `git diff --check` — passed.

Only `aarch64-apple-darwin` is installed locally, so the new 32-bit branch was
reviewed against Go 1.25.10 source but not cross-compiled locally. No Go or
Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the source test passes on both sides and the two runtime
  algorithms follow the declared Go toolchain. The 32-bit branch lacks a local
  cross-compilation gate because no 32-bit target is installed.
- Compatibility: removes only package-local supplemental tests; all public
  function signatures and in-tree consumers are unchanged.
- Performance: 64-bit production code is unchanged. The added 32-bit branch
  is the same fixed arithmetic step as Go and introduces no lock or policy.
