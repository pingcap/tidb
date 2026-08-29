# `pkg/util/zeropool` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full: `pool.go`,
`pool_test.go`, and `BUILD.bazel`. There is no package doc, README, fixture,
generated or platform variant, or ownership file. The local Go package is
byte-identical to the pin.

Production behavior includes the valid generic zero value, optional factory,
concurrent `Get` and `Put`, move-out without retaining the returned value, and
the no-copy-after-use contract. The source has exactly one test, `TestPool`,
with four subtests, plus four benchmarks.

## Rust ownership and audit result

`rust/crates/tidb-util/src/zeropool/mod.rs` owns production and the single
source test. Rust moves `T` directly through a mutex-protected native value
pool, so Go's secondary pointer pool and interface-boxing workaround are not
needed. `Default` represents Go's valid zero value, and the absence of
`Clone`/`Copy` preserves its no-copy contract. Mutex poison is recovered
because Go mutexes do not introduce poison failures.

`rust/crates/tidb-util/benches/zeropool.rs` contains the four source benchmark
translations. The audit removed four supplemental Rust tests with no Go
equivalent; the remaining test is exactly `TestPool` and retains all four Go
subtest behaviors.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `cargo test -q -p tidb-util zeropool::tests::TestPool --lib --locked -- --exact --test-threads=1` — passed (the one source-owned test and all four subtests).
- `cargo check -p tidb-util --all-targets --locked` — passed, including all
  four benchmark translations.
- `cargo fmt --all --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production and source-owned test/benchmark behavior are
  unchanged.
- Compatibility: only internal supplemental tests were removed.
- Performance: unchanged.
