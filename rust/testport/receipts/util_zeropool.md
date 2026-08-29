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
translations. The `BenchmarkSyncPoolValue` translation type-erases each value
and allocates a fresh box on every `Put`, preserving the allocation behavior
that benchmark exists to contrast; the old concrete `Vec` pool silently
removed that source behavior. The audit removed four supplemental Rust tests
with no Go equivalent; the remaining test is exactly `TestPool` and retains
all four Go subtest behaviors.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/zeropool` — passed.
- `cargo test -q -p tidb-util zeropool::tests::TestPool --lib --locked -- --exact --test-threads=1` — passed (the one source-owned test and all four subtests).
- `cargo check -p tidb-util --all-targets --locked` — passed, including all
  four benchmark translations.
- `cargo bench --offline --locked -p tidb-util --bench zeropool` — ran all four
  translated workloads.
- `cargo clippy --offline --locked -p tidb-util --bench zeropool --no-deps -- -A clippy::needless-borrows-for-generic-args -A clippy::chunks-exact-to-as-chunks -A clippy::new-without-default -D warnings` — passed.
- `cargo fmt --all --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production and source-owned test behavior are unchanged; the
  comparative value-pool benchmark now measures the source workload.
- Compatibility: no production API or source-owned test changed; the earlier
  audit removed only internal supplemental tests.
- Performance: production is unchanged. Only the intentionally allocating
  comparison benchmark becomes slower and representative of Go.
