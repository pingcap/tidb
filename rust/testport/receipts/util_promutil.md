# `pkg/util/promutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `factory.go`,
`registry.go`, `registry_test.go`, and `BUILD.bazel`. There is no package doc,
README, fixture, benchmark, generated or platform variant, test main, or
ownership file. The local Go package is byte-identical to the pin.

The production package defines six direct-return metric factory methods, a
default factory, the Prometheus registerer surface, a registry that discards
all operations, and a fresh default registry. Its only test is
`TestNoopRegistry`.

## Rust ownership and audit result

`rust/crates/tidb-util/src/promutil/mod.rs` and `tests.rs` are the production
and test owners. Native Prometheus metric and collector types provide the
external package boundary. Where the Rust client rejects an invalid metric
descriptor during construction, the adapter panics instead of adding a
result-returning API that does not exist in Go.

The audit removed that result-returning divergence from all six `Factory`
methods and removed unused metric/error re-exports. It also removed the two
Rust-only tests for the default factory and default registry, plus the
`MustRegister` branch added to the noop-registry test. The remaining test maps
only Go `TestNoopRegistry`, including duplicate registration and unconditional
unregistration success. A strict-surface re-audit removed the two remaining
Rust-only option aliases and the `Send + Sync` supertrait restrictions that Go
does not impose on implementations of either interface.

The three source-shaped constructors (`new_default_factory`,
`new_noop_registry`, and `new_default_registry`) also carried explicit
Rust-only `#[must_use]` diagnostics. The focused
`return_values_may_be_ignored_like_go` regression discards all three under
`#[deny(unused_must_use)]`: the detached pre-fix owner failed with exactly
three diagnostics, and the corrected owner passes.

## Validation

Profile: Ready for this focused parity fix within the continuing package-by-
package audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/promutil` — passed.
- `go test ./pkg/util/promutil -run '^TestNoopRegistry$' -count=1` — passed.
- `cargo test --offline --locked -p tidb-util --lib promutil::tests --no-fail-fast` — passed, 1 test.
- `cargo check --offline --locked -p tidb-util` — passed.
- `cargo fmt -p tidb-util -- --check` and `git diff --check` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib promutil::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the three-error pre-fix failure.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib 'promutil::tests' -- --test-threads=1` — passed; 2 tests including the discard-contract regression.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed as the Ready gate.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: normal metric creation now has Go's direct-return contract;
  malformed native descriptors panic at the Rust client boundary.
- Compatibility: this intentionally removes Rust-only `Result` return values
  and unused re-exports; repository-wide search found no external callers.
- Performance: unchanged.
