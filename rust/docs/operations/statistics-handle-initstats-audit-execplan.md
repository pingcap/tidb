# `pkg/statistics/handle/initstats` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/statistics/handle/initstats` controls bounded stats-loading concurrency,
shared progress, and range-worker task processing. The three-artifact Go
package is one atomic parity unit; its Rust owner must preserve the complete
worker behavior and caller contract.

## Progress

- [x] Re-read all three Go artifacts at current master
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`: 159 lines, no tests, fixtures,
  generated inputs/outputs, benchmarks, examples, fuzz targets, or platform
  variants.
- [x] Read the complete Rust manifest and 220-line owner, all public/private
  functions, and workspace/lock registration.
- [x] Classify four explicit annotations: retain Rust-only `AtomicF64::new`,
  remove direct Go-shaped `AtomicF64::load`, `get_concurrency`, and
  `RangeWorker::new` annotations.
- [x] Add a focused deny-on-discard regression. It failed before the source
  edit with exactly three diagnostics and passes afterward.
- [x] Run the full owner test and all-target compile, formatting, repository
  lint, and diff hygiene.
- [x] Commit once for this Go package, rebase/push to `hparser-integration`,
  and verify the remote SHA.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

Only Rust return-contract metadata and its focused regression change. Go allows
discarding the three direct results, so Rust removes only those three
`#[must_use]` attributes. Static initialization, atomic ordering, CPU policy,
worker channel capacity, task accounting, progress logging, and error handling
remain unchanged. No Go, Bazel, Cargo metadata, or dependency files change.

## Validation gate

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-stats-handle-initstats --lib \
      source_return_values_may_be_ignored_like_go --offline --locked -- --nocapture

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-stats-handle-initstats --offline --locked -- --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
      -p tidb-stats-handle-initstats --all-targets --offline --locked

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint
    git diff --check

No Go/import/Bazel/Cargo-module file changes, new Go tests, or Go file moves are
in scope, so `make bazel_prepare` is not required.

## Surprises & Discoveries

The package had no Go tests and already had a complete Rust worker owner. The
only parity gap was Rust's stricter discard enforcement on one external atomic
method and two package functions; no runtime implementation change was needed.

## Decision Log

- 2026-09-06: Retain `AtomicF64::new` because it has no callable Go
  constructor counterpart and exists only to initialize the Rust static.
- 2026-09-06: Exercise `get_concurrency` and `RangeWorker::new` in the same
  focused regression while retaining the existing constructor binding for the
  static atomic.
- 2026-09-06: Skip Go execution and live server integration in this Rust-only
  follow-up; the complete Rust owner and all-target gates are proportional.

## Outcomes & Retrospective

Pending publication. The intended outcome is a behavior-neutral three-
annotation Rust parity fix with one package-scoped commit and remote
verification.
