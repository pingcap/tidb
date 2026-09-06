# `pkg/statistics` builder parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root. The package is
the direct `pkg/statistics` boundary; nested `asyncload`, `handle`, and `util`
packages retain separate atomic receipts.

## Purpose / Big Picture

Keep the Rust `tidb-stats` builder surface behaviorally aligned with Go
`origin/master` while preserving the source package's complete inventory.
This follow-up addresses only return-value consumption contracts: Go callers
may discard the four builder results, so equivalent Rust APIs must not impose
Rust-only `unused_must_use` failures.

## Progress

- [x] Re-read the complete direct Go package at
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`: 33 tracked artifacts (15
  production and 16 test/benchmark/fuzz Go files, plus BUILD/OWNERS) and 13,905
  direct lines, plus two JSON fixtures totaling 71 lines. This includes
  production files, every test/benchmark/fuzz file, BUILD/OWNERS metadata,
  and fixture inputs/outputs; no direct generated/platform/example artifact
  exists.
- [x] Read the complete Rust `tidb-stats` builder owner, public/private
  functions, all-target registration, direct callers, and existing source
  tests before editing.
- [x] Classify the explicit return contracts: remove `#[must_use]` from
  `SortedHistogramBuilder::{new,histogram}`,
  `SequentialRangeChecker::from_ranges`, and `build_column`; retain the
  Rust-only `count` and `from_ranges_in_place` annotations.
- [x] Add a focused deny-on-discard regression. The pre-fix source emitted
  exactly four diagnostics; the edited source passes.
- [x] Run the full `tidb-stats` owner suite, all-target compile, formatting,
  Ready lint, and diff hygiene.
- [x] Commit this package-scoped change once and publish it to
  `origin/hparser-integration`; the remote SHA is reported in the task
  handoff.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

Only Rust source attributes, one Rust source regression, and parity evidence
change. The four removed attributes are direct counterparts of Go APIs whose
results are routinely discardable. `SortedHistogramBuilder::count` and
`SequentialRangeChecker::from_ranges_in_place` are Rust-only conveniences (the
latter preserves Go's in-place sorting adaptation) and remain `#[must_use]`.
No histogram arithmetic, TopN selection, sample ordering, allocation,
concurrency, error, or fixture behavior changes. No Go, Bazel, Cargo metadata,
or dependency file changes are in scope.

## Validation gate

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats --offline --locked --test all go_builder_returns_may_be_ignored_like_go -- --nocapture
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml -p tidb-stats --offline --locked --no-fail-fast
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-stats --all-targets --offline --locked
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
git diff --check
```

No Go tests are run for this Rust-only alignment batch. No failpoint or
`make bazel_prepare` gate applies because no Go source/import, Bazel, module,
or generated metadata changed.

## Surprises & Discoveries

The builder implementation already matched Go's runtime behavior. The only
observed gap was four stricter Rust discard diagnostics on source-shaped
constructors/accessors; two Rust-only helper annotations were correctly
retained after the inventory.

## Decision Log

- 2026-09-06: Treat the direct `pkg/statistics` root as one inventory unit,
  while leaving nested packages to their own receipts.
- 2026-09-06: Remove only four direct Go-shaped `#[must_use]` attributes and
  prove the contract with a deny-on-discard source regression.
- 2026-09-06: Skip Go execution and live SQL integration in this Rust-only
  follow-up; the complete Rust owner and Ready gates provide proportional
  validation.

## Outcomes & Retrospective

The intended outcome is a behavior-neutral builder return-contract fix in one
package-scoped commit, with the remote publication SHA recorded in the final
task handoff while the rolling audit continues.
