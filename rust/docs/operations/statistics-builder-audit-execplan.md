# `pkg/statistics` builder parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root. The package is
the direct `pkg/statistics` boundary; nested `asyncload`, `handle`, and `util`
packages retain separate atomic receipts. The original builder-only follow-up
is complete; the active continuation below audits the remaining direct
return-contract surface.

## Purpose / Big Picture

Keep the Rust `tidb-stats` direct `pkg/statistics` surface behaviorally
aligned with Go `origin/master` while preserving the source package's complete
inventory. This continuation addresses return-value consumption contracts:
Go callers may discard ordinary direct results, so equivalent Rust APIs must
not impose Rust-only `unused_must_use` failures. Native `Option`/`Result`
boundaries and genuinely Rust-only helpers remain annotated.

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
- [x] Classify the direct root return contracts across analysis policy/jobs,
  scalar arithmetic and estimators, FM/CMS/TopN, histogram, column/index/
  table/status/memory, and sampling. Remove only ordinary Go-shaped
  diagnostics; retain `Option`/`Result` and Rust-only helper annotations.
- [x] Add focused deny-on-discard regressions for the direct return surface.
  Restoring the annotations emitted exactly 162 diagnostics; the edited
  source passes all 52 focused tests.
- [x] Run the full `tidb-stats` owner suite (302 tests) and all-target compile.
- [ ] Run Ready lint, update the package receipt and root ExecPlan, then make
  one package-scoped commit and publish it to `origin/hparser-integration`.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

Only Rust source attributes, source-derived regressions, and parity evidence
change. The removed annotations are direct counterparts of Go APIs whose
ordinary results are routinely discardable. Rust-only builder buffers,
count/accessor summaries, hash/counter/query helpers, TopN metadata,
stable-map helpers, quota APIs, and weighted-reservoir internals remain
annotated, as do `Option`/`Result` boundaries. No histogram arithmetic, TopN
selection, sample ordering, allocation, concurrency, error, or fixture
behavior changes. No Go, Bazel, Cargo metadata, or dependency file changes
are in scope.

## Validation gate

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats --test all go_ -- --nocapture --test-threads=1
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats --no-fail-fast
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats --all-targets
git diff --name-only -- 'rust/crates/tidb-stats/**/*.rs' 'rust/crates/tidb-stats/*.rs' | xargs rustfmt +nightly-2026-08-22 --edition 2021 --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
git diff --check
```

No Go tests are run for this Rust-only alignment batch. No failpoint or
`make bazel_prepare` gate applies because no Go source/import, Bazel, module,
or generated metadata changed.

## Surprises & Discoveries

The builder implementation and the broader direct statistics owner already
matched Go's runtime behavior. The observed gaps were Rust-only discard
diagnostics on ordinary source-shaped constructors, accessors, scalar
calculations, and collection/value returns. The 162-diagnostic pre-fix probe
also confirmed that the retained native `Option`/`Result` and helper
annotations are the correct boundary.

## Decision Log

- 2026-09-06: Treat the direct `pkg/statistics` root as one inventory unit,
  while leaving nested packages to their own receipts.
- 2026-09-07: Extend the return-contract audit from the builder cluster to the
  complete direct statistics root. Remove only ordinary Go-shaped
  `#[must_use]` attributes and prove the contract with a deny-on-discard
  source regression (162 diagnostics before, 0 after).
- 2026-09-06: Skip Go execution and live SQL integration in this Rust-only
  follow-up; the complete Rust owner and Ready gates provide proportional
  validation.

## Outcomes & Retrospective

The intended outcome is a behavior-neutral direct `pkg/statistics`
return-contract fix in one package-scoped commit, with the remote publication
SHA recorded in the final task handoff while the rolling audit continues.
