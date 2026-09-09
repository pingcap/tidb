# Align Rust return contracts for `br/pkg/streamhelper/spans`

This ExecPlan is a living document maintained according to `PLANS.md` at the
repository root. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`,
and `Outcomes & Retrospective` current while this package batch proceeds.

## Purpose / Big Picture

The Rust `tidb-br::spans` module implements the complete
`br/pkg/streamhelper/spans` package. After this change, direct source-shaped
scalar and struct returns may be ignored just as they can in Go, so enabling
Rust's `unused_must_use` lint does not introduce a caller restriction absent
from the source package. The observable proof is a focused compile-time
regression plus the complete deterministic `tidb-br` test suite.

## Progress

- [x] Reuse the complete pinned Go inventory in
  `rust/testport/receipts/br_streamhelper_spans.md`, honoring the user's request
  not to reopen Go code.
- [x] Read the four Rust owner modules, shared manifest and crate root,
  workspace/lock entries, every function, all four source-derived tests, and
  all callers; confirm no fixture, generated, platform, or custom-build input.
- [x] Classify all thirteen explicit annotations: nine direct scalar/struct
  source returns and four inherent/native boundaries.
- [x] Add a focused nine-call regression and capture exactly nine pre-fix
  `unused_must_use` errors.
- [x] Remove only the nine source-shaped annotations and verify the regression
  passes.
- [x] Run all 32 active `tidb-br` tests (two unrelated skips) and the owner
  all-target build.
- [x] Complete scoped formatting, Ready repository lint, and diff review; make
  one package commit, rebase it onto the latest remote, push without force, and
  verify the remote SHA.
- [ ] Continue the rolling Rust package audit.

## Surprises & Discoveries

- Observation: `tidb-br` is a workspace leaf; `cargo tree -i tidb-br` reports
  only the crate itself, and source search finds no span caller outside the
  owning modules.
  Evidence: all direct uses are in `src/spans/{sorted,utils,value_sorted}.rs`.
- Observation: removing the explicit annotations on `String` and `Vec` returns
  would not restore Go's discardability because those standard-library types
  carry their own must-use contract.
  Evidence: `stringify_range`, `collapse`, and `full` retain their annotations
  and are excluded from the focused scalar/struct regression.

## Decision Log

- Decision: Change only the nine direct Go-shaped scalar/struct annotations;
  retain `Valued::new` as the native construction helper and retain the three
  inherent `String`/`Vec` boundaries.
  Rationale: this removes enforceable Rust-only caller diagnostics without
  pretending a redundant annotation edit can override standard-library type
  semantics or deleting a native representation helper.
  Date/Author: 2026-09-07, Codex.
- Decision: Validate the entire shared `tidb-br` crate even though the changed
  Go package owns only the span modules.
  Rationale: the crate is small, has no reverse dependency, and the full gate
  detects accidental interaction with its two sibling complete package ports.
  Date/Author: 2026-09-07, Codex.

## Milestones

The inventory milestone is complete when the four byte-unchanged span modules,
their shared Cargo integration, tests, annotations, and callers are accounted
for. The implementation milestone is complete when the deny-on-discard test
fails with nine errors before the edit and passes after only nine attributes
are removed. The publication milestone is complete when formatting, Ready
lint, and diff hygiene pass, one package commit is rebased onto the latest
`origin/hparser-integration`, pushed without force, and the remote SHA matches.

Run validation from the repository root with the bundled OpenSSL environment:

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    OPENSSL_STATIC=0 \
    DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline \
      --locked -p tidb-br --lib \
      spans::return_contract_tests::direct_source_returns_may_be_ignored_like_go \
      -- --exact --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    OPENSSL_STATIC=0 \
    DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml \
      --offline --locked -p tidb-br --no-fail-fast

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    OPENSSL_STATIC=0 \
    DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline \
      --locked -p tidb-br --all-targets

    rustfmt +nightly-2026-08-22 --edition 2021 --check \
      rust/crates/tidb-br/src/spans.rs rust/crates/tidb-br/src/spans/*.rs

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint

    git diff --check

No Go, Go import, top-level Go test, Bazel, Cargo metadata, module dependency,
or build-target input changes, so `make bazel_prepare` is not required.

## Outcomes & Retrospective

Nine direct source-shaped APIs now accept ignored results without a Rust-only
diagnostic, while span execution remains unchanged and all active shared-crate
tests pass. The package is published as one commit after the Ready gate and
remote race check; the rolling audit continues at the next Rust owner.
