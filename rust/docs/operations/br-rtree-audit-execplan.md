# Align Rust return contracts for `br/pkg/rtree`

This ExecPlan is a living document maintained according to `PLANS.md` at the
repository root. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`,
and `Outcomes & Retrospective` current while the rolling audit continues.

## Purpose / Big Picture

The `tidb-br::rtree` owner is the complete Rust implementation of
`br/pkg/rtree`. This batch removes compiler restrictions that Go does not
impose on twelve direct scalar or struct returns. A focused deny-on-discard
test demonstrates the caller contract, while the complete `tidb-br` suite
proves range-tree behavior remains intact.

## Progress

- [x] Reuse the existing complete seven-artifact Go inventory without
  reopening Go source, per the user's Rust-only direction.
- [x] Read all three Rust owner modules (1,678 pre-edit lines), shared manifest
  and crate root, workspace/lock entries, every function, ten active tests, one
  ignored benchmark-shaped test, and all internal callers.
- [x] Confirm there is no fixture, generated/platform/custom-build surface,
  separate Rust test artifact, or reverse Cargo dependency.
- [x] Classify all twenty-nine explicit annotations as twelve direct
  scalar/struct source returns and seventeen inherent/native boundaries.
- [x] Add the focused twelve-call regression, correct its generic test payload,
  and capture exactly twelve valid pre-fix diagnostics.
- [x] Remove only the twelve direct source-shaped annotations and verify the
  focused regression passes.
- [x] Run all 33 active `tidb-br` tests (two unrelated skips) and the owner
  all-target build.
- [x] Complete Ready lint and diff review; make one package commit, rebase it
  onto the latest remote, push without force, and verify the remote SHA.
- [ ] Continue with the next complete Rust package.

## Surprises & Discoveries

- Observation: the first pre-fix regression attempt failed at generic type
  inference before linting return values.
  Evidence: adding `RangeStats<TestFile>` to the two local values produced the
  intended twelve diagnostics without changing production code.
- Observation: the sibling `restore_utils` package is the sole production
  consumer and remains inside the same leaf Cargo crate.
  Evidence: source search finds imports only in `merge.rs`, `proto.rs`, and
  `rewrite_rule.rs`; `cargo tree -i tidb-br` reports no reverse dependency.

## Decision Log

- Decision: Remove annotations from direct source-shaped scalar/struct calls,
  including the `Len` methods promoted by Go's embedded B-tree.
  Rationale: those calls are part of the Go-visible package surface and Go
  permits their results to be ignored.
  Date/Author: 2026-09-07, Codex.
- Decision: Retain annotations on inherent `Option`/`Vec`/`String` returns and
  native Rust constructors, field accessors, and `is_empty` conveniences.
  Rationale: removing them either cannot change the standard-library must-use
  behavior or does not align a direct source declaration.
  Date/Author: 2026-09-07, Codex.

## Milestones

Inventory is complete when every Rust owner line, test, annotation, Cargo
surface, and caller is accounted for against the existing package inventory.
Implementation is complete when the same twelve-call regression fails before
and passes after the annotation-only edit. Publication is complete when Ready
lint and diff hygiene pass, one package commit is rebased onto the latest
`origin/hparser-integration`, pushed without force, and the remote SHA matches.

Run these commands from the repository root with the bundled OpenSSL runtime:

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    OPENSSL_STATIC=0 \
    DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline \
      --locked -p tidb-br --lib \
      rtree::rtree::tests::direct_source_returns_may_be_ignored_like_go \
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
      rust/crates/tidb-br/src/rtree.rs rust/crates/tidb-br/src/rtree/*.rs

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint

    git diff --check

No Go/import/Bazel/Cargo metadata, dependency, generated, or build-target input
changes, so `make bazel_prepare` is not required.

## Outcomes & Retrospective

The twelve direct source-shaped returns now accept discarded results without a
Rust-only diagnostic. Runtime behavior is unchanged, the complete shared-crate
suite and owner build pass, and the batch is published as one package commit
after the Ready gate and remote race check.
