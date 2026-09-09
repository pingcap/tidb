# Align Rust return contracts for `br/pkg/restore/utils`

This ExecPlan is a living document maintained according to `PLANS.md` at the
repository root. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`,
and `Outcomes & Retrospective` current while the rolling audit continues.

## Purpose / Big Picture

The `tidb-br::restore_utils` owner is the complete Rust implementation of
`br/pkg/restore/utils`. This package batch removes fifteen compiler
restrictions absent from the direct Go API. A focused deny-on-discard test
demonstrates the caller contract, while the complete `tidb-br` suite protects
the rewrite-rule, key-transform, time-filter, and range-merging behavior.

## Progress

- [x] Reuse the original complete eight-artifact Go package inventory without
  reopening Go source, per the user's Rust-only direction.
- [x] Read all six Rust owner modules (2,285 pre-edit lines), the shared
  manifest and crate root, workspace/lock entries, every function, seventeen
  active source-derived tests, one ignored benchmark-shaped test, and all
  internal callers.
- [x] Confirm there is no fixture, generated/platform/custom-build surface,
  feature, separate Rust test artifact, or reverse Cargo dependency.
- [x] Classify all twenty-five explicit annotations as fifteen direct source
  returns and ten generated/error/`Option` boundaries.
- [x] Add the focused fifteen-call regression and capture exactly fifteen
  valid pre-fix `unused_must_use` diagnostics.
- [x] Remove only the fifteen direct source-shaped annotations and verify the
  focused regression passes.
- [x] Run all 34 active `tidb-br` tests (two unrelated skips), the owner
  all-target build, and scoped formatting.
- [x] Complete Ready lint and diff review; make one package commit, rebase it
  onto the latest remote, push without force, and verify the remote SHA.
- [ ] Continue with the next complete Rust package.

## Surprises & Discoveries

- Observation: the entire restore-utils owner is byte-identical to its
  original complete landing at `e40dbe9f6a7a41f910450b9874fdfddeaacc484c`.
  Evidence: a path-scoped `git diff --exit-code` against that commit is empty.
- Observation: `tidb-br` is a workspace leaf and all production interaction
  with `restore_utils` stays inside that crate.
  Evidence: `cargo tree -i tidb-br` reports only the crate itself; source
  search finds the sibling `rtree` relationship but no external caller.

## Decision Log

- Decision: Remove annotations from the fifteen direct source declarations:
  the five ID/key helpers, three `RewriteRules` methods, two empty values,
  three rule constructors, raw-key encoding, and table-ID lookup.
  Rationale: Go permits every result to be discarded, and the annotations
  imposed an additional Rust-only caller error without protecting runtime
  behavior.
  Date/Author: 2026-09-07, Codex.
- Decision: Retain the five local protobuf accessor annotations, the four
  normalized-error boundary annotations, and `FindMatchedRewriteRule`'s
  `Option` contract.
  Rationale: the first nine belong to explicit Rust representation boundaries;
  the last is already enforced by Rust's `Option` type and deleting the
  function attribute would not make the result discardable.
  Date/Author: 2026-09-07, Codex.

## Milestones

Inventory is complete when every Rust owner line, function, test, annotation,
Cargo surface, and caller is accounted for against the inherited complete
package inventory. Implementation is complete when the same fifteen-call
regression fails before and passes after the annotation-only edit. Publication
is complete when Ready lint and diff hygiene pass, one package commit is
rebased onto the latest `origin/hparser-integration`, pushed without force,
and the remote SHA matches.

Run these commands from the repository root with the bundled OpenSSL runtime:

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
    OPENSSL_STATIC=0 \
    DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline \
      --locked -p tidb-br --lib \
      restore_utils::return_contract_tests::direct_source_returns_may_be_ignored_like_go \
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
      rust/crates/tidb-br/src/restore_utils.rs \
      rust/crates/tidb-br/src/restore_utils/*.rs

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint

    git diff --check

No Go/import/Bazel/Cargo metadata, dependency, generated, or build-target input
changes, so `make bazel_prepare` is not required.

## Outcomes & Retrospective

Fifteen direct source-shaped APIs now accept ignored results without a
Rust-only diagnostic. Runtime behavior is unchanged, the focused contract,
complete shared-crate suite, owner build, formatting, Ready lint, and diff
hygiene pass, and the package is published as one remote-verified commit. The
rolling audit continues at the next Rust owner.
