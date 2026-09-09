# `pkg/dxf/operator` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/dxf/operator` defines the asynchronous data-channel, worker-pool, and
pipeline contracts used by DXF. The six-artifact Go package is one atomic
parity unit; its Rust owner must preserve channel closure, cancellation,
worker lifecycle, ordering, and caller return contracts.

## Progress

- [x] Re-read current Go master
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`: all six artifacts and 581 lines,
  including BUILD metadata, four production files, and the complete pipeline
  test. No fixtures, generated inputs, benchmarks, fuzz targets, examples,
  or platform variants exist; the tree is byte-identical to the historical
  pin.
- [x] Read the complete Rust owner (`compose.rs`, `operator.rs`, `pipeline.rs`,
  `wrapper.rs`, `lib.rs`, and `pipeline_test.rs`), every public/private
  function and caller, and workspace registration before editing.
- [x] Classify the four direct Go-shaped `#[must_use]` annotations on
  `AsyncPipeline::new`, `SimpleDataSource::new`, `SimpleSink::new`, and
  `SimpleOperator::new`; remove them without changing runtime behavior.
- [x] Add the deny-on-discard constructor regression. It failed before the
  source edit with exactly four diagnostics and passes afterward.
- [x] Run the complete two-test owner suite, all-target compile, formatting,
  repository lint, and diff hygiene gates.
- [x] Update the global rolling ExecPlan, commit once for `pkg/dxf/operator`,
  rebase/push to `hparser-integration`, and verify the remote SHA.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

Only Rust caller-contract metadata and its focused regression are in scope.
Go permits discarding the four constructor results, so Rust removes only the
matching `#[must_use]` attributes. Channel capacity/closure, context
cancellation, worker-pool operation, pipeline open/close ordering, and error
propagation remain unchanged. No Go or Bazel artifact is edited.

## Validation gate

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-dxf-operator --offline --locked \
      go_constructor_return_values_can_be_ignored -- --nocapture

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-dxf-operator --offline --locked -- --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
      -p tidb-dxf-operator --all-targets --offline --locked

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint
    git diff --check

No Go/import/Bazel/Cargo-module file changes, new Go tests, or Go file moves
are in scope, so `make bazel_prepare` is not required.

## Surprises & Discoveries

The complete Rust owner already uses the canonical resource-manager worker
pool and has executable pipeline success/error coverage. The only current
parity gap was Rust's stricter discard enforcement on four direct constructor
counterparts; no missing runtime behavior was found.

## Decision Log

- 2026-09-06: Treat all four pipeline/source/sink/operator constructors as
  callable Go API counterparts and remove only their `#[must_use]` metadata.
- 2026-09-06: Exercise public and package-private constructors together in a
  single deny-on-discard regression while retaining the full pipeline test for
  runtime behavior.
- 2026-09-06: Skip Go execution because the user requested Rust-only
  alignment; the complete Rust owner suite and Ready gates provide scoped
  evidence.

## Outcomes & Retrospective

The behavior-neutral four-annotation Rust parity fix is published in one
package-scoped commit. The post-rebase remote SHA is recorded in the task
handoff; the rolling audit continues with the next complete package boundary.
