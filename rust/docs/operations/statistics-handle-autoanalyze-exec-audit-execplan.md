# `pkg/statistics/handle/autoanalyze/exec` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/statistics/handle/autoanalyze/exec` executes ANALYZE statements with
current-session, snapshot, partition-prune, statistics-version, and process-
tracking options. The complete Go package is one atomic parity unit; its Rust
owner must keep both runtime behavior and caller contracts aligned.

## Progress

- [x] Re-read all three Go artifacts at current master
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`: 387 lines, three production/
  test/BUILD artifacts, three integration tests, and no fixtures, generated
  inputs/outputs, benchmarks, examples, fuzz targets, or platform variants.
- [x] Read the complete 617-line Rust owner and manifest; inventory direct
  callers in the priority-queue, refresher, and server owners.
- [x] Classify the two explicit `#[must_use]` annotations as direct Go API
  counterparts (`AutoAnalyze` and `ParseAutoAnalyzeRatio`).
- [x] Add an executable deny-on-discard regression. It failed before the
  source edit with exactly two diagnostics; remove both annotations and verify
  the regression passes.
- [x] Run the complete six-test owner suite and all-target compile.
- [x] Run workspace formatting, repository lint, and diff hygiene.
- [x] Commit once for this Go package, rebase/push to `hparser-integration`,
  and verify the remote SHA.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

Only the Rust caller contract changes. Go allows discarded results from both
functions, so Rust removes the two `#[must_use]` attributes while preserving
ANALYZE execution, process-ID guards, tracking callbacks, panic recovery,
metrics, logging, warning escaping, and ratio/window parsing. No Go or Bazel
file, Cargo metadata, or dependency changes are needed.

## Validation gate

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-stats-handle-autoanalyze-exec --lib \
      source_return_values_may_be_ignored_like_go --offline --locked -- --nocapture

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-stats-handle-autoanalyze-exec --offline --locked -- --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
      -p tidb-stats-handle-autoanalyze-exec --all-targets --offline --locked

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint
    git diff --check

No Go/import/Bazel/Cargo-module file changes, new Go tests, or Go file moves are
in scope, so `make bazel_prepare` is not required.

## Surprises & Discoveries

The package had a complete Rust behavioral owner and five source-derived tests,
but two direct public functions still had Rust-only discard enforcement. The
older receipt's WIP note about a priority-queue asynchronous lifecycle failure
is outside this package and remains unchanged.

## Decision Log

- 2026-09-06: Remove only the two annotations whose docs name callable Go
  functions; keep no unrelated Rust contract changes.
- 2026-09-06: Use the existing mock restricted executor in the regression so
  `AutoAnalyze` executes its real option/process-release path.
- 2026-09-06: Skip Go/live-server execution in this Rust-only follow-up while
  retaining the complete Rust owner tests and all-target compile as gates.

## Outcomes & Retrospective

Pending publication. The intended outcome is a behavior-neutral two-annotation
Rust parity fix with one package-scoped commit and remote SHA verification.
