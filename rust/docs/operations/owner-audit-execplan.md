# `pkg/owner` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/owner` coordinates TiDB's etcd-backed owner elections, listener
notifications, distributed locks, and the local-store mock. The package is one
atomic Go-to-Rust parity unit: all source, tests, metadata, Rust owners, and
validation evidence must be reviewed before a package commit is published.

## Progress

- [x] Recheck current Go master
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; read all eight artifacts and
  all 1,883 lines. Production, tests, and BUILD remain identical to the
  historical pin; only `OWNERS` changed.
- [x] Read all five crate-local Rust artifacts, the shared aggregate-test build
  script, workspace/lock registration, generated-output shape, public and
  private functions, and all eleven source-derived tests.
- [x] Classify all fourteen explicit Rust `#[must_use]` annotations. Seven map
  directly to discardable Go calls; seven belong to Rust-only context,
  encoding, or global-state adapters and remain.
- [x] Add one focused aggregate regression. It failed before the source edit
  with exactly seven diagnostics; remove the seven direct Go-shaped
  annotations and verify the regression passes.
- [x] Run the complete twelve-test owner suite and all-target compile.
- [x] Complete shared Ready formatting, repository lint, and diff-hygiene
  gates; update the package receipt and global ExecPlan.
- [x] Commit once for `pkg/owner`, rebase/push to `hparser-integration`, and
  verify the remote SHA.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

The implementation owner is `rust/crates/tidb-owner`; its ordinary production
adapter is `tidb-pd-client::EtcdClient`. This batch changes only a Rust compile-
time caller contract: Go accepts ignored results for the seven direct APIs, so
Rust must not reject those same calls under `unused_must_use`. Election,
session, watcher, listener, lock, operation-value, and mock-state execution are
unchanged.

`OpType::from_byte`, `OpType::as_byte`, the three `Context` constructors,
`MockManager::global_state`, and private `mock_owner_op_value` remain annotated
because they are Rust-owned helpers rather than callable declarations in Go
`pkg/owner`.

## Validation gate

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-owner --test all source_return_values_may_be_ignored_like_go \
      --offline --locked -- --nocapture

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-owner --offline --locked -- --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
      -p tidb-owner --all-targets --offline --locked

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint
    git diff --check

No Go/import/Bazel/Cargo-module file changes, new Go tests, or Go file moves are
in scope, so `make bazel_prepare` is not required.

## Surprises & Discoveries

The older plan predated the completed Rust owner crate and incorrectly said no
dependency-closed Rust owner existed. The receipt and current tree show a full
owner implementation with eleven source-derived tests. The current Go master
delta is only the already-accounted-for `OWNERS` routing expansion.

## Decision Log

- 2026-09-06: Preserve Rust-only helper annotations rather than treating every
  crate annotation as a Go API mismatch.
- 2026-09-06: Reuse the existing deterministic `FakeStore` in the aggregate
  test so the return-contract regression covers constructors without adding a
  second owner-store scaffold.
- 2026-09-06: Do not rerun the live-PD or Go failpoint suite for a compile-time-
  only Rust annotation change; the full Rust owner suite and all-target build
  are the proportional runtime/compile gates.

## Outcomes & Retrospective

The owner package remains behaviorally unchanged while all seven direct
Go-shaped APIs now accept ignored results. The focused regression establishes
the caller contract, and the package remains one inventory-complete commit.
