# `pkg/ddl/mock` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root and the DDL
execution guidance in `docs/agents/ddl/README.md`.

## Purpose / Big Picture

`pkg/ddl/mock` contains generated GoMock test doubles for the DDL schema
loader and system-table manager interfaces. The three-artifact package is one
atomic parity unit; its Rust test-double owner must preserve every generated
method/recorder surface and caller contract.

## Progress

- [x] Re-read current Go master
  `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`: all three artifacts and 196
  lines, including BUILD metadata and both MockGen-generated sources. No Go
  tests, fixtures, generated inputs, benchmarks, fuzz targets, examples, or
  platform variants exist; the tree is byte-identical to the historical pin.
- [x] Read the complete 15-line Rust manifest and 326-line owner, every mock
  method/recorder, trait consumer, inline test, workspace/lock registration,
  and aggregate-test behavior before editing.
- [x] Classify the four direct Go-shaped `#[must_use]` annotations on the two
  mock constructors and two `EXPECT` accessors; remove them without changing
  callback queues, verification, or trait dispatch.
- [x] Add the deny-on-discard constructor/EXPECT regression. It failed before
  the source edit with exactly four diagnostics and passes afterward.
- [x] Run the complete three-test owner suite, all-target compile, formatting,
  repository lint, and diff hygiene gates.
- [x] Update the global rolling ExecPlan, commit once for `pkg/ddl/mock`,
  rebase/push to `hparser-integration`, and verify the remote SHA.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

Only Rust caller-contract metadata and its focused regression are in scope.
Go permits discarding generated `NewMockSchemaLoader`, `NewMockManager`, and
`EXPECT` results, so Rust removes only the four matching `#[must_use]`
attributes. Mock callback ordering, drop-time verification, generated method
names, and scheduler behavior remain unchanged. No Go or Bazel artifact is
edited.

## Validation gate

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-ddl-mock --offline --locked \
      go_mock_constructor_and_expect_returns_can_be_ignored -- --nocapture

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-ddl-mock --offline --locked -- --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
      -p tidb-ddl-mock --all-targets --offline --locked

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint
    git diff --check

No Go/import/Bazel/Cargo-module file changes, new Go tests, or Go file moves
are in scope, so `make bazel_prepare` is not required.

## Surprises & Discoveries

The package is entirely generated Go test doubles but already has a complete
native Rust owner and executable scheduler tests. The only current parity gap
was Rust's stricter discard enforcement on four direct generated API
counterparts; no missing runtime behavior was found.

## Decision Log

- 2026-09-06: Treat both constructors and both `EXPECT` accessors as direct
  generated Go API counterparts and remove only their `#[must_use]` metadata.
- 2026-09-06: Exercise each return in one deny-on-discard regression while
  retaining the existing callback/verification tests for runtime behavior.
- 2026-09-06: Skip Go execution because the user requested Rust-only alignment;
  the complete Rust owner suite and Ready gates provide the scoped evidence.

## Outcomes & Retrospective

The behavior-neutral four-annotation Rust parity fix is published in one
package-scoped commit. The post-rebase remote SHA is recorded in the task
handoff; the rolling audit continues with the next complete package boundary.
