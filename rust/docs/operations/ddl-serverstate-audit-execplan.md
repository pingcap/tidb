# `pkg/ddl/serverstate` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root and the DDL
execution guidance in `docs/agents/ddl/README.md`.

## Purpose / Big Picture

`pkg/ddl/serverstate` synchronizes TiDB's smooth-upgrade state through etcd
and provides the process-global in-memory implementation used by unistore.
The four-artifact Go package is one atomic parity unit; its Rust owner,
watch/retry behavior, source-derived tests, and live-etcd carrier must remain
aligned at the current Go master revision.

## Progress

- [x] Re-read current Go master `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`:
  all four artifacts and 454 lines, including BUILD metadata, production
  declarations, the helper, and `TestStateSyncerSimple`. No generated,
  platform-specific, fixture, benchmark, fuzz, example, or extra build-input
  artifact exists; the tree is byte-identical to the historical pin.
- [x] Read the complete 26-line Rust manifest and 1,021-line owner, every
  production/private function, inline test, aggregate-test registration,
  workspace/lock entry, and direct caller seam before editing.
- [x] Classify all three explicit constructor `#[must_use]` annotations as
  direct Go API counterparts and remove them; no runtime implementation or
  Rust-only helper behavior changes.
- [x] Add the deny-on-discard constructor regression. It failed before the
  source edit with exactly three diagnostics and passes afterward.
- [x] Run the complete owner tests, all-target compile, formatting, repository
  lint, and diff hygiene gates.
- [x] Update the package receipt, this plan, the b110 cross-reference, and the
  global rolling ExecPlan.
- [x] Commit once for `pkg/ddl/serverstate`, rebase/push to
  `hparser-integration`, and verify the remote SHA.
- [ ] Continue the rolling audit with the next complete package boundary.

## Scope and decision

Only Rust caller-contract metadata and its focused regression are in scope.
Go permits discarding `NewStateInfo`, `NewEtcdSyncer`, and `NewMemSyncer`
results, so Rust removes only the corresponding `#[must_use]` attributes.
State initialization, etcd sessions, retries, watches, cancellation,
serialization, metrics, process-global memory state, and failpoint behavior
remain unchanged. The live-PD test stays an explicit ignored integration gate.

## Validation gate

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-ddl-serverstate --offline --locked \
      go_constructor_return_values_can_be_ignored -- --nocapture

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
      -p tidb-ddl-serverstate --offline --locked -- --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install \
    OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target \
    cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
      -p tidb-ddl-serverstate --all-targets --offline --locked

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
      GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
      TMPDIR=/tmp/tidb-codex make lint
    git diff --check

No Go/import/Bazel/Cargo-module file changes, new Go tests, or Go file moves
are in scope, so `make bazel_prepare` is not required.

## Surprises & Discoveries

The complete Rust owner already covered all state-sync runtime behavior and
the Go live-etcd test as an ignored carrier. The only current parity gap was
Rust's stricter discard enforcement on the three direct constructors; no
missing runtime implementation was found.

## Decision Log

- 2026-09-06: Treat the three constructors as callable Go API counterparts;
  remove their `#[must_use]` annotations while retaining no unrelated Rust
  contract changes.
- 2026-09-06: Use the existing loopback `EtcdClient` constructor in one
  deny-on-discard test so all three returns are checked without requiring a
  live etcd process.
- 2026-09-06: Skip Go execution and the ignored live-PD integration in this
  Rust-only follow-up; the full Rust owner suite, all-target check, and Ready
  lint/format gates are proportional.

## Outcomes & Retrospective

The behavior-neutral three-annotation Rust parity fix is published in one
package-scoped commit. The post-rebase remote SHA is recorded in the task
handoff; the rolling audit continues with the next complete package boundary.
