# `pkg/domain/crossks` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every cross-keyspace production, test, support, fixture, generated,
platform, and build artifact; restore missing Go-master server-info lifecycle
behavior; and prove runtime/DDL integration with focused regressions.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all eight artifacts and
  2,094 lines, including all 61 production declaration lines and eight
  top-level tests.
- [x] (2026-09-02) Restored server-info cleanup/revocation on failed bootstrap
  and manager close, the min-job-ID refresher control seam, the test accessor,
  and the close/bootstrap regression cases. Four test-only protobuf literals
  retain this branch's older `kvproto` field spelling.
- [x] (2026-09-02) Focused cleanup regressions and the complete failpoint-aware
  package suite passed; failpoints were disabled by the wrapper afterward.
- [ ] Run the required `make bazel_prepare` gate (blocked locally because
  `bazel` is not installed), publish one batch commit with serverinfo, pull the
  remote tip, and continue the rolling audit.

## Scope and decision

`pkg/domain/crossks` owns cross-keyspace session/runtime lifecycle and DDL
submit-only integration. It depends on Go-native server-info leases and etcd
schema sync; Rust has no dependency-closed domain owner, so no speculative Rust
implementation is added.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/domain/crossks -count=1
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
make bazel_prepare
```

The Bazel gate is mandatory because Go test support changed and the serverinfo
dependency/build shape changed; the local executable is absent.

## Outcome

The complete inventory, exact Go-master hashes, and explicit Go-only boundary
are recorded in `rust/testport/receipts/domain_crossks.md`. Publication and
remote synchronization remain before the next package audit.
