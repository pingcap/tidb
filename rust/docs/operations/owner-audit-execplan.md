# `pkg/owner` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/owner` coordinates TiDB's etcd-backed owner elections, listener
notifications, and distributed locks. This audit verifies the complete source,
test, build, and ownership-policy surface against Go master and restores the
repository approver routing without changing election behavior.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all eight artifacts and
  all 1,883 Go-master lines, including failpoint/goleak tests and BUILD data.
- [x] (2026-09-02) Confirmed all Go source, tests, and BUILD metadata match;
  restored the five-line Go-master OWNERS filter.
- [x] (2026-09-02) Ran the full failpoint-aware owner suite and shared Ready
  gates; all passed and failpoints returned to refcount zero.
- [ ] Publish one scoped metadata commit, push to `hparser-integration`, pull
  the remote tip, and continue the rolling audit.

## Scope and decision

The atomic unit is the complete owner package, including OWNERS policy. Owner
election and lock semantics are Go-native etcd/metrics/session infrastructure;
Rust has no dependency-closed replacement. Only metadata was changed.

## Validation gate

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/owner -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    make lint
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    git diff --check

The package suite and all shared checks must pass. Bazel preparation is
unnecessary because no Go/Bazel source changed.

## Surprises & Discoveries

The owner implementation already matches current Go master byte-for-byte,
including 12 etcd/failpoint tests. The only drift was ownership routing in
`OWNERS`; restoring it does not affect generated or runtime artifacts.

## Decision Log

- 2026-09-02: Restore OWNERS exactly from pinned Go master rather than leave a
  package-level policy mismatch.
- 2026-09-02: Do not add Rust owner-election code because no dependency-closed
  etcd/DDL listener owner exists.

## Outcomes & Retrospective

The complete owner package is inventory-complete and runtime-verified. Go
master metadata is restored, with no Rust behavior removed or duplicated and no
runtime risk introduced.
