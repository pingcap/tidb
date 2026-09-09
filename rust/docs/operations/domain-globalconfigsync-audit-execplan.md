# `pkg/domain/globalconfigsync` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/domain/globalconfigsync` is the small bridge that sends TiDB global
variable changes to PD. This audit makes the complete ownership boundary
explicit and verifies that notification buffering and PD persistence still
work, without inventing a Rust replacement for a Go-native domain adapter.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all three artifacts and
  all 203 lines, including BUILD metadata and both tests.
- [x] (2026-09-02) Compared every artifact byte-for-byte; no source, test,
  fixture, generated, platform, or build mismatch was found.
- [x] (2026-09-02) Ran `go test ./pkg/domain/globalconfigsync -count=1`; both
  integration tests passed.
- [ ] Publish this receipt/plan boundary in one scoped commit, push to
  `hparser-integration`, pull the remote tip, and continue the rolling audit.

## Scope and decision

The atomic unit is the complete package: its one production file, one test
file, and BUILD target. It is Go-native because it depends on TiDB sessions,
OpenCensus lifecycle, mockstore, and PD global-config APIs. No dependency-closed
Rust owner exists, so the correct parity action is documentation and validation
only.

## Validation gate

Run from the repository root:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    go test ./pkg/domain/globalconfigsync -count=1
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    git diff --check

The Go package test must pass. The Rust formatting and diff checks are shared
receipt gates. Bazel preparation is not required for this documentation-only
change; if the repository gate is attempted, the local environment reports the
known missing `bazel` executable.

## Surprises & Discoveries

The package is already byte-identical to the pinned Go master despite adjacent
domain packages having active parity gaps. Its second test exercises a real
mockstore/session/PD path and requires the existing OpenCensus and etcd cleanup
harness.

## Decision Log

- 2026-09-02: Keep the package unchanged because the complete Go source and
  test/build artifacts already match the authority; add no speculative Rust
  facade or duplicate global-config implementation.

## Outcomes & Retrospective

The package boundary is now inventory-complete and test-verified. The existing
Go implementation remains the sole owner of PD global-config synchronization,
with no Rust-only behavior to remove and no safe missing behavior to port.
