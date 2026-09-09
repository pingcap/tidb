# `pkg/ttl/session` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package batch.

## Purpose / Big Picture

`pkg/ttl/session` owns the TTL worker's internal SQL session, transaction
bracketing, time-zone reset, interruption, and session-reuse hook. The Rust
owner is `rust/crates/tidb-ttl/src/session.rs`. This batch restores the two
omitted Go interface accessors and removes a Rust-only constructor/state shape.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all five Go artifacts
      (527 lines), including BUILD metadata and every test. Confirmed no doc,
      fixture, generated/platform, benchmark, fuzz, or ownership artifact.
- [x] (2026-09-02) Read the Rust session owner and all 12 source-shaped tests;
      identified `GetSessionVars`/`GetSQLExecutor` omissions and the
      `without_avoid_reuse` Rust-only API.
- [x] (2026-09-02) Added opaque context/session handle forwarding, removed the
      optional callback constructor path, and added the focused handle-identity
      regression.
- [x] (2026-09-02) Ran the complete Rust TTL tests, Go tagged session tests,
      Rust formatting/check, repository lint, and diff hygiene; the Ready
      profile is green.
- [x] (2026-09-02) Published implementation commit
      `b3ca14f947d393671698acb7139befa543eaf665`, fetched the latest branch
      tip, fast-forward pulled, and verified matching local/tracking/remote
      SHAs.

## Scope and decisions

The atomic unit is the complete Go package: `session.go`, its three test files,
and `BUILD.bazel`. The unported Go `sessionctx`, `variable`, and `sqlexec`
types remain opaque associated-type boundaries, but every method on the Go
`Session` interface is represented. The source constructor's non-nil callback
is modeled as a required closure; no Rust-only absent-callback constructor is
retained.

## Validation gate

Run from the repository root with the pinned local toolchains:

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    OPENSSL_DIR=<pinned OpenSSL dir> DYLD_LIBRARY_PATH=<pinned OpenSSL lib> \
      cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --all-targets
    OPENSSL_DIR=<pinned OpenSSL dir> DYLD_LIBRARY_PATH=<pinned OpenSSL lib> \
      cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --tests -- --test-threads=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test -tags=intest ./pkg/ttl/session -count=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

No Go/Bazel artifact changed, so `make bazel_prepare` is not required for this
Rust-only batch.

## Decision log

- 2026-09-02: Treat the omitted interface accessors and optional callback
  constructor as one session package contract fix; both are dependency-closed
  at the existing context boundary.
- 2026-09-02: Keep live system-variable, server-backed kill, and concrete SQL
  executor behavior as explicit boundaries rather than inventing Rust server
  infrastructure.

## Outcomes and retrospective

The source-shaped session interface and constructor behavior are complete in
published commit `b3ca14f947d393671698acb7139befa543eaf665`; live server and
unported dependency surfaces remain explicit and unverified.
