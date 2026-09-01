# `pkg/domain/sqlsvrapi/mock` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

The nested `mock` package supplies generated GoMock test doubles for the SQL
server/runtime interfaces. This plan verifies every generated output and its
BUILD target against Go master so downstream tests can rely on a complete
method surface.

## Progress

- [x] (2026-09-02) Read all four artifacts and all 301 lines after pinning Go
  master at `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Confirmed all generated files and BUILD metadata are
  byte-identical; no fixtures, tests, platform variants, or generated inputs
  exist.
- [x] (2026-09-02) Compiled the mock package with its parent using `go test`.
- [ ] Publish the receipt/plan boundary in one scoped commit, push to
  `hparser-integration`, pull the remote tip, and continue the audit.

## Scope and decision

The package is generated support, not a runtime implementation. Its source of
truth is `pkg/domain/sqlsvrapi/server.go`; MockGen output must be regenerated
when that interface changes. Rust has no GoMock-compatible owner, so no manual
Rust or generated-code port is appropriate.

## Validation gate

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    go test ./pkg/domain/sqlsvrapi ./pkg/domain/sqlsvrapi/mock
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    git diff --check

The parent/mock compile and shared checks must pass. Do not hand-edit generated
outputs; use the repository's MockGen workflow if the parent interface changes.

## Surprises & Discoveries

All three generated files already contain `AlterTableMode` and `Release`, so
their method surface matches the parent interfaces without regeneration.

## Decision Log

- 2026-09-02: Treat generated files as required package artifacts and preserve
  them unchanged because their pinned hashes match Go master.

## Outcomes & Retrospective

The complete generated mock boundary is recorded and compile-verified. No Rust
behavior was removed or duplicated.
