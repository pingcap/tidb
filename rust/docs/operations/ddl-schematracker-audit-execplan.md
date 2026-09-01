# `pkg/ddl/schematracker` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/ddl/schematracker` maintains an in-memory schema view while DDL runs and
checks it against the real executor. This plan inventories every package
artifact and records why Rust's existing InfoStore seed cannot yet replace the
full Go tracker.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all six artifacts and all
  2,979 lines, including BUILD metadata and 17 top-level tests.
- [x] (2026-09-02) Confirmed every Go artifact is byte-identical to the pinned
  source; no Go behavior or build drift was found.
- [x] (2026-09-02) Ran the complete failpoint-aware Go suite. Rust's partial
  InfoStore test target is unregistered and remains an explicit seed boundary.
- [ ] Publish the receipt/plan in one scoped documentation commit, push to
  `hparser-integration`, pull the remote tip, and continue the audit.

## Scope and decision

The atomic unit is all six schematracker artifacts. The Go package owns the
full DDL tracker/checker contract; Rust's `tidb-exec` owns only a narrow
InfoStore seed with documented omissions. Do not add a partial Rust tracker or
change generated/build inputs without first closing the DDL/session dependency
graph.

## Validation gate

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/ddl/schematracker -count=1
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    git diff --check

The Go suite and shared formatting/diff checks must pass. A direct Rust test
target command is expected to fail because the source file is not listed in
`crates/tidb-exec/Cargo.toml`; this is evidence of the seed boundary, not a
Go regression.

## Surprises & Discoveries

Rust documentation accurately identifies that `dm_tracker.go` calls roughly
forty unported DDL/parser/session functions and that `checker.go` requires the
live DDL executor and infoschema. The Go source itself needs no parity edit.

## Decision Log

- 2026-09-02: Keep the Go package unchanged because its six artifacts already
  match Go master exactly.
- 2026-09-02: Preserve Rust's InfoStore seed and report the package as an
  explicit boundary until a complete dependency-closed tracker exists.

## Outcomes & Retrospective

The package is inventory-complete and Go-test-verified. No Rust-only behavior
was removed and no speculative DDL implementation was added; the next safe
step is closing the documented dependency graph before any transcreation claim.
