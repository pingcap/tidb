# `pkg/domain/sqlsvrapi` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/domain/sqlsvrapi` defines the public SQL-server/runtime interfaces used by
TiDB's domain and keyspace code. This audit records the complete interface and
BUILD surface and verifies that the companion generated mocks still compile.
No Rust facade is introduced because the contract spans Go KV, metadata, DDL,
owner, and session-pool types.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read both root artifacts (82
  lines) in full and confirmed no tests or fixtures.
- [x] (2026-09-02) Compared the root package byte-for-byte; no source or BUILD
  mismatch and no Rust-only behavior were found.
- [x] (2026-09-02) Compiled both root and nested mock packages with `go test`.
- [ ] Publish the receipt/plan boundary in one scoped commit, push to
  `hparser-integration`, pull the remote tip, and continue the audit.

## Scope and decision

The atomic root package consists of `BUILD.bazel` and `server.go`. It owns the
`Runtime`, `KSRuntimeHandle`, and `Server` interfaces, including table-mode DDL
submission semantics. Rust has no dependency-closed implementation, so the
correct action is documentation and compile validation only.

## Validation gate

Run from the repository root:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    go test ./pkg/domain/sqlsvrapi ./pkg/domain/sqlsvrapi/mock
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    git diff --check

The Go compile and shared formatting/diff checks must pass. Bazel preparation is
not required because this batch changes no Go or Bazel source.

## Surprises & Discoveries

The root API contains the newer `AlterTableMode` contract and its generated
MockGen methods, yet all six artifacts are already exactly aligned with Go
master. The nested directory is a separate importable package and therefore
receives its own receipt.

## Decision Log

- 2026-09-02: Keep both packages unchanged; generated outputs are inventory
  inputs, not hand-edit targets, and no source gap exists.
- 2026-09-02: Record root and mock packages in separate receipts while
  publishing them in one adjacent domain documentation batch.

## Outcomes & Retrospective

The root interface and generated mock package are fully inventoried and
compile-verified. They remain Go-native boundaries with no safe Rust behavior to
remove or port; future changes must regenerate mocks from the source interface.
