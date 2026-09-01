# `pkg/parser/util` parity audit ExecPlan

## Objective

Inventory the complete parser utility package, compare escape and hashing
contracts with Rust owners, and record a dependency-closed parity result.

## Completed

- Read all four pinned artifacts (152 lines, one production function, one
  test, and the eleven-method interface contract).
- Verified the hparser branch is byte-identical to Go master.
- Confirmed `tidb-lexer` and `tidb-hash` provide the native dependency-closed
  owners with source-derived tests; no speculative adapter or Rust-only
  behavior removal was needed.
- Ran the current Go package compile and focused Rust source suites plus Ready
  gates.

## Validation gate

- [x] Current parser util package compiles.
- [x] Focused Rust lexer and hash source suites pass.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push receipt/ExecPlan with the next batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Remaining boundary

Future changes to parser escapes or cascades hashing must update the lexer,
planner, and model consumers together while preserving Go's byte-domain rules.
