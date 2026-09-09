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
- Re-inventoried the complete split Rust owner under the user's Rust-only
  direction, including both manifests, the aggregate-test build script and
  output, workspace/lock integration, all owner tests, and every direct caller.
- Added a one-call deny-on-discard regression, captured exactly one pre-fix
  diagnostic, removed the sole direct `UnescapeChar` annotation, and verified
  all 99 split-owner tests pass.

## Validation gate

- [x] Current parser util package compiles.
- [x] Focused Rust lexer and hash source suites pass.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [x] Owner all-target and direct production-consumer compilation pass; the
      broader planner test target's unrelated missing-field baseline failure
      is recorded in the receipt.
- [x] Push one `pkg/parser/util` package commit, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Remaining boundary

Future changes to parser escapes or cascades hashing must update the lexer,
planner, and model consumers together while preserving Go's byte-domain rules.

The 2026-09-07 outcome is a behavior-neutral caller-contract correction:
`unescape_char` can now be discarded like Go's `UnescapeChar`, while every
escape vector, the full byte domain, the unchanged hash interface, and all
production consumers remain covered.
