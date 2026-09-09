# `pkg/parser/duration` parity audit ExecPlan

## Objective

Inventory the complete duration parser, compare it with the Rust parser owner,
and record a dependency-closed parity result.

## Completed

- Read all three pinned artifacts (156 lines, two production declarations, and
  one test).
- Verified the hparser branch is byte-identical to Go master.
- Confirmed Rust's parser implementation owns the duration contract and its
  TTL/CALIBRATE consumers, with expanded source-derived diagnostics tests.
- Added no speculative code because no Go-master behavior delta exists.

## Validation gate

- [x] Current failpoint-aware Go package suite passes with refcount zero.
- [x] Focused Rust duration consumer suite passes.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push receipt/ExecPlan with the next batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Remaining boundary

Future changes to duration syntax or diagnostics must update the parser and
both SQL consumers together.
