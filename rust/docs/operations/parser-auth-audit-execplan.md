# `pkg/parser/auth` parity audit ExecPlan

## Objective

Inventory the complete parser authentication package, compare its Go-master
cryptographic and identity contracts with Rust, and record the native owner.

## Completed

- Read all eight pinned artifacts (920 lines, 31 production declarations and
  18 test/benchmark declarations), including BUILD metadata and every hash
  vector.
- Verified the hparser branch is byte-identical to Go master for this package.
- Confirmed `tidb-parser::auth` owns the identity, SHA-1, SHA-crypt, and SM3
  behavior with source-derived tests, including byte-domain and malformed-input
  regressions.
- Ran the current Go auth suite and focused Rust source suite plus Ready gates.

## Validation gate

- [x] Current parser auth suite passes.
- [x] Focused Rust auth source suite passes.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push receipt/ExecPlan with the next batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Remaining boundary

Protocol handshake integration and live account migration remain consumers of
this stable auth owner; future hash-format changes must update those paths and
the source vectors together.
