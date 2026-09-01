# `pkg/parser/tidb` parity audit ExecPlan

## Objective

Inventory the complete parser feature-ID package, compare its Go-master
contract with Rust consumers, and record an explicit dependency boundary.

## Completed

- Read both pinned artifacts (75 lines, twelve constants, one function, and
  public BUILD metadata).
- Verified the hparser branch is byte-identical to Go master.
- Searched all Rust crates and found no dependency-closed owner for the public
  `CanParseFeature` allowlist; no speculative API or Rust-only behavior was
  added.
- Ran current and exact Go-master package compile checks plus Ready gates.

## Validation gate

- [x] Current parser/tidb package compiles.
- [x] Exact Go-master parser/tidb package compiles.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push receipt/ExecPlan with the next batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Remaining boundary

If Rust begins consuming feature-gated parser comments, introduce the registry
with source-derived allowlist tests and parser/planner integration as one
dependency-closed change.
