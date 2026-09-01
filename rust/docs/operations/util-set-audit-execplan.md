# `pkg/util/set` parity audit ExecPlan

## Objective

Keep the complete Go-master set package aligned with its native Rust owner,
including concrete memory accounting, map iteration, keyed-set contracts, and
Go's discardable return-value surface.

## Completed

- Read all 12 Go-master artifacts in full: one Bazel target, five production
  files, five test/benchmark files, and the test harness (1,001 lines, 60
  declarations, seven unit tests, and three benchmarks). No fixtures,
  generated/platform variants, examples, or ownership files exist.
- Confirmed the package is source-identical at Go master
  `c6054025ed4c32ab3672a2a24ea46892714d21ec` (unchanged from the prior pin).
- Preserved the dependency-closed `tidb-util` owner and prior focused fixes:
  concrete memory-aware types and tracker rules, hash-map iteration, free
  keyed-set operations, current-key clone/order behavior, and HashAgg use of
  `StringSetWithMemoryUsage`.
- Removed all 55 explicit Rust-only `#[must_use]` annotations from the owner.
  The focused deny-lint regression failed with 47 errors before the edit and
  passes afterward; current and detached Go package tests also pass.

## Validation gate

- [x] Complete 12-artifact inventory and current-authority receipt recorded in
      `rust/testport/receipts/util_set.md`.
- [x] Existing Rust owner, benchmark, and HashAgg regressions pass per receipt,
      including the new return-contract regression.
- [x] Ready formatting, clean-tree repository lint, and diff checks pass for
      the audit batches.
- [x] Current and exact detached latest-master Go package suites pass.
- [ ] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Any future set change must retain all seven Go test identities, three benchmark
families, concrete type layout/accounting, unspecified map iteration, and
current-key clone behavior. Do not restore generic or sorted Rust-only APIs.

Plan revision note (2026-09-02): refreshed the complete package at current Go
master, removed the explicit return diagnostics, recorded the fail-before/
fail-after regression, and updated the Ready evidence.
