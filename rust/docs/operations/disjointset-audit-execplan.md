# Align `pkg/util/disjointset` with the pinned Go package

This ExecPlan follows `PLANS.md` and uses Go commit
`e2788410d8d696605e8cb002585877a063ccc909` as authority.

## Goal

Treat all six Go artifacts as one package. Preserve both source tests, dense
and sparse union direction, path compression, missing-value insertion,
signed `int` sizes/indexes, `FindVal`, clear/grow behavior, and the live chunk
column-owner consumer. Remove Rust-only diagnostics and obsolete duplicate
test/audit artifacts.

## Progress

- [x] Read all pinned production, test, build, and `TestMain` artifacts.
- [x] Mapped all Rust owners and the live `tidb-chunk` consumer.
- [x] Added a regression proving negative Go inputs were unrepresentable with
  the former unsigned API.
- [x] Changed parent indexes, capacities, and public indexes to signed values;
  the regression now passes.
- [x] Renamed Rust `find_value` to source-shaped `find_val` and updated the
  consumer.
- [x] Removed `Debug`, `must_use`, two supplemental owner tests, two duplicate
  integration contracts, and the retired semantic manifest.
- [x] Run focused owner/consumer tests, full owner tests, all-target checks,
  scoped Clippy, formatting, and diff review.
- [x] Complete the receipt and prepare the verified package snapshot for a
  normal commit and push.

## Validation

Use the WIP profile because package-by-package parity work continues. No Go or
Bazel file changes are made, so `make bazel_prepare` is not required. Run the
Go package tests, Rust disjoint-set tests, the chunk owner/contract tests that
exercise column ownership, complete `tidb-util` tests, owner/consumer checks,
formatting, and diff checks.

The WIP gate passed the Go package, all three Rust owner tests, both focused
chunk owner tests, the chunk integration contract, complete `tidb-util`, both
changed crates' checks, scoped owner Clippy, formatting, and diff checks.
Direct `tidb-chunk` Clippy remains blocked by seven pre-existing warnings in
`chunk.rs`, `codec.rs`, and `mutrow.rs`, outside this package change.
