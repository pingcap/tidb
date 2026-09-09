# `pkg/sessiontxn/isolation` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for `pkg/sessiontxn/isolation`,
including its nested metrics package, and record a safe package-atomic Rust
ownership boundary. Read every Go production, test, metrics, and Bazel
artifact before editing; do not enable a partial transaction-provider port.

## Completed this batch

1. Inventoried all 13 tracked artifacts (3,992 lines), including shared
   provider lifecycle, optimistic, read-committed, repeatable-read, and
   serializable implementations, RC metrics, all 29 package tests, failpoint
   and goleak setup, and both BUILD targets. No fixtures, generated outputs,
   benchmarks, fuzz inputs, or platform variants were omitted.
2. Ran the exact Go-master package suite through the repository failpoint
   wrapper; all tests passed and failpoints were disabled during teardown.
3. Compared the complete package with Rust. Rust owns isolation value
   semantics and partial optimistic/pessimistic cluster transaction seams,
   but lacks a dependency-closed equivalent of the Go provider lifecycle,
   snapshot overlays, per-isolation timestamp policy, RC metrics/retries, and
   full lock/error integration.
4. Found no safe missing behavior to implement in isolation and no Rust-only
   behavior to remove. Recorded the complete inventory, hashes, counts,
   validation evidence, and explicit SEED boundary in
   `rust/testport/receipts/sessiontxn_isolation.md`.

## Validation gate

- [x] Complete Go source/test/metrics/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The integrated transaction-context provider, read-committed conflict checks
and counters, repeatable-read timestamp reuse, serializable policy,
`tidb_snapshot` schema overlays, temporary-table interception, and full
pessimistic lock lifecycle remain explicit gaps. The repository package loop
continues after this receipt; this plan does not claim whole-repository
completion.
