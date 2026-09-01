# `pkg/sessiontxn/staleread` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for `pkg/sessiontxn/staleread` and
record a safe package-atomic Rust ownership boundary. Read every Go
production, test, failpoint, and Bazel artifact before editing; do not enable
a partial stale-read port without storage, session, planner, and prepared
statement owners.

## Completed this batch

1. Inventoried all 10 tracked Go artifacts (1,746 lines), including the
   processor, stale transaction-context provider, AS OF/read-staleness/
   external-ts utilities, failpoint hook, test lifecycle, and BUILD target.
   No fixtures, generated outputs, benchmarks, fuzz inputs, or platform
   variants were omitted.
2. Ran the exact Go-master package suite through the repository failpoint
   wrapper; all 13 package tests passed and failpoints were disabled during
   teardown.
3. Compared the full behavior with Rust. Rust has bounded `AS OF TIMESTAMP`
   history support and timestamp parsing, but no dependency-closed owner for
   the Go provider lifecycle, read-staleness evaluator, external timestamp
   cache, follower-read options, or prepared-statement integration.
4. Found no safe missing behavior to implement in isolation and no Rust-only
   behavior to remove. Recorded the complete inventory, hashes, counts,
   validation evidence, and explicit SEED boundary in
   `rust/testport/receipts/sessiontxn_staleread.md`.

## Validation gate

- [x] Complete Go source/test/failpoint/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Historical session snapshots, `tidb_read_staleness`, external timestamp
read-only semantics, follower-read routing, prepared-statement timestamp
evaluation, transaction-provider hooks, and temporary-table snapshot
integration remain explicit gaps. The repository package loop continues after
this receipt; this plan does not claim whole-repository completion.
