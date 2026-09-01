# `pkg/session/test/clusteredindextest` parity audit ExecPlan

## Objective

Inventory the complete clustered-index Go test package, including its race/
flaky BUILD target and testdata glob, compare every test/helper with Rust
owners, and record a dependency-closed implementation boundary.

## Completed this batch

1. Read all three Go-master artifacts (253 lines): the TestMain/goleak
   harness, three clustered-index tests, `createTestKit`, the
   `SnapCacheSizeGetter` interface, and the three-shard race/flaky BUILD
   target. The referenced `testdata` glob has no tracked fixture; no generated,
   benchmark, fuzz, platform, or build-tag artifact was omitted.
2. Ran the exact Go-master failpoint-managed package suite from a detached
   worktree; all three tests passed in 5.087s and failpoints were disabled in
   teardown.
3. Compared every test and helper with Rust's session/storage/partition owners
   and existing ignored source carriers. Rust lacks the dependency-closed
   mock-TiKV snapshot cache, old-row-format DML, TestKit executor, and
   randomized partition-scan composition.
4. Found no Rust-only behavior to remove and no safe package-local behavior to
   implement. Recorded hashes, test inventory, validation evidence, and the
   explicit SEED boundary in
   `rust/testport/receipts/session_test_clusteredindextest.md`.

## Validation gate

- [x] Complete Go-master test/Bazel/testdata inventory and Rust comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated storage snapshot inspection, clustered-row
encoding, session/executor DML, partition pruning, and randomized test
harnesses. The loop continues with the next unrecorded package; this plan does
not claim whole-repository parity or completion.
