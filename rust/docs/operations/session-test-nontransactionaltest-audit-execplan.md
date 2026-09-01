# `pkg/session/test/nontransactionaltest` parity audit ExecPlan

## Objective

Inventory the complete non-transactional batch-DML Go test package, including
its six-shard flaky BUILD target, failpoint lifecycle, worker error messages,
constraint checks, metrics, and max-execution-time behavior; compare every
test/helper with Rust owners and record a dependency-closed boundary.

## Completed this batch

1. Read all three Go-master artifacts (614 lines): six behavior tests, the
   `testSharding` helper, TestMain/goleak harness, failpoint and metric
   dependencies, and BUILD target. No fixture, generated, benchmark, fuzz,
   platform, or generator artifact exists.
2. Ran two focused exact Go-master tests from a detached worktree through the
   failpoint wrapper: sharding and max-execution-time behavior passed.
3. Compared each test/helper with Rust's typed non-transactional admission
   policy, metric-label vocabulary, AST, session, executor, and storage
   owners. Rust lacks the dependency-closed shard planner/worker,
   cancellation/error aggregation, constraint/foreign-key execution, live
   metrics, and failpoint lifecycle.
4. Found no Rust-only behavior to remove and no safe package-local behavior to
   implement. Recorded exact hashes, the empty Go-master delta, validation
   evidence, and the explicit boundary in
   `rust/testport/receipts/session_test_nontransactionaltest.md`.

## Validation gate

- [x] Complete Go-master test/Bazel inventory and Rust comparison.
- [x] Focused exact Go-master failpoint-managed tests passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated session admission, shard range planning,
worker scheduling/cancellation, storage-backed DML, constraint checks, metric
publication, SQL redaction, and failpoint-controlled execution. The loop
continues with the next unrecorded package; this plan does not claim
whole-repository parity.
