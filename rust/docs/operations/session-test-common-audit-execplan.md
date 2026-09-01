# `pkg/session/test/common` parity audit ExecPlan

## Objective

Inventory the complete common Go session-test package, including prepared
statement dedup-cache regressions, the twelve-shard flaky BUILD target, and
the TestMain harness; compare each test/helper with Rust owners and record a
dependency-closed boundary.

## Completed this batch

1. Read all four Go-master artifacts (600 lines): seven session metadata and
   protocol tests, five prepare-dedup-cache tests, the TestMain/goleak
   harness, and the BUILD dependency closure. No fixture, generated,
   benchmark, fuzz, platform, or generator artifact exists.
2. Ran a focused exact Go-master test from a detached worktree with the
   required `intest,deadlock` tags; `TestMiscs` passed.
3. Compared every test and helper with Rust session/executor owners and the
   existing ignored source carriers. Rust lacks the dependency-closed Go
   TestKit + Domain + storage transaction + PlanCacheStmt protocol owner.
4. Found no Rust-only behavior to remove and no safe package-local behavior to
   implement. Recorded hashes, test inventory, validation evidence, and the
   explicit boundary in
   `rust/testport/receipts/session_test_common.md`.

## Validation gate

- [x] Complete Go-master test/Bazel inventory and Rust comparison.
- [x] Focused exact Go-master test passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated session lifecycle, protocol metadata,
storage-backed DML, prepared statement execution, schema invalidation, and
database-scoped PlanCacheStmt ownership. The loop continues with the next
unrecorded package; this plan does not claim whole-repository parity.
