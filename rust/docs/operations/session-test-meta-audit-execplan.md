# `pkg/session/test/meta` parity audit ExecPlan

## Objective

Inventory the complete metadata/bootstrap Go test package, including DDL
table-version transitions, region keys, TTL metrics, timezone behavior,
next-generation reserved IDs, the six-shard flaky BUILD target, and TestMain;
compare every test/helper with Rust owners and record a dependency-closed
boundary.

## Completed this batch

1. Read all three Go-master artifacts (376 lines): six metadata/bootstrap
   behavior tests plus `MustReadCounter`, the TestMain/goleak harness, and the
   BUILD dependency closure. No fixture, generated, benchmark, fuzz,
   platform, or generator artifact exists.
2. Ran a focused exact Go-master test from a detached worktree with the
   required `intest,deadlock` tags; `TestInitDDLTables` passed.
3. Compared every test/helper with Rust bootstrap, metadata, tablecodec,
   metrics, and session owners and the existing ignored source carriers. The
   dependency-closed Domain + mock TiKV + DDL + SQL owner is not transcreated.
4. Recorded the sole Go-master delta (reserved base-table assertion 60→65),
   exact hashes, test inventory, validation evidence, and the explicit
   boundary in `rust/testport/receipts/session_test_meta.md`.

## Validation gate

- [x] Complete Go-master test/Bazel inventory and Rust comparison.
- [x] Focused exact Go-master test passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated bootstrap/session Domain ownership,
metadata persistence, tablecodec region inspection, TTL transaction metrics,
timezone-aware SQL execution, and next-generation catalog publication. The
loop continues with the next unrecorded package; this plan does not claim
whole-repository parity.
