# `pkg/sessionctx/stmtctx` parity audit ExecPlan

## Objective

Inventory the complete Go-master statement-context package, including its
production lifecycle/state machine, test and benchmark surface, TestMain, and
17-shard flaky BUILD target; compare every behavior with Rust owners and record
a dependency-closed implementation boundary.

## Completed this batch

1. Read all four Go-master artifacts (2,416 lines): the 1,674-line production
   implementation, 641-line test suite, TestMain, and BUILD metadata. The
   package has 129 production functions, 17 tests, one benchmark, and no
   fixture, generated, platform, fuzz, or generator-input artifact.
2. Ran the exact Go-master failpoint-managed package suite from a detached
   worktree; all tests passed in 2.811s and failpoints were disabled in
   teardown.
3. Compared the complete contract with Rust executor/session/exec statement
   context owners and source-derived tests. Rust covers selected leaves but
   lacks a dependency-closed owner for the cross-cutting StatementContext,
   TestKit/Domain integration, and complete test surface.
4. Found no Rust-only behavior to remove and no safe package-local behavior to
   implement. Recorded hashes, function/test inventory, validation evidence,
   and the explicit SEED boundary in
   `rust/testport/receipts/sessionctx_stmtctx.md`.

## Validation gate

- [x] Complete Go-master production/test/build inventory and Rust comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated session, executor, planner, error/type,
metrics, and TestKit/Domain ownership. The rolling audit continues with the
next unrecorded package; this plan does not claim whole-repository parity or
completion.
