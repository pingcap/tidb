# `pkg/session/test/resourcegrouptest` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session resource-group test
package and map its statement/transaction semantics to existing Rust owners
without introducing a duplicate resource manager or observation hook.

## Completed this batch

1. Inventoried both artifacts (76 lines): the single failpoint-driven SQL
   test and flaky BUILD target. No production, TestMain, fixture, generated,
   benchmark, fuzz, or platform artifact was omitted.
2. Ran the exact Go-master failpoint-managed suite; the test passed in 2.214s
   and failpoints were disabled during teardown.
3. Compared the package with Rust. Session hint resolution and transaction
   propagation exist with focused Rust coverage, while the resource-group
   catalog/cost controller and Go failpoint observation seam remain outside a
   dependency-closed owner.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded inventory, hashes, validation evidence, and the explicit
   SEED boundary in
   `rust/testport/receipts/session_test_resourcegrouptest.md`.

## Validation gate

- [x] Complete Go test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Resource-group catalog validation, unknown-name fallback, privilege warnings,
pessimistic-lock/prewrite/commit tagging, and the resource-control observation
seam remain coordinated work across session, transaction, and resource-manager
owners. The package loop continues with the next unrecorded session test
package; this plan does not claim whole-repo completion.
