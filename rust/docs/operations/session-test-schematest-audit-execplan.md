# `pkg/session/test/schematest` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session schema test package
and map its schema, chunk execution, transaction, and variable-validation
semantics to existing Rust owners without introducing fake subsystem seams.

## Completed this batch

1. Inventoried all three artifacts (506 lines): the TestMain/goleak harness,
   ten tests, helper, and ten-shard flaky BUILD target. No production,
   fixture, generated, benchmark, fuzz, or platform artifact was omitted.
2. Ran the exact Go-master failpoint-managed suite; all ten tests passed in
   9.895s and failpoints were disabled during teardown.
3. Compared every test with Rust. Session/transaction and chunk/recordset
   primitives exist, while schema lease/MDL, mock-cluster DistSQL,
   transaction-size observation, and recursive global-variable validation do
   not form a dependency-closed owner.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded inventory, hashes, validation evidence, and the explicit
   SEED boundary in `rust/testport/receipts/session_test_schematest.md`.

## Validation gate

- [x] Complete Go test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Faithful execution requires coordinated schema lease/MDL, session,
mock-cluster/DistSQL, transaction-accounting, and global-variable validation
owners. The package loop continues with the next unrecorded session test
package; this plan does not claim whole-repo completion.
