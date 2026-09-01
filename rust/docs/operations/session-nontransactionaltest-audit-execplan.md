# `pkg/session/test/nontransactionaltest` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the nontransactional session test
package and record a safe package-atomic Rust ownership boundary. Read every
test source and build artifact before editing; do not treat parser/admission
stubs or ignored source carriers as execution parity.

## Completed this batch

1. Inventoried all three artifacts (614 lines): the TestMain/goleak harness,
   six SQL behavior tests, the sharding helper, and the six-shard flaky Bazel
   target. No production, fixture, generated, benchmark, fuzz, or platform
   artifact was omitted.
2. Ran the exact Go-master failpoint-managed package suite; all seven tests
   passed in 16.845s and failpoints were disabled during teardown.
3. Compared the complete test package with Rust. Rust has BATCH-DML parsing,
   admission checks, and six ignored source carriers, but no session batch
   planner/worker execution or matching constraints, metrics, and timing
   behavior. The production owner is the un-audited
   `pkg/session/nontransactional.go` package and its consumers.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove in this test-only package. Recorded the complete inventory, hashes,
   validation evidence, and explicit SEED boundary in
   `rust/testport/receipts/session_nontransactionaltest.md`.

## Validation gate

- [x] Complete Go test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Batch-DML planning, execution workers, error aggregation, constraint and
foreign-key behavior, metrics, and max-execution-time handling remain explicit
boundaries in the production session/executor owners. The repository package
loop continues with `pkg/session/nontransactional`; this plan does not claim
whole-repository completion.
