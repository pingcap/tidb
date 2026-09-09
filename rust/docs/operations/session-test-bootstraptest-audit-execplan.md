# `pkg/session/test/bootstraptest` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the bootstrap and historical
upgrade test package, compare every test/helper/build artifact with Rust
owners, and record a dependency-closed implementation boundary without
inventing a second bootstrap pipeline.

## Completed this batch

1. Read and inventoried all four Go-master artifacts (2,967 lines): the
   TestMain/goleak harness, 50 runnable tests, ten helpers, and the 45-shard
   flaky BUILD target. No fixture, generated, benchmark, fuzz, platform, or
   build-tag artifact was omitted. The branch's separate local Go test edits
   were preserved and excluded from the receipt commit.
2. Ran the exact Go-master failpoint-managed package suite. It reached
   `TestUpgradeVersionForSystemPausedJob` and timed out at the test binary's
   ten-minute limit; the failure stack is recorded as an existing mock-store /
   DDL scheduling boundary.
3. Compared every test and helper with Rust's session/meta/metadef/exec/server
   owners and existing ignored source carriers. Rust owns selected metadata,
   first-boot publication, and variable definitions, but not the combined
   versioned schema-upgrade, Domain, DDL pause/resume, failpoint, and mock-TiKV
   lifecycle.
4. Found no Rust-only behavior to remove and no safe missing behavior to
   implement in this test-only package. Recorded the complete inventory,
   hashes, validation evidence, and explicit SEED boundary in
   `rust/testport/receipts/session_test_bootstraptest.md`.

## Validation gate

- [x] Complete Go-master test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed command attempted; timeout captured.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated session bootstrap, Domain/DDL ownership,
versioned metadata migration, variable persistence, system-table validation,
failpoint choreography, and mock TiKV lifecycle. The loop continues with the
separate `pkg/session/test/bootstraptest2` package; this plan does not claim
whole-repository parity or completion.
