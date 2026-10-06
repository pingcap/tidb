# Consolidate transaction tests in their owning crates

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove the redundant difftest-transaction-tests package and binary while
preserving source-derived primitive cases and live-cluster tests. Work in
/workspace/tidb on hparser-integration from
0f8a99306297fabece0c6575d7b060a955749807. Nine primitive suites belong to
tidb-txnkv; five suites using DistSQL belong to tidb-distsql. Both crates
already have all required dependencies and an aggregate integration target.

Fresh Go master is 3ca96b1d5df8da123e7a650512654eedab12c861. Its eight-file
statistics delta changes unique-value NDV/TopN and local-unique partition NDV
merging. The comparison export is refreshed after checking prior file bytes;
that behavioral delta and the prior FM-sketch delta are not implemented here.
Go module dependencies are unchanged.

## Progress


- [x] Inspect all suite dependencies, fixture paths and global-state needs.
- [x] Move 14 suites and update all seven live runner selections.
- [x] Remove redundant manifest/member/lock entry; retain shared fixtures.
- [x] Run grouped primitive tests (42 passed) and compile seven ignored live cases.
- [x] Verify all seven exact selections, shell syntax, continuity and lint.
- [ ] Complete actual hook, fresh pre-push build, remote and Cloud checks.

## Milestones and Plan of Work


Move nine primitive files to tidb-txnkv/tests/primitives and five realtikv
files to tidb-distsql/tests/transaction_runtime, registering each namespace in
its existing tests/all.rs. Change only two relative include_str fixture paths.
Keep all 27 shared fixture and generator files at their existing paths for
other consumers. Remove the old manifest, all.rs and Cargo member/lock entry.

Update the seven run-realtikv scripts to select tidb-distsql --test all and
prefix their exact test names with transaction_runtime::. Preserve all cluster
startup, cleanup, environment, transport checks and exit handling. Update
current instructions; dated receipts retain their historical paths/results.

## Concrete Steps and Acceptance


Source /workspace/.cloud-setup/env.sh in each build shell. From rust/ run:

    cargo metadata --locked --no-deps --format-version 1
    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-txnkv -p tidb-distsql --test all -- primitives:: transaction_runtime:: --test-threads=1

Expect 42 primitive cases to pass and seven live-cluster cases to remain
ignored. Compilation and exact-name listing validate registration only, not
live behavior. Match each runner's --exact selection to a compiled ignored
test. Run bash -n on all seven scripts and make lint from the repository root.
Compare moved bytes (apart from two includes), all fixtures and runner
before-images. Run git diff --check. No Go/Bazel source changes are made.

Commit normally so the actual hook runs its locked server build. Immediately
before authorized push run CARGO_BUILD_JOBS=1 cargo build --locked -p tidb-server
from rust/. Push normally to pingcap/tidb hparser-integration and verify SHA.

## Surprises & Discoveries


The old test crate has no test-specific dependency or harness. DistSQL tests
belong with the upper-layer owner; putting them in txnkv would require an
unnecessary dependency back to DistSQL. Fixture generators have other users,
so their existing directory is retained even though its Cargo package is gone.

## Decision Log


Consolidate tests without deleting useful Go-derived cases or weakening live
runner safeguards. Preserve ignored live tests and report compilation/listing
separately from execution. Use one grouped validation, not seven cluster runs
for unchanged test bodies. No new dependency, runtime behavior or package
acceptance is claimed.

## Outcomes & Retrospective


Migration and validation complete; publication evidence belongs in external
final-handoff.json after commit. The first test link failed with a bus error
while disk was nearly full; after removing identified inactive artifacts and
the partial output, the same command passed. One fewer Cargo
package/test binary, 14 suites preserved, seven runner commands migrated.
No measured speedup or finding closure.

## Recovery, Artifacts and Dependencies


Restore only affected paths from the base commit if needed, preserving
concurrent work. Inventory, Go refresh and final-handoff.json live under
/workspace/.cloud-setup/transaction-harness-cleanup. The durable receipt is
rust/docs/parity/current-audit/transaction-harness-cleanup-validation.json.
Cloud draft saving, Publish and fresh-task restoration are separate states.
