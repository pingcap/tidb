# Consolidate planner tests in their owning crate

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove the redundant difftest-planner-tests workspace crate and test binary.
Its 35 source-translation suites exercise tidb-planner directly and require
only dependencies that the owner already has. Keep every assertion and Go
case, under rust/crates/tidb-planner/tests/primitives, registered in the
existing tests/all.rs aggregate. Base: 9076a9094665a4118d863ff2c8f1512af2bc581d;
Go comparison: 5b7e1eb8f5f8252391b6e68330d1648a26a80c17.

## Progress


- [x] Inspect dependencies, module references, fixtures and global mutations.
- [x] Move all 35 suites and their module registry byte-identically.
- [x] Remove the package/member/lock entry; update current guidance.
- [x] Run grouped tests (113 passed), lint, continuity and self-review.
- [ ] Run actual precommit gate, fresh pre-push build and verify publication.

## Milestones and Plan of Work


Move tests/all.rs to primitives/mod.rs and all its sibling suites into that
module directory; add mod primitives to the owner's existing all.rs. Remove
rust/difftests/planner-tests/Cargo.toml, its workspace member and only its
lockfile package entry. No new dependency or production behavior is needed.
Update the source pointer and current commands. Preserve dated old commands
as historical evidence with the current replacement documented.

The second milestone validates every migrated case in one serial run,
checks byte continuity and locked Cargo metadata, then runs make lint and
publication gates. Keep both finding registers accurate: cleanup does not
repair any of the 56 unresolved findings or establish complete Go-package
acceptance.

## Concrete Steps and Acceptance


Source /workspace/.cloud-setup/env.sh in each build shell. From rust/:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-planner --test all -- primitives:: --test-threads=1
    cargo metadata --locked --no-deps --format-version 1

Every migrated test must pass without changing its assertions. From the
repository root run make lint and git diff --check. Verify all 36 file hashes
against the external migration inventory; the retained owner suites must
also remain unchanged. Commit normally so hooks/pre-commit runs its locked
server build, then immediately before authorized push run from rust/:

    CARGO_BUILD_JOBS=1 cargo build --locked -p tidb-server

Push normally to pingcap/tidb hparser-integration and verify remote SHA.

## Surprises & Discoveries


The separate crate supplies no library, fixtures, special dependencies or
isolated global-state harness. Its tests already use external tidb_planner
imports, so nesting them preserves resolution. Use the existing serial
validation convention; no fixture or process-global mutation was found in
the migrated suites.

## Decision Log


Consolidate the whole redundant harness in one batch rather than deleting
useful Go-derived tests. Preserve every suite byte-for-byte. Removing one
Cargo package and test binary reduces maintained targets; no measured build
speedup is claimed. Historical receipts retain their original commands.

## Outcomes & Retrospective


Migration and validation complete: 113 cases passed, no failures/ignores;
locked metadata, make lint and continuity passed. Actual hook and publication
evidence will be recorded after commit in external final-handoff.json. No behavioral
parity closure, live cluster, broad Go-suite or benchmark claim.

## Recovery, Artifacts and Dependencies


Work in /workspace/tidb on hparser-integration. Before-images are recoverable
from the base commit; restore only affected paths, preserving concurrent
work. /workspace/.cloud-setup/planner-harness-cleanup/migration.json records
all source/destination hashes. Durable results belong in
rust/docs/parity/current-audit/planner-harness-cleanup-validation.json;
external final-handoff.json records post-commit publication and Cloud draft
verification. No new dependencies or interfaces. Draft saving is separate
from Publish and fresh-task restoration.
