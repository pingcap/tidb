# Retire superseded test-harness plans

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove four completed test-retirement plans that still prescribe the deleted
rust/scripts/aggregate-tests.rs generator and superseded execution steps.
Work in /workspace/tidb on hparser-integration from
6492b8f2723415997d74f68ea480c4c74ead7d2f. Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Current explicit test registration
and runtime instructions remain in source and the Cloud startup handoff.

## Progress


- [x] Inspect four plans, retained receipts/obligation ledgers and incoming references.
- [x] Delete plans and replace two historical links with immutable Git archives.
- [x] Verify archived bytes, retained evidence, executable continuity and diff.
- [ ] Commit through actual build hook; fresh locked build, push and verify.
- [ ] Verify recovery bundle and save/read back the Cloud checkpoint.

## Milestones and Plan of Work


Delete placeholder-test-removal, transport-expression-empty-test-removal,
planner-empty-module-removal and test-harness-retirement ExecPlans from rust/docs.
Their durable validation receipts remain under rust/docs/parity/current-audit;
full upstream obligations stay in the three corresponding obligation ledgers.
Keep historical deleted-path inventories as historical data. Update the two
Markdown references to commit-pinned archives, and update the current cleanup
index without changing any finding disposition. Keep import/cluster plans
because those also serve as their removal receipts and boundary evidence.

## Validation and Acceptance


Verify each archived blob's hash equals its deleted before-image. Verify all
retained receipts and ledgers byte-for-byte. Require the tracked diff to contain
only Markdown and the cleanup/index JSON; no executable, test, manifest or
fixture changes. Run git diff --check. No behavioral suite rerun is needed.
Source /workspace/.cloud-setup/env.sh and set CARGO_BUILD_JOBS=1. The actual
pre-commit hook must run cd rust && cargo build --locked -p tidb-server.
Repeat that exact build immediately before the authorized normal push to
pingcap/tidb hparser-integration, then verify the remote SHA. Never force push
or bypass hooks. Native client-rust remains unchanged.

## Surprises & Discoveries


Three plans still describe generated test discovery even though static roots
replaced that mechanism. Historical unchecked publication boxes are superseded
by committed receipts and Cloud final handoffs, not new implementation work.

## Decision Log


Retire duplicate instructions, preserving receipt/obligation evidence and
immutable history. Do not delete sole receipts or active implementation plans.
This reduces stale guidance, not compilation time or unresolved parity counts.

## Outcomes & Retrospective


Four plans (257 lines) removed; archive, receipt, ledger, executable continuity
and diff checks pass. Publication remains pending. No finding repair
or complete Go package acceptance is implied.

## Recovery, Artifacts and Dependencies


Restore individual files with git show 6492b8f272:<path>, preserving concurrent
changes. Hashes and archive links live in current-audit/retirement-plan-cleanup-validation.json.
Logs and final handoff live in /workspace/.cloud-setup/retirement-plan-cleanup.
No dependencies change. Cloud draft save, Publish and fresh-task restoration
remain distinct steps.

Revision: replace the completed utility contract consolidation with retirement
of duplicate historical harness plans.
