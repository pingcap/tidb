# Retire the unused semantic validation workflow

This living ExecPlan follows root PLANS.md. Keep progress, discoveries, decisions
and outcomes current. The previous vardef cleanup is committed as
88bcdc4d81fab9c4c280abc0ad3f516dc3b65764: 55 tests, lint, formatting and its actual
commit-hook build passed. Its pre-push build was interrupted by cloud transport
failure. Recovery confirmed the same clean local commit and remote 3af4618443;
no completed pre-push build or push log exists. Include both commits in the next
normal, validated publication rather than repeating the previous source edits.

## Purpose and Context


Remove misleading commands and superseded plans so future work uses the actual
owner tests and current validation workflow. Work in /workspace/tidb on
hparser-integration, at the base above. Fresh Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Seven .semantic.toml files under crate
test directories belong to the already removed semantic-package-gate.py. They
are not Cargo manifests and have no maintained executable consumer.

## Progress


- [x] Recover the environment; verify checkout, native HEAD and interrupted push.
- [x] Inventory all seven manifests and seven associated package audit plans.
- [x] Verify retained package receipts contain original Go artifact obligations.
- [x] Delete obsolete manifests/plans and update the current workflow index.
- [x] Compare source/target continuity, root lint and final diff.
- [ ] Commit through actual hook; fresh locked build immediately before push.
- [ ] Verify remote SHA, recovery bundle and saved cloud startup instructions.

## Milestones and Plan of Work


Remove manifests for globalconn, intest, keydecoder, kvcache, logutil, sem and
size, together with their matching rust/docs/operations/*-audit-execplan.md
plans. Preserve rust/testport/receipts/util_*.md byte-for-byte: each owns the
Go inventory, historical validation and language/platform limits. The cleanup
receipt stores hashes and immutable Git archive links for removed files. Keep
historical citations as dated evidence, not current executable instructions.

Update rust/scripts/README.md and current-audit indexes to use maintained owner
commands. Preserve every source, executable test, build target, dependency and
lockfile. This batch changes documentation and unused metadata only.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust, compare before
and after output of cargo metadata --locked --no-deps --format-version 1;
package targets, features and dependencies must be identical. No .semantic.toml
files or gate consumers may remain. Verify all seven retained receipt hashes
and that the Git diff contains only this documented removal/edit inventory.
Run make lint and git diff --check from repository root. No executable behavior
changes, so do not rerun unrelated Rust suites; the previous 55 tests remain
the evidence for the carried vardef commit.

Commit normally with executable hooks/pre-commit selected by core.hooksPath=hooks.
It must pass cd rust && cargo build --locked -p tidb-server. Repeat that exact
locked build immediately before authorized normal push to origin
hparser-integration, then verify the remote SHA. Never force-push or bypass hooks.

## Surprises & Discoveries


The seven manifests survived retirement of their runner. Several point at tests
or source files already removed by later repairs. Superseded plans still suggest
historical broad sweeps, laptop tool paths and reviving the old gate through Git.
Their seven package receipts retain the actual Go inventories independently.

## Decision Log


On 2026-10-06 remove the entire unused metadata workflow together, retaining
package evidence and immutable before-image links. Do not remove tests merely
because their Rust names differ from Go. No Go, Bazel or Cargo build metadata
changes, so no Bazel preparation or unrelated behavioral suite is needed.

## Outcomes & Retrospective


Fourteen files (1437 lines, 29 obsolete command declarations) removed. Cargo
metadata is identical; all seven package receipts are byte-identical. Source,
test, dependency and lockfile continuity, root lint and diff checks passed.
Publication and cloud checkpoint remain pending; external final-handoff.json
will record their final results.
No production code or executable test registration changes. The 56 unresolved
structural findings and complete Go-package obligations remain unchanged.

## Recovery, Interfaces and Dependencies


Recover removed files with git show 88bcdc4d81fab9c4c280abc0ad3f516dc3b65764:<path>.
Do not overwrite concurrent changes. The inventory and archive links live in
rust/docs/parity/current-audit/semantic-workflow-cleanup-validation.json;
external metadata comparisons and logs live in
/workspace/.cloud-setup/semantic-workflow-cleanup. Verify the replacement recovery
bundle before replacing the old one. Saving startup instructions is distinct
from environment Publish and validation in a new task.

Revision 2026-10-06: replace the committed vardef plan with metadata retirement;
carry its interrupted publication explicitly until remote verification succeeds.
