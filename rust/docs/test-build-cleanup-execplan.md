# Retire completed statistics audit plans

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove obsolete instructions that repeat completed audit work and reference
retired discard-only tests. Work in /workspace/tidb on hparser-integration from
4ec5785552f169a2a01652ff9b669a3c5b9f7e05. Refreshed Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Eleven completed statistics-handle
leaf plans repeat inventories and validation boundaries retained in their
package receipts. Their old pins, test counts and publication sequences must
not be treated as current Cloud instructions.

## Progress


- [x] Read all eleven plans; verify completed checklists and retained receipts.
- [x] Remove eleven plans (511 lines) and archive 18 historical references.
- [x] Verify eleven unchanged receipts, eleven archive blobs and byte-identical metadata.
- [x] Review documentation, run lint/diff checks and record evidence.
- [ ] Commit through actual hook, fresh locked build, normal push and remote verification.
- [ ] Save verified recovery bundle and Cloud checkpoint.

## Milestones and Plan of Work


Retire the eleven statistics-handle leaf audit plans selected in
rust/docs/parity/current-audit/statistics-plan-cleanup-validation.json. Keep
every package receipt byte-identical. Preserve the active parent
rust/docs/statistics-package-parity-execplan.md, including its unresolved
syncload, storage, metrics and dependency boundaries. Preserve the newer LFU
lifecycle repair and failing retained admission evidence. Do not change any
finding disposition or infer package acceptance from retirement.

Replace each historical reference to a removed plan with an immutable archive
link at the base commit. Update rust/docs/operations/README.md to point to the
retirement inventory and current parity workflow. Record exact file hashes,
line counts and retained-receipt paths. Update both finding registers' cleanup
pointers while leaving all findings and counts unchanged.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust compare
cargo metadata --locked --no-deps --format-version 1 with the before-image.
All production source, tests, scripts, fixtures, manifests and lockfiles must
remain unchanged; no behavioral rerun is necessary for this documentation-only
batch. Verify all eleven receipt hashes and archived Git blobs. Scan tracked
Markdown for dangling current references and verify the parent plan only changes
its historical archive pointer. Run make lint and git diff --check from root.
No Go/Bazel change requires bazel_prepare.

Commit normally with core.hooksPath=hooks and the actual
cd rust && cargo build --locked -p tidb-server gate. Repeat that exact locked
build immediately before normal push to origin hparser-integration, then verify
remote SHA. Never bypass hooks, force-push or overwrite concurrent changes.

## Surprises & Discoveries


Completed mapcache, testutil, usage and utility plans still direct readers to
old deny-on-discard regressions that earlier cleanup retired. LFU and cache
metrics plans explicitly retain incomplete external/shared-owner boundaries;
these remain in their receipts and current audit, not silently accepted.

## Decision Log


Retire only the eleven reviewed completed leaf plans. Keep active parent work,
all original source inventories and validation limits. Archive historical links
instead of rewriting old evidence into a current acceptance claim. No executable
change or measured build/runtime speedup is claimed.

## Outcomes & Retrospective


Eleven completed leaf plans (511 lines) are retired; 18 historical references
resolve to immutable archives. All eleven receipts remain byte-identical, and
metadata/source/test continuity, root lint and diff/reference review pass.
Publication and checkpoint remain pending. The current register retains 86 tracked
findings: 30 repaired and 56 unresolved (27 open, 29 partial). Existing parser,
source-inventory and table-corpus failures are unaffected.

## Recovery, Artifacts and Dependencies


Restore individual before-images with git show
4ec5785552f169a2a01652ff9b669a3c5b9f7e05:<path>, preserving other work. Logs and
final publication/checkpoint evidence live in
/workspace/.cloud-setup/statistics-plan-cleanup. Saved configuration, environment
Publish and fresh-task restoration are distinct claims. No dependencies or
executable targets change.

Revision: replace completed result-harness cleanup with retirement of duplicate
statistics instructions and explicit preservation of unresolved boundaries.
