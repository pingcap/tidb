# Retire superseded utility audit workflows

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove duplicated historical work instructions that prescribe old laptop
paths, completed edits/publication steps and broad test sweeps. Retain the
original package receipts and their limits. Work in /workspace/tidb on
hparser-integration from ee1c97cedc77d374303d09d4f20ab4360a39451f.
Fresh Go master remains 5b7e1eb8f5f8252391b6e68330d1648a26a80c17.
No current Go-package acceptance follows from this documentation cleanup.

## Progress


- [x] Review 20 superseded plans and map their 19 retained receipts.
- [x] Confirm publication ancestry and preserve before-image hashes.
- [x] Remove plans, redirect references and collapse duplicate receipt links.
- [x] Verify continuity, links, documentation-only scope and diff quality.
- [ ] Complete actual hook, fresh pre-push build and publication checks.

## Milestones and Plan of Work


Retire the plans listed in utility-workflow-cleanup-validation.json from
rust/docs/operations. Keep all receipts under rust/testport/receipts byte
identical. Redirect TESTPORT_EXECPLAN references to those receipts and update
the operations/current-audit indexes and both finding registers. The remaining
implementation, integration-test and blocked validation plans stay in place.

The removed DDL-checker and external-sort plans describe completed audits of
unclaimed packages. Their receipts retain the missing ownership obligations;
retiring those plans does not declare their implementations complete. Counts
stay at 86 tracked, 30 repaired and 56 unresolved findings.

## Validation and Acceptance


From /workspace/tidb run the external continuity check under
/workspace/.cloud-setup/utility-workflow-cleanup and git diff --check. Require
all before-image/receipt hashes to match, all retired document last-change
commits to be published ancestors, and no current reference to a removed plan.
Only Markdown/JSON may change. No executable sources, tests, harnesses,
scripts, dependencies, fixtures or Go/Bazel files change; no test sweep or
make lint is warranted.

Source /workspace/.cloud-setup/env.sh before normal commit; the actual hook
must run cd rust && cargo build --locked -p tidb-server. Rerun from rust/
CARGO_BUILD_JOBS=1 cargo build --locked -p tidb-server immediately before
normal authorized push to pingcap/tidb hparser-integration. Verify remote SHA.

## Surprises & Discoveries


Two plans describe the same disjoint-set audit. Several others remain open
only to publish already-published documents or audit an unrelated next package.
Plans for unfinished monitor/comparator owners, interrupted trace-event tests,
and blocked Bazel validation remain. Original receipts distinguish historical
host results, platform limits and unclaimed packages.

## Decision Log


Remove duplicated instructions in one batch and retain evidence once. Do not
rerun runtime suites for unchanged executable files. Do not treat absence of
a same-named Go test as proof a Rust correctness test is useless.

## Outcomes & Retrospective


Twenty plans /1338 lines removed, 19 receipts preserved; continuity, ancestry,
reference and documentation-scope checks passed. Publication evidence belongs
in external final-handoff.json after commit. No behavior change, finding closure or measured speedup.

## Recovery, Artifacts and Dependencies


Restore individual deleted documents from the base commit without overwriting
concurrent work. Inventory, hashes and final-handoff.json live outside the
checkout under /workspace/.cloud-setup/utility-workflow-cleanup. The durable
receipt is rust/docs/parity/current-audit/utility-workflow-cleanup-validation.json.
No dependency changes. Cloud draft save, Publish and fresh-task restoration
remain distinct states.
