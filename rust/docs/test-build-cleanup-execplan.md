# Remove redundant test and audit scaffolding

This living ExecPlan follows root PLANS.md. Current work starts at 70b351a84f16233d76cff1ce35e8f11af1fc8aeb against Go master 93a01d31f6da205ae4bf376825293903a6899fdb. Earlier build-input and statistics cleanup results remain in [their receipt](parity/current-audit/test-build-cleanup-validation.json) and [statistics receipt](parity/current-audit/statistics-test-cleanup-validation.json); their completed step-by-step plans are retired from this working document.

## Purpose / Big Picture


Remove unnecessary compilation and runtime work from lint-only tests while retaining Go behavior and Rust correctness checks. Production APIs and lint policy remain unchanged. Current publication/access rules are in [the audit index](parity/current-audit/README.md).

## Workspace discard-check removal, 2026-10-05 UTC


This continuation starts at 70b351a84f16233d76cff1ce35e8f11af1fc8aeb and follows pinned Go master 93a01d31f6da205ae4bf376825293903a6899fdb. Remove the remaining workspace-wide test functions whose sole purpose is forbidding Rust unused_must_use diagnostics on discarded Go API results. Inspection found 124 annotated cases; 122 discard outputs without behavioral assertions. The two cases asserting placement mutations and nil transaction-event behavior remain. No production annotation, API, fixture or failing behavioral test is removed.

## Progress


- [x] Inventory and inspect the entire remaining class, preserve before-images and remove 122 nonbehavioral checks in one batch.
- [x] Remove resulting empty harnesses, dead imports and obsolete attribute-only audit documents; verify retained bodies and test registrations.
- [x] Run one affected all-target check, representative retained behavioral suites, root lint and the actual locked-build commit hook.
- [x] Update the cleanup receipt; refresh Cloud recovery/startup state after the commit.

## Milestones, validation and recovery


The first milestone removes discard-only functions across owner families together. The second removes only resulting unused scaffolding and completed one-off audit prose; original Go package inventories and meaningful tests remain obligations. Exact before hashes and removed names live in /workspace/.cloud-setup/discard-check-cleanup/inventory.json. Verify source equals before-images minus explicitly inventoried removals and retained test bodies/attributes stay byte-identical. Run Cargo from rust/ with the Cloud env sourced and one build job; check all affected packages with --all-targets, run retained behavioral coverage in the shared utility and source-adapter targets, then make lint and the mandatory actual precommit server build. Do not rebuild all unrelated test executables merely to validate removals. Recover before-images from the starting Git commit; preserve current work and the known push grant blocker. No new performance result or package/finding closure is implied.

## Surprises & Discoveries


Some discard-only tests open workers, touch global settings or build fixtures without examining results. Others contain unreachable black_box(false) branches purely to type-check calls. Their Go behavior is covered by adjacent source-derived tests. Remove these runtime lint harnesses while keeping Rust safety and genuine state/result assertions. No absence-of-Go-filename heuristic is used to delete behavioral coverage.

## Outcomes & Retrospective


Removed 122 discard-only checks, 20 empty modules, three integration files, one unused tracing helper and three completed plans across 37 crates. All 681 retained test bodies are byte-identical. The affected all-target check, selected behavioral suites and root lint pass; the actual precommit hook gates the commit on the locked server build. Exact commands and results are in [the validation receipt](parity/current-audit/discard-check-cleanup-validation.json). No production behavior, finding count or Go package acceptance changed; performance gain and fresh-task restoration remain unverified.

## Decision Log


The selected boundary is the entire workspace class of deny-on-discard runtime checks, rather than one utility at a time. Twenty empty unit-test modules and three empty integration files have no retained assertions or helpers; remove them. The completed generic, disk and model-reorganization annotation audit plans duplicate historical receipts. Remove the plans, redirect their testport references to those receipts, and retain all underlying package obligations. All 681 remaining test bodies in touched files are byte-identical.
