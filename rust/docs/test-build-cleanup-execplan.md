# Remove obsolete test scaffolding and audit plans

This living ExecPlan follows root PLANS.md. Current base is 2087a78910ddbfece658b0b52a75ffcd39068f35; comparison authority remains Go master 93a01d31f6da205ae4bf376825293903a6899fdb. Earlier cleanup evidence is in [the discard-check receipt](parity/current-audit/discard-check-cleanup-validation.json).

## Purpose / Big Picture


Reduce repeated test compilation/execution and obsolete instructions. Remove metadata type-presence markers, a duplicate integration-module registration, print-only probes and retired annotation-audit plans. Keep Go behavioral obligations, native ownership/error checks, actual failing tests, source fixtures and production code.

## Progress


- [x] Inspect candidates and preserve exact before-images in /workspace/.cloud-setup/audit-plan-cleanup/before.
- [x] Remove ten redundant/nonbehavioral test functions, sixteen nonzero-size assertions, one duplicate metadata module registration and nineteen obsolete plans; redirect references to retained receipts.
- [x] Verify retained source bodies and harness registration; run metadata integration tests and affected expression/unistore test compilation together, then root lint.
- [x] Self-review, commit through the actual locked-server-build hook and refresh recovery/startup state.

## Milestones and validation


The metadata carrier milestone leaves pkg_meta_model_package_anchors.rs as its existing standalone Cargo test target, with twenty-five meaningful cases registered once. pkg_meta_model_semantics.rs retains five distinct behavioral cases. Six removed cases only discard size_of, one repeats the retained index assertion, and another only asserts positive native sizes. Sixteen additional positive-size markers convey no Go layout contract. Preserve the separate u64 flag-width and shared-ownership tests.

The diagnostic milestone removes probe2 from tidb-expr/src/pushdown_catalog.rs and probe_pow_expr from tidb-unistore/src/cophandler.rs. Both print results without checking them; neighboring actual pushdown and expression assertions remain. The document milestone retires nineteen annotation-audit plans for deleted test names, preserving package receipts and their unverified boundaries. Actual multi-node harnesses remain because they exercise live behavior.

From rust/ after sourcing /workspace/.cloud-setup/env.sh, run CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-model --test pkg_meta_model_semantics --test pkg_meta_model_package_anchors -- --test-threads=1 and cargo check --locked -p tidb-expr -p tidb-unistore --tests. From root run make lint and git diff --check. Expected metadata outcome is 30 passing distinct cases; verify Cargo metadata still lists each standalone target once. Commit normally with core.hooksPath=hooks, TERM=xterm and CARGO_BUILD_JOBS=1; the actual hook must pass cd rust && cargo build --locked -p tidb-server. No Go/Bazel files changed. No broad behavioral sweep is warranted for these deletions.

## Surprises & Discoveries


Cargo auto-discovers the metadata anchor file while the semantics target also includes it as a module, compiling and running all twenty-six original cases twice. Most no-assertion search hits are meaningful helper-based tests or Go benchmark translations and remain. The dbterror audit contains an outstanding generated-catalog concern and is retained; stringutil contains substantive byte semantics and is retained.

Two initial hooks exhausted disk and correctly aborted. Remove failed temporaries, three inactive test executables and check-only metadata without paired compiled libraries; record hashes and actual free-space deltas. Free space recovered to 1.1 GB. Retry the same normal hook; preserve source, compiled dependencies and recovery data.

## Decision Log


Retire the complete selected class of obsolete documentation and metadata markers together, preserving semantic receipts rather than another permanent duplicate inventory. The exact deletion inventory is outside the tree; Git at the base commit preserves every removed byte. No package or structural finding is accepted by cleanup. Date: 2026-10-05 UTC.

## Outcomes & Retrospective


Removed 19 obsolete plans, ten unique redundant/nonbehavioral cases, 26 duplicate registrations and sixteen size markers. All 30 metadata cases pass, affected expression/unistore test targets compile, root lint and diff checks pass. Source comparison confirms 113 retained bodies differ only by the sixteen removed markers. The normal commit is gated by the actual locked server build; recovery/startup refresh follows it. See [exact validation](parity/current-audit/audit-plan-cleanup-validation.json). No speedup measurement or fresh-task restoration is claimed. The user corrected the repository installation grant; verify managed Git write access through authorized publication after the fresh locked build.

## Recovery


Recover selected removed files from the base commit into a separate path and review before restoring. Preserve concurrent work and never reset or force-push. After the normal commit, refresh and verify the unpublished bundle and saved Cloud draft without changing network or credential settings.
