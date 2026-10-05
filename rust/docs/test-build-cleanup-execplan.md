# Retire the disconnected memo/pattern model

This living ExecPlan follows root PLANS.md. Earlier cleanup evidence remains
indexed in parity/current-audit/README.md; retired source is recoverable from Git.

## Purpose / Big Picture


Remove the unused alternative memo representation and its duplicate private
harnesses as a single dependency group. Future builds and test discovery have
fewer inputs. Live optimizer behavior is unchanged, and original Go memo/pattern
contracts remain integration obligations. No measured speedup is claimed.

## Context and Orientation


Base 0209bbec2088f13ed551b982c474b9621a58dc96, hparser-integration in
/workspace/tidb. Refreshed Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed;
native client remains cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1. The removal group
is tidb-planner/src/{group_expr,explore_mark,expr_iterator,pattern,pattern_engine}.rs,
five corresponding difftests/planner-tests/tests files, and four private planner
tests: memo_group_source, memo_group_operations_source, memo_expr_iterator_source,
and cascades_pattern_operand_engine_source. Delete their five lib.rs declarations.

## Progress


- [x] Refresh refs and prove the whole dependency group has no production callers.
- [x] Remove five modules, nine harnesses, 27 tests and 1,803 source/test lines; retire obsolete pattern receipt and update its references.
- [x] Verify 3,694 retained files and fresh test registration; grouped all-target check, lint and self-review pass.
- [ ] Commit with actual locked-build hook; repeat locked server build immediately before push, verify remote and refresh Cloud recovery/startup state.

## Milestones and Plan of Work


Inventory scoped module references and exported symbols across tracked sources,
including difftests outside crates/. Remove the whole unused dependency group
rather than leaving its private tests behind. Replace stale ownership assertions
and retire the obsolete pattern-only receipt. Keep historical mixed-scope
receipts explicitly historical; preserve upstream source/test/build inventories
and hashes in parity/current-audit/memo-model-cleanup-validation.json.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in each shell. From rust/ run:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-planner -p difftest-planner-tests -p tidb-server --all-targets

From root run make lint and git diff --check. Compare retained Rust source,
manifests and scripts with their before hashes: only tidb-planner/src/lib.rs
may change, losing exactly five declarations. Fresh generated all_tests.rs must
exclude all nine retired harnesses. Pure unreachable deletion requires no new
regression or behavioral suite rerun. Normal commit must run hooks/pre-commit
through core.hooksPath=hooks and pass cd rust && cargo build --locked -p tidb-server.
Repeat that exact build immediately before pushing; verify remote SHA.

## Surprises & Discoveries


A leaf-only scan missed the dependency group because generic names Group,
Pattern and Operand collide with unrelated owners. Scoped imports and the full
symbol inventory establish the closed group. Go ExprIter retains shared Group
and list.Element identities and lazily rebinds children; this Rust model builds
owned Cartesian vectors. Its tests never exercise the live optimizer lifecycle.

## Decision Log


Retire the complete disconnected model, not tests from live behavior. Keep
original Go obligations, runtime planner coverage, Rust correctness regressions,
maintained scripts and repository build gates. The old pattern-only receipt
claimed consumers that the current tree does not have; Git preserves its history.
Date: 2026-10-05 UTC.

## Outcomes & Retrospective


Current evidence is in parity/current-audit/memo-model-cleanup-validation.json.
Findings remain 86 tracked / 30 repaired / 56 unresolved. Final publication and
Cloud draft evidence goes in /workspace/.cloud-setup/memo-model-cleanup/final-handoff.json.

## Recovery and Dependencies


Before-images and hashes are in /workspace/.cloud-setup/memo-model-cleanup.
Recover an individual file with git show 0209bbec2088f13ed551b982c474b9621a58dc96:<path>
into a temporary file before reviewing restoration. Never reset or force-push;
preserve concurrent changes. No dependency, manifest, live interface, original
Go file or permanent script changes are needed. Updated for this removal batch.
