# Retire disconnected planner, statistics and session models

This living ExecPlan follows root PLANS.md. Earlier cleanup evidence remains
indexed in parity/current-audit/README.md; Git preserves retired source.

## Purpose / Big Picture


Remove compiled Rust models with no production callers, including their private
harnesses and stale integration claims. This reduces inputs to future builds and
test runs. It does not repair a runtime finding or establish whole Go package
parity. No timing improvement is claimed.

## Context and Orientation


Base a94bac4fe8a3792161397e7d3d7d4a4c7194956d, branch hparser-integration in
/workspace/tidb. Refreshed Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
Native client remains cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1.
Delete tidb-stats average_count/count_metrics/weighted_reservoir;
tidb-planner logical_property/memo_group_id/plan_context/storage_engine_usage;
tidb-domain optimize_trace; and tidb-session binding_plan_evolution. Seven private
test files include two duplicate planner difftest harnesses. Remove declarations
and statistics reexports from the four owning lib.rs files.

## Progress


- [x] Refresh refs; inventory all tracked caller references and original Go anchors.
- [x] Delete nine models and seven private harnesses, totaling 26 tests and 1,636 file lines; remove declarations and stale integration claims.
- [x] Verify 3,702 unchanged files and regenerated test registration; grouped all-target check, ten refreshed difftests, lint and self-review pass.
- [ ] Commit through actual locked-build hook, repeat locked build immediately before push, verify remote and refresh Cloud recovery/startup state.

## Milestones and Plan of Work


First prove the deletion set has no executable consumers outside its private
tests and module declarations. Then delete the whole unused unit; do not leave
its tests registered or remove checks from surviving runtime code. Finally
correct current documentation and mark old receipts historical, retaining the
original Go obligations and their source hashes in the cleanup receipt.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in each shell. From rust/ run:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-stats -p tidb-planner -p tidb-domain -p tidb-session -p difftest-planner-tests -p tidb-server --all-targets

From the root run make lint and git diff --check. Verify retained Rust source,
manifests and scripts are byte-identical except the four lib.rs declaration/doc
edits and three retained difftest fixture migrations; generated all_tests.rs must no longer register deleted harnesses. No
behavioral tests need rerunning for unreachable pure deletions. Run the ten
retained cross_estimation, physical_topn and physical_union_all difftests after
updating obsolete fixture types and function arguments. Normal commit
must run hooks/pre-commit with core.hooksPath=hooks and pass cd rust && cargo
build --locked -p tidb-server. Repeat that exact build immediately before push.

## Surprises & Discoveries


Initial leaf-symbol scanning missed the live async-loading static reexport.
A complete exported-symbol/caller search retained that module and its consumers.
Two planner difftest files duplicated private model coverage outside crates/;
the all-tracked-file search found and included both. Go still contains the
retired models' contracts: lack of integration, not missing Go behavior, is the
reason for removal. The old property plan's completion claim is historical.
Grouped compilation exposed nine pre-existing errors in three retained planner
harnesses. Migrate their fixtures to live Datum ranges, TopN defaults and the
explicit MPP switch; retain every assertion and execute all ten cases.

## Decision Log


Retire disconnected models and their private tests as one batch; retain connected
Go regressions, Rust correctness tests, native sources and build gates. Dated
receipts remain evidence rather than current acceptance claims. Decision date:
2026-10-05 UTC. No manifest or dependency changes are needed.

## Outcomes & Retrospective


Evidence and original Go file hashes are in
parity/current-audit/disconnected-model-cleanup-validation.json. Findings remain
86 tracked / 30 repaired / 56 unresolved. Publication results are recorded in
/workspace/.cloud-setup/disconnected-model-cleanup/final-handoff.json after commit.

## Recovery and Dependencies


Before-images, hashes and logs are in /workspace/.cloud-setup/disconnected-model-cleanup.
Recover individual files with git show a94bac4fe8a3792161397e7d3d7d4a4c7194956d:<path>
into a temporary file before reviewing restoration. Preserve concurrent edits;
never reset or force-push. No new interfaces, dependencies or permanent cleanup
scripts are introduced. Updated 2026-10-05 for this removal batch.
