# Retire disconnected scheduler, stack and cost models

This living ExecPlan follows root PLANS.md. Earlier cleanup receipts remain
indexed in parity/current-audit/README.md; Git preserves historical source.

## Purpose / Big Picture


Remove unused cascades task interfaces, separate scheduler/stack implementations
and the scalar implementation-cost adapter with their private test harnesses.
Keep live planner owners and hash/equality regressions unchanged. Fewer compiled
inputs are observable; no measured speedup or Go package acceptance is claimed.

## Context and Orientation


Base eb31889eab13d07727868682a95efed6402fc385 on /workspace/tidb hparser-integration.
Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed; native client remains
cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1. Delete tidb-planner/src modules
implementation_cost, scheduler_contract, stack_contract, task_scheduler and
task_stack, five corresponding difftest files, and planner tests
base_impl_cost_arithmetic_source, cascades_task_scheduler_stack_source and
cascades_task_stack_source. Remove five lib.rs declarations and two task-related
cascades_base.rs reexports; preserve that module's hash tests byte-for-byte.

## Progress


- [x] Refresh refs and trace all tracked caller references; no live optimizer consumers.
- [x] Delete five modules, eight harnesses, 15 tests and 927 source/test lines; correct stale ownership and complete-package claims.
- [x] Verify 3,680 retained files, unchanged hash tests and fresh registration; grouped all-target check, lint and self-review pass.
- [ ] Commit with actual locked-build hook, rebuild immediately before push, verify remote and update Cloud recovery/startup state.

## Milestones and Plan of Work


Trace the complete dependency group, including reexports and difftests outside
crates/. Delete unused owners and their tests together. Update the retained
cascades_base hash surface to state its actual scope; mark old mixed-scope
receipts historical without discarding valid hash test evidence. Record original
Go artifact hashes as obligations in task-model-cleanup-validation.json.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in every shell. From rust/ run:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-planner -p difftest-planner-tests -p tidb-server --all-targets

From root run make lint and git diff --check. Compare retained source, manifests
and scripts with before hashes; only lib.rs declarations and cascades_base.rs
doc/reexport lines may differ. Its hash tests must remain byte-identical. Fresh
all_tests.rs files must exclude all eight retired harnesses. No behavioral
rerun is needed for pure unreachable deletion. Normal commit must execute
hooks/pre-commit through core.hooksPath=hooks and pass cd rust && cargo build
--locked -p tidb-server. Repeat that exact build immediately before each push.

## Surprises & Discoveries


Go's scheduler obtains base.Stack from stackPool and executes the same base.Task
that supplies descriptions. The disconnected Rust copies split this into three
incompatible task traits. ImplementationCost accepts only scalar costs and has
no physical-plan attachment or production caller. Generic TaskError and
task_stack matches in statistics/memory-alarm code are unrelated and retained.

## Decision Log


Delete the complete unused task/cost group while preserving the shared hashing
owner and its tests. Original Go task lifecycle and complete-package obligations
remain open; standalone private checks do not establish integration. Decision
date: 2026-10-05 UTC. No dependencies or maintained scripts change.

## Outcomes & Retrospective


Evidence is in parity/current-audit/task-model-cleanup-validation.json; findings
remain 86 tracked / 30 repaired / 56 unresolved. Before-images, hashes and final
publication handoff are in /workspace/.cloud-setup/task-model-cleanup. Recover
individual files with git show eb31889eab13d07727868682a95efed6402fc385:<path> into
a temporary file before reviewing restoration. Preserve concurrent changes and
never reset or force-push. No new interface or permanent script is introduced.
