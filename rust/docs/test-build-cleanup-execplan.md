# Retire superseded planner and ranger models

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove obsolete alternate representations instead of maintaining their private
tests. Work in /workspace/tidb on hparser-integration from
887c4d9f4c483d581f0f2cc0893398a3686a93f9. Fresh Go master remains
5b7e1eb8f5f8252391b6e68330d1648a26a80c17. Go baseimpl.Plan and ranger/detacher.go
operate on actual plan/expression owners; the deleted normalized models had
no runtime callers. Live PhysicalPlan, BasePlan and ranger::detacher already
supply those operations and tests.

## Progress


- [x] Verify complete Rust callers and compare Go owners.
- [x] Remove five obsolete files and their registrations.
- [x] Move two live executor cases intact; retain duplicate-candidate coverage
  on the real Expression condition-list helper.
- [x] Run grouped owner tests (23 passed), lint, continuity and self-review.
- [ ] Complete actual precommit build, fresh pre-push build and publication.

## Milestones and Plan of Work


Delete planner src/plan.rs and src/range_detacher.rs plus their tests/primitives
suites. Delete executor src/ranger_detacher.rs, whose generic integer helpers
and Boolean model tests have no live callers. Move its two actual index-range
cases with their helper functions to src/index_range/detacher_tests.rs and
register that test-only module in index_range.rs. Preserve byte-equivalent
function bodies. Extend the existing expression-owner condition-set test with
the removed duplicate-candidate and empty-input checks; no new test harness.
Update historical receipt guidance and both finding registers.

Keep index_columns and its original Go cases: no verified migration exists.
Do not remove actual planner/ranger owners, mandatory build checks or useful
Rust-specific correctness tests merely because test names differ from Go.

## Concrete Steps and Acceptance


Source /workspace/.cloud-setup/env.sh in each build shell. From rust/ run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-planner -p tidb-executor --lib -- ranger::detacher::tests:: plan_base::tests:: physical::tests::tree_construction_and_base_accessors index_range::detacher_tests:: --test-threads=1

All selected live-owner cases must pass. From repository root run make lint
and git diff --check. Verify deleted symbols have no executable references,
retained test/helper bodies match the before-image and production function
bodies in retained files are unchanged. No Go/Bazel changes require regeneration.
Commit normally with the actual locked server build hook, then from rust/
run CARGO_BUILD_JOBS=1 cargo build --locked -p tidb-server immediately before
normal authorized push to pingcap/tidb hparser-integration. Verify remote SHA.

## Surprises & Discoveries


The deleted PlanNode test cited a Go planbuilder line now testing ALTER DDL
jobs; its string metadata carrier is not a live Go plan. Real plan-tree tests
already cover preorder, IDs and missing statistics. The normalized ranger
model's assertion that expression owners do not exist is stale: the live
ranger uses Expression, ConditionChecker and semantic equality. Executor
integer-list helpers duplicate those live operations but have no callers.

## Decision Log


Retire the complete unused models as one batch. Preserve the two real range
cases by moving them, preserve semantic list coverage on its actual owner,
and retain original index-column cases until a proper migration. Tests of
removed private models are not evidence of complete Go-package coverage.

## Outcomes & Retrospective


Implementation and grouped validation complete: 23 passed; make lint,
continuity and diff checks passed. Publication evidence is recorded after
commit in external final-handoff.json. Five
files removed, two live tests retained, eleven private test declarations
removed; 827 net Rust lines deleted. No finding closure, complete package acceptance or measured speedup.

## Recovery, Artifacts and Dependencies


Before-images are recoverable from the base commit; restore affected paths
only, preserving concurrent work. External inventory and validation live in
/workspace/.cloud-setup/planner-model-cleanup; the durable receipt is
rust/docs/parity/current-audit/planner-model-cleanup-validation.json.
Postcommit publication and Cloud draft results belong in final-handoff.json.
No new dependencies. Draft save, Publish and fresh-task restoration remain
separate states.
