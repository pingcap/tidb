# Remove disconnected planner paths and private harnesses

This living ExecPlan follows root PLANS.md. Earlier cleanup receipts remain
indexed by parity/current-audit/README.md; Git retains retired implementations.

## Purpose / Big Picture


Remove seven disconnected planner modules, their eight private integration
harnesses and stale documentation together. Real planner/expression/executor
owners and their tests stay byte-identical. Reduce compiled inputs without
claiming measured speedup or complete Go-package acceptance.

## Context and Orientation


Base e0f77d1aab4ade4ca40643f709948f21cd563d2f on hparser-integration.
Refreshed Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Native master
cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 is unchanged. Retire columnar_index_extra,
index_advisor_model, predicate_partition, typed_condition, schema_table_key,
string_writer and telemetry at the tidb-planner crate root. The predicate pair
only calls each other and private tests. Other candidates only have private
harness consumers. Go uses actual logical/physical operators, metadata and
expression owners; Rust's normalized copies do not validate those paths.

## Progress


- [x] Refresh refs, inspect source owners and trace all Rust references.
- [x] Delete seven modules, eight harnesses and stale documentation together.
- [x] Verify retained inputs, generated test registration, grouped all-target checking and lint.
- [x] Self-review and unchanged remote base; normal hook/pre-push gate results are recorded after commit in the external final handoff.

## Milestones and Plan of Work


Remove the seven root module declarations and corresponding source/test files.
Retire the disconnected writer parity receipt and its index entry. Remove the
obsolete predicate hazard/remediation instructions; preserve live rule findings.
Correct telemetry and identifier historical receipts and the stale crate header.
Record exact removed hashes and unchanged inputs in the cleanup receipt.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From rust/ run:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-planner -p difftest-planner-tests -p tidb-server --all-targets

Run root make lint and git diff --check. Verify every retained Rust source,
manifest and script hash, allowing only the root module declaration/header edit.
Inspect freshly generated registrations for both affected aggregates. No runtime
rerun is required when all retained executable behavior and tests are unchanged.
Normal commit must execute hooks/pre-commit and its locked tidb-server build.
Immediately before every push rerun cd rust && cargo build --locked -p tidb-server;
verify remote SHA and preserve concurrent work. No Go/Bazel/dependency edits.

## Surprises & Discoveries


The predicate pair advertises a future evaluator and has no live caller. A prior
audit already recommended removing this latent join-type-blind routing API.
The private telemetry test constructs its own PlanNode, not the shared plan tree.
Keep real Go-contract and Rust ownership tests even when names differ from Go.

## Decision Log


Delete entire disconnected groups; do not weaken retained assertions or remove
required build hooks. Retain shared condition_binding/residual_condition and
hashing implementations because they have real callers or useful owner tests.
No new permanent script or harness. Date: 2026-10-05 UTC.

## Outcomes & Retrospective


Evidence belongs in parity/current-audit/planner-private-path-cleanup-validation.json
and /workspace/.cloud-setup/planner-private-path-cleanup. Counts remain
86 tracked / 30 repaired / 56 unresolved. No source behavior fix, whole-package
acceptance, full Go/live TiKV run or performance claim follows from dead deletion.
Recover files with git show e0f77d1aab:<path> into temporary files before restoring.
External final-handoff.json records post-commit gates, remote SHA and setup draft.

Validation result: 24 private tests and 1,701 source/test lines removed; 3,594
retained inputs are byte-identical. Fresh aggregate registrations exclude all eight
retired harnesses. Grouped all-target checking, make lint and diff checks pass.
