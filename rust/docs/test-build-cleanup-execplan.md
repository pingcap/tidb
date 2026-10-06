# Retire unused process and order-limit metadata models

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove two unused metadata models and their private harnesses in one batch.
Work in /workspace/tidb on hparser-integration from
dd556ddcbb552e2127f3081a2966df617b02b559. Refreshed Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Go sessmgr owns actual process
state and ByItems owns order expressions/direction. The removed process
marker record and configured order-key/limit/spec records have no live users.

## Progress


- [x] Trace types, imports, aliases and registrations across Rust sources.
- [x] Remove unused records, private harnesses and the completed sessmgr plan.
- [x] Retain prepared-read direction and actual session/process-list owners.
- [x] Validate retained SQL/prepared-read cases, source continuity and lint.
- [ ] Pass actual hook/fresh pre-push builds, remote and Cloud checkpoint checks.

## Milestones and Plan of Work


Delete tidb-exec process_info.rs and its test carrier. Trim planner
configured_order_limit_contract.rs to ConfiguredOrderDirection and
from_descending, keeping their implementation unchanged. Delete its private
record tests and the unused is_descending method. Remove registrations.
Retain existing read_only_prepared_order_source tests, which exercise real
SQL lowering and both sort directions. Remove the completed sessmgr audit
plan; keep its dated receipt with corrected current ownership. Mark retired
historical order-test commands explicitly instead of recommending them.

## Validation and Acceptance


Activate /workspace/.cloud-setup/env.sh; from /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-planner --test all -- read_only_prepared_order_source:: --test-threads=1
    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-session --lib -- tests_grants::processlist::show_processlist_lists_this_session --exact --test-threads=1

Expect existing prepared ordering/aggregate metadata assertions and actual
SHOW PROCESSLIST rows to pass. Verify all retained consumers/tests and the
remaining direction code against before-images. From repository root run
make lint and git diff --check. Normal commit must run the real hook's
cd rust && cargo build --locked -p tidb-server; rerun immediately before push
and verify remote SHA. No Go/Bazel/dependency/native changes are needed.

## Surprises & Discoveries


The clone test only proves Arc cloning on empty marker types; it never
exercises session ownership. Only the direction enum from the configured
contract has live callers; prepared SQL tests already cover both values.

## Decision Log


Preserve real SQL and Rust correctness tests. Retire disconnected records and
constructor-only assertions after tracing all callers. Keep complete upstream
obligations; this is neither sessmgr nor planner package acceptance.

## Outcomes & Retrospective


Implementation and validation complete: 20 retained SQL/planner tests passed,
along with lint and continuity/diff checks. Removed 450 net Rust lines and
five private tests. Publication gates remain pending here; external
final-handoff.json records their later results. No full Go suite,
live cluster run or measured speedup is claimed.

## Recovery, Artifacts and Dependencies


Before-images: git show dd556ddcbb:<path>. Restore individual files only and
preserve concurrent work. External logs/inventory:
/workspace/.cloud-setup/metadata-model-cleanup. Durable receipt:
rust/docs/parity/current-audit/metadata-model-cleanup-validation.json.
Draft save, Publish and future restore are separate. Revision: replace
completed execution-detail cleanup with unused metadata model retirement.
