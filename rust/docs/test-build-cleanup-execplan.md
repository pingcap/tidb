# Consolidate metadata model tests

This living ExecPlan follows root PLANS.md. The preceding semantic-workflow
cleanup was published as 493a04b1c6c1c1eb9c178f48d8fa88842e7af8c0; its completed
publication evidence is in /workspace/.cloud-setup/semantic-workflow-cleanup.

## Purpose and Context


Reduce redundant test execution and linking in rust/crates/tidb-model, the Rust
owner of Go pkg/meta/model. Cargo currently discovers three integration test
binaries. Register the two retained suites in tests/all.rs and move distinct
assertions from the third into existing owner tests. Preserve production code,
Go behavior checks, JSON representation, alias identity and process defaults.
Work in /workspace/tidb on hparser-integration. The comparison source is fetched
Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.

## Progress


- [x] Inspect all three integration suites, existing owners and Go source.
- [x] Consolidate binary registrations and migrate distinct boundary assertions.
- [x] Remove eight redundant registrations and one inactive placeholder.
- [x] Run grouped tests, metadata continuity, lint and final diff review.
- [ ] Commit through the actual hook; build immediately before normal push.
- [ ] Verify remote SHA and save the cloud checkpoint.

## Milestones and Plan of Work


The first milestone retires tests/pkg_meta_model_semantics.rs. Its BDR class,
DB JSON/clone, flag constants and unknown table-mode cases are checked in bdr.rs,
db.rs, table_mode.rs and the retained flag-width integration test. Existing job
enum, schema-state and table-identity owners replace three redundant anchor
tests; preserve schema value 255 and table version 5 in those owners.

The second milestone disables Cargo automatic integration discovery, registers
one all.rs binary and explicitly includes pkg_meta_model_package_anchors and
resource_group_semantics. Keep default process state in this separate binary;
library tests mutate global settings. Replace synthetic success/failure strings
with direct assertions in the retained runtime, process and reorg tests.
Remove only the inactive nextgen case from tests_pkg_meta_model_part2.rs, leaving
its other Go vectors unchanged. Correct b008.md: NextGen acceptance is still an
outstanding platform obligation, not a passing Classic test.

The final milestone validates all changed test owners together, updates the
current audit indexes without changing finding dispositions, and publishes using
the mandatory locked server gates. No full Go package acceptance is claimed.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-model --lib -- bdr::tests db::tests table_mode::tests schema_state::tests job_enums::tests table_info::tests::table_identity_hash_and_equality_use_only_id index::tests::global_index_v1_flag --test-threads=1
    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-model --test all -- --test-threads=1
    cargo metadata --locked --no-deps --format-version 1

Require every selected case to execute and pass. Compare metadata with the
before image: only model integration targets change from three to one; features,
dependencies and lockfile remain unchanged. Verify production prefixes before
cfg(test) and the resource-group suite are byte-identical. From repository root
run make lint, git diff --check and formatting checks on changed Rust files.
No Go or Bazel change requires bazel_prepare. This is test cleanup, not a bug fix.

Commit normally with core.hooksPath=hooks. The executable hook must pass cd rust
&& cargo build --locked -p tidb-server. Repeat that exact build immediately
before normal push to origin hparser-integration and verify the remote SHA.
Never bypass hooks or force-push.

## Surprises & Discoveries


The nextgen placeholder cannot assert: Cargo defines no nextgen feature. Go's
TestGlobalIndexV1SupportedForNextGen checks kerneltype.IsNextGen(), so retaining
an inactive Rust registration does not establish the platform obligation.
Library tests toggle globals; process-default checks must remain separate.

## Decision Log


On 2026-10-06 consolidate redundant registrations while preserving distinct
assertions in existing owners. Use one integration binary because neither of
its two retained modules mutates global model state. Keep Rust-specific JSON,
wide integer, nil/empty and alias checks; source-language name differences alone
do not establish that a test is useless. Do not claim measured speedup from a
binary count reduction.

## Outcomes & Retrospective


Implementation removes nine registrations and two integration binaries. All 57 grouped tests pass (29 library, 28 integration), with zero failed or ignored.
Metadata continuity, root lint and diff review pass; publication is pending. The 86 structural findings remain
30 repaired and 56 unresolved (27 open, 29 partial); this cleanup does not repair
production behavior.

## Recovery, Artifacts and Dependencies


Recover before-images using git show 493a04b1c6c1c1eb9c178f48d8fa88842e7af8c0:<path>;
do not overwrite concurrent work. Evidence belongs in
rust/docs/parity/current-audit/model-test-owner-cleanup-validation.json and
/workspace/.cloud-setup/model-test-owner-cleanup. Existing model APIs, dependency
versions and production consumers are unchanged. All future integration modules
must be registered explicitly in tests/all.rs. Cloud draft saving is distinct
from Publish and validation in a fresh task.

Revision 2026-10-06: replace the completed semantic retirement plan with model
owner consolidation and retain outstanding NextGen obligations explicitly.
