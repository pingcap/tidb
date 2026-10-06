# Retire disconnected join and index-split planning leaves

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove two unused planning implementations and their private tests together.
Work in /workspace/tidb on hparser-integration from
e48cdc301e7d609adbb4c5727b77c28c2607d952. Fresh Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. The standalone JoinSchema classifier
has no caller in live planning: Go binds joins in PlanBuilder.buildJoin and
maintains FullSchema in logical operators. The AUTO split helper likewise
has no live caller; Go's autoPreSplitIndexRegion integrates stats loading,
shared deadlines, boundary caching and region operations. Retiring isolated
prototypes reduces maintained code without changing those runtime owners.

## Progress


- [x] Trace both implementations, test-only callers and Go owner boundaries.
- [x] Remove the two modules, two private carriers and four registrations.
- [x] Correct the historical automatic index-split receipt.
- [x] Validate retained join SQL and durable DDL marker, source continuity and lint.
- [ ] Complete actual hook, fresh pre-push build, remote and Cloud checkpoint checks.

## Milestones and Plan of Work


Remove rust/crates/tidb-planner/src/join_condition.rs and
rust/difftests/planner-tests/tests/join_condition.rs. Remove
rust/crates/tidb-exec/src/auto_pre_split.rs and its tests/auto_pre_split_source.rs.
Unregister them from their lib.rs and tests/all.rs files. Preserve the actual
logical join rewrite, session SQL paths, parser/AST options, durable IndexArg
and cluster_ddl marker propagation byte-for-byte. Mark the old claims in
rust/testport/receipts/ddl_auto_presplit_audit.md as historical; retain Go's
full original package obligations. Update both structural registers and the
current-audit index without closing findings.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh in every build shell; work in
/workspace/tidb/rust, with CARGO_BUILD_JOBS=1:

    cargo test --locked -p tidb-session --lib -- tests_coalesced_joins:: tests_join_predicate_placement:: --test-threads=1
    cargo test --locked -p tidb-exec --test all -- cluster_ddl_source::create_index_auto_pre_split_marker_reaches_catalog_write --exact --test-threads=1

Existing SQL rows, null semantics, USING visibility and join predicates must
pass. The retained catalog-write test must preserve AUTO/manual precedence.
Source continuity must show only deleted modules and registrations changed
among Rust code; no dependency changes or surviving retired imports. Run
make lint and git diff --check at root. Commit normally through the actual
locked server-build hook; run cargo build --locked -p tidb-server immediately
before authorized push and verify remote SHA. No Go/Bazel changes or new
behavioral fixes, so no oracle regeneration or new regression required.

## Surprises & Discoveries


The join model's sole consumer is under difftests/planner-tests, not the
planner crate's own test tree. AUTO parser/catalog markers are active, but
the statistics-to-keys helper is not called by any production source.

## Decision Log


Remove test-only models and their private assertions after tracing all users.
Preserve useful tests on actual owners and all original Go obligations.
Do not equate prototype removal with implemented AUTO region splitting or
complete join/package parity. Keep historical receipts explicitly dated.

## Outcomes & Retrospective


Implementation and validation complete: 43 retained-owner tests passed.
Lint and source continuity passed. Removed 1247 net Rust lines and ten
private tests. Publication gates remain pending in this committed receipt;
external final-handoff.json records their completion. No measured build-speed claim, full
Go suite, live region split or cluster acceptance is implied by this cleanup.

## Recovery, Artifacts and Dependencies


Restore individual before-images with git show e48cdc301e:<path>, preserving
concurrent work. External logs/inventory live at
/workspace/.cloud-setup/planning-leaf-cleanup. Durable evidence belongs in
rust/docs/parity/current-audit/planning-leaf-cleanup-validation.json.
Publication gates are recorded externally after the commit to avoid another
source mutation solely for a receipt. Cloud draft save is separate from
Publish and fresh-task restoration. Revision: replace completed metadata
cleanup with disconnected join/AUTO planning retirement.
