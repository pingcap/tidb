# Remove disconnected logical-operator models

This living ExecPlan follows root PLANS.md. Earlier receipts remain indexed in
parity/current-audit/README.md. Git preserves retired source and prior plans.

## Purpose / Big Picture


Remove unused normalized operator copies and private test harnesses as one
batch. The shared logical-plan tree remains the implementation owner. Success
means fewer compiled inputs with all retained production code and useful test
bodies unchanged; no measured speedup or complete package acceptance is claimed.

## Context and Orientation


Base 89417915aff3596dd9b5a30361532133027337ab on /workspace/tidb hparser-integration.
Fresh Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed; native client remains
cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1. The thirteen crate-root models are
logical_union_all, logical_max_one_row, logical_top_n, logical_table_dual,
logical_sequence, logical_show, logical_show_ddl_jobs, logical_limit,
logical_sort, logical_mem_table, logical_schema_producer, logical_cte_table and
resolve_grouping_expand. These independent representations have no production
callers. Shared logical/ implementations and logical_lock/data_source owners
are retained. Go's generated hash methods belong to actual logical operators;
tests of normalized identity tokens do not validate those operators.

## Progress


- [x] Refresh refs, inspect Go owners and trace all tracked Rust references.
- [x] Remove thirteen models and thirteen private harnesses; trim ten adapter tests from the mixed hash harness while preserving its two real-owner test bodies.
- [x] Remove stale ownership and complete-suite claims.
- [x] Grouped checks, 3,631 unchanged retained inputs, two byte-identical retained test bodies, fresh registration, lint and self-review.
- [ ] Actual precommit build, fresh prepush build, normal push, remote verification and Cloud draft handoff.

## Milestones and Plan of Work


Inventory complete dependency groups before deleting them. Remove root module
declarations and their difftests together; trim only the private portion of
logicalop_hash64_equals_source.rs. Remove obsolete comments from six real-owner
files and correct the historical b089 receipt. Record deletion hashes and Go
artifact obligations in operator-model-cleanup-validation.json. No original Go
test, dependency, maintained script or production method changes.

## Validation and Acceptance


Activate source /workspace/.cloud-setup/env.sh in each build shell. From rust/:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-planner -p difftest-planner-tests -p tidb-server --all-targets

From repository root run make lint and git diff --check. Verify every retained
source/manifest/script hash: only lib.rs declarations, six comment-only files
and the mixed test harness may differ. Its two retained function bodies must
be byte-identical. Fresh generated test registration must drop thirteen private
harnesses while retaining logicalop_hash64_equals_source. No runtime test rerun
is required for pure unreachable deletion with unchanged retained behavior.
Normal commit must run the actual executable hooks/pre-commit selected by
core.hooksPath=hooks, including cd rust && cargo build --locked -p tidb-server.
Repeat that locked build immediately before each push and verify remote SHA.

## Surprises & Discoveries


The hash harness mixes ten tests of normalized identity copies with two tests
of real Projection and SchemaProducer implementations. Deleting it wholesale
would remove valid coverage. Keep those two tests unchanged. ColumnIdentity
and SortByItem matches elsewhere belong to independent retained owners.

## Decision Log


Delete disconnected models together with their private consumers; retain shared
hashing, real logical operators and all their tests. The cleanup removes 3,178 source/test lines and 71
test functions without weakening retained assertions. Date: 2026-10-05 UTC.
No new harness or permanent script is introduced. Keep full Go package/test
obligations separate from retired Rust adapter claims.

## Outcomes & Retrospective


Evidence: parity/current-audit/operator-model-cleanup-validation.json. Register
counts remain 86 tracked / 30 repaired / 56 unresolved. No behavioral finding
closure, full Go suite, live TiKV validation or measured performance claim.
Before-images and final publication handoff are under
/workspace/.cloud-setup/operator-model-cleanup. Recover individual files with
git show 89417915aff3596dd9b5a30361532133027337ab:<path> into a temporary file
before reviewing restoration. Preserve concurrent work and never force-push.
