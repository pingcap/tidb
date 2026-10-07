# Share view admission, privileges and persistent publication


This living ExecPlan follows PLANS.md. Update progress, discoveries, decisions
and outcomes at every boundary.

## Purpose and context


CREATE VIEW must validate its query and required SELECT privileges before
publishing a persistent object. Local temporary target names must retain their
rows; CREATE OR REPLACE must not destroy an ordinary table. Query names belong
to the submitting database, independently of a qualified destination schema.
This connected A01/D01 maintenance does not accept complete upstream packages.

Work in /workspace/tidb on hparser-integration from 894292170d. Native master
8b752f9638ad157931725b66ffdc57e0465432a9 is unchanged. Fresh Go master is
7a3dacb52efe58d28db360ae8639d8838c376544, exported in
/workspace/.cloud-setup/go-master. The source owners are planner/core/planbuilder.go
CreateViewStmt, executor/ddl.go::executeCreateView, and
ddl/executor.go::CreateView/createTableWithInfoJob. Go parser/parser.y has no
ordinary ALTER VIEW grammar. The Rust parser already refuses it; its separate
AST and executor path are dead. Remove the executor now; the generated visitor surface has no discoverable Rust generator and must not be hand-edited.

## Progress


- [x] Confirm clean checkouts and refresh Go/integration refs.
- [x] Record 9/9 failing session regressions and 9/16 failing live checks.
- [x] Repair shared definition/target admission and privilege callers; remove dead ALTER VIEW execution.
- [x] Validate the complete batch boundary and update both finding registers: 217 Rust cases and 16 real MySQL assertions pass; all-target check, lint and locked build pass.
- [ ] Commit with actual hook, fresh locked build, normal push and remote SHA check (external final-handoff.json records these post-commit outcomes).

## Plan of work and milestones


Extend existing view and grant suites, using real Session SQL and restricted
accounts. Capture failures before changes, along with a live unistore/MySQL
baseline. Add persistent target lookup/publication to the existing Catalog
overlay owner, preserving local objects. In view.rs validate the body before
durable target admission, reject replacing non-views, and bind body tables using
the submitting database. Session privilege collection must share SELECT body
visits, CREATE VIEW, and replacement DROP ordering. Migrate all consumers and
remove the unparsed AlterView executor and dispatch branches. Leave generated AST/visitor artifacts unchanged until their generator is restored.
Retain the existing source refusal test; do not invent ALTER VIEW support.

## Validation and acceptance


Activate /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. From rust/:

    cargo test --locked -p tidb-session --lib -- view_owner_batch --test-threads=1
    cargo test --locked -p tidb-session --lib -- tests_views tests_grants tests_temporary_tables --test-threads=1
    cargo test --locked -p tidb-session -p tidb-exec --test all -- nested_view_and_alter_view_source a_create_view_publishes_a_view_table_info --test-threads=1
    cargo check --locked --all-targets -p tidb-executor -p tidb-session -p tidb-server -p tidb-exec
    cargo build --locked -p tidb-server

Run make lint from the repository root. The external wire.py under
/workspace/.cloud-setup/view-owner-batch starts and joins its own unistore server;
run with PYTHONPATH=/workspace/.cloud-setup/python. Failed replacement and denied
grants must preserve both persistent and local objects. Targeted controls that
already pass are not repaired defects. The actual hook must run the locked
server build; repeat immediately before normal push and verify the remote SHA.

## Idempotence and recovery


Keep concurrent edits and dependency caches. Retire only identified completed
executables after hash, hard-link and accessible process checks when link space
is needed. Never force-push or bypass hooks. Update a verified external recovery
bundle and the cloud checkpoint after publication.

## Surprises & Discoveries


Go does not parse ALTER VIEW. Rust carries an unreachable AST/executor claiming
that Go has such an owner, while an existing retained test already expects its
syntax refusal. Remove that implementation instead of enabling it.

## Decision Log


- Keep session query visibility separate from persistent target visibility in
  the same Catalog owner. This also protects temporary source rejection.
- Shared privilege collection, rather than a view-private grant checker, owns
  body SELECT and replacement DROP visits for embedded and cluster callers.

## Outcomes & Retrospective


Shared implementation, 217 selected Rust tests, 16 live MySQL assertions, all-target checking, lint and the locked server build are complete. Commit/push gates are recorded in the external final-handoff.json after this tracked receipt is committed. Durable DDL, complete privilege/planner
packages, CTE view canonicalization and broader runtime findings remain open.


Scope correction: repository search found generated AST visitor blocks but no maintained Rust generator. This batch removes the dead executor API and dispatch, retaining the generated AST artifact rather than hand-editing generated code. The parser already rejects ALTER VIEW; its existing refusal test remains.

Self-review correction: persistent view publication beneath a local shadow now detaches and restores the existing overlay around normal register_view_in. Reusing that owner preserves transaction version and metadata invalidation instead of duplicating bookkeeping. S03 concerns session migration and is not advanced by this batch; the selected parents are A01 and D01.
