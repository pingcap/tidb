# Separate persistent DDL from session temporary visibility


This living ExecPlan follows root PLANS.md. Maintain Progress, discoveries,
decisions and outcomes as implementation proceeds.

## Purpose and context


Persistent schema operations must see the durable table and its foreign keys
even when this session owns a local temporary table of the same name. Ordinary
queries must continue to see the local table. This repairs connected D01/E02/D11
existing-owner behavior; it does not accept the complete Go DDL/session packages
or supply durable job recovery.

Work in /workspace/tidb on hparser-integration from 7742e4db58. Native
/workspace/client-rust master 8b752f9638ad157931725b66ffdc57e0465432a9 is unchanged.
Freshly fetched Go master is 7a3dacb52efe58d28db360ae8639d8838c376544, exported
under /workspace/.cloud-setup/go-master. Go pkg/executor/ddl.go separates local
CREATE, DROP and TRUNCATE from persistent ddl.Executor calls. The latter use
infoCache.GetLatest in pkg/ddl/executor.go, including admitted CREATE LIKE and FK lookup. Go planner preprocessing first rejects a local LIKE source; retain that early refusal.
Rust Session::with_catalog_mut currently overlays local tables for all callers.

## Progress


- [x] Refresh source pins and inspect shared catalog and DDL dispatch owners.
- [x] Record grouped fail-before regressions: all11 initially fail; ten are valid defect regressions, one LIKE expectation required correction.
- [x] Introduce explicit persistent catalog access and migrate connected DDL callers.
- [x] Validate202 grouped cases,51 live MySQL checks, affected all targets, lint and locked build; update both registers.
- [ ] Commit with the real hook, fresh locked build, normal push and SHA verification.

## Plan of work and milestones


First extend tests_temporary_tables.rs with real SQL cases for hidden FK children,
persistent CREATE/LIKE, rename destinations, view and sequence targets. Run the
group before production changes. Then add a persistent catalog access method in
txn.rs, sharing the existing storage overlay lifecycle while excluding local
name shadowing. In dispatch.rs select that method only for durable operations;
keep local CREATE/TRUNCATE and explicit local DROP on the session overlay. Split
mixed DROP once before durable execution and retire its local targets only after
success. Preserve CREATE VIEW query validation against local references.

Finally run grouped session tests, affected all-target checks, make lint and the
locked server build. Record exact failures and limits in the validation receipt.

## Validation and acceptance


Activate /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. From rust/:

    cargo test --locked -p tidb-session --lib -- persistent_ddl_ --test-threads=1
    cargo test --locked -p tidb-session --lib -- tests_temporary_tables tests_foreign_key tests_core::ddl --test-threads=1
    cargo check --locked --all-targets -p tidb-session -p tidb-server
    cargo build --locked -p tidb-server

Run make lint from the repository root. Target tests must fail before and pass
after repairs; local tables and rows must remain intact after persistent success
and refusal. No zero-match test run counts as validation. The actual executable
hooks/pre-commit must run its locked server build; repeat immediately before push.

## Idempotence and recovery


Preserve concurrent edits and dependency caches. Retire only identified inactive
completed executables or failed linker outputs with hashes and hard-link/process
checks when space is needed. Do not reset branches or bypass hooks. Failed SQL
cases use isolated Session instances. No native synchronization is needed.

## Surprises & Discoveries


The catalog's universal local overlay can hide persistent FK children and object
identities even though the cluster DROP preflight already bypasses it. Local CREATE incorrectly revalidates existing persistent child FKs; bypass that
durable-parent validation for local creation. The baseline local TRUNCATE case
fails at this setup step, before reaching TRUNCATE. Database
drop also exposes a separate known lifetime gap for local tables whose database
is dropped; do not claim this gap repaired by catalog selection alone.

## Decision Log


- Repair the shared access policy and connected callers together, retaining
  session query visibility and global temporary row isolation. This follows
  Go's two schema scopes without introducing a competing catalog owner.

## Outcomes & Retrospective


Implementation and validation are complete:202 Rust cases and51 live MySQL/unistore checks pass, with affected all-target checking, make lint and the locked server build. Ten accepted overlay regressions and the existing ALTER-option regression fail before repair. LIKE remains a source-owned refusal control. Broad structural findings remain unresolved until complete-owner obligations are met. Hook/publication results follow in /workspace/.cloud-setup/persistent-ddl-batch/final-handoff.json.


Revision: grouped testing passed200 cases and exposed an existing ALTER UNION/INSERT_METHOD error-owner mismatch plus the incorrect new LIKE expectation. Preserve the Go LIKE refusal (8006); move ALTER option checks through the shared preprocessor before grants/implicit commit and migrate every caller to validate_ddl_preprocess. Initial option wiring used an unexported root path; corrected to tidb_executor::ddl::validate_table_options before rerunning.
