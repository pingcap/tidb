# Remove matrix-table write execution

This living ExecPlan follows root PLANS.md.

## Purpose and context


Remove the Rust-only mutable value-matrix storage path from INSERT, UPDATE and DELETE. Ordinary tables already use KvTable and TiKV-format bytes. MemTable remains the read-only materialization used by virtual tables and query fixtures. Go pkg/infoschema/tables.go rejects AddRecord, UpdateRecord and RemoveRecord; Go buildMemTable has no invented snapshot-position column. Writable fixtures must use CREATE TABLE and the shared writer.

Cloud source starts at 35fcc5d8538014d65fb40b3d662afb22557411a9 on hparser-integration in /workspace/tidb. Go comparison is 93a01d31f6da205ae4bf376825293903a6899fdb under /workspace/.cloud-setup/go-master. Native remains 19a56ccda1e128218cd33c69709038219aced9bc. Remote integration through 91010d96bb3e32468bd56ea2b07d255248485489 stays preserved unmerged. No push or dry run. This advances E03 maintenance, not whole-package acceptance.

## Progress


- [x] Trace matrix mutation branches, synthetic positions and writable fixtures against Go.
- [x] Capture rejection regression: all six original write forms succeed and change matrix rows.
- [x] Remove mutation/position machinery and unused rollback query; migrate fixtures.
- [x] Group behavior tests:208 driver passes with eight unchanged baseline failures,156 session passes; affected all-target checks, lint and self-review pass.
- [ ] Update registers/evidence, gated local commit, recovery and Cloud draft.

## Work and milestones


First capture a regression in existing driver DML tests: materialized read sources must not mutate. Then remove alternate writers and replace UpdateRowId with TableHandle. Delete SourceTable.has_row_position and its planner/scan consumers, descending-removal accommodation and unused Catalog.has_mem_tables. Keep MemTableStorage, the distinct real byte store. Migrate two writable driver fixtures to ordinary SQL while retaining their assertions. Virtual-table SQL error ordering remains outside this maintenance claim.

## Validation and acceptance


Activate /workspace/.cloud-setup/env.sh in each shell; run Cargo in /workspace/tidb/rust with CARGO_BUILD_JOBS=1. First run cargo test --locked -p tidb-executor --lib -- matrix_tables_are_read_only --test-threads=1 and record successful unwanted writes as the failure. After edits group driver DML/round-trip/aggregation and session joined/prepared/rollback cases. Run cargo check --locked -p tidb-planner -p tidb-executor -p tidb-session --all-targets, root make lint and git diff --check. Normal git commit must pass the actual hooks/pre-commit locked tidb-server build. Do not duplicate that build merely to prefill a receipt. Exact commands/outcomes belong in current-audit/matrix-write-removal-validation.json and external /workspace/.cloud-setup/matrix-write-removal logs.

## Surprises & Discoveries


Catalog.has_mem_tables has no caller. Only two driver functions write matrix fixtures; the aggregation search hit writes an ordinary table, not its read-only fixture. Production-created ordinary tables already use KvTable. The first pre-edit command used the wrong directory and started a zero-filter baseline build; it was stopped and is not validation.

## Decision Log


- Remove the entire mutation/identity lifecycle together; preserve virtual reads and meaningful SQL assertions. Date: 2026-10-04.
- Keep upstream package/test obligations and existing unresolved dispositions. Date: 2026-10-04.

## Outcomes & Retrospective


The batch removes matrix append/replacement/removal, synthetic row-position metadata and consumers, four unreferenced interpreter helpers, the unused rollback query and duplicate INSERT SELECT staging. Two fixtures migrate to ordinary CREATE TABLE; all their SQL assertions remain. The new regression rejects six originally successful matrix mutations and proves read-only sources still feed ordinary INSERT SELECT and joined UPDATE/DELETE. Grouped validation passes364 distinct cases; eight driver failures reproduce in the preserved original executable and remain unresolved. Final helper-only deletions are covered by all-target checking and the actual commit hook. E03 remains partial for retained write-row vectors and complete planner handle-column metadata. No full Go suite, live cluster or benchmark claim.

## Recovery and interfaces


No new dependencies. Restore only owned hunks if needed; never reset concurrent changes. After gated local commit verify /workspace/.cloud-setup/tidb-unpublished.bundle and save exact repository refs/startup instructions in the Cloud draft. Saving does not publish or prove fresh-task restoration.

Final evidence lives in parity/current-audit/matrix-write-removal-validation.json. Exact post-commit hook/wire/recovery/draft outcomes are recorded in /workspace/.cloud-setup/matrix-write-removal/final-handoff.json to avoid another documentation-only build cycle.
