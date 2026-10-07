# Prepare ALTER constraint jobs against the original schema

This living ExecPlan follows `PLANS.md`.

## Purpose / Big Picture

A multi-clause ALTER must admit every job against the original table before
scanning rows or publishing metadata. This batch repairs FK admission, implicit
index ownership and CHECK job eligibility together, following Go master
3ca96b1d5df8da123e7a650512654eedab12c861. Users should receive Go's original-schema
errors, normal index conflicts and unsupported CHECK combinations without partial
metadata changes or prematurely consumed foreign-key identifiers.

## Progress

- [x] Read instructions and compare Go executor, foreign-key and multi-schema owners.
- [x] Add regressions in existing session constraint suites.
- [x] Separate constraint preparation from application and compose implicit indexes.
- [x] Run grouped regressions, affected checking, lint and real SQL validation.
- [x] Update finding evidence and prepare publication through the actual build hook and fresh push gate. Post-commit results belong in `/workspace/.cloud-setup/constraint-admission-batch/final-handoff.json`.

## Context and Orientation

`rust/crates/tidb-executor/src/ddl/alter_table.rs` already stages catalog changes
and prepares column/index actions against the original table. Its FK and CHECK
arms still run sequentially during application. Go `pkg/ddl/executor.go` instead
collects jobs; `multi_schema_change.go::fillMultiSchemaInfo` rejects CHECK jobs and
collects implicit FK index jobs through ordinary index admission. D11/D02/D01
remain unresolved ownership findings; this is existing-owner repair, not acceptance
of a complete Go package or a durable distributed DDL worker.

## Plan of Work

Extend the existing preparation phase with owned constraint changes. Build FK
metadata without reading rows or consuming IDs, prepare each required index with
the shared index builder, then apply it before the FK using existing staged
storage. Resolve anonymous FK names once from the original maximum. Admit CHECK
metadata before rejecting unsupported multi-schema jobs; retain single-action
behavior and filter LOCK specifications as Go does. Remove the superseded direct
constraint dispatcher and manual implicit-index construction.

## Milestones

First reproduce the failures with existing session test helpers. Then integrate
constraint preparation into the same admission and conflict phase as columns and
indexes. Finally prove diagnostics and atomicity over Rust tests and real MySQL
unistore, update both audit registers without closing broad findings, and run the
required publication gates.

## Concrete Steps

In `/workspace/tidb/rust`, activate `/workspace/.cloud-setup/env.sh` and set
`CARGO_BUILD_JOBS=1`. Run the existing session constraint and ALTER test filters,
then `cargo check --locked --all-targets -p tidb-executor -p tidb-session -p tidb-server`.
Run `make lint` from the repository root. Run
`cargo build --locked -p tidb-server` from `rust` for server validation, in the
actual pre-commit hook, and freshly immediately before push.

## Validation and Acceptance

Expect missing original FK/CHECK columns, duplicate implicit index names, and
unsupported CHECK combinations to match Go diagnostics. Admission failures must
precede row violations and preserve the next anonymous FK name. Independent
implicit indexes must not disappear through application-time coverage checks.
Retain disabled CHECK compatibility and single-action CHECK row validation.
The real server refuses multi-clause FK ALTER with1105 before the local owner; in-process tests validate FK admission/backfill, while eleven live MySQL checks validate the supported persisted/grouped CHECK routes. Do not claim live FK parity. Logs live outside the repository in
`/workspace/.cloud-setup/constraint-admission-batch`.

## Idempotence and Recovery

Start from clean TiDB commit64a1784320da8ee7887bcda356ff8b4e70b8cfb7 and native
8b752f9638ad157931725b66ffdc57e0465432a9. Preserve other work. Never force-push,
bypass hooks or remove live MV/DDL owners. Test fixtures use fresh local sessions.
The shared CHECK builder now carries explicit CREATE/ALTER mode to both local and persisted-DDL callers. The grouped CHECK success test and two history assumptions were stale: CREATE history must survive while CHECK moves from active to history. Native client changes are unnecessary for this SQL admission batch.

## Surprises & Discoveries

Go does not allow CHECK jobs in multi-schema changes, even enforcement no-ops.
Go admits an implicit index for each FK against original metadata, so a sibling
FK index must not suppress it at application time.

## Decision Log

- Decision: batch the FK, implicit-index and CHECK repairs in the shared ALTER
  admission owner. Rationale: they share the same ordering defect and validation
  surface. Date/author: 2026-10-07, Codex.

## Outcomes & Retrospective

157 selected Rust tests and 11 real MySQL/unistore assertions pass after seven baseline Rust failures and two baseline wire mismatches. Shared CREATE/ALTER CHECK modes also repair persisted admission; the previous grouped-CHECK success test was stale and now asserts Go refusal plus unchanged metadata. The post-commit publication outcome is recorded in `/workspace/.cloud-setup/constraint-admission-batch/final-handoff.json`; it cannot be embedded in its own commit. No full package, distributed DDL, live TiKV
or performance acceptance is implied.

## Artifacts and Notes

The committed validation receipt will record exact commands and source hashes.

## Interfaces and Dependencies

Use existing `Catalog`, `KvForeignKey`, `PreparedIndexChange`, `IndexSpec`, and
CHECK metadata builders. Add no dependencies or alternate storage owners.
