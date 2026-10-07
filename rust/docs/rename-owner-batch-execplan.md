# Share rename admission, metadata and publication


This living ExecPlan follows root PLANS.md. Keep progress, discoveries, decisions and outcomes current.

## Purpose and context


Repair the connected D01/D11/E02/I04 rename lifecycle. Local RENAME, ALTER RENAME and direct persistent planning disagree on source/destination error precedence, view cross-schema admission and target identifier limits. Persistent renaming does not update referring foreign-key metadata or publish those changed children to peer catalogs. Follow fresh Go master 7a3dacb52efe58d28db360ae8639d8838c376544, pkg/ddl/executor.go ExtractTblInfos, table.go rename workers and pkg/infoschema/builder.go. Baseline TiDB is 08a31d9e2beb5f32b7b7107a5072fa73e1285ed1; native aa2c60f37481c2fe7f03997535d4f238ed485956 is unchanged. Work in the existing Cloud checkout, preserving concurrent changes.

## Progress


- [x] Refresh Go and integration; trace existing rename owners and source contracts.
- [x] Capture grouped baseline failures in existing SQL and metadata suites.
- [x] Replace duplicated admission with shared policy; compose retained FK identities and affected-table publication.
- [x] Run grouped regressions, affected checks, lint and real-server validation.
- [x] Update both registers and the durable validation receipt.
- [ ] Publish through actual hook/fresh locked-build gates and save the Cloud checkpoint; final-handoff.json records post-commit results.

## Work and acceptance


Keep source and destination identities through ordered staging. RENAME missing source checks an occupied destination before returning1146; ALTER missing source stays1146. ALTER identity is a no-op. Existing views cannot move schemas. Missing destination schema is1025; occupied destination precedes identifier length. Preserve all-or-nothing admission, local temporary refusal, cached-table policy, table IDs and auto-ID schema anchors. Resolve all callers before deleting private checks.

Persist renamed parents and affected child foreign keys through the same metadata transaction and publish every modified table in its schema difference, including chained and self references. Inspect persisted handlers before selecting their shared mutation boundary; do not claim the direct DDL path is complete durable execution. Go's FK helper skips unchanged table names, including schema-only moves; preserve that source behavior rather than inventing a stronger contract. Full durable DDL, masking/placement, metadata locks and complete package acceptance remain unresolved.

## Validation and recovery


Activate /workspace/.cloud-setup/env.sh and CARGO_BUILD_JOBS=1. Run TiDB Cargo from rust/. Group baseline and final filters by target, retain actual failure counts, and run affected all-target checks plus make lint. Reuse the existing real-server MySQL helper infrastructure outside the checkout. Every normal commit must execute hooks/pre-commit and cargo build --locked -p tidb-server; repeat that exact build immediately before every push and verify remote SHA. Preserve exact destinations and never force-push. Retire only completed executables with hash/hard-link/accessible process receipts when necessary; keep sources and compiler caches. External evidence lives in /workspace/.cloud-setup/rename-owner-batch.

## Surprises & Discoveries


The local rename path checks missing source before destination while persistent planning returns1049 for absent schemas. Current Go ExtractTblInfos defines a shared ordered contract. Persistent ordinary renaming emits only moved TableInfo records; its existing child FK owner and publication are disconnected.

## Decision Log


Use existing production owners and original suites. Do not add another standalone rename engine, change generated artifacts, or accept a package from a subset of checks. 2026-10-07, Cloud continuation.

## Outcomes & Retrospective


Shared rename admission and direct FK metadata/publication repairs are implemented. Seven Rust regressions and nine of18 live checks fail before repair. Final grouped checks pass257 Rust tests and18 real MySQL/unistore assertions; affected all-target checks, lint and locked server build pass. One obsolete private DROP planner test block is retired while SQL coverage remains. The direct multi-table diff now retires original schema entries; changed children appear once in affected options. D01/D11/E02/I04 remain open/partial for their documented durable/package obligations; counts remain86 tracked/30 repaired/56 unresolved. Publication and configuration outcomes are recorded in the external final handoff.

Review boundary: ordinary SQL uses the existing direct metadata transaction, while persisted rename actions retain separate job/phase ownership. This batch does not accept or repair that full durable helper, masking/placement effects or process-wide foreign-key enablement. Additional self-reference, moved-child and schema-only controls supplement the seven baseline regressions. Multi-table direct publication must use Go's first-phase source schema IDs because no prior publication retired the old entries.

Grouped validation found one stale block in cluster_ddl_alter_source::a_missing_table_is_1146_everywhere_except_drop_table. It still invoked the removed atomic multi-target DROP planner. Remove only that block; keep single-target error checks and the passing SQL if_exists_demotes_the_error_it_swallowed_to_a_note coverage plus the prior live DROP receipt. Initial metadata result57 passed/1 failed is retained separately; rerun the affected metadata group after this test-only cleanup. A first cleanup command used the wrong working directory and did not edit files; its unchanged rerun is not a new behavioral result.
