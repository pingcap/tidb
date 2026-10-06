# Consolidate Domain tests at their existing owners

This living ExecPlan follows root PLANS.md. Earlier cleanup receipts remain
indexed in parity/current-audit/README.md.

## Purpose / Big Picture


Remove four session-side Domain carriers and their repeated fixtures, including
a miniature SQL-table interpreter and a scripted schema-checker harness. Keep
useful vectors in existing tidb-domain tests so future changes exercise the
owning crate directly. No production behavior or complete package claim changes.

## Context and Orientation


Base 13d1bc836009c3cba540995e00eda76fefd91bad in /workspace/tidb on
hparser-integration. Go master remains b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
The session carriers tests_domain_ru_stats_source.rs,
tests_domain_topn_slow_query_source.rs, tests_domain_domain_utils_source.rs and
tests_domain_schema_checker_source.rs duplicate owner tests or script outcomes
that Go obtains from real storage/validator composition. Remove their four
cfg(test) module declarations from session/src/lib.rs.

Domain ru_stats.rs already covers all interval cases, day-by-day SQL generation,
same-bucket suppression and inclusive GC. Generalize only its test helpers to
retain chrono::Local alongside named zones/UTC, including the GC case; preserve
both changed group counters before the same-bucket suppression check. Domain
topn_slow_query.rs already carries original heap/FIFO vectors; move the remaining
sorted [2,2,1,0] expiration assertion there. schema_checker.rs already verifies
both MySQL codes and checker retry behavior. All production bodies stay unchanged.

## Progress


- [x] Map duplicate checks and unique vectors to existing Domain owners.
- [x] Remove four carriers/seven registrations; migrate unique vectors.
- [x] Run grouped owner tests, session test-target check, lint and continuity checks.
- [ ] Complete hook, fresh pre-push build, remote verification and cloud checkpoint.

## Milestones and Plan of Work


Trace Go TestWriteRUStatistics, TestGetLastExpectedTime, TestPush,
TestRemoveExpired, TestQueue, TestErrorCode and TestSchemaCheckerSimple.
Preserve exact before-images and document retained owners and original integration
obligations. Delete the repeated session scaffolding only after moving unique
vectors. Update historical b117/b118 receipt notes and both current registers.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-domain --lib -- ru_stats::tests topn_slow_query::tests schema_checker::tests --test-threads=1
    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-session --tests

Verify all selected tests ran and passed, then root make lint and git diff --check.
The external domain-test-owner-cleanup/verify.py must confirm unchanged production,
all original owner assertions, exact session module removal and unchanged findings.
Normal commit executes actual hooks/pre-commit and its locked server build.
Immediately before the authorized push repeat cd rust && cargo build --locked
-p tidb-server with CARGO_BUILD_JOBS=1. Verify remote SHA; never bypass hooks or
force-push. Postcommit evidence is external final-handoff.json.

## Surprises & Discoveries


The RU carrier's apparent table checks use a hand-written string parser, not
TiDB storage. The schema carrier scripts every verdict and never exercises the
Go validator ring. Neither establishes the original Go integration. The removed
registrations comprise six active tests and one ignored test; useful overlapping
behavior remains in the owner tests and unique vectors are migrated.

## Decision Log


On 2026-10-06, remove repeated Domain scaffolding as one owner batch. Preserve
Local timezone, changed-group inputs and sorted-expiration vectors. Keep serverinfo,
plan-replayer and real session integration suites, whose behavior differs.
Do not declare real RU table persistence or checker/ring integration verified.

## Recovery / Interfaces and Dependencies


Recover any before-image using git show 13d1bc836009c3cba540995e00eda76fefd91bad:<path>.
External artifacts live in /workspace/.cloud-setup/domain-test-owner-cleanup.
No manifest, lockfile, dependency, generated code or native-client change.

## Outcomes & Retrospective


Four carriers are removed and unique assertions are migrated. All 35 selected
owner tests, session test-target compilation, lint and continuity checks passed.
Publication remains. All 56 unresolved findings and complete original Go obligations remain.
This replaces the completed utility/protocol harness plan; its receipt remains
utility-proto-harness-cleanup-validation.json.
