# Retire obsolete result-harness bookkeeping

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove stale historical narratives and inactive bookkeeping from result replay
without removing any Go inputs or weakening comparisons. Work in /workspace/tidb
on hparser-integration from 2186a9eecb6e0166adf7a6943604a8e60edb16be. Refreshed
Go master remains b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Shared topic entries
carry long unused reason strings; every consumer discards them. The integration
and query gates have zero known divergences but retain old two-sided ratchets.
The table harness has an empty exclusion list with unreachable skipping code.

## Progress


- [x] Verify restored checkout, callers, zero thresholds and empty exclusion list.
- [x] Preserve all 105 topic names and their order; remove unused narratives.
- [x] Remove 2777 historical comment lines and replace zero ratchets with empty checks.
- [x] Delete unreachable table exclusion bookkeeping and correct stale docs.
- [x] Validate continuity, grouped checks and replay; table retains five baseline-identical differences.
- [ ] Lint, review, commit through actual hook, fresh build, normal push and checkpoint.

## Milestones and Plan of Work


In rust/difftests/result-tests/src/enrolled_topics.rs retain every topic as a
string in the same order. Migrate all consumers in integration_diff and
join_shape. Remove the unused narratives rather than freezing obsolete counts
in executable constants. In integration_diff remove only the historical comment
block between the current report and final assertion, along with dated survey
censuses. Preserve the operational survey instructions, child isolation and all
comparison logic. Remove the join-shape measurement history while preserving
its nonzero constants, assertions and explicit limit that an earlier 19-plan
increase was not reviewed statement by statement. In query_diff and integration_diff replace zero thresholds
with failures.is_empty()/total.divergences.is_empty(). They accept exactly the
same outcomes. In table_diff remove UNSUPPORTED_TOPICS=[] and its dead branch,
empty skip report and count; keep ERR-result skips and all active comparisons.
Replace obsolete never-run/dead-engine prose with current corpus semantics.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh for every build. From /workspace/tidb/rust
check all difftest-result-tests targets, run query_diff and table_diff together,
and run integration_diff topics_are_listed_once_each plus the existing ignored
single-topic replay on a representative enrolled topic. Compare serialized topic
lists before/after, exact corpus/manifest hashes and the unchanged engine files.
Prove removed historical blocks contain comments and only the zero constant;
verify both acceptance predicates remain equivalent for zero/nonzero failures.
Previous table validation has five baseline-confirmed differences; preserve and
compare those diagnostics, never lower assertions or regenerate fixtures.
From repository root run make lint and git diff --check.

No new regression test is required for equivalent predicates and deletion of
unreachable bookkeeping. Do not run unrelated full suites. No Go/Bazel source
or dependencies change. Commit normally with core.hooksPath=hooks and the actual
cd rust && cargo build --locked -p tidb-server gate. Repeat that locked build
immediately before normal push to origin hparser-integration and verify SHA.
Never force-push or bypass hooks. Preserve concurrent changes.

## Surprises & Discoveries


The final integration threshold is zero, while its preceding history repeatedly
describes incompatible old remaining-debt counts. Topic explanations are runtime
string constants even though every caller discards them. Table topic exclusion
has no entries, so its branch and report cannot affect corpus selection.

## Decision Log


Keep every fixture, topic, active comparison, nonzero catalog/join snapshot and
operational survey. Remove historical source narratives; recover them from the
immutable before-image if needed. Replace zero ratchets with exact empty checks,
not a changed tolerance. No performance timing claim or package acceptance.

## Outcomes & Retrospective


Implementation, metadata/continuity, all-test-target compilation and lint are
complete. Query and topic tests pass; util/admin replay matches 141 statements
with zero divergence and two explicit skips. Table retains five differences
identical to the prior original-harness run (1937/1942 match, 127 skips).
The batch removes 3393 net source lines. Publication/checkpoint remains pending. All 86 findings retain 30 repaired
and 56 unresolved (27 open, 29 partial). Existing table/parser failures remain
recorded in the preceding cleanup receipts and are not resolved by this work.

## Recovery, Artifacts and Dependencies


Use git show 2186a9eecb6e0166adf7a6943604a8e60edb16be:<path> for individual
before-images, preserving other work. Logs and final publication/checkpoint
results live in /workspace/.cloud-setup/result-bookkeeping-cleanup; the committed
receipt is rust/docs/parity/current-audit/result-bookkeeping-cleanup-validation.json.
No engine API, dependency or fixture changes. Saved configuration, environment
Publish and fresh-task restoration remain separate claims.

Revision: replace completed parser-tool cleanup with equivalent result gates
and deletion of stale narrative/bookkeeping.
