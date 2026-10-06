# Consolidate plan-replayer tests in the Domain owner

This living ExecPlan follows root PLANS.md. Keep progress, discoveries, decisions
and outcomes current. Previous Domain-owner cleanup completed at 17d23b3e2e;
its external final-handoff.json records publication and checkpoint validation.

## Purpose and Context


Remove duplicate plan-replayer scaffolding so future fixes maintain one SQL
executor mock and one set of parser/channel assertions. Work in /workspace/tidb
on hparser-integration, base 17d23b3e2e1b4193a5cc0c0b9a52ed727ec94f8a.
Go comparison is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed, freshly fetched.
The session handle carrier repeats Domain's SQL table model. Its apparent
post-dump persistence is manually inserted fixture state, not storage integration.

## Progress


- [x] Map all six removed registrations to existing Domain owners.
- [x] Move quoted SQL, collector transitions and shared worker-lifecycle checks.
- [x] Remove the handle carrier/module and duplicate parser/channel functions.
- [x] Grouped owner tests, session test compilation, lint and self-review.
- [ ] Normal commit hook, immediate-prepush build, remote SHA and cloud checkpoint.

## Milestones and Plan of Work


Extend existing tests in rust/crates/tidb-domain/src/plan_replayer.rs using its
MockExec, MockDumper and status owner. Remove session's
tests_domain_plan_replayer_handle_source.rs and its lib.rs declaration.
Trim only parser/channel duplicates and imports/docs from
tests_domain_plan_replayer_source.rs. Keep its actual filesystem GC body unchanged.
Update both finding registers, README and historical receipt pointers without
changing dispositions. No production or dependency change is intended.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-domain --lib -- plan_replayer::tests:: --test-threads=1
    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-session --tests

From repository root run make lint and git diff --check. Verify production
before/after bytes, retained filesystem test and all original owner assertions.
Do not rerun broad unrelated suites. Commit normally: actual executable
hooks/pre-commit must pass cd rust && cargo build --locked -p tidb-server.
Repeat that same locked build immediately before the authorized normal push to
origin hparser-integration, with CARGO_BUILD_JOBS=1; verify the remote SHA.

## Surprises & Discoveries


Go handle tests drive actual SQL capture and persistent system tables. Both Rust
mock copies only validate boundary behavior. Removing a second handwritten model
does not establish or remove real storage integration. The filesystem-backed GC
test is distinct and retained. Domain already tests GC errors and exact cutoffs.

## Decision Log


On 2026-10-06, consolidate collection, dump lifecycle, GC, insertion, filename and
channel checks as one batch. Preserve useful Rust error-path tests even when
their names do not appear in Go. Keep unique filesystem coverage and migrate the
quoted SQL vector exactly; no unsupported package-completion claim.

## Outcomes & Retrospective


Source cleanup removes 464 net Rust lines. All 32 selected owner tests passed,
with zero failed/ignored and 130 unrelated filtered out. Final session test-target
compilation, formatting, lint, diff and continuity checks passed. Publication
results belong in the external final-handoff.json after commit. All 56 unresolved findings
and original Go integration/package obligations remain unchanged. Six duplicate
registrations are retired. Compilation/test speedup has not been measured.

## Recovery, Interfaces and Dependencies


Recover before-images with git show 17d23b3e2e1b4193a5cc0c0b9a52ed727ec94f8a:<path>.
No manifests, lockfiles, generated sources or native-client files change.
Evidence: rust/docs/parity/current-audit/plan-replayer-cleanup-validation.json;
external logs and postcommit handoff: /workspace/.cloud-setup/plan-replayer-cleanup.
Preserve concurrent work; no force push or hook bypass. Saved setup configuration
does not prove environment Publish or fresh-task restoration.
