# Remove test-only work from production builds

This living ExecPlan follows root PLANS.md. Work starts at local b58b84f359e92e7614980a3dd4211ea5202785d5, with Go master 93a01d31f6da205ae4bf376825293903a6899fdb. All work stays in Cloud; no push or dry run.

## Purpose / Big Picture


Normal server builds should not load test-only modules. Keep behavioral cases under the test harness, remove a constant-only inventory assertion and an unused duplicate validation helper, and stop launching a generator comparison from ordinary runtime tests. Existing Unicode fixture tests retain behavioral coverage; the generator remains available for regeneration. Working documentation should point to the current register and durable receipts instead of repeating old counts and checkpoint summaries.

## Progress


- [x] Inspect instructions, current source, generated harness and compiled dependency inputs.
- [x] Capture existing harness lists and source hashes before editing.
- [x] Move test gates and remove redundant checks/helpers in one batch.
- [x] Consolidate stale working-document summaries while preserving receipt links.
- [x] Verify retained case bodies/registration, run grouped affected tests, lint and build gates.
- [x] Record results and prepare local commit/recovery; operational identities follow in external final-handoff.json. No push.

## Context and Orientation


The planner logical module accidentally puts two cfg(test) attributes on derive_stats_tests and none on operator_tests. Thirteen session modules have an inner cfg(test), which requires reading the test source before excluding it. Move these gates to their parent module declarations. The distsql request-builder suite contains an inventory-length assertion independent of request-builder behavior. The AST format suite launches Python and Go to compare generated text even though adjacent tests exercise every generated mapping. The session sysvar test module contains an unregistered bool_validation helper. None is grounds to remove Go behavioral coverage.

## Milestones and Plan of Work


First record the 14 module inputs and existing harness names, then gate them at their declarations without changing test bodies. Remove the two nonbehavioral registered checks and the unused helper. Next replace repeated status paragraphs in the audit index and working plans with links to the current register, preserving historical receipts as links. Finally run one grouped validation pass for affected suites, compare harness membership and production dependency inputs, and run lint plus the mandatory actual precommit locked build.

## Concrete Steps and Acceptance


Source /workspace/.cloud-setup/env.sh in every shell. Cargo runs from /workspace/tidb/rust with CARGO_BUILD_JOBS=1. Group planner logical operator and affected session module filters in one cargo test --locked --lib invocation; group AST format and distsql request-builder tests in one --test all invocation. Check the affected targets, run make lint from repository root, and let the normal precommit hook run cargo build --locked -p tidb-server. Evidence lives in /workspace/.cloud-setup/test-build-cleanup and the durable cleanup receipt. Acceptance requires unchanged behavioral bodies and registrations, exactly the intended removed checks, and exclusion of the 14 modules from normal server dependency inputs. No wall-clock speedup is assumed from fewer inputs.

## Surprises & Discoveries


The suspected nested aggregate suites are already deduplicated by scripts/aggregate-tests.rs; leave them unchanged. Go names alone are not a deletion rule: the logical operator tests exercise Go optimizer contracts, and the Unicode fixtures protect Rust's different case-conversion behavior.

## Decision Log


Decision: remove production parsing of test source and nonbehavioral checks, not useful behavior assertions. Rationale: this reduces unnecessary build dependencies without concealing missing parity. Date: 2026-10-04.

## Outcomes & Retrospective


Cleanup verification passes: 441 distinct selected cases, affected all-target check, lint and locked server build. Three pre-existing failures remain enabled and independently reproduced; the original grouped run is not reported as passing. All 370 moved-module tests remain registered; 367 bodies are byte-identical and three stale assertions are corrected. Two descriptive test names change, and the two intended nonbehavioral tests are the only registrations removed without replacement. All 14 test modules are absent from normal server dependency metadata. See parity/current-audit/test-build-cleanup-validation.json. This cleanup does not repair a structural finding or accept a complete Go package.

## Idempotence, Recovery, Interfaces and Dependencies


Preserve both working trees, native source, current build artifacts and unpublished commits. Changes affect module cfg attributes and test/docs only; no dependency or runtime interface changes are intended. Existing fixture data and generators stay authoritative. Recover removed text through Git history; record exact removed test names and source hashes. Never bypass the build hook, reset concurrent work, or push.

Implementation checkpoint: 14 modules contain 13,829 lines and 370 tests. Normal server dependency metadata includes all 14 before the change. Source comparison confirms every retained module is byte-identical after removing only its inner cfg attribute. Two nonbehavioral registered cases and one already-unregistered helper are removed. Grouped validation is complete; counts and unresolved findings are unchanged.

Validation discovery: the first grouped run passed 408 cases and failed seven. All seven reproduce on the unchanged pre-cleanup session binary. Four assertions are stale: a fixed sysvar total, missing system schemas in SHOW DATABASES, equating a captured fixture size to every served table, and Rust-only cast_signed EXPLAIN text. Correct those while preserving ordering/lookup, all captured CREATE TABLE rows, predicate placement and SQL-result checks. Three other failures remain enabled: a blanket CLUSTER_LOG scan without required predicates and two information-schema fixed-column append panics. They are outside this build/test cleanup and are not reported as passing.
