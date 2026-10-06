# Consolidate parser tool ownership

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Reduce repeated tool compilation and retire superseded diagnostic paths while
preserving Go fixture replay. Work in /workspace/tidb on hparser-integration,
based on 8c4238c01d2c32c5d5aa6a781c052e46822d4258. Refreshed Go master remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. The difftest package's parser oracle
was included as source in its library, its own binary and the plan inventory
binary. Three oracle tests were registered three times. The maintained replay
in difftest-parser-tests already diagnoses parse/restore failures against the
checked Go oracle, including multiline and non-UTF-8 restores.

## Progress


- [x] Inspect tool ownership, original Go fixture pipeline and current callers.
- [x] Share tool modules in the library and retain three thin CLI entrypoints.
- [x] Retire the optional coverage reporter, duplicate decoder example and unused merge helper.
- [x] Correct stale lexer regeneration instructions to the current test target.
- [x] Run grouped tests: 21 pass, two parser replay tests fail identically on original code.
- [x] Validate CLI checks, metadata, references, lint and diff; original inventory reproduces stale-fixture failure.
- [ ] Record evidence and commit through the actual locked-build hook.
- [ ] Fresh locked server build, normal push, remote verification and Cloud checkpoint.

## Milestones and Plan of Work


Move integration_parser_golden into src/parser_oracle.rs, source fixture
inventory into src/parser_inventory.rs and plan inventory into
src/plan_inventory.rs under rust/difftests. Keep all private implementation
functions and assertions. Expose run_cli for each thin existing binary to call.
Plan inventory imports the library's parser_oracle rather than including its
source again. Set test=false on the three CLI targets because their unit tests
now have one library owner; test CLI behavior through real command execution.
The existing subprocess plan-inventory integration test remains.

Remove the ignored parser coverage_report and examples/dump_unhandled.rs;
their arbitrary env-file diagnostic workflow is retired, not migrated to a new
arbitrary-input interface. The checked integration replay reports missing inputs
and asserts complete outcome counts. Preserve curated parser restore tests and
the embedded-CR decoder regression. Remove the unreferenced Ruby
resolve-ratchet-conflict.rb tool; reviewed conflicts use ordinary Git resolution.
Keep all actual ratchet assertions and regeneration implementations. Correct
regen-golden.sh's obsolete package-only test command to the lexer owner.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run one batch:

    CARGO_BUILD_JOBS=1 cargo test --locked -p difftest --lib --test integration_plan_inventory -p difftest-parser-tests --test all

Require every selected test to execute; retain baseline failures as failures.
Run all three existing CLI
--check commands and verify they preserve checked artifacts; no Go regeneration
or fixture rewriting is authorized by cleanup. Compare metadata: only the three
bin test flags and removed dump_unhandled example may change. Compare original
tool bodies after normalizing their import and main/run_cli wrapper changes.
Verify original Go replay assertions and fixture bytes remain intact.
From repository root run make lint and git diff --check. No Go/Bazel files or
dependencies change. No whole upstream package acceptance or timing claim.

Commit normally with core.hooksPath=hooks; the actual hook must pass
cd rust && cargo build --locked -p tidb-server. Repeat the locked build
immediately before normal push to origin hparser-integration and verify remote
SHA. Never bypass hooks or force-push. Preserve concurrent changes.

## Surprises & Discoveries


The parser oracle's three unit tests were compiled into three test owners.
The old reporter only asserted that some input matched and depended on arbitrary
PARSER_COV files; the maintained replay asserts source outcome counts and prints
specific parse/restore errors. Its removal does not remove accepted Go fixtures.

## Decision Log


On 2026-10-06 centralize the three tools, keeping their existing CLI names and
arguments. Preserve useful adapter tests even where Go has no identical test:
they protect input decoding and correct Go comparison. Retire only the reviewed
optional reporting/merge paths, not their replacement's assertions.

## Outcomes & Retrospective


Implementation, continuity, lint and CLI validation completed. Seventeen library
tests, one CLI integration and three parser/lexer tests pass. Two parser tests
fail exactly as before: five curated mismatches and twelve integration mismatches
(four rejected Go-accepted inputs, eight restore differences). Both original and
shared source inventory commands report stale checked inventory. Oracle and plan
checks pass. Do not claim parser parity or silently regenerate fixtures. Six duplicate
oracle registrations, three binary unit-test harnesses, one ignored reporter
and one example target are retired. All 86 findings retain 30 repaired and 56
unresolved (27 open, 29 partial). Prior table-corpus differences remain recorded
in result-support-cleanup-validation.json and are outside this parser cleanup.

## Recovery, Artifacts and Dependencies


Restore individual before-images with git show
8c4238c01d2c32c5d5aa6a781c052e46822d4258:<path>, preserving other changes.
Logs and final publication/checkpoint evidence live in
/workspace/.cloud-setup/parser-tools-cleanup. Record durable evidence in
rust/docs/parity/current-audit/parser-tools-cleanup-validation.json. No engine
behavior, dependency, lockfile or oracle fixture changes. Draft save, environment
Publish and fresh-task restoration are separate steps.

Revision 2026-10-06: replace completed shared-result-support work with parser
tool consolidation and retirement of the superseded ad hoc workflow.
