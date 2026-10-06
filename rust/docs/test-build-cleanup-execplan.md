# Share result differential support

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Reduce repeated compilation and execution of identical harness helpers while
preserving comparisons with Go results. Work in /workspace/tidb on
hparser-integration, based on ea458df03b4d0c60edec0570e3cf9e2555dd570e.
Refreshed Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
The Rust-only harness adapts tests/integrationtest inputs to the live Rust
session; its useful adapter tests need not themselves exist in Go.

## Progress


- [x] Confirm five shared helpers and 26 tests duplicated across three binaries.
- [x] Move helpers unchanged into one library; migrate every include/caller.
- [x] Remove the ignored assertion-free timestamp scratch probe.
- [x] Validate 26 helper tests and query corpus; check all integration targets.
- [x] Diagnose five table-corpus differences with identical original-harness results.
- [x] Record evidence, lint and review the batch.
- [ ] Commit through the actual hook, build immediately before push, verify SHA.
- [ ] Save reusable cloud checkpoint and recovery bundle.

## Milestones and Plan of Work


Move enrolled_topics, integration_plan_property, mysqltest_connections,
mysqltest_script and result_label from rust/difftests/result-tests/tests to src.
Export them in src/lib.rs and import from difftest_result_tests in the five
consuming suites. This package already owns all needed dependencies. Keep its
seven integration targets, names and child replay entrypoints unchanged.
The 26 helper tests now run once instead of three times (52 duplicate
registrations removed). Remove zz_scratch_probe, which only prints results.
Retain the documented scale_probe benchmark and all actual regression assertions.

Then verify byte-identical helper moves, unchanged fixtures/dependencies and
unchanged integration target names. Run the shared helper tests once, check all
integration targets and execute query/table result comparisons. Update the
current audit receipt and both registers' cleanup pointers; preserve all finding
statuses. Finally publish through normal gates and save the cloud checkpoint.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh before builds. From /workspace/tidb/rust:

    CARGO_BUILD_JOBS=1 cargo test --locked -p difftest-result-tests --lib
    CARGO_BUILD_JOBS=1 cargo check --locked -p difftest-result-tests --tests
    CARGO_BUILD_JOBS=1 cargo test --locked -p difftest-result-tests --test query_diff --test table_diff

Require 26 helper tests and the query corpus to pass. The table corpus retains
five baseline-identical differences; do not claim it passed. Inspect
failures without deleting assertions or rewriting golden results. Compare Cargo
metadata: only the new support library target may be added. From repository
root run make lint and git diff --check. No Go/Bazel changes are planned.
Commit with executable hooks/pre-commit selected by core.hooksPath=hooks; it
must pass cd rust && cargo build --locked -p tidb-server. Repeat that build
immediately before normal push to origin hparser-integration, then verify remote
SHA. Never bypass hooks or force-push.

## Surprises & Discoveries


Integration replay launches its own binary with an exact root test name.
Combining those binaries risks changing replay behavior; sharing their helper
library removes repetition while preserving those entrypoints. The performance
probe documents a Go join fixture and useful cost curve, unlike the scratch test.

## Decision Log


On 2026-10-06 consolidate shared ownership rather than erase adapter coverage.
Move all five helpers byte-for-byte. Preserve the benchmark and suite isolation.
No dependency or engine behavior changes and no package acceptance claim.

## Outcomes & Retrospective


Implementation, compilation, helper/query tests and lint passed. The table
corpus reports five differences (1937/1942 in-domain statements match; 127 are
skipped). Restoring the original table harness and label helper reproduces the
exact same diagnostics: constant ordering, two recursive CTE name-resolution
cases and two lowercase-utc outcomes. Assertions and fixtures remain intact.
Publication remains pending. This
cleanup leaves 86 findings: 30 repaired, 56 unresolved (27 open, 29 partial).
No elapsed-time speedup is claimed without a measured comparison.

## Recovery, Artifacts and Dependencies


Recover individual before-images with git show ea458df03b4d0c60edec0570e3cf9e2555dd570e:<path>,
preserving concurrent changes. No manifest/lockfile dependencies change. Logs
and publication evidence live in /workspace/.cloud-setup/result-support-cleanup;
committed evidence belongs in rust/docs/parity/current-audit/result-support-cleanup-validation.json.
Draft persistence, Publish and fresh-task restoration are separate claims.

Revision 2026-10-06: replace the completed utility-plan retirement with shared
result harness support and explicit retained behavior.
