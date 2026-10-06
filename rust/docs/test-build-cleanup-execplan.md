# Remove stale system-variable test scaffolding

This living ExecPlan follows root PLANS.md. Previous chunk-owner cleanup was
pushed at 3af4618443fd3d41801bb1639b64c582eed6e853; its external final-handoff.json
records successful publication and cloud draft readback.

## Purpose and Context


Replace copied literal checks and stale partial-port wrappers with tests of the
actual shared owners. In /workspace/tidb, hparser-integration, start at the commit
above. Fresh Go master remains b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
A carrier is a separately registered test source; the three retired carriers
live under rust/crates/tidb-vardef/src. Production constants and conversion
helpers live beside their owner tests. The live variable registry and validation
belong to rust/crates/tidb-session/src/sysvar.rs.

## Progress


- [x] Compare three carriers with Go variable/vardef and current Rust owners.
- [x] Migrate production-backed default/name assertions and initialization tables.
- [x] Remove copied registry, local bounds checks, private closure and stale prose.
- [x] Verify unchanged production code and preserved meaningful assertions.
- [x] Run all eight vardef library tests: eight passed, none failed or ignored.
- [x] Complete grouped session owner tests (47 passed), lint, formatting and self-review.
- [ ] Normal commit hook, fresh prepush locked build and verified normal push.
- [ ] Verify recovery bundle and save/read back cloud startup checkpoint.

## Milestones and Plan of Work


Remove tests_vardef_port.rs, tests_sysvar_port.rs, tests_variable_p2_port.rs and
their lib.rs registrations after migrating useful checks. defaults.rs and
tidb_vars.rs retain existing owner assertions plus real constant comparisons.
modes.rs retains the bogus clustered-mode input. global_sysvar_initial.rs owns
the two moved initialization tables; NextGen checks both in_test values.

In the existing sysvar registry test, check lowercase for every actual entry,
including the final one; preserve sort and case-insensitive lookup assertions.
The existing GOGC validation test compares real registry defaults for Go's
threshold relationship. Remove the test-local threshold and private optimized
join predicate. Keep actual SessionVars join-version and analyze hook cases.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-vardef --lib
    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-session --lib -- sysvar::tests tests_global_vars::analyze_default_bucket_and_topn_global_hooks_match_go tests_core::session_state::hash_join_versions_accept_only_legacy_or_optimized --test-threads=1

Expect retained owner tests to pass, with named filters actually executing.
Run root make lint and git diff --check; check changed Rust formatting without
unrelated churn. Compare production code and migrated assertions to the base.
Commit normally through executable hooks/pre-commit selected by core.hooksPath:
it must pass cd rust && cargo build --locked -p tidb-server. Repeat that exact
locked build immediately before authorized push to origin hparser-integration,
then verify remote SHA. Do not bypass hooks or force-push.

## Surprises & Discoveries


The 489-row alleged registry table contains only quoted literals, so neither
lowercase test observes production names. The registry already exists and has
its own tests. Flashback concurrency's real constant also already exists;
local constants asserting equality to themselves provide no coverage. Similar
analyze checks ignore the maintained SQL clamping/publication tests.

## Decision Log


On 2026-10-06 consolidate the real 55 production constant/default assertions,
12 name pairs and initialization inputs before removing all three carriers.
Move Go's threshold relation to the real registry rather than leaving a
hardcoded tuner threshold. Preserve Rust ownership and process-global tests;
absence of an identical Go test name does not make these redundant.

## Outcomes & Retrospective


Three carriers (2053 lines) are removed, with 37 net registrations and 1885 net
Rust lines removed after migration. All eight vardef tests and 47 session owner tests pass, with zero failures or
ignored tests; 1975 unrelated session tests were filtered. Root lint and
formatting passed. Publication and checkpoint remain pending; their final
results go in external final-handoff.json. No production behavior changes,
complete package acceptance, structural finding closures or measured speedup.

## Recovery, Interfaces and Dependencies


Recover before-images with git show 3af4618443fd3d41801bb1639b64c582eed6e853:<path>;
preserve concurrent changes. Dependencies, Cargo manifests and locks are
unchanged. The complete migration map is rust/docs/parity/current-audit/
vardef-test-owner-cleanup-validation.json. Logs and continuity verification live
under /workspace/.cloud-setup/vardef-test-owner-cleanup. Both finding registers
retain all 86 dispositions. Replace the recovery bundle only after verification.
Saving the cloud draft does not prove Publish or fresh-task restoration.

Revision 2026-10-06: replace the completed chunk plan with the connected vardef
cleanup, preserving the previous receipt and final publication evidence.
