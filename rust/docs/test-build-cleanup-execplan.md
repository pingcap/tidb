# Remove test-list generation from production builds

This living ExecPlan follows root PLANS.md. Keep progress, discoveries, decisions
and outcomes current. Previous plan-replayer cleanup completed at 9c253898c5;
its external final-handoff.json records publication and checkpoint validation.

## Purpose and Context


Stop test-only edits from invalidating ordinary library/server builds through a
shared test-directory watcher. Use /workspace/tidb, hparser-integration, base
9c253898c5cc631624301c73668a4fd9c9c4cda7. Go master freshly fetched remains
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. This is Rust build maintenance, not a
behavioral parity claim. All Go package obligations and 56 findings remain.

## Progress


- [x] Execute the original generator for every manifest; inventory 26 consumers.
- [x] Replace all generated includes with 852 explicit module registrations.
- [x] Remove 26 build declarations, shared script and five obsolete markers.
- [x] Preserve helper ownership, isolated suites and production generators.
- [x] Reproduce 13 stale live-harness errors with original generated registration.
- [x] Remove two private loaders and duplicate transport binary; retain all assertions.
- [x] Update current workflow documentation and verify Cargo metadata equivalence.
- [x] Check all 26 aggregate targets and run 53 representative tests.
- [x] Final formatting, bash syntax, metadata/source equivalence, lint and diff checks.
- [ ] Normal commit hook, immediate-prepush build, remote SHA and cloud checkpoint.

## Milestones and Plan of Work


Read the complete package list in static-test-roots-cleanup-validation.json.
For each manifest, remove only build = ../../scripts/aggregate-tests.rs; keep
all targets and dependencies except the duplicate transport-retry standalone target. In tests/all.rs register exactly the modules
emitted by the original generator in the same order. Keep direct_unary_table_index_reader_source
owned by table_index_reader_runtime_source rather than registering it twice.
Keep five standalone session/transaction suites as explicit Cargo targets.
Delete the shared generator only after every consumer, including three difftest
crates, is migrated. Remove obsolete markers and update workspace architecture,
scripts/README and the outdated Cargo profile comment. Existing test registrations and Rust
correctness assertions stay intact apart from that duplicate registration. No generated production artifact is edited.

## Validation and Acceptance


Activate /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust with
CARGO_BUILD_JOBS=1, run the exact 26-package cargo check --locked --test all
command in the receipt. Then run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-lexer -p tidb-error -p tidb-config --test all -- --test-threads=1

Compare the original generator output with static roots; verify 854 retained
test source bodies unchanged and all 101 assertions in four maintained live
harnesses preserved. Non-build targets change only by retiring the duplicate
transport-retry target; its script selects the same case from all.rs. No remaining
Rust/Cargo consumer may refer to aggregate-tests.rs or all_tests.rs. Run root
make lint and git diff --check. Do not run unrelated suites or claim all 852
modules executed. Commit normally through hooks/pre-commit, which must pass
cd rust && cargo build --locked -p tidb-server. Repeat immediately before the
normal authorized push to origin hparser-integration, then verify remote SHA.

## Surprises & Discoveries


Initial crate-only discovery found 23 users, but exhaustive manifest search found
three differential-test crates too. The old script watched every test source
and the tests directory even in ordinary builds. Removing it requires a one-time
workspace rebuild. One helper and five process-isolated suites are excluded.
Existing real code-generation scripts and their platform obligations remain.
The transaction harness also registered transport retry both in all.rs and as a
standalone Cargo target. Remove the duplicate and update its runner exact filter.

## Decision Log


On 2026-10-06 replace dynamic discovery with explicit conventional Rust modules,
keeping test count and process layout. Adding a suite now requires registering
it in all.rs; document this tradeoff rather than adding another source scanner.
Reclaim 31 identified inactive obsolete test executables (2286287128 bytes) for
the metadata transition; preserve library caches and current server executable.
No cargo clean, broad cache purge, force push or hook bypass.

## Outcomes & Retrospective


Implementation and registration equivalence passed. All 26 aggregate targets
compile; 53 representative tests pass with zero failed/ignored/filtered. Four
live harnesses initially failed on stale APIs; the original generator reproduced
the same 13 errors. Removing two private primed loaders in favor of warming the
production cache, using thread-safe recorders and the actual monotonic clock type
restores compilation. Their multi-node execution remains unverified. This removes a production build dependency on tests without deleting
coverage. No measured timing improvement or complete Go package acceptance.

## Recovery, Interfaces and Dependencies


Recover any before-image with git show 9c253898c5cc631624301c73668a4fd9c9c4cda7:<path>.
No production bodies, dependencies or Cargo.lock changed. Four live test harnesses
use existing production APIs directly; their original assertions remain. Retired cache outputs
are regeneratable. Evidence and exact command: rust/docs/parity/current-audit/static-test-roots-cleanup-validation.json.
External original outputs, source hashes, logs, pruned paths and final handoff
live in /workspace/.cloud-setup/static-test-roots. Saved setup configuration does
not prove environment Publish or fresh-task restoration.
