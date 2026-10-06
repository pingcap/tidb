# Retire unused session models and share option conversion

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove unused executable seed models and their private tests while retaining
Go behavior checks at the shared owner. Work in /workspace/tidb on
hparser-integration from c186e317dafef92f4e19f6b1a9dae3480885c308.
Fresh Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
The nontransactional admission model has no production caller. The upgrade
registry lists names but executes no migrations; only its bootstrap constant
has a runtime consumer. Four crates duplicate variable.TiDBOptOn.

## Progress


- [x] Trace callers and compare retained conversions against Go varsutil.go.
- [x] Remove unused nontransactional and upgrade registry models and private tests.
- [x] Move option conversion and useful tests to tidb-vardef; migrate every caller.
- [x] Validate owner tests, consumer behavior, bootstrap rows, lint and continuity.
- [ ] Pass actual precommit hook and fresh pre-push build; verify remote and Cloud draft.

## Milestones and Plan of Work


Delete tidb-exec/src/nontransactional.rs and upgrade_versions.rs with their
private integration carriers and registrations. Keep parser BATCH support and
all remaining implementation obligations. Put the unchanged bootstrap marker
287 in tidb-exec/src/mysql_bootstrap/rows.rs, its runtime owner.

Move tidb-exec option_values functions and two tests into tidb-vardef. Use its
existing ON/OFF constants. Migrate server calls, Domain dynamic options,
statistics switches and password policy. Remove three duplicate predicates
and one duplicate Domain test after moving its unique negative cases.
No dependency or lockfile change is needed.

## Validation and Acceptance


Activate /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-vardef -p tidb-domain -p tidb-util -p tidb-stats-handle-util --lib -- option_values:: domain_sysvars:: password_validation:: util::tests:: --test-threads=1
    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-exec --test all -- mysql_bootstrap_source:: --test-threads=1

Expect canonical ON/OFF conversions, unchanged dynamic option effects,
password policy and bootstrap readback to pass. Compare moved function bodies
and all migrated assertions against before-images. Search all Rust consumers
for retired modules and helper aliases. From repository root run make lint
and git diff --check. The actual hook must run cd rust && cargo build --locked
-p tidb-server, followed by the same fresh build immediately before normal push.

## Surprises & Discoveries


Go master writes bootstrap version 318. Rust writes 287, while its unused
upgrade-name list ends at 263. A test asserting the list ends at the current
version was stale; repairing that list would still implement no migrations.

## Decision Log


Keep runtime marker 287 and document the missing migrations. Do not advance
it merely to match Go. Retire model-only tests; preserve useful conversion
vectors at the shared owner and all real bootstrap and consumer tests.
This is bounded cleanup, not complete package transcreation or finding repair.

## Outcomes & Retrospective


Implementation and validation complete: 71 targeted tests passed, including
shared conversions, consumer side effects and bootstrap readback. Lint and
continuity/diff checks passed. Removed 469 net Rust lines. Publication gates
remain pending here; external final-handoff.json records their later results. No measured
speedup, live TiKV, full Go suite or migration completeness is claimed.

## Recovery, Artifacts and Dependencies


Before-images are available with git show c186e317:<path>. Restore individual
files only and preserve concurrent work. External logs and inventory live in
/workspace/.cloud-setup/session-policy-cleanup. The durable receipt will be
rust/docs/parity/current-audit/session-policy-cleanup-validation.json.
Native client remains unchanged. Cloud save, Publish and fresh-task restore
are separate. Revision: replace the completed runner cleanup plan with this
session policy owner batch.
