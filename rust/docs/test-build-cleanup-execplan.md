# Retire orphan source carriers and unused catalog adapters

This living ExecPlan follows root PLANS.md. Earlier cleanup receipts remain
indexed in parity/current-audit/README.md; Git preserves removed before-images.

## Purpose / Big Picture


Remove unregistered execution/planner drafts and the unused alternate catalog
adapter/loader together. Future searches will find the active owners rather than
orphan implementations and never-run tests. This removes 3523 Rust lines; no
build or workload speedup is claimed for files that were already unregistered.

## Context and Orientation


Base a4b2f466595db9f65876cbbff656922df4613cd2 on hparser-integration in
/workspace/tidb. Fresh Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed;
native master cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 remains unchanged.
A workspace-wide source-module scan identified six orphan Rust files. Cargo
manifests, mod/path/include references and aggregate-tests.rs confirm none is a
registered input. The lexer keyword generator is an explicit Cargo binary and
both server binaries are automatically discovered, so all three are retained.

Retired carriers are executor index_lookup_join.rs, index_range_tests.rs,
driver/ast_rewrite.rs; planner fragment.rs and plan_builder/from_tests.rs; and
expression vs_helper.rs. Active index joins use executor join.rs and its physical
builder; ranges use index_range.rs, access_path.rs and registered suites. Planner
join/MPP task and expression owners remain. Fragment/vector-search completeness
is not established by these unregistered drafts and remains an original Go
obligation. The 48 orphan test declarations include four ignore attributes;
none was registered, passed, failed or skipped in this workspace's test runner.

real_tikv_catalog.rs retains TransactionMetaSnapshot, SnapshotMetaSnapshot,
load_catalog_from_cluster and reload_catalog_from_cluster unchanged. Only
TikvMetaSnapshot and its sole caller load_catalog_from_tikv_cluster are removed.
The alternate loader has no callers. TikvTransactionOpener is retained because
real unistore transaction tests and native commit-outcome tests still consume it.

## Progress


- [x] Refresh refs and trace Cargo/source registrations and live owners.
- [x] Remove six orphans and two disconnected catalog declarations.
- [x] Verify retained source/test bytes; run affected checks and root lint.
- [ ] Complete actual hook, fresh pre-push locked build, remote verification and cloud save.

## Milestones and Plan of Work


Retire the entire orphan set without replacing or enabling its incomplete paths.
Remove both alternate catalog blocks while keeping the shared transaction and
snapshot adapters byte-identical. Record deleted file hashes, original test
names, registration evidence and remaining Go obligations in
parity/current-audit/orphan-storage-cleanup-validation.json. Update both cleanup
register links without changing behavioral finding dispositions.

## Concrete Steps / Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From rust/ run:

    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-exec -p tidb-server --all-targets
    cargo metadata --locked --no-deps --format-version 1

Run make lint and git diff --check at repository root. Verify all retained Rust,
manifest and executable-script inputs against base hashes and confirm the six
retired carriers have no Cargo target/registration. No new test is needed for
unregistered-file and unused-function deletion; all retained test bodies and
compiled algorithms remain unchanged. The normal commit must execute actual
hooks/pre-commit with cd rust && cargo build --locked -p tidb-server. Repeat that
locked build immediately before the authorized normal push and verify remote
SHA. Preserve concurrent changes; never bypass hooks or force push.

## Surprises & Discoveries


The module scan's candidates included legitimate standalone binaries. Checking
Cargo targets prevented their deletion. Native opener APIs also have meaningful
real-storage test callers; no opener/driver or client-rust source is removed.

## Decision Log


Remove unreachable drafts without claiming their Go packages implemented or
their never-run tests passed. Preserve actual storage and query owners and all
original package obligations. Date: 2026-10-06 UTC.

## Recovery / Interfaces and Dependencies


Recover individual before-images with git show a4b2f46659:<path> into temporary
files for review. External inventory/logs are /workspace/.cloud-setup/orphan-storage-cleanup.
No Cargo manifest, lockfile, dependency, hook or supported SQL interface changes.
Rerun checks safely while preserving caches and concurrent work.

## Outcomes & Retrospective


All-target checking, root lint, Cargo target verification and diff checks pass.
Retained source/test bodies and all remaining catalog blocks are byte-identical. Counts remain 86 tracked /
30 repaired /56 unresolved. No complete package acceptance, full Go suite,
live multi-node TiKV, runtime or performance validation is claimed for this
cleanup. Final post-commit gates and cloud persistence are recorded externally.
