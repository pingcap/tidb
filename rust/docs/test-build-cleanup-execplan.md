# Consolidate utility and protocol integration harnesses

This living ExecPlan follows root PLANS.md. Prior evidence remains indexed in
parity/current-audit/README.md.

## Purpose / Big Picture


Reduce integration-test binaries from fifteen to four across tidb-util and
tidb-proto. Thirteen module-safe suites share two harnesses; the logger and
infinite clock-monitor suites remain isolated. Remove one racy Rust-only check
that assumes two filesystem free-space samples are identical. No production
behavior changes, and no measured wall-clock speedup is claimed.

## Context and Orientation


Base cdc7d7256f27f352df2a1f8555115c6713330fce on hparser-integration in
/workspace/tidb. Refreshed Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
Cargo auto-discovered seven utility and eight protocol integration roots.
Existing custom build scripts own version/protobuf generation and remain intact.
The new tests/all.rs roots explicitly declare module-safe suites. Set autotests
false and declare all plus the two retained utility targets in Cargo.toml.
Future suite additions must be registered in these roots or explicit targets.

All existing suite files retain their bytes except sys_storage_source.rs, which
loses uses_statfs_available_bytes. Concurrent disk activity can change statfs
between calls; Go pkg/util/sys/storage/sys_test.go checks positive capacity only.
Keep that original case and the missing-path OS-error regression. Keep cgroup's
non-Linux case compiled conditionally, even though it does not run on Linux.

## Progress


- [x] Trace all fifteen suites, process isolation and maintained command callers.
- [x] Consolidate thirteen suites and remove the racy capacity comparison.
- [x] Run both aggregate suites, verify metadata/input continuity and run lint.
- [ ] Complete hook, fresh pre-push build, remote verification and cloud checkpoint.

## Milestones and Plan of Work


Preserve baseline Cargo metadata and test hashes. Add two small module roots and
explicit Cargo targets without changing build scripts or dependencies. Update
rust/scripts/README.md with current commands; historical receipts keep their
original invocations. Prove every old suite remains registered once, with only
the documented racy case removed, before publication.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From /workspace/tidb/rust run:

    cargo metadata --locked --no-deps --format-version 1
    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-util -p tidb-proto --test all

Expected Linux result is 44 protocol and 14 utility tests, zero failures or
ignored cases. The non-Linux cgroup case is conditional, not a skipped Linux test.
The two isolated suites are unchanged and not rerun. Verify before/after target
counts 15 to 4, all suite hashes except the exact statfs block, manifests changed
only for target registration, and unchanged build scripts/lockfile/production.
Run root make lint and git diff --check. Normal commit must execute the actual
hooks/pre-commit locked server build. Immediately before the authorized normal
push repeat cd rust && cargo build --locked -p tidb-server with CARGO_BUILD_JOBS=1.
Verify remote SHA; no force-push or hook bypass. Postcommit evidence is external
/workspace/.cloud-setup/utility-proto-harness-cleanup/final-handoff.json.

## Surprises & Discoveries


Blind aggregation would share a process with a permanently running time monitor
and a global logger reset. Those suites retain isolation. Two-sample statfs
comparison is absent from Go and is invalid under concurrent filesystem writes.

## Decision Log


On 2026-10-06, retain behavioral and native type-identity coverage while removing
redundant executable harnesses. Use explicit module roots because both crates
already have custom build scripts; preserve those generation/version owners.
The supported platform cases and original Go obligations remain unchanged.

## Recovery / Interfaces and Dependencies


Recover files with git show cdc7d7256f27f352df2a1f8555115c6713330fce:<path>.
External inventories/logs live in /workspace/.cloud-setup/utility-proto-harness-cleanup.
No production API, dependency version, lockfile or native-client source changes.

## Outcomes & Retrospective


Eleven redundant integration harnesses and one racy check are retired. Both suites
passed (58 tests), target/input verification and lint passed; publication remains. All 56 behavioral findings remain unchanged; no package acceptance.
This replaces the completed placeholder cleanup plan, whose receipt remains
placeholder-macro-cleanup-validation.json.
