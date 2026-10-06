# Remove executor test forwarding layers

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Run join and statement-policy tests against their real tidb-executor owner,
without compiling the higher-level tidb-exec adapter crate for those tests.
Work in /workspace/tidb on hparser-integration from
8046b8718917696c5b3c652f8aca752b3465b2ad. Go master is
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Implementations already belong to
executor; eleven forwarding re-exports remain solely as obsolete indirection.

## Progress


- [x] Trace ten source suites, eleven forwarding exports and live callers.
- [x] Move suites and explicit registrations; migrate callers before removing exports.
- [x] Verify all 80 bodies unchanged except owner paths; grouped tests and lint pass.
- [ ] Pass actual commit-hook build, fresh pre-push build and remote verification.
- [ ] Verify recovery bundle and save/read back Cloud startup checkpoint.

## Milestones and Plan of Work


Move base_join_probe, concurrent_entry_map, hash_join_version, hash_join_v2,
hash_table_v2, join_row_table, join_table_meta, tagged_ptr, statement_pushdown
and used_stats *_source.rs from tidb-exec/tests to tidb-executor/tests. Change
only tidb_exec:: owner paths and register each module exactly once in executor's
all.rs. Remove eleven re-exports, including row_table_builder used by these
suites. Switch session stmt_ctx hash-join selection, real_tikv_read pushdown and
slow_log_format statistics imports to tidb_executor. Preserve real adapters
such as keydecoder and warning publication in tidb-exec.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh, set CARGO_BUILD_JOBS=1, work in rust/:

    cargo test --locked -p tidb-executor --test all -- base_join_probe_source:: concurrent_entry_map_source:: hash_join_version_source:: hash_join_v2_source:: hash_table_v2_source:: join_row_table_source:: join_table_meta_source:: tagged_ptr_source:: statement_pushdown_source:: used_stats_source:: --test-threads=1
    cargo test --locked -p tidb-exec --lib -- slow_log_format:: --test-threads=1

Expect 80 migrated cases and nonzero slow-log tests; report any failures without
weakening expectations. Compare every migrated byte after reversing the owner
path replacement; no Go case or Rust correctness regression is discarded.
From root run make lint and git diff --check. No Go/Bazel/dependency changes.
The normal pre-commit hook must pass cd rust && cargo build --locked -p
tidb-server; rerun immediately before the authorized normal push to pingcap/tidb
hparser-integration and verify remote SHA. Never bypass hooks or force push.

## Surprises & Discoveries


The old facade's tests already exercise executor implementations, including
worker cancellation and spill behavior. Deleting those tests would discard real
coverage. Relocate them and remove their now-unused forwarding API instead.

## Decision Log


Preserve every assertion, fixture and algorithm. Only remove forwarding paths
after migrating every live caller. This is ownership cleanup, not full upstream
package acceptance or a measured performance claim.

## Outcomes & Retrospective


Ten suites moved and eleven exports removed. All 80 migrated tests and
6 slow-log consumer cases pass; lint, continuity and diff checks pass.
Publication remains pending.
Finding dispositions stay unchanged. Native client-rust stays unchanged.

## Recovery, Artifacts and Dependencies


Restore individual before-images with git show 8046b87189:<path>, preserving
concurrent work. Inventory and logs are under
/workspace/.cloud-setup/executor-owner-cleanup. Durable receipt:
rust/docs/parity/current-audit/executor-owner-cleanup-validation.json.
Dependencies remain unchanged. Cloud draft save, Publish and fresh-task
restoration remain distinct operations.

Revision: replace completed harness-plan retirement with executor test ownership.
