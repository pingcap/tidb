# Share ALTER metadata admission with column and index jobs

This living ExecPlan follows PLANS.md.

## Purpose / Big Picture

Prepare table options, charset conversion, rename and TTL removal from the original table before any backfill. Follow freshly fetched Go 7a3dacb52efe58d28db360ae8639d8838c376544 job admission and multi-schema allowlist. Retire the late metadata dispatcher and obsolete tests that accept unsupported jobs. This advances D11 and allocator/error ordering obligations; it does not implement online recovery or accept a complete Go package.

## Progress

- [x] Confirm clean 766ad3f, refreshed Go master and read DDL contracts.
- [x] Select metadata, allocator, charset and compatibility admission segments; extend existing suite.
- [x] Baseline91 passed/7 failed: five valid production mismatches and two invalid new fixtures, recorded separately.
- [x] Shared typed metadata, charset/no-op policy, allocator validation and compatibility warnings implemented; late branches and stale success cases retired.
- [x] Final101 unit +57 integration tests, downstream all-target check, lint and locked build passed. Twelve wire discovery probes confirm cluster-lowering/parser limits; no wire parity pass.
- [ ] Update registers/receipts, commit through actual hook, fresh prepush locked build, verify push and save cloud checkpoint.

## Context and Orientation

rust/crates/tidb-executor/src/ddl/alter_table.rs prepares column/index/constraint jobs but applies other actions later. Go pkg/ddl/executor.go builds all jobs before MultiSchemaChange; multi_schema_change.go fillMultiSchemaInfo refuses rename, cache, AUTO_RANDOM and TTL jobs. Go also prechecks COMPRESSION and resolves repeated charset/collation declarations. Existing Rust tests incorrectly accept several refused jobs.

## Plan of Work

Capture typed metadata changes using immutable original metadata. Resolve TTL, placement and charset once, defer allocator execution until all actions succeed, and publish preparation warnings in source order. Use Go's job allowlist and preserve no-job no-ops. Keep partition mutation/reorganization outside this scoped admission repair and record that residual explicitly.

## Concrete Steps

From /workspace/tidb/rust after sourcing /workspace/.cloud-setup/env.sh and exporting CARGO_BUILD_JOBS=1:

    cargo test --locked -p tidb-executor --lib -- multi_schema_change ddl:: --test-threads=1
    cargo test --locked -p tidb-executor --test all -- db_integration_ddl_types_source ddl_ttl_info_options_source table_options_source table_lifecycle_job_source placement_policy_ddl_source --test-threads=1
    cargo check --locked --all-targets -p tidb-executor -p tidb-session -p tidb-server
    cargo build --locked -p tidb-server

Run make lint from repository root. Reuse existing integration target for charset/TTL controls. No Go/failpoint/Bazel inputs change. No full suite or repeated per-fix build.

## Validation and Acceptance

Admission diagnostics precede row-rewrite errors. Failed statements preserve catalog and shared counters. Supported sibling changes compose; unsupported jobs refuse with Go error identities; no-op actions create no jobs. Use Ready profile: its named skill is absent here, so follow the root task matrix. Logs: /workspace/.cloud-setup/alter-admission-batch. No live TiKV or performance claim.

## Idempotence and Recovery

Preserve branch, origins, concurrent work and shared caches. Never force push. Before each push run the locked server build, and verify remote SHA. Keep reusable cloud setup separate from source changes.

## Surprises & Discoveries

Go master accepts only COMPRESSION=NONE as a no-op for ALTER. Rust stores arbitrary compression strings. AUTO_RANDOM/cache multi-schema success tests also contradict the current Go job allowlist.

## Decision Log

Capture typed changes instead of catalog snapshots per option, which would overwrite sibling changes. Keep partition preparation and distributed recovery visibly unresolved.

## Outcomes & Retrospective

Typed metadata and compatibility admission now compose with existing column/index/constraint jobs. 158 distinct Rust cases and required local gates pass. D11 remains partial for cluster lowering, partition/cache admission, schema-wide CHECK timing and durable recovery. The other55 roots are carried, not re-proven. Actual publication evidence follows in external final-handoff.json.

The baseline wire probe was useful boundary evidence: unistore uses the cluster DDL lowerer, which rejects these combinations before this executor. Future work must connect that owner, not claim local tests establish distributed behavior. Two fixture corrections and a captured-counter correction are retained in the validation receipt; they were not hidden by deleting tests.
