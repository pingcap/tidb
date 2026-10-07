# Carry partition identity through locking reads and writes

This living ExecPlan follows repository `PLANS.md`.

## Purpose / Big Picture

Repair the shared physical partition identity needed by SELECT FOR UPDATE, UPDATE and DELETE. A record key contains both a physical table ID and a handle; a handle alone cannot identify a partitioned record. Follow Go master 7a3dacb52efe58d28db360ae8639d8838c376544, refreshed from origin. This is maintenance of existing planner/executor owners, not acceptance of a complete Go package.

## Progress

- [x] Confirm clean integration baseline 3b7b000bad672056a0eef3c9b88c4f56a54b65f4 and unchanged Go master.
- [x] Trace Go buildSelectLock, addExtraPhysTblIDColumn4DS and logical pruning against the missing Rust producer.
- [x] Capture grouped regressions before implementation: all three failed with missing physical partition ID.
- [x] Implement planner identity and shared table/index reader propagation, preserving lock-key validation.
- [x] Validate reads, writes, pruning and outer joins together; exact selected record keys and all 220 grouped tests pass.
- [x] Run scoped checks, lint and locked build; update receipts/registers. Actual hook, fresh prepush build, remote verification and reusable checkpoint are mandatory publication steps recorded in the Cloud final handoff.

## Context and Orientation

`rust/crates/tidb-planner/src/plan_builder/from.rs::build_select_lock` currently builds handle columns but never fills physical identity. `logical/lock.rs` and physical index resolution already carry the map. `rust/crates/tidb-executor/src/driver/physical_builder.rs` correctly refuses partition locks without it. Table and index scans must emit each row's own partition ID, including dynamic pruning; joins must retain it through pruning. `rust/crates/tidb-session/src/tests_partition.rs` exercises shared SQL paths and permits an explicit SelectedLockKeys collector without a live cluster.

## Plan of Work

Add partition lock regressions to the existing session tests. Add synthetic datasource columns following Go's helper, with one unique expression identity and aligned metadata/schema/output names. Feed the logical lock's existing map. Repair any reader handoff exposed by those tests, using physical routes already owned by the shared source, never guessing from handles. Remove obsolete boundary comments. Validate SELECT, UPDATE and DELETE with one grouped compilation, then test real MySQL/unistore behavior.

## Milestones

First establish failures on the baseline. Next repair the shared producer and readers so the same operations work under static and dynamic pruning. Finally check exact lock-key partition prefixes and outer-join null behavior, update the audit evidence, and publish only through mandatory build gates.

## Concrete Steps

From `/workspace/tidb/rust`, source `/workspace/.cloud-setup/env.sh`, set CARGO_BUILD_JOBS=1 and run `cargo test --locked -p tidb-session --lib -- partition_lock_batch --test-threads=1`. Capture baseline/final output under `/workspace/.cloud-setup/partition-lock-batch`. Run the affected planner/executor/session groups together after implementation, `cargo check --locked --all-targets -p tidb-planner -p tidb-executor -p tidb-session -p tidb-server`, and root `make lint`. The actual hooks/pre-commit must execute `cd rust && cargo build --locked -p tidb-server`; repeat immediately before push to pingcap/tidb hparser-integration and verify remote HEAD.

## Validation and Acceptance

Locking reads return ordinary SQL columns and collect record keys for the actual partitions. UPDATE and DELETE operate across selected partitions, preserve rollback and named partition restrictions. Unmatched outer-join rows create no physical-zero lock. Regression failures must become passes; no full Go suites, multi-node TiKV or performance claim follows from local validation.

## Idempotence and Recovery

Tests use isolated sessions. Preserve existing changes, never reset the checkout or force-push. Commands can be rerun; preserve failed receipts. If the shared scan cannot retain row identity, keep the existing refusal until it can.

## Surprises & Discoveries

The previous partition reader batch repaired routed lookups but exposed the independent missing locking identity producer. Broad findings remain partial until their other obligations are implemented.

Grouped tests exposed two older assertions. The residual plan-shape test pinned a fallback estimate even though Go evaluates available TopN values before that fallback; retain operator and row assertions and leave selectivity to its owner tests. The REORGANIZE refusal was initially suspected stale, but a stronger two-row check proved actual data disappearance: the local helper replaces physical IDs without moving rows. Remove that metadata-only helper and include REORGANIZE in the existing pre-mutation durable-owner gate. Preserve and strengthen the refusal regression with unchanged schema and per-partition data checks. Coalesce/add hash paths actually rehash rows and remain intact. Full durable DDL acceptance remains open.

## Decision Log

Use one connected read/write batch and grouped compilation. Retain meaningful semantic tests and existing lock safety checks. Do not add a parallel SQL execution path.

## Outcomes & Retrospective

Shared implementation and reorganization safety removal are complete. Three initial locking regressions failed together, and the stronger reorganization check reproduced two disappearing rows in both the session and real MySQL/unistore baseline. All 220 grouped tests, all-target checking, lint, locked server build and 21 real MySQL/unistore cases pass. E03/N03/D11 remain partial, and counts remain 86 tracked/30 repaired/56 unresolved. The committed partition-lock-batch-validation.json records source pins and failed/passed evidence. Publication results and reusable checkpoint are recorded in the Cloud final handoff after the mandatory hook and fresh prepush build.

## Artifacts and Notes

Go owners: pkg/planner/core/planbuilder.go, logical_plan_builder.go, operator/logicalop/logical_lock.go and pkg/executor/union_scan_test.go::TestIssue28073. Durable receipt will record exact source hashes and commands.

## Interfaces and Dependencies

The additional deletion is `tidb-executor/src/ddl/alter_table.rs::reorganize_partition_action`, formerly called only by its local ALTER match. Go requires `pkg/ddl/partition.go::onReorganizePartition` and resumable data movement; the synchronous owner cannot substitute a metadata edit. No new partition DDL implementation is claimed.

Keep LogicalLock.tbl_id_to_phys_tbl_id_col and SelectedLockKeys as the shared interfaces. No new dependency or native client change is planned.
