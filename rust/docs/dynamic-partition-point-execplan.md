# Restore dynamic partition PointGet with execution-time routing

This living ExecPlan follows PLANS.md. The overall objective remains all Rust failed cases, not just this category.

## Purpose / Big Picture

A WHERE/HAVING intersection that identifies one clustered key must use Point_Get with the correct partition, matching Go master. Prepared executions must follow changed parameter values across partition boundaries rather than pinning the first partition.

## Progress

- [x] Original failing SQL captured in /tmp/grouped-partition-point.sql and Go output /tmp/grouped-partition-point-go.out.
- [x] Go master fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85 returns Point_Get, partition:p2, and j9FsMawX5uBro%$p.
- [x] Pre-rebase Rust red log /tmp/grouped-partition-point-red.log returns TableRangeScan; current rebuild is /tmp/grouped-point-current-red.log.
- [x] Identified missing dynamic conversion and cache routing contract.
- [x] Add compact cross-partition and prepared execution regression; Go /tmp/dynamic-partition-point-compact-go.out proves cache hit on the second execution and correct P0/P1 Point_Get plans.
- [x] Implement native point routing metadata and resolve it for fresh/cached execution and SELECT/DML EXPLAIN.
- [x] Original regression passes; integer/common-handle prepared routing, absent/NULL keys, explicit partitions and cache hits match the real Go oracle.
- [x] 42 prepared-cache tests, 46 planner physical tests, 12 executor builder tests and final 310 session integrations pass. Final DML publication coverage passes its two targeted regressions.
- [x] Ready lint passes. Real access-path first exposed Go master / PD v8.5.6 QueryRegion incompatibility; retained evidence and selected the previously validated nightly dependency version.
- [x] Complete final integration/RealTiKV checks and write DYNAMIC_PARTITION_POINT_FIX.zh-CN.md. Real access-path exits 0 and reports ready with schema_version=73 and stats_loaded=4. Final partition suite: 97 passed / 1 unrelated failed / 0 ignored.
- [ ] Commit/push this category independently.

## Surprises & Discoveries

find_best_task/dispatch.rs allows point conversion only for unpartitioned sources, static physical partitions, or explicit PARTITION. A single range and a dynamically pruned partition do not suffice in Rust. Go find_best_task.go permits the single-point case; only multi-point dynamic partition conversion is refused.

Go PointGetPlan.PrunePartitions in physical_batch_point_get.go re-evaluates the point key against the current partition metadata on cache hits. For non-binary common-key collations, it rebuilds original SQL values from access conditions because collation sort keys cannot route partitions correctly. Rust CachedPlanRebuildContext only updates ranges, and CachedSelectPlan.bind does not receive Catalog. Simply substituting the initially pruned physical table ID would make later parameter values read the wrong partition.

## Decision Log

- Do not remove the planner guard while hard-coding the first pruned partition ID. Preserve the logical table identity and explicit partition restrictions independently from derived routing.
- The shared executor construction boundary must resolve the current key for both cached and fresh point plans. Retained EXPLAIN metadata must also reflect current routing.
- Use Go source/real oracle output; keep original grouped HAVING result and plan assertions.

## Context and Orientation

Checkout /tmp/tidb-hparser-current, current HEAD 6a118e7a04. Native point node is rust/crates/tidb-planner/src/physical/mod.rs::PhysicalPointGet. Ordinary conversion is find_best_task/dispatch.rs near point_handle_ok. Executor build_point_get is in tidb-executor/src/driver/physical_builder.rs. Go-equivalent partition evaluation helpers exist on KvTable, including row_partition_route and handle_partition_routes. Common-key routing must use original values where necessary.

CachedSelectPlan.bind in driver/planner_bridge.rs and CachedDmlPlan.bind in driver/dml.rs call physical_plan_cache.rs::rebuild_plan_for_cache_in_place. Their later execution_mut returns the mutable physical tree. Query execution receives Catalog and StmtContext; use an existing boundary with those dependencies to update derived routing before executor construction and process-plan publication. Plain EXPLAIN also optimizes without executing, so it needs the same route preparation.

## Plan of Work

First add a compact RANGE-partitioned integer PK test with residual predicates so it reaches ordinary planning. Verify two constants reach two different partition PointGet plans; prepare the equivalent parameterized query and execute keys in different partitions and an absent key, asserting exact rows. Confirm cache behavior against Go before asserting a cache hit. Then design a native point routing description that retains logical table ID, explicit restriction and original key values/access conditions. Route at the shared execution/preparation boundary; do not add AST access-path selection to the executor. Update clones and cache rebuilding contracts, then run the original string/common-handle regression.

## Concrete Steps

From the checkout root, use RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432 for cargo:

    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib grouped_having_intersection_reads_one_clustered_partition_point
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib dynamic_partition_points_follow_each_bound_key
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_prepared_plan_cache
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
    make lint
    git diff --check

## Validation and Acceptance

Original Go and Rust SQL must return the same row and Point_Get partition:p2. The compact fixture must prove changing partition targets does not lose or leak rows. Null/absent keys and explicit partition exclusions must return zero rows. Common-handle collations need original key-value routing coverage. No stale partition access objects may be published after cached executions.

## Idempotence and Recovery

Tests use independent session fixtures. Preserve existing shared-statistics WIP in tests_analyze.rs and BLOCKER_RESOLUTION_REPORT.zh-CN.md. Only stage this category. Inspect remote movement before rebasing and never force push.

## Interfaces and Dependencies

Planner nodes remain runtime-independent. Catalog and statement-aware routing belongs in tidb-executor. Cache range rebuilding must preserve enough original identity to refresh derived partition routes without replanning unrelated operators.

## Outcomes & Retrospective

The original dynamic PointGet failure and cache-routing regression are resolved with Go evidence and passing targeted/integration checks. The existing readiness issue is resolved and is not a blocker for this work. This document does not claim all remaining failed cases fixed.

2026-09-12 update: production implementation now passes the original failure and both new routing regressions. The full partition suite before adding the final common-key/DML checks was 96 passed / 1 failed; the remaining static residual batch failure is unchanged. Its real Go output in /tmp/point-static-batch-go.out confirms the old expected topology and estimates, so it requires an independent implementation fix. Go master with PD v8.5.6 fails before Rust starts (unknown QueryRegion RPC), whereas v9.0.0-beta.2.pre-nightly starts and creates the access-path fixture successfully. Final real replay is /tmp/dynamic-point-access-nightly.log.
