# Restore partitioned integer BatchPointGet

This living ExecPlan follows `PLANS.md`. The overall objective remains all Rust failed cases; this milestone repairs the integer-handle partition batch category.

## Purpose / Big Picture

An integer primary-key IN query on a HASH-column partitioned table must use Go's single Batch_Point_Get, route each key to its physical partition, and print reached partition names in declaration order. Explicit PARTITION continues through the ordinary planner, as Go master requires.

## Progress

- [x] Reproduced `a_batch_point_get_names_the_partitions_its_handles_reach` at 846ba44e10; exit 101, separate partition and table access objects.
- [x] Read fixed Go master fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85 newBatchPointGetPlan and BatchPointGetPlan pruning.
- [x] Retain and execute per-key partition IDs in the native physical plan.
- [x] Original regression and storage routing test pass; related suite is 89 passed / 7 failed; integration 310 and physical-plan tests 46 pass; lint exits 0.
- [x] Independently commit empty partition read fix as 0cc26bc6f3.
- [x] Prepare integer batch routing as its own verified commit; push to origin/hparser-integration with this receipt.

## Surprises & Discoveries

Go newBatchPointGetPlan requires PartitionExpr.Expr to be a Column. Explicit PARTITION is rejected by tryWhereIn2BatchPointGet and handled by ordinary planning. Rust currently rejects every partitioned fast point plan. Existing stored_records_batched already accepts one physical ID per handle, but HandleSourceExec discards this facility by passing None.

The original explicit exclusion assertion mixed pruning modes. Fresh Go evidence in /tmp/partition-batch-explicit-go.out proves dynamic uses TableReader partition:dual, while static uses TableDual. Go find_best_task.go:2224 disallows ordinary multi-point partition conversion in dynamic mode. Restoring this condition exposed a separate reader bug: an empty partition list was treated as no restriction, causing wrong rows. That independent fix is 0cc26bc6f3, with unchanged RANGE/LIST/LIST COLUMNS regressions.

## Decision Log

- Keep point-get, unique-index, and common-handle partition fast paths outside this integer-handle milestone; their different key encodings require separate regression evidence. Do not claim the broader partition category complete.
- Retain routes in the physical plan so execution and EXPLAIN share authoritative partition selection, rather than patching display text alone.

## Context and Orientation

The checkout is /tmp/tidb-hparser-current. driver/access.rs creates fast physical plans. physical/mod.rs defines PhysicalBatchPointGet. driver/physical_builder.rs builds HandleSourceExec. access_path.rs opens the source and kv_table.rs performs grouped storage reads. explain.rs renders access metadata.

## Plan of Work

Add optional per-range physical partition IDs to PhysicalBatchPointGet, preserve them during plan cloning, and default existing ordinary plans to None. In the fast planner allow the Go-supported HASH column shape for integer keys, route keys before creating the physical plan, preserve pre-dedup row estimates, and use TableDual when no key routes. Carry routes through runtime dedup/order into HandleSourceExec and stored_records_batched. Resolve reached partition names from the plan IDs in definition order for EXPLAIN.

## Concrete Steps

From the checkout root use RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432 for cargo commands:

    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib a_batch_point_get_names_the_partitions_its_handles_reach
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_partition
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
    make lint
    git diff --check

## Validation and Acceptance

The original failing test must pass including duplicate estimate 3, mixed-case partition names, query rows, dynamic explicit exclusion to partition:dual and static exclusion to TableDual. Routed storage coverage exercises the production builder and requires exactly two batched reads for keys 1,2,1 across three partitions. Record other partition failures without weakening assertions. Go oracle output is /tmp/partition-point-go.out; original red log is /tmp/partition-batch-current-red.log.

## Idempotence and Recovery

Tests use isolated in-process session state. Preserve shared-statistics dirty work in tests_analyze.rs and BLOCKER_RESOLUTION_REPORT.zh-CN.md. Stage only this category. Fetch and inspect remote changes before any necessary rebase; never force push.

## Interfaces and Dependencies

PhysicalBatchPointGet retains Option<Vec<i64>> aligned with ranges. A routed HandleSourceExec uses IDs aligned with handles. Length disagreement must fail rather than silently fan out to every partition. Existing unpartitioned and ordinary plans keep None.

## Outcomes & Retrospective

The integer partition batch category passes, including actual storage routing and both pruning modes. /tmp/partition-batch-final-suite.log records 89 passed / 7 failed versus 85 / 11 before the empty-set fix; /tmp/partition-batch-final-integration.log records 310 passed; /tmp/partition-batch-planner.log records 46 passed. Storage counter regression /tmp/partition-batch-storage.log and final lint /tmp/partition-batch-final-lint.log passed. Other seven partition-related cases remain genuine outstanding work. This is not completion of all Rust failed cases or a complete Go package transcreation claim.
