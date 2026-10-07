# Preserve partition identity across shared table readers

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


Ordinary index lookup, index-join lookup and index-merge table tasks must retain physical partition identity while using the shared remote table-request owner. Equal handles in different partitions must not alias, missing/filtered rows must not shift physical-ID output, and request policy must survive every caller. A physical partition ID identifies a record-key namespace; a handle identifies a row only within it.

## Progress


- [x] Confirm clean c31b8d0 and refresh Go master 7a3; read repository instructions and source owners.
- [x] Segment producer routing, request grouping, row/chunk identity, caller policy and fallback/close ownership.
- [x] Capture grouped baseline regressions for ordinary lookup, index join and index merge.
- [x] Implement connected consumers and retire partition-only bypasses.
- [x] Validate grouped tests, downstream targets, real SQL, lint and locked build.
- [x] Update both registers and prepare the actual-hook/fresh-build publication gates. Actual commit, remote SHA and saved Cloud revision are recorded in /workspace/.cloud-setup/partition-reader-batch/final-handoff.json after execution.

## Context and Orientation


rust/crates/tidb-executor/src/kv_table/table_scan.rs owns build_table_reader_from_handles and row/chunk completion but refuses partitioned tables. access_path.rs retains routes in LookupBatchJob and HandleSourceExec but bypasses remote requests; IndexJoinLookupExec discards its index cursor's partition ordinal. index_merge_reader.rs supplies physical IDs to HandleSourceExec. Go pkg/executor/builder.go buildTableReaderFromHandles consumes PartitionHandle through RequestBuilder.SetPartitionsAndHandles; pkg/executor/distsql.go retains partition-aware row identity. This is existing-owner maintenance, not complete package acceptance.

## Plan of Work


Accept aligned physical routes in the shared builder, validate before I/O, and open per-physical-table scans with the existing range builder. Retain original positions through row/chunk completion using partition plus handle identity. Preserve missing-row gaps for physical-ID projections. Migrate ordinary inline/worker lookup, HandleSourceExec/index merge and index-join producers. Keep dirty reads on transaction-aware fallback; close every opened response on refusal/error. Preserve evaluation context, adaptive estimates, cancellation and counters.

## Milestones


Reproduce absent remote requests while checking SQL rows, implement every selected producer/consumer, then run grouped final validation and publish evidence. N03/O13 retain broader obligations.

## Concrete Steps


Use the existing Cloud checkouts. Source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. From /workspace/tidb/rust:

    cargo test --locked -p tidb-executor --lib -- partition_reader common_reader reader_task_ remote_scan::tests:: access_path::tests:: kv_table::table_scan::remote_cursor_tests:: index_merge point_get --test-threads=1
    cargo check --locked --all-targets -p tidb-executor -p tidb-session -p tidb-server
    cargo build --locked -p tidb-server

Run make lint from repository root. No Go/dependency/generated changes are intended; Bazel regeneration is unnecessary. Commit through hooks/pre-commit; repeat the locked build immediately before each normal push and verify the remote SHA. Never force-push.

## Validation and Acceptance


Each selected reader must produce correct SQL rows and table-request captures, preserving input order/physical identity when responses reverse or omit rows. Cover row/chunk paths, common/integer handles, pruning, duplicate handles across partitions, fallback and error cleanup. Fake transport does not establish live multi-node TiKV. Use the existing temporary MySQL/unistore recipe for supported SQL controls; add no permanent harness.

## Idempotence and Recovery


Preserve concurrent changes and caches. Logs live under /workspace/.cloud-setup/partition-reader-batch. Refresh the verified recovery bundle before retiring owned one-shot helpers; never replay historical mutation scripts.

## Surprises & Discoveries


Ordinary and merge producers already retain routes. Index-join iteration loses its partition ordinal; preserve that before enabling remote admission. The first plan-writing command used the wrong working directory and failed before running Cargo or changing production source. The first grouped compile caught a missed remote-row tuple type; it was corrected before the 221-case pass. Range-limited formatting requires nightly --unstable-features; the rejected invocation changed no source.

The first wire setup passed six clean controls, then partition UPDATE failed with 1105 because unchanged plan_builder/from.rs leaves the SelectLock physical-ID map empty. wire-review.json retains this pre-existing blocker. Final dirty-read controls use an in-transaction INSERT; all nine pass. Partition-lock planner ownership is a separate remaining prerequisite, not repaired by the reader changes.

## Decision Log


Carry physical identity as task data rather than infer it from output values, matching Go's PartitionHandle. Repair all three consumer paths together. Do not enable dirty remote merging without its distinct UnionScan contract.

## Outcomes & Retrospective


Three baseline SQL regressions failed because no table requests consumed the shared policy. The first grouped implementation run passed 221 tests. Final review adds later-partition refusal/error cleanup and physical-ID chunk projection controls, and preserves the original mutable storage owner for unpartitioned reads. Final 221-case grouped tests, downstream all-target checking, lint, locked server build and nine real MySQL/unistore controls passed. This batch does not accept a complete package or close N03/O13.
