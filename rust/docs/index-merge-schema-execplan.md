# Align index-merge schemas and partition handles with Go

This living plan follows root `PLANS.md`.

## Purpose / Big Picture


Union index merge combines row identifiers from several index/table scans before fetching rows once. Restore pruned index columns, preserve common-primary-key suffixes, and retain physical partition identity through that fetch, so Rust returns the same rows as Go. This is a bounded schema/executor repair, not whole-planner parity.

## Progress


- [x] Read Go `PhysicalIndexScan.InitSchema`, partial-worker handle extraction, and Rust planner/reader/cursor ownership.
- [x] Reproduce the schema and partition failures in grouped regressions.
- [x] Repair planner schemas and executor handle routing together.
- [x] Run grouped tests, server compilation and lint; review the diff and hand off local changes.
- [x] Follow-up review: reproduce and fix final-fetch physical-table ID rejection and partial-reader teardown ownership.
- [x] Run the grouped checks after these review fixes and record current results.

## Surprises & Discoveries


Rust's storage index cursor already decodes global-index partition IDs. The index-merge builder rejects multiple partitions, its partial adapter replaces their identity with zero, and its final lookup discards identity. Partial index scans also fetch table rows unnecessarily before the final fetch.

The retained-plan regressions failed with `physical index merge needs retained partition identity` and schema `[1]` versus Go's `[1,2,1,-3]`. Both passed after repair. Broader validation exposed the insert duplicate check probing every partition for the same hidden handle; Go `TableCommon.addRecord` checks only the destination `RecordKey`. Reuse Rust's existing `check_insert_key` for that exact key, including lazy and storage-error behavior. This is required to exercise legitimate equal handles across distinct partitions without test-only writes.

## Decision Log


Use Go's handle-only output projection for unordered bare index partials, reusing Rust's existing index request/cursor. Preserve canonical partition ordinals across partials and pass explicit physical IDs to the existing batch row lookup. Table partials read one partition per child; concatenate children as one logical path so intersection counting is unchanged. Restore every physical index column independently of the shorter range-building prefix, and append handle suffixes without deduplication. Decision: 2026-09-15, Codex.

For ordered or wrapped partials, retain ordinary executor evaluation. A per-partition scan supplies the synthetic table ID as a constant expression, not a stored column lookup. Preserve close-time index-usage reporting and retain runtime row counters for both direct index workers and concatenated partition results.

2026-09-15 review: Go `BuildIndexMergeTableScan` also adds the physical-table ID to the final ordered fetch. Add a synthetic `HandleOutputColumn::PhysicalTableId` resolved from the same per-handle partition route used for the record read; never substitute the logical table ID for an ambiguous partition. Extend the retained-plan matrix to check this final column in both sort directions. Track the prefix of partial readers whose Open was attempted, close all of them once on success and error, and preserve the read/open error over cleanup errors. This follows Go's deferred result cleanup and its `TestIndexMergeError` / `TestIndexMergeCoprGoroutinesLeak` coverage.

## Context and Orientation


Go owns behavior in `pkg/planner/core/operator/physicalop/physical_index_scan.go`, `pkg/planner/core/find_best_task.go`, and `pkg/executor/index_merge_reader.go`. Rust constructs union candidates in `rust/crates/tidb-planner/src/find_best_task/index_merge_union.rs`, builds readers in `rust/crates/tidb-executor/src/driver/physical_builder.rs`, and merges handles in `rust/crates/tidb-executor/src/index_merge_reader.rs`. `access_path.rs` and `kv_table/table_scan.rs` already implement partition-aware index handles and explicit batch lookup routes.

## Plan of Work and Milestones


First extend the Go-derived partition suite with deterministic `TestIdexMerge` inputs and check retained schema layout. Record baseline failures. Then repair physical column reconstruction, common-handle suffixes and global-index hidden table ID; consume index handles directly and route final rows by physical ID. Finally run grouped validation and inspect the complete diff. Acceptance is correct retained column order and rows across heap, integer/common handles and partitioned/global index paths, including duplicate handles in different partitions.

## Concrete Steps and Validation


From repository root, run targeted regressions before the production edits, then:

    cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib index_merge -j12 -- --test-threads=12
    cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib -j12 -- --test-threads=12
    cargo test --manifest-path rust/Cargo.toml -p tidb-executor --test all -j12 -- --test-threads=12
    cargo check --manifest-path rust/Cargo.toml -p tidb-server -j12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint

Expect no failures; existing ignored Go gaps remain explicit. Rust-only edits need no Bazel regeneration. No performance claim requires a benchmark in this correctness batch.

## Interfaces and Dependencies


Use `PartialHandleSource` as the index worker boundary, `HandleRef.partition_index` as the canonical partition key and `HandleSourceExec::with_partition_ids` for final lookup. No new dependency is required.

## Idempotence and Recovery


Tests are rerunnable. Preserve concurrent work; reconcile the shared remote before any push to `hparser-integration`. Do not reset or force-push. Recover this bounded change by reverting its commit if necessary.

## Outcomes & Retrospective


Reviewed locally on 2026-09-15. The physical schema and partition-routing regressions pass, including the final-fetch output and cleanup fixes below. Executor unit tests: 1,312 passed, zero ignored. Chunk tests: 267 passed, four existing ignored. Executor integration target: 328 passed, 184 existing ignored gaps. Targeted planner tests: six passed. Release compilation, `make lint` and `git diff --check` passed; lint required a network-enabled retry to resolve the Go tool dependency.

The retained-reader matrix covers heap/integer/common handles, local/global indexes, index/table partials, ordered index partials, unrestricted/named/empty partition selections, duplicate hidden handles across partitions, and retained runtime row counts. Ordered checks no longer sort results before asserting them. It also proves that physical-table ID is emitted rather than looked up as a stored SQL column. The Go-derived SQL suite covers RANGE/HASH/LIST partition comparisons.

Follow-up review found that concatenating individually ordered partitions violates the process worker's sorted-path assumption: its top-N early termination returned handles `[10,20]` instead of `[1,2]`. Reproduced in `/private/tmp/index-merge-partition-topn-before.log`. Go `startPartialIndexWorker` uses `distsql.NewSortedSelectResults` for this case. `PartitionedHandleSource` now retains one batch per partition and heap-merges their first rows, preserving sort direction, collation, physical identity and bounded buffering. The existing regression exercises ascending/descending order, offsets, empty partitions and one-row batch refills. The final grouped executor suite, six targeted planner tests, release build, lint and `git diff --check` pass.

This work does not establish live TiKV behavior, a measured performance improvement, full optimizer candidate generation, or whole-package parity. Global multi-partition ordinary scans outside these partition-specific workers remain outside this change's qualification. Changes are local and have not been committed or pushed in this task.

Changed production files are `find_best_task/index_merge_union.rs` in tidb-planner and `access_path.rs`, `driver/physical_builder.rs`, `index_merge_reader.rs`, `kv_table.rs` in tidb-executor. Regressions extend the owning planner/builder modules and `tests_partition_table_sql_source.rs`. No Go or generated files changed.

## Artifacts and Notes


Review reproduction: `/private/tmp/index-merge-review-before.log` has two failed tests: final output column -3 rejected as absent from table 42; a partial opened once closed three times. After the fixes, `/private/tmp/index-merge-review-after.log` passes all 24 index-merge tests, including first-error preservation, cleanup of later readers after a close failure, and no Close on unopened readers.

The completed follow-up review changes `access_path.rs`, `driver/physical_builder.rs`, and `index_merge_reader.rs`. Grouped validation passes: chunk 267 (four existing ignored), executor 1,312, targeted planner six, integration 328 (184 existing ignored). Release compilation, lint and whitespace checks pass. Exact commands, run from the isolated checkout `/private/tmp/tidb-read-setup.APKpRf/server-build`:

    cargo test --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -j12 -- --test-threads=12
    cargo test --manifest-path rust/Cargo.toml -p tidb-chunk -p tidb-executor --lib -j12 -- --test-threads=12
    cargo test --manifest-path rust/Cargo.toml -p tidb-planner --lib index_merge -j12 -- --test-threads=12
    cargo test --manifest-path rust/Cargo.toml -p tidb-executor --test all -j12 -- --test-threads=12
    cargo build --manifest-path rust/Cargo.toml -p tidb-server --bin tidb-server --release -j12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint
    git diff --check

Current logs are `/private/tmp/index-merge-review-unit.log`, `/private/tmp/index-merge-review-planner.log`, `/private/tmp/index-merge-review-integration.log`, `/private/tmp/index-merge-review-build.log`, and `/private/tmp/index-merge-review-lint.log`. Lint required a network-enabled retry to resolve its Go tool dependency. No benchmark, Go tests, live TiKV qualification, or deployment was run for these two correctness fixes. Existing ignored cases and whole-package parity remain open. The main checkout's unrelated edits are untouched. No commit or push was made.

2026-09-15 update: record final-fetch physical-ID emission, exact-once partial teardown, reproduced failures and current grouped validation. The scope remains the reviewed index-merge schema/reader path.
