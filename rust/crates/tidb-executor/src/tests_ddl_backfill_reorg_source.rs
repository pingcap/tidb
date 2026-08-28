// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

//! GO PORT of the DDL backfill/reorg unit surface of `pkg/ddl` (items 14-32
//! of the pkg/ddl.part1 slice, read from `origin/master`):
//! `backfill_metrics_test.go`, `backfilling_dist_scheduler_test.go`,
//! `backfilling_test.go` and `backfilling_txn_executor_test.go`.
//!
//! Everything these files exercise belongs to the distributed reorg
//! backfill — the DXF task/subtask framework, the ingest (lightning) local
//! sorters, the reorg expression/table-mutate/DistSQL context builders, and
//! the DDL job metrics — none of which is transcreated in this workspace.
//! Every test below is a documentary `#[ignore]` naming the exact missing
//! Go symbol; the pure functions among them are NOT re-derived here because
//! inventing a Rust formula to assert against would approximate the very
//! behavior the port must pin.

// ---------------------------- backfill_metrics_test.go --------------------

/// GO PORT of `pkg/ddl/backfill_metrics_test.go:68
/// TestBackfillMetricsCleanupByTableID`.
///
/// Go pins that the Prometheus metrics registered per physical table id
/// (`backfill_total_gauge`, `backfill_progress_gauge`) are removed when the
/// backfill of that table finishes.
#[test]
#[ignore = "go-parity-gap: DDL job metrics (pkg/ddl metrics.go gauges) and the job pipeline that cleans them are not transcreated"]
fn backfill_metrics_cleanup_by_table_id() {}

/// GO PORT of `pkg/ddl/backfill_metrics_test.go:101
/// TestBackfillMetricsCleanupPartitionedTable`.
///
/// Go pins the same cleanup for one gauge set PER PARTITION physical id of a
/// partitioned table.
#[test]
#[ignore = "go-parity-gap: DDL job metrics and the partitioned backfill pipeline are not transcreated"]
fn backfill_metrics_cleanup_partitioned_table() {}

/// GO PORT of `pkg/ddl/backfill_metrics_test.go:277
/// TestBackfillMetricsIdempotentCleanup`.
///
/// Go pins that cleaning metrics for an already-cleaned table id is a no-op
/// rather than a double-registration panic.
#[test]
#[ignore = "go-parity-gap: DDL job metrics and the job pipeline are not transcreated"]
fn backfill_metrics_idempotent_cleanup() {}

// --------------------- backfilling_dist_scheduler_test.go -----------------

/// GO PORT of `pkg/ddl/backfilling_dist_scheduler_test.go:60
/// TestBackfillingSchedulerLocalMode`.
///
/// Go drives `LitBackfillScheduler.OnNextSubtasksBatch` for local (non
/// global-sort) add-index: one BackfillSubTaskMeta per partition in
/// partition order, `GetNextStep` walking Init -> BackfillStepReadIndex ->
/// StepDone, empty metas for an empty table, and `OnDone` cleanup.
#[test]
#[ignore = "go-parity-gap: LitBackfillScheduler (backfilling_dist_scheduler.go) and the DXF scheduler framework it extends are not transcreated"]
fn backfilling_scheduler_local_mode() {}

/// GO PORT of `pkg/ddl/backfilling_dist_scheduler_test.go:194
/// TestCalculateRegionBatch`.
///
/// Go pins `CalculateRegionBatch` (backfilling_dist_scheduler.go:428) over
/// nine (regionCnt, nodeCnt, useLocalDisk) cases: cloud storage
/// ceil(regionCnt/nodeCnt) (100/8 -> 13, 2/8 -> 1, 8/8 -> 1); local storage
/// with TiKV-amplified accounting (1000/8 -> 334, 1000/2 -> 500, 200/3 ->
/// 100, and pass-through when under one node batch).
#[test]
#[ignore = "go-parity-gap: pkg/ddl/backfilling_dist_scheduler.go:428 CalculateRegionBatch is not transcreated; the local-disk amplification formula has no Rust counterpart to assert against and is not re-derived here"]
fn calculate_region_batch() {}

/// GO PORT of `pkg/ddl/backfilling_dist_scheduler_test.go:218
/// TestBackfillingSchedulerGlobalSortMode`.
///
/// Go runs the scheduler with `GlobalSort = true` against a fake GCS server
/// and the DXF task manager: read-index -> merge-sort -> write&ingest step
/// walk with cloud-storage URIs in the subtask metas.
#[test]
#[ignore = "go-parity-gap: needs the DXF task manager, the fake GCS server and LitBackfillScheduler's global-sort steps; none are transcreated"]
fn backfilling_scheduler_global_sort_mode() {}

/// GO PORT of `pkg/ddl/backfilling_dist_scheduler_test.go:347
/// TestGetNextStep`.
///
/// Go pins the step machines: local mode Init -> ReadIndex -> Done; global
/// sort Init -> ReadIndex -> MergeSort -> WriteAndIngest; merge-temp-index
/// Init -> MergeTempIndex -> Done.
#[test]
#[ignore = "go-parity-gap: LitBackfillScheduler.GetNextStep (backfilling_dist_scheduler.go:217) and the proto.Step constants are not transcreated"]
fn get_next_step() {}

/// GO PORT of `pkg/ddl/backfilling_dist_scheduler_test.go:468
/// TestBackfillTaskMetaVersion`.
///
/// Go pins that `BackfillTaskMeta.Version` defaults to
/// `BackfillTaskMetaVersion0` and round-trips `BackfillTaskMetaVersion1`.
#[test]
#[ignore = "go-parity-gap: the BackfillTaskMeta struct and its version constants are not transcreated"]
fn backfill_task_meta_version() {}

// ------------------------------ backfilling_test.go -----------------------

/// GO PORT of `pkg/ddl/backfilling_test.go:53 TestDoneTaskKeeper`.
///
/// Go pins `newDoneTaskKeeper` (backfilling.go:1257): out-of-order task-id
/// completion buffers per-task next keys in `doneTaskNextKey` and advances
/// `nextKey` contiguously (a..c, then buffering 4/3/5, then flushing all
/// when 2 lands so nextKey jumps to g).
#[test]
#[ignore = "go-parity-gap: pkg/ddl/backfilling.go:1257 newDoneTaskKeeper is not transcreated"]
fn done_task_keeper() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:75 TestBackfillRetryableErrors`.
///
/// Go pins retryability classification: `errIndexInfoNotFound` is
/// recognized by `isIndexInfoNotFoundErr` (backfilling_dist_executor.go:38)
/// but NON-retryable for `backfillDistExecutor`; `ErrTooManyDataFiles`
/// wrapped in a merge-sort failure is non-retryable for `LitBackfillScheduler`
/// while a plain error IS retryable.
#[test]
#[ignore = "go-parity-gap: isIndexInfoNotFoundErr (backfilling_dist_executor.go:38), backfillDistExecutor and LitBackfillScheduler::IsRetryableErr are not transcreated"]
fn backfill_retryable_errors() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:93
/// TestBuildIndexConditionCheckerUsesFixedCollation`.
///
/// Go pins that `buildIndexConditionChecker` (index.go:4296) evaluates the
/// index condition with the collation FIXED by the reorg meta, not the
/// session's: with new collation disabled 'a' = 'A' is false, with it
/// enabled (utf8mb4_general_ci) it is true.
#[test]
#[ignore = "go-parity-gap: buildIndexConditionChecker (pkg/ddl/index.go:4296), copr.CopContextSingleIndex and the BuildSimpleExpr injection seam are not transcreated"]
fn build_index_condition_checker_uses_fixed_collation() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:172 TestPickBackfillType`.
///
/// Go pins `pickBackfillType` (index.go:1826): ReorgTypeTxn stays txn;
/// ReorgTypeNone becomes TxnMerge when ingest is uninitialized and
/// ReorgTypeIngest when it is; the cloud-storage subtest pins that a
/// `s3://` CloudStorageURI with `mockIngestCheckEnvFailed` still picks
/// ingest and sets UseCloudStorage.
#[test]
#[ignore = "go-parity-gap: pickBackfillType (pkg/ddl/index.go:1826) and the ingest Lit roots it consults are not transcreated"]
fn pick_backfill_type() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:361 TestReorgExprContext`.
///
/// Go pins that `newReorgExprCtx` (backfilling.go:99) and
/// `newReorgExprCtxWithReorgMeta` (:114) build an expr context whose SQL
/// mode, timezone and new-collation flag come from `DDLReorgMeta`
/// (Asia/Tokyo location honored; nil location falls back to the system
/// zone; UseNewCollate=false/nil decided per case), deep-cloned-equal to the
/// session-derived context modulo the documented fields.
#[test]
#[ignore = "go-parity-gap: newReorgExprCtx/newReorgExprCtxWithReorgMeta (pkg/ddl/backfilling.go:99/:114) and the exprstatic context they clone are not transcreated"]
fn reorg_expr_context() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:441 TestReorgTableMutateContext`.
///
/// Go pins `newReorgTableMutateContext` (backfilling.go:233): row-encoding
/// config follows `tidb_ddl_reorg_row_format` (v1 disables the row encoder),
/// assertion level off, reserved row-id alloc shared and exhausted, and no
/// statistics/cached/temporary/exchange-partition supports.
#[test]
#[ignore = "go-parity-gap: newReorgTableMutateContext (pkg/ddl/backfilling.go:233) and the table.MutateContext plumbing it checks are not transcreated"]
fn reorg_table_mutate_context() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:534 TestReorgDistSQLCtxNotFillCache`.
///
/// Go pins that both `newDefaultReorgDistSQLCtx`
/// (backfilling_txn_executor.go:148) and
/// `newReorgDistSQLCtxWithReorgMeta` (:184) set `NotFillCache` — reorg
/// coprocessor scans must not pollute the TiKV block cache.
#[test]
#[ignore = "go-parity-gap: newDefaultReorgDistSQLCtx (pkg/ddl/backfilling_txn_executor.go:148) and the DistSQLContext it builds are not transcreated"]
fn reorg_distsql_ctx_not_fill_cache() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:545 TestValidateAndFillRanges`.
///
/// Go pins `validateAndFillRanges` (backfilling.go:598): empty first-start /
/// last-end keys are filled from the job's [startKey, endKey); genuinely
/// non-covering ranges (gap between `c..` and `e..`, or a start below the
/// job start) are errors; the exact adjusted range slices are asserted.
#[test]
#[ignore = "go-parity-gap: pkg/ddl/backfilling.go:598 validateAndFillRanges is not transcreated; asserting an invented equivalent would approximate"]
fn validate_and_fill_ranges() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:634 TestTuneTableScanWorkerBatchSize`.
///
/// Go pins that `tableScanWorker.getChunk` (backfilling_operators.go:623)
/// re-reads `reorgMeta.BatchSize` on every checkout (32 then 64 after a live
/// `SetBatchSize(64)`), so a mid-backfill batch-size change takes effect
/// without restarting the worker.
#[test]
#[ignore = "go-parity-gap: tableScanWorker.getChunk (pkg/ddl/backfilling_operators.go:623) and the reorg-meta atomic batch size are not transcreated"]
fn tune_table_scan_worker_batch_size() {}

/// GO PORT of `pkg/ddl/backfilling_test.go:665 TestSplitRangesByKeys`.
///
/// Go pins `splitRangesByKeys` (backfilling.go:569) over a table of
/// (ranges, splitKeys, expected) cases: empty split keys pass through, keys
/// inside one range split it in place, keys at boundaries are dropped, and
/// keys outside all ranges are ignored.
#[test]
#[ignore = "go-parity-gap: pkg/ddl/backfilling.go:569 splitRangesByKeys is not transcreated; asserting an invented equivalent would approximate"]
fn split_ranges_by_keys() {}

// ------------------------ backfilling_txn_executor_test.go ----------------

/// GO PORT of `pkg/ddl/backfilling_txn_executor_test.go:23
/// TestExpectedIngestWorkerCnt`.
///
/// Go pins `expectedIngestWorkerCnt` (backfilling_txn_executor.go:347) over
/// ten cases: the global-sort path returns (concurrency, concurrency); the
/// local path derives reader/writer counts from avgRowSize with floors
/// (10/0 -> 5/7, 40/0 -> 16/16, 1/0 -> 1/2) and caps (10/5000 -> 80/10).
#[test]
#[ignore = "go-parity-gap: pkg/ddl/backfilling_txn_executor.go:347 expectedIngestWorkerCnt is not transcreated; the sizing formula has no Rust counterpart and is not re-derived here"]
fn expected_ingest_worker_cnt() {}
