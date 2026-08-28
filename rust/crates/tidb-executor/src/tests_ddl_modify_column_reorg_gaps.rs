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

//! Documented go-parity-gap ports of the `pkg/ddl/modify_column_test.go`
//! tests whose contract is the ONLINE-DDL state machine itself: reorg-info
//! element bookkeeping, mid-backfill failpoint hooks (`beforeRunOneJobStep`,
//! `getModifyColumnType`, `beforeLoadAndDeliverJobs`, ...), region splits,
//! and `admin check table`. This tier applies DDL synchronously to metadata
//! with a synchronous per-row rewrite (`crate::ddl::alter_table`
//! module doc: "the schema-version/DDL-job machinery ... is a separate
//! tier"), so none of those hooks exist to observe. Each port carries the
//! re-derived contract with its Go symbol locations; the assertions run
//! once the job-queue tier exists.

/// Go `modify_column_test.go:53::TestModifyColumnReorgInfo`. Go splits a
/// 16000-row table into 8 regions, fails the backfill through
/// `MockGetIndexRecordErr` (`return("cantDecodeRecordErr")`), and requires
/// `[ddl:8202]Cannot decode index value ...`; the `beforeRunOneJobStep` /
/// `modifyColumnTypeWithData` hooks then assert the reorg snapshot
/// (`checkReorgHandle`) tracks the changing column + its changing indexes
/// (`ddl.BuildElements`) while the job runs and is CLEANED UP after the
/// failure (no stale `mysql.tidb_ddl_reorg` handles survive).
// go-parity-gap: reorg-info element snapshot/cleanup needs the online-DDL
// job queue and its failpoints; this tier has neither.
#[test]
#[ignore]
fn modify_column_reorg_info_snapshot_is_cleaned_up_on_failure() {
}

/// Go `modify_column_test.go:188::TestModifyColumnNullToNotNull`. Go
/// enables `beforeRunOneJobStep` to insert a NULL row and a done-channel
/// block mid-DDL, requires `[ddl:1138]Invalid use of NULL value`, and then
/// verifies a second session can keep inserting/deleting through the
/// write-reorg states. The refusal half (NULL data present) is pinned
/// running in `tests_ddl_modify_column_types`; this port is the
/// concurrency-during-backfill half.
// go-parity-gap: concurrent DML through write-reorg states needs the
// online-DDL job queue; this tier rewrites rows synchronously.
#[test]
#[ignore]
fn modify_column_null_to_not_null_concurrent_dml_during_backfill() {
}

/// Go `modify_column_test.go:240::TestModifyColumnNullToNotNullWithChangingVal`.
/// Same hook set as the sibling test but with a CHANGING column: Go asserts
/// the old column keeps serving reads (`select ... where` over the
/// not-yet-converted rows) while `changingCol` backfills, and that a NULL
/// inserted between the two write states lands in the converted column and
/// is reported as `[ddl:1138]Invalid use of NULL value`.
// go-parity-gap: the changing-column dual-write window needs the online-DDL
// job queue's write-reorg states; this tier rewrites rows in one step.
#[test]
#[ignore]
fn modify_column_null_to_not_null_with_changing_val_dual_write_window() {
}

/// Go `modify_column_test.go:416::TestModifyColumnTime`. Go builds a
/// day-anchored clock table (`timeToDate1..2`, `timeToDatetime1..5`,
/// `timeToTimestamp1..5` from `time.Now().UTC()` truncated to midnight) and
/// drives the time→date/datetime/timestamp conversion matrix through real
/// ALTER reorgs, asserting every converted cell. The conversion rules live
/// in `tidb-datatype`'s `convert_to`; the ALTER-with-reorg delivery and the
/// `timstamp-vs-timezone` storage check Go adds through
/// `@` session time zones are the missing half here.
// go-parity-gap: the time-type conversion matrix is delivered through
// online-DDL reorg backfill; only the metadata-tier rewrite exists.
#[test]
#[ignore]
fn modify_column_time_conversion_matrix_through_reorg() {
}

/// Go `modify_column_test.go:520::TestModifyColumnTypeWhenInterception`. Go
/// inserts `defaultBatchSize * 4` rows so the backfill runs several
/// batches, plants values that produce TRUNCATED warnings
/// (`11.22` into `decimal(4,2)`-family changes), and asserts the warnings
/// accumulate on the session (`show warnings` counts) while the job's
/// `ddl:error-count-limit` interplay (`vardef.GetDDLErrorCountLimit` /
/// `SetDDLErrorCountLimit`) aborts only after the configured count.
// go-parity-gap: batched backfill warning accounting and the DDL error
// count limit need the online-DDL job queue.
#[test]
#[ignore]
fn modify_column_type_when_interception_counts_truncated_warnings() {
}

/// Go `modify_column_test.go:547::TestModifyColumnWithIndexesWriteConflict`.
/// Go enables `disableLossyDDLOptimization` (`return(true)`), creates a
/// table with a composite of secondary indexes over the changed column, and
/// forces a write conflict between the backfill and a concurrent UPDATE so
/// the job retries; the assertion is that the retried backfill converges
/// (`admin check table t` passes) with every index entry rebuilt.
// go-parity-gap: backfill retry after a write conflict needs the online-DDL
// job queue and the lossy-DDL-optimization switch.
#[test]
#[ignore]
fn modify_column_with_indexes_write_conflict_retries_backfill() {
}

/// Go `modify_column_test.go:624::TestModifyColumnWithSkipReorg`. Go runs
/// INT→MEDIUMINT under `afterDoModifyColumnSkipReorgCheck`, inserting
/// `2147483648` mid-check so the post-skip insert VALIDATION fails, then
/// asserts the skip-reorg path still rebuilt the affected indexes and the
/// table (`admin check table t`), with the new type enforced on later
/// inserts.
// go-parity-gap: the skip-reorg check hook and its post-check insert
// validation need the online-DDL job queue.
#[test]
#[ignore]
fn modify_column_with_skip_reorg_enforces_new_type_after_check() {
}

/// Go `modify_column_test.go:686::TestGetModifyColumnType`. Go's table
/// drives the reorg-DECISION function (`pkg/ddl/modify_column.go`
/// `getModifyColumnType`, observed through the `getModifyColumnType`
/// failpoint): int→bigint = `ModifyTypeNoReorg`; bigint→int =
/// `ModifyTypeNoReorgWithCheck` (same with or without an index);
/// signed↔unsigned = `ModifyTypeReorg`; char/varchar widen = NoReorg,
/// shrink = NoReorgWithCheck; char↔varchar with an index is
/// `ModifyTypeIndexReorg` under `_bin` collations but
/// NoReorgWithCheck under `_ci`; cross-collation moves are
/// `ModifyTypeReorg`; and the non-strict `sql_mode` table demotes every
/// NoReorgWithCheck to `ModifyTypeReorg`. The decision function is not a
/// separate symbol in this tier (the rewrite is unconditional), so the
/// decision TABLE has nothing to pin yet.
// go-parity-gap: getModifyColumnType's reorg-type decision function is not
// transcreated; this tier always rewrites synchronously.
#[test]
#[ignore]
fn get_modify_column_type_reorg_decision_table() {
}

/// Go `modify_column_test.go:912::TestMultiSchemaModifyColumnWithIndex`. Go
/// modifies two indexed columns in one multi-schema ALTER and asserts every
/// index keeps its name, column list, and per-part offsets with unchanged
/// index IDs. The identity preservation of single-column MODIFYs is pinned
/// running in `tests_ddl_modify_column_types`
/// (`multi_schema_modify_column_positions_keep_column_identity`); the
/// multi-schema job's index-ID stability across two sub-jobs is this half.
// go-parity-gap: multi-schema sub-job index-ID stability needs the online-DDL
// job queue.
#[test]
#[ignore]
fn multi_schema_modify_column_with_index_keeps_index_identity() {
}

/// Go `modify_column_test.go:944::TestParallelAlterTable`. Go blocks job
/// scheduling behind `beforeLoadAndDeliverJobs`, submits two conflicting
/// ALTERs (e.g. two MODIFYs of one column), releases the queue, and asserts
/// exactly one succeeds and the loser answers a duplicate/conflict error —
/// the serial-ownership guarantee of the job queue.
// go-parity-gap: conflicting-ALTER serialization is a job-queue property;
// this tier applies DDL statements synchronously in arrival order.
#[test]
#[ignore]
fn parallel_alter_table_conflicts_serialize_through_the_job_queue() {
}

/// Go `modify_column_test.go:1302::TestModifyColumnWithDifferentCollation`.
/// Go walks the 6x6 matrix of char/varchar(32)→(23) across
/// utf8mb4_bin/utf8_unicode_ci/utf8mb4_general_ci (36 subtests, same-index
/// pairs skipped), inserting rows and mutating data behind
/// `beforeRunOneJobStep` to prove the reorg's insert/delete consistency
/// check tolerates concurrent writes, finishing with
/// `admin check table t1`. Collation-aware index rebuilds during backfill
/// are the online-DDL half.
// go-parity-gap: collation-changing reorg backfill with concurrent-DML
// consistency checks needs the online-DDL job queue.
#[test]
#[ignore]
fn modify_column_with_different_collation_rebuilds_index_entries() {
}

/// Go `modify_column_test.go:1367::TestStatsAfterModifyColumn`. Go modifies
/// indexed columns with `tidb_analyze_skip_column_types`-style options and
/// embedded analyze, then asserts the column/index statistics
/// (`mysql.stats_buckets` row counts, `show stats_meta`) survive the
/// reorg with the modified column's stats re-collected. Statistics are a
/// separate tier here (`show_stats.rs` reads what analyze produced; no
/// analyze runs inside DDL).
// go-parity-gap: embedded-analyze-after-modify-column needs the statistics
// tier this workspace keeps out of the DDL path.
#[test]
#[ignore]
fn stats_after_modify_column_are_recollected() {
}

/// Go `modify_column_test.go:1494::TestModifyColumnLoadTableRangeError`. Go
/// plants `loadTableRangesFromPDErr` (`1*return("All returned regions have
/// no leaders, limit: 1")`) so the FIRST backfill range load fails, and
/// requires the retried job to succeed (`alter table t change column b b
/// varchar(16)` completes; `admin check table t` passes) — the transient
/// PD-error retry path of `loadTableRanges`.
// go-parity-gap: loadTableRanges' PD-retry path needs the online-DDL job
// queue and a region-split store.
#[test]
#[ignore]
fn modify_column_load_table_range_error_is_retried() {
}
