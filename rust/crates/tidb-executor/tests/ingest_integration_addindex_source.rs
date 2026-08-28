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

//! Port ledger for `pkg/ddl/ingest/integration_test.go` (pkg/ddl.part7 items
//! 369-392 of the local enumeration: every `func Test*` in that file, lines
//! 41-945). All twenty-four are mockstore SQL tests that drive
//! `ALTER TABLE ... ADD INDEX` through the lightning ingest backfill with
//! failpoint injections; the crate's DDL tier applies metadata directly and
//! has no ingest/backfill machinery, so every one is a documentary gap port
//! whose contract is re-derived from the Go body it asserts.

/// GO PORT of `pkg/ddl/ingest/integration_test.go:41
/// TestAddIndexIngestGeneratedColumns`.
///
/// Re-derived contract: indexes over stored/virtual generated columns
/// (single, composite, prefixed, and over generated-of-generated, on
/// clustered PK tables) are backfilled through ingest; `admin check table`
/// passes, the plain SELECT shows the computed values, and the last N DDL
/// jobs all report the `ingest` job type.
#[test]
#[ignore = "go-parity-gap: needs the ingest add-index backfill (pkg/ddl/ingest) and admin show ddl jobs surfaces, none transcreated"]
fn add_index_ingest_backfills_indexed_generated_columns() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:92 TestIngestError`.
///
/// Re-derived contract: a cop-sender error (`mockCopSenderError`) or a local
/// writer error (`mockLocalWriterError`) during ingest add-index makes the
/// job fall back internally yet still finish usable — `admin check table`
/// passes and the job still reports the `ingest` type.
#[test]
#[ignore = "go-parity-gap: needs ingest error-injection failpoints (mockCopSenderError/mockLocalWriterError) and the ingest job-type reporting surface"]
fn ingest_error_falls_back_and_still_reports_the_ingest_job_type() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:143 TestAddIndexIngestPanic`.
///
/// Re-derived contract: a panic in the scan-record operator or in the local
/// engine writer surfaces as `[ddl:%d]ErrReorgPanic` from the ALTER, not a
/// crash (both subtests require errno.ErrReorgPanic).
#[test]
#[ignore = "go-parity-gap: needs the ingest scan-record/writer panic failpoints and reorg-panic error mapping, none transcreated"]
fn add_index_ingest_maps_panics_to_reorg_panic_error() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:173
/// TestAddIndexSetInternalSessions`.
///
/// Re-derived contract: the one internal transaction the ingest backfill
/// opens (`wrapInBeginRollbackStartTS` start TS) is registered with the
/// session manager, i.e. `GetInternalSessionStartTSList` contains it when
/// observed from `scanRecordExec`.
#[test]
#[ignore = "go-parity-gap: needs the ingest internal-session registration and session-manager start-TS list surfaces"]
fn add_index_registers_internal_sessions_in_the_session_manager() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:202 TestAddIndexIngestCancel`.
///
/// Re-derived contract: cancelling an add-index job during
/// write-reorganization (BackfillStateRunning) ends it with
/// ErrCancelledDDLJob and leaves `ingest.LitDiskRoot` usage at zero.
#[test]
#[ignore = "go-parity-gap: needs admin cancel ddl jobs plus the ingest disk-root accounting, none transcreated"]
fn add_index_ingest_cancel_during_reorg_leaves_no_disk_usage() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:236
/// TestAddIndexGetChunkCancel`.
///
/// Re-derived contract: a cancel issued inside `beforeGetChunk` still ends
/// the job with ErrCancelledDDLJob and the table stays admin-check clean.
#[test]
#[ignore = "go-parity-gap: needs the beforeGetChunk failpoint and admin-cancel machinery"]
fn add_index_cancel_during_get_chunk_keeps_the_table_consistent() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:266
/// TestIngestPartitionRowCount`.
///
/// Re-derived contract: adding an index over a generated column on a
/// range-partitioned table reports row_count 3 in `admin show ddl jobs` —
/// rows from all partitions count toward the job's row counter.
#[test]
#[ignore = "go-parity-gap: needs the ingest job row-count reporting across partitions"]
fn ingest_partition_row_count_counts_rows_from_all_partitions() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:287
/// TestAddIndexIngestClientError`.
///
/// Re-derived contract: `create index ... ((cast(f1 as unsigned array)))` on
/// a JSON column holding invalid JSON fails with
/// ErrInvalidJSONValueForFuncIndex during the index-build read.
#[test]
#[ignore = "go-parity-gap: needs the multivalued/functionaI index ingest read path and its client-error mapping"]
fn add_index_client_error_surfaces_invalid_json_func_index_value() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:298
/// TestAddIndexCancelOnNoneState`.
///
/// Re-derived contract: cancelling the add-index job while it is still in
/// SchemaStateNone ends it ErrCancelledDDLJob before any backfill work, and
/// the ingest disk root stays at zero usage.
#[test]
#[ignore = "go-parity-gap: needs admin cancel at StateNone plus ingest disk-root accounting"]
fn add_index_cancel_on_none_state_leaves_no_disk_usage() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:321
/// TestAddIndexIngestTimezone`.
///
/// Re-derived contract: a timestamp-column index built under session zone
/// `-06:00` (including a DST-gap repeat) and rebuilt under
/// `Asia/Shanghai` both leave the table admin-check clean — the ingest
/// backfill encodes timestamps with the session timezone.
#[test]
#[ignore = "go-parity-gap: needs the ingest backfill's session-timezone timestamp encoding"]
fn add_index_ingest_indexes_timestamps_across_session_timezones() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:343
/// TestAddIndexIngestMultiSchemaChange`.
///
/// Re-derived contract: multi-schema changes mixing several add/drop index
/// actions (plain and unique, also over generated columns on a partitioned
/// table) backfill all indexes and leave the table admin-check clean.
#[test]
#[ignore = "go-parity-gap: needs multi-schema-change index backfill through ingest"]
fn add_index_ingest_multi_schema_change_builds_every_index() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:374
/// TestAddIndexDuplicateMessage`.
///
/// Re-derived contract: when a concurrent DML inserts a duplicate of a key
/// already written by the backfill writer (`afterMockWriterWriteRow`), the
/// ALTER fails with `[kv:1062]Duplicate entry '1' for key 't.idx'`, the
/// racing DML itself succeeds, and the final table content shows both rows.
#[test]
#[ignore = "go-parity-gap: needs the ingest duplicate-entry detection across backfill and DML writes"]
fn add_index_duplicate_message_reports_duplicate_entry_key() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:400
/// TestMultiSchemaAddIndexMerge`.
///
/// Re-derived contract: concurrent DML admitted between the two index
/// builds of a multi-schema add-index job (plain and hash-partitioned
/// tables) is captured by the second index — the statement succeeds and the
/// table stays admin-check clean.
#[test]
#[ignore = "go-parity-gap: needs the multi-schema merge backfill and MockExecAfterWriteRow hook"]
fn multi_schema_add_index_merge_captures_concurrent_writes() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:437
/// TestAddIndexIngestJobWriteConflict`.
///
/// Re-derived contract: a processing-flag write conflict on the job's
/// `mysql.tidb_ddl_job` row retries only the conflicted transaction, not the
/// whole backfill — the writer hook counts exactly 3 row writes (not 6).
#[test]
#[ignore = "go-parity-gap: needs the ingest job-table retry semantics and onMockWriterWriteRow counting"]
fn add_index_ingest_job_write_conflict_does_not_retry_whole_job() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:473
/// TestAddIndexIngestPartitionCheckpoint`.
///
/// Re-derived contract: when a write conflict interrupts the backfill of a
/// hash-partitioned table at row 10, the resume continues from the correct
/// partition checkpoint so exactly 20 total rows are written (no re-writes).
#[test]
#[ignore = "go-parity-gap: needs the ingest per-partition checkpoint resume"]
fn add_index_ingest_partition_checkpoint_resumes_at_the_right_partition() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:514
/// TestAddGlobalIndexInIngest`.
///
/// Re-derived contract: adding a mix of plain and UNIQUE GLOBAL indexes on a
/// hash-partitioned table (in one multi-schema change, then in further
/// batches, unique and non-unique) through ingest keeps every index read
/// equal to the table read even while `writeLocalExec` admits new rows
/// during the build.
#[test]
#[ignore = "go-parity-gap: needs the global-index ingest backfill and use-index read routing"]
fn add_global_index_in_ingest_keeps_index_reads_equal_to_table_reads() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:569
/// TestAddGlobalIndexInIngestWithUpdate`.
///
/// Re-derived contract: rows updated while a UNIQUE GLOBAL index is being
/// backfilled remain visible identically through the index and the table
/// (compared with `_tidb_rowid` projection) after the ALTER completes.
#[test]
#[ignore = "go-parity-gap: needs the global-index ingest backfill under concurrent updates"]
fn add_global_index_in_ingest_with_update_keeps_index_consistent() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:598
/// TestAddIndexValidateRangesFailed`.
///
/// Re-derived contract: `validateAndFillRangesErr` failing twice (retryable,
/// `loadTableRangesNoRetry` armed) does not wedge the job — the ALTER
/// completes and the table stays admin-check clean.
#[test]
#[ignore = "go-parity-gap: needs the validate-and-fill-ranges retry path of the ingest backfill"]
fn add_index_validate_ranges_failed_retries_without_hanging() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:612
/// TestIndexChangeWithModifyColumn`.
///
/// Re-derived contract: while an index over a
/// `utf8mb4_unicode_ci` varchar is being built, a concurrent
/// `modify column ... collate utf8mb4_general_ci` fails with an error
/// containing "when index is defined" — collation change under an existing
/// index build is rejected.
#[test]
#[ignore = "go-parity-gap: needs the concurrent modify-column vs index-build mutual exclusion"]
fn index_change_with_modify_column_rejects_collation_change_under_index() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:652
/// TestModifyColumnWithMultipleIndex`.
///
/// Re-derived contract: `alter table t modify a bit(5) not null` on a table
/// holding four indexes over `a` (plain, unique, composite two- and
/// three-column) succeeds under txn, local-ingest and dxf-ingest modes and
/// the table stays admin-check clean — every index entry is rewritten.
#[test]
#[ignore = "go-parity-gap: needs the modify-column index rewrite across all three backfill executors"]
fn modify_column_with_multiple_index_rewrites_every_index_entry() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:701
/// TestCheckpointInstanceAddrValidation`.
///
/// Re-derived contract (issues #43983/#43957): `ingest.InstanceAddr()` is
/// `AdvertiseAddress:Port:TempDir` — it ends with `":"+TempDir`, its
/// host:port prefix carries the configured port and advertise address — and
/// the checkpoint mechanism is exercised during a plain ingest add-index.
#[test]
#[ignore = "go-parity-gap: no Rust carrier for ingest InstanceAddr (pkg/ddl/ingest/checkpoint.go:287-291) or the checkpoint identity validation"]
fn checkpoint_instance_addr_validation_uses_advertise_address_plus_tempdir() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:755
/// TestCheckpointPhysicalIDValidation`.
///
/// Re-derived contract: the `physical_id` persisted in `mysql.tidb_ddl_reorg`
/// during a partitioned add-index always names a real partition of the
/// table (queried from information_schema.partitions), so a stale
/// checkpoint from a dropped/recreated partition can never be trusted.
#[test]
#[ignore = "go-parity-gap: needs the reorg-table physical_id persistence and information_schema.partitions surface"]
fn checkpoint_physical_id_validation_matches_real_partition_ids() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:836
/// TestAddIndexWithEmptyPartitions`.
///
/// Re-derived contract: a range-partitioned table where p1 and p3 hold no
/// rows still walks every partition during ingest add-index — the reorg
/// info advances through at least 3 partition switches, every observed
/// physical_id is a valid partition ID (empty ones included), index and
/// table counts agree at 20, and admin check passes.
#[test]
#[ignore = "go-parity-gap: needs the per-partition reorg-info advance over empty partitions"]
fn add_index_with_empty_partitions_still_walks_every_partition() {}

/// GO PORT of `pkg/ddl/ingest/integration_test.go:945
/// TestModifyColumnWithIndexWithDefaultValue`.
///
/// Re-derived contract: a datetime column whose default is the expression
/// `date_format(now(),'%Y-%m-%d')` keeps working across `add index`,
/// a modify to varchar default 'xx', and a modify back to the expression
/// default, with the index present throughout — expression defaults survive
/// modify-column rewrites under an index.
#[test]
#[ignore = "go-parity-gap: needs the modify-column default-expression rewrite under an index in all three executors"]
fn modify_column_with_index_keeps_expression_default_values() {}
