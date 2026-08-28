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

//! Port of Go `pkg/ddl/column_type_change_test.go` (part2 slice: all twelve
//! `TestXxx` functions, lines 42–615).
//!
//! Every test in this file exercises the "changing column" machinery
//! (`_Col$_<col>_<n>` shadow columns, `ChangeStateInfo`, origin-default
//! backfill): a failpoint hook (`beforeRunOneJobStep` /
//! `afterWaitSchemaSynced`) holds a `MODIFY COLUMN` job in a named
//! intermediate schema state and observes DML and raw encoded row values
//! against that intermediate schema. This crate applies a modify column
//! synchronously with no changing-column phase (documented in `crate::ddl`'s
//! module header), so each Go test is recorded as an ignored parity gap while
//! keeping its recipe here.

/// `pkg/ddl/column_type_change_test.go::TestColumnTypeChangeStateBetweenInteger`
/// (line 42): while `ALTER TABLE t modify column c2 tinyint not null` walks
/// None→DeleteOnly→WriteOnly→WriteReorg, the intermediate metadata carries a
/// third (changing) column `_Col$_c2_0`, the old column carries
/// `PreventNullInsertFlag`, and afterwards `c2` is `mysql.TypeTiny` with the
/// NOT NULL flag, no `ChangeStateInfo`, and the row survives unchanged.
// go-parity-gap: the changing-column state walk and its per-state metadata
// (`_Col$_` columns, PreventNullInsertFlag) are not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: changing-column intermediate states are not modeled in this crate"]
fn column_type_change_state_between_integer() {}

/// `pkg/ddl/column_type_change_test.go::TestRollbackColumnTypeChangeBetweenInteger`
/// (line 109): a `modify column c2 varchar(16) not null` job force-rolled
/// back at EACH of None/DeleteOnly/WriteOnly/WriteReorganization leaves the
/// column's flag, type (`mysql.TypeLonglong`), `ChangeStateInfo`, and the row
/// data byte-identical (`MockRollingBackInCallBack-<state>` errors).
// go-parity-gap: job rollback from injected intermediate-state errors is not
// modeled in this crate.
#[test]
#[ignore = "go-parity-gap: changing-column job rollback from injected state errors is not modeled"]
fn rollback_column_type_change_between_integer() {}

/// `pkg/ddl/column_type_change_test.go::TestColumnTypeChangeIgnoreDisplayLength`
/// (line 185, issue #20529): `alter table t modify column a tinyint(1)` on a
/// `tinyint(3)` column must NOT enter write-reorganization (display-length
/// changes alone never reorg; the default flen is unchanged).
// go-parity-gap: the observable is the absence of a write-reorg phase, which
// only exists in the online state machine.
#[test]
#[ignore = "go-parity-gap: observing the absent write-reorg phase needs the online state machine"]
fn column_type_change_ignores_display_length() {}

/// `pkg/ddl/column_type_change_test.go::TestRowFormat` (line 217, issue
/// #21391): with `disableLossyDDLOptimization`, a forced-reorg `modify column
/// v varchar(5)` rewrites the stored row so the encoded value starts with the
/// NEW row codec (`CodecVer = 128`), byte-for-byte:
/// `[0x80, 0x0, 0x3, ..., 0x31, 0x32, 0x33, 0x31, 0x32, 0x33]`.
// go-parity-gap: pins raw MVCC ShortValue bytes after a forced reorg; this
// crate performs no lossy-DDL reorg and exposes no MVCC per-key history.
#[test]
#[ignore = "go-parity-gap: forced-lossy reorg row rewriting is not modeled in this crate"]
fn row_format_tracks_the_new_row_codec() {}

/// `pkg/ddl/column_type_change_test.go::TestRowFormatWithChecksums` (line
/// 239): the same forced reorg with `tidb_enable_row_level_checksum`
/// produces the checksummed row value (codec ver 0x80|0x2, trailing 5-byte
/// checksum) byte-for-byte.
// go-parity-gap: row-level checksums on rewritten rows are not modeled in
// this crate.
#[test]
#[ignore = "go-parity-gap: row-level checksums on reorg-rewritten rows are not modeled"]
fn row_format_with_checksums() {}

/// `pkg/ddl/column_type_change_test.go::TestRowLevelChecksumWithMultiSchemaChange`
/// (line 263): a multi-schema change (`add column vv int, modify column v
/// varchar(5)`) with the forced-update-column-backfill failpoint writes the
/// checksummed row with the null `vv` column and a skipped checksum.
// go-parity-gap: multi-schema-change backfill with row checksums is not
// modeled in this crate.
#[test]
#[ignore = "go-parity-gap: multi-schema-change backfill with row checksums is not modeled"]
fn row_level_checksum_with_multi_schema_change() {}

/// `pkg/ddl/column_type_change_test.go::TestChangingColOriginDefaultValue`
/// (line 295, issue #22395): `modify column b varchar(16) DEFAULT '0' NOT
/// NULL` gives the changing column the CAST origin default `'0'`; DML run in
/// write-only/write-reorg states inserts rows whose cast value reaches the
/// changing column, and the final rows read `1 -1`, `2 -2`, `3 3`, `4 4`,
/// `5 5`.
// go-parity-gap: changing-column origin defaults with state-interleaved DML
// are not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: changing-column origin defaults with state-interleaved DML are not modeled"]
fn changing_col_origin_default_value() {}

/// `pkg/ddl/column_type_change_test.go::TestChangingColOriginDefaultValueAfterAddColAndCastSucc`
/// (line 370): after `add column c timestamp default '1971-06-09' not null`,
/// the change to `date NOT NULL` computes the changing column's origin
/// default as `0000-00-00` in UTC, and state-interleaved inserts/updates land
/// `1 -1 1971-06-09`, `2 -2 1971-06-09`, then three `2021-06-06` rows.
// go-parity-gap: changing-column origin defaults with state-interleaved DML
// are not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: changing-column origin defaults with state-interleaved DML are not modeled"]
fn changing_col_origin_default_value_after_add_col_and_cast_succ() {}

/// `pkg/ddl/column_type_change_test.go::TestChangingColOriginDefaultValueAfterAddColAndCastFail`
/// (line 456, issue #25383): two sequential change jobs (x→DATETIME with a
/// far-future default `3771-02-28 13:00:11`, then b→`varchar(256)` with an
/// expression default) each carry the right origin default on their changing
/// column while updates interleave, and the final row reads
/// `18apf -729850476163 3771-02-28 13:00:11`.
// go-parity-gap: changing-column origin defaults with state-interleaved DML
// are not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: changing-column origin defaults with state-interleaved DML are not modeled"]
fn changing_col_origin_default_value_after_add_col_and_cast_fail() {}

/// `pkg/ddl/column_type_change_test.go::TestDDLExitWhenCancelMeetPanic`
/// (line 527, issue #23202): a DROP INDEX job whose write-reorg phase panics
/// (mockExceedErrorLimit) is retried, exhausts `tidb_ddl_error_count_limit=3`
/// (ErrorCount reaches 4), fails the statement with
/// `[ddl:-1]panic in handling DDL logic and error count beyond the limitation
/// 3, cancelled`, and the history job records both.
// go-parity-gap: DDL job error counters, panic recovery, and history jobs
// are not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: DDL job error-count/panic lifecycle is not modeled in this crate"]
fn ddl_exit_when_cancel_meets_panic() {}

/// `pkg/ddl/column_type_change_test.go::TestCancelCTCInReorgStateWillCauseGoroutineLeak`
/// (line 568, issue #24584): cancelling a modify-column job stuck in an
/// infinite reorg loop (`admin cancel ddl jobs <id>`) fails the statement
/// with `[ddl:8214]Cancelled DDL job` and must not leak the reorg goroutine.
// go-parity-gap: cancellable background reorg goroutines are not modeled in
// this crate.
#[test]
#[ignore = "go-parity-gap: cancellable background reorg workers are not modeled in this crate"]
fn cancel_ctc_in_reorg_state_will_cause_goroutine_leak() {}

/// `pkg/ddl/column_type_change_test.go::TestCastDateToTimestampInReorgAttribute`
/// (line 615, issue #26292): during the change `a DATE → TIMESTAMP`, DML in
/// the write-only state writing `'3977-02-22'` must report
/// `[types:1292]Incorrect timestamp value: '3977-02-22'` — the reorg/casting
/// path is STRICTER than a SELECT cast, which would silently truncate.
// go-parity-gap: the reorg-attribute cast path with state-interleaved DML is
// not modeled in this crate.
#[test]
#[ignore = "go-parity-gap: the reorg-attribute cast error path is not modeled in this crate"]
fn cast_date_to_timestamp_in_reorg_attribute() {}
