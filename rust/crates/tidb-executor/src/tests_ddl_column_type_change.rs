// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, 2.0 (the "License");
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

//! Ports of the twelve `pkg/ddl/column_type_change_test.go` tests assigned to
//! this batch (origin/master).
//!
//! Every test in the Go file drives a modify-column-type DDL job through its
//! `model.SchemaState`s with `beforeRunOneJobStep` /
//! `afterWaitSchemaSynced` failpoint hooks, observing the hidden changing
//! column (`_Col$_c2_0`), the old column's `PreventNullInsertFlag`, row
//! backfill bytes, rollbacks and cancels. This tier applies MODIFY COLUMN as
//! synchronous metadata (with the type-compatibility refusals of
//! `modify_column_action`) and has no job queue, changing column or backfill,
//! so all twelve are recorded as `#[ignore]`d gaps carrying their re-derived
//! contracts with Go symbol citations.

/// `column_type_change_test.go:42::TestColumnTypeChangeStateBetweenInteger`.
#[test]
#[ignore = "go-parity-gap: per-state meta assertions (changing column _Col$_c2_0, PreventNullInsertFlag) need the CTC schema-state machine (Go column_type_change_test.go:42-107)"]
fn column_type_change_state_between_integer_walks_every_state() {
    // Derivation: t (c1 int, c2 int) with row (1,1); `alter table t modify
    // column c2 tinyint not null`. Hook: at StateNone meta still has 2 cols;
    // at DeleteOnly/WriteOnly/WriteReorganization meta has 3 cols (changing
    // col `_Col$_c2_0` present) and the OLD c2 carries
    // mysql.PreventNullInsertFlag. After completion c2 carries NotNullFlag |
    // NoDefaultValueFlag, type mysql.TypeTiny, ChangeStateInfo nil, and the
    // row is still "1 1".
}

/// `column_type_change_test.go:109::TestRollbackColumnTypeChangeBetweenInteger`.
#[test]
#[ignore = "go-parity-gap: forces job.State = JobStateRollingback at each of the four schema states via beforeRunOneJobStep; no job state machine here (Go column_type_change_test.go:109-149)"]
fn rollback_column_type_change_between_integer_leaves_the_column_unchanged() {
    // Derivation: t (c1 bigint, c2 bigint), row (1,1); `alter table t modify
    // column c2 varchar(16) not null` is rolled back at StateNone /
    // StateDeleteOnly / StateWriteOnly / StateWriteReorganization with error
    // "[ddl:1]MockRollingBackInCallBack-<state>"; after each,
    // assertRollBackedColUnchanged: 2 cols, c2 flag 0, type TypeLonglong,
    // ChangeStateInfo nil, row "1 1".
}

/// `column_type_change_test.go:185::TestColumnTypeChangeIgnoreDisplayLength`
/// (issue #20529).
#[test]
#[ignore = "go-parity-gap: asserts NO StateWriteReorganization visit via beforeRunOneJobStep — the reorg-skip decision of a lossless display-length change (Go column_type_change_test.go:185-214)"]
fn column_type_change_ignore_display_length_skips_the_reorg() {
    // Derivation: t (a tinyint(3)); `alter table t modify column a
    // tinyint(1)` must NOT put the job into StateWriteReorganization
    // (display length shrank but the default flen is unchanged, so the
    // change is not lossy and no backfill is scheduled).
}

/// `column_type_change_test.go:217::TestRowFormat` (issue #21391).
#[test]
#[ignore = "go-parity-gap: reads the backfilled row's encoded bytes from the MVCC store after a forced-reorg modify; backfill and raw MVCC reads are out of this tier (Go column_type_change_test.go:217-237)"]
fn row_format_backfills_rows_in_the_new_format() {
    // Derivation: with disableLossyDDLOptimization = true, t (id int primary
    // key, v varchar(10)) row (1, "123"); `alter table t modify column v
    // varchar(5)`; the MVCC short value of the row key equals exactly
    // [0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x1, 0x0, 0x4, 0x0,
    // 0x7, 0x0, 0x1, '1','2','3','1','2','3'] — CodecVer 0x80, the NEW row
    // format.
}

/// `column_type_change_test.go:239::TestRowFormatWithChecksums`.
#[test]
#[ignore = "go-parity-gap: same MVCC byte-level read with tidb_enable_row_level_checksum = 1; checksummed backfill bytes are out of this tier (Go column_type_change_test.go:239-261)"]
fn row_format_with_checksums_appends_the_checksum() {
    // Derivation: as TestRowFormat but with row-level checksums enabled; the
    // encoded value ends with the 4 checksum bytes
    // [0x2, 0x9e, 0x56, 0xf5, 0x45] appended to the same 0x80-prefixed body.
}

/// `column_type_change_test.go:263::TestRowLevelChecksumWithMultiSchemaChange`.
#[test]
#[ignore = "go-parity-gap: multi-schema-change backfill bytes (added NULL column + forced checksum skip via forceRowLevelChecksumOnUpdateColumnBackfill) are out of this tier (Go column_type_change_test.go:263-293)"]
fn row_level_checksum_with_multi_schema_change_skips_and_fills_null() {
    // Derivation: t (id int primary key, v varchar(10)) row (1,"123");
    // `alter table t add column vv int, modify column v varchar(5)` with the
    // forced-checksum failpoint; encoded value carries a NULL vv slot and
    // checksum bytes [0x2, 0x0, 0x4f, 0xd2, 0x26].
}

/// `column_type_change_test.go:295::TestChangingColOriginDefaultValue`
/// (issue #22395).
#[test]
#[ignore = "go-parity-gap: inserts/updates during WriteOnly/WriteReorganization states hitting the changing column's OriginDefaultValue; needs the state machine (Go column_type_change_test.go:295-368)"]
fn changing_col_origin_default_value_casts_instead_of_defaulting() {
    // Derivation: t (a int, b int not null, unique key(a)) rows (1,1),(2,2);
    // `alter table t modify column b varchar(16) DEFAULT '0' NOT NULL`. In
    // each of three observed rounds the writable column count is 3 and the
    // changing column's OriginDefaultValue is "0"; inserts write the CASTED
    // b value into the changing column (not the default), updates at
    // WriteOnly/WriteReorg cast too. Final `select * from t order by a`:
    // "1 -1", "2 -2", "3 3", "4 4", "5 5".
}

/// `column_type_change_test.go:370::TestChangingColOriginDefaultValueAfterAddColAndCastSucc`.
#[test]
#[ignore = "go-parity-gap: timestamp->date CTC under UTC with origin-default rewriting of an added column; state machine + zone-scoped reorg out of this tier (Go column_type_change_test.go:370-454)"]
fn changing_col_origin_default_value_after_add_col_casts_succ() {
    // Derivation: t (a int, b int not null, unique key(a)) plus c timestamp
    // default '1971-06-09' not null; `alter table t modify column c date NOT
    // NULL` under time_zone UTC. The changing column's OriginDefaultValue is
    // "0000-00-00"; three observed rounds insert (i, i, '2021-06-06
    // 12:13:14') and update b to -1/-2. Final rows: "1 -1 1971-06-09",
    // "2 -2 1971-06-09", "5 5 2021-06-06", "6 6 2021-06-06",
    // "7 7 2021-06-06".
}

/// `column_type_change_test.go:456::TestChangingColOriginDefaultValueAfterAddColAndCastFail`
/// (issue #25383).
#[test]
#[ignore = "go-parity-gap: two sequential CTC jobs with expression defaults and out-of-range datetime origin defaults; state machine out of this tier (Go column_type_change_test.go:456-525)"]
fn changing_col_origin_default_value_after_add_col_cast_fail_keeps_metadata() {
    // Derivation: t (a varchar(31) default 'wwrzfwzb01j6ddj', b decimal(12,0)
    // default -729850476163) plus x char(218) default 'lkittuae';
    // `modify column x datetime null default '3771-02-28 13:00:11' after b`
    // then insert a='1' then `modify column b varchar(256) default
    // (REPLACE(UPPER(UUID()), '-', ''))`. Hook checks 4 writable columns in
    // both jobs and the first job's origin default "3771-02-28 13:00:11"
    // (second: length 32). Final row: "18apf -729850476163 3771-02-28
    // 13:00:11".
}

/// `column_type_change_test.go:527::TestDDLExitWhenCancelMeetPanic` (issue #23202).
#[test]
#[ignore = "go-parity-gap: panic-in-DDL error counting (mockExceedErrorLimit) and history-job ErrorCount are DDL-worker machinery (Go column_type_change_test.go:527-566)"]
fn ddl_exit_when_cancel_meets_panic_counts_errors_to_the_limit() {
    // Derivation: with tidb_ddl_error_count_limit = 3 and
    // mockExceedErrorLimit on, `alter table t drop index b` fails with
    // "[ddl:-1]panic in handling DDL logic and error count beyond the
    // limitation 3, cancelled"; the history job records ErrorCount = 4 and
    // the same error message.
}

/// `column_type_change_test.go:568::TestCancelCTCInReorgStateWillCauseGoroutineLeak`
/// (issue #24584).
#[test]
#[ignore = "go-parity-gap: cancels a job stuck in mockInfiniteReorgLogic and relies on goleak; no reorg workers exist here to leak (Go column_type_change_test.go:568-613)"]
fn cancel_ctc_in_reorg_state_will_not_leak_goroutines() {
    // Derivation: `admin cancel ddl jobs <id>` on the stuck
    // `alter table ctc_goroutine_leak modify column a varchar(16)` returns
    // and the ALTER reports "[ddl:8214]Cancelled DDL job"; TestMain's goleak
    // then proves no reorg goroutine survived.
}

/// `column_type_change_test.go:615::TestCastDateToTimestampInReorgAttribute`
/// (issue #26292).
#[test]
#[ignore = "go-parity-gap: strictness split between SELECT's truncating cast and the reorg attribute's reporting cast during a CTC write; needs the state machine (Go column_type_change_test.go:615-645)"]
fn cast_date_to_timestamp_in_reorg_attribute_reports_instead_of_truncating() {
    // Derivation: t (a date null default '8497-01-06'); during
    // `alter table t modify column a timestamp null default '2021-04-28
    // 03:35:11' first`'s StateWriteOnly, `insert into t set a = '3977-02-22'`
    // and `update t set a = '3977-02-22'` both fail
    // "[types:1292]Incorrect timestamp value: '3977-02-22'" — unlike SELECT,
    // which would truncate silently.
}
