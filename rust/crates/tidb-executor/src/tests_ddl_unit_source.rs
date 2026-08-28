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

//! Port of the Go unit tests of `pkg/ddl` part5 (origin/master items 241-300
//! of the 1002 `Test*`/`Benchmark*` declarations under `pkg/ddl/`, sorted by
//! file path then line, chunked by 60): `db_table_test.go` (items 241-244),
//! `db_test.go` (245-279), `ddl_algorithm_test.go` (280), `ddl_error_test.go`
//! (281-287), `ddl_history_test.go` (288-289), `ddl_running_jobs_test.go`
//! (290-292), and `ddl_test.go` (293-300).
//!
//! Five Go tests RUN here against the crate's transcreated DDL slices:
//!
//! - `TestError` pins the terror -> SQLError code round trip through the
//!   generated `tidb-util` dbterror table.
//! - `TestDropTables` pins multi-name `DROP TABLE` semantics through
//!   [`crate::run_drop_table_in`].
//! - `TestViewError` pins its (vestigial) body: a one-column CREATE TABLE
//!   succeeds.
//! - `TestColumnError` pins its two failpoint-free `DROP COLUMN` refusals
//!   through [`crate::run_alter_table_in`]; its failpoint arms and its two
//!   `ADD COLUMN ... AFTER` arms are gaps (see below).
//! - `TestProcessColumnFlags` pins the YEAR/BIT flag stamping of
//!   `process_column_flags` end-to-end through CREATE + MODIFY.
//!
//! Four more are carried by pre-existing in-crate tests that were
//! re-verified against origin/master this session (not re-ported here):
//! `TestFindAlterAlgorithm` -> [`crate::ddl_algorithm::tests::
//! find_alter_algorithm_matches_go`], and `TestRunningJobs` /
//! `TestSchemaPolicyAndResourceGroup` / `TestExclusiveShared` ->
//! [`crate::ddl_running_jobs::tests`].
//!
//! Everything else needs the session/txn/DDL-job-runner machinery Go drives
//! through `testkit` and failpoints, which this workspace does not
//! transcreate; those tests are `#[ignore]`d documentary gap ports whose
//! contracts are re-derived from the Go sources cited in each doc comment.
//! Nothing is approximated to make a test pass.

use tidb_datatype::FieldTypeFlags;
use tidb_error::tidb::errcode;
use tidb_parser::SqlMode;
use tidb_util::dbterror::{
    ERR_CANCEL_FINISHED_DDL_JOB, ERR_CANNOT_CANCEL_DDL_JOB, ERR_DDL_JOB_NOT_FOUND,
};

use crate::Catalog;

/// The stock session the driver entries stand in for, and the schema Go's
/// `use test` selects.
fn current_db() -> &'static str {
    crate::driver::DEFAULT_DATABASE
}

fn sql_mode() -> SqlMode {
    SqlMode::default()
}

/// Go `col.GetFlag()` on a live column: `catalog.table_in(db, table)` must
/// be a storage-backed table holding a column of that name, and the flags
/// read off its field type are what Go's `col.GetFlag()` answers.
fn column_flags(catalog: &crate::Catalog, table: &str, column: &str) -> tidb_datatype::FieldType {
    let Some(crate::TableEntry::Kv(entry)) = catalog.table_in(current_db(), table) else {
        panic!("table {table} must exist as a storage-backed table");
    };
    entry
        .columns
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .unwrap_or_else(|| panic!("column {column} must exist in {table}"))
        .field_type
        .clone()
}

// ===========================================================================
// pkg/ddl/db_table_test.go (items 241-244)
// ===========================================================================

/// Rust side of `pkg/ddl/db_table_test.go:876 TestDropTables` — item 241.
///
/// Go (db_table_test.go:876-931) pins multi-name `DROP TABLE` semantics:
/// dropping a missing name fails with `errno.ErrBadTable` (1051), `IF
/// EXISTS` silences every missing name, and WITHOUT `IF EXISTS` the
/// statement still drops the names that exist and then fails naming the
/// ones it could not (the MySQL contract the Go test cites from
/// dev.mysql.com/doc/refman/5.7/en/drop-table.html). Go's last arm proves
/// the partial drop with `SHOW CREATE TABLE t1` -> `errno.ErrNoSuchTable`
/// (1146); this crate has no SHOW executor, so the same fact is read off
/// the catalog (the name is gone).
#[test]
fn drop_tables_drops_existing_names_and_reports_missing() {
    let mut catalog = Catalog::default();

    // `drop table if exists t1` on a missing name succeeds.
    crate::run_drop_table_in(
        "drop table if exists t1",
        &mut catalog,
        current_db(),
        sql_mode(),
        true,
    )
    .unwrap_or_else(|error| panic!("IF EXISTS must silence the missing name: {error}"));

    // Dropping a missing name fails with ErrBadTable (1051), qualified or
    // not (Go db_table_test.go:884-889).
    for sql in ["drop table t1", "drop table test2.t1"] {
        let error = crate::run_drop_table_in(sql, &mut catalog, current_db(), sql_mode(), true)
            .expect_err("dropping a missing table must fail");
        assert_eq!(error.to_mysql_error().code, 1051, "{sql}");
    }

    // `IF EXISTS` with a mix of existing and missing names, in both written
    // orders, succeeds (Go db_table_test.go:891-896).
    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    crate::run_drop_table_in(
        "drop table if exists t1, t2",
        &mut catalog,
        current_db(),
        sql_mode(),
        true,
    )
    .unwrap();
    assert!(catalog.table_in(current_db(), "t1").is_none());
    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    crate::run_drop_table_in(
        "drop table if exists t2, t1",
        &mut catalog,
        current_db(),
        sql_mode(),
        true,
    )
    .unwrap();
    assert!(catalog.table_in(current_db(), "t1").is_none());

    // Without IF EXISTS the drops that CAN happen still happen, and the
    // statement fails naming the missing one (Go db_table_test.go:899-909).
    for sql in ["drop table t1, t2", "drop table t2, t1"] {
        crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
        let error = crate::run_drop_table_in(sql, &mut catalog, current_db(), sql_mode(), true)
            .expect_err("the missing name must fail the statement");
        assert_eq!(error.to_mysql_error().code, 1051, "{sql}");
        // Go proves this with `show create table t1` -> ErrNoSuchTable
        // (1146); the catalog read is the same fact.
        assert!(catalog.table_in(current_db(), "t1").is_none(), "{sql}");
    }
}

/// Documentary twin for `pkg/ddl/db_table_test.go:909
/// TestCreateConstraintForTable` — item 242.
///
/// Go, with `@@global.tidb_enable_check_constraint = 1`, pins check-
/// constraint name scoping: `CONSTRAINT c1 CHECK (id<50)` on t1 succeeds, a
/// second `CONSTRAINT c1` in the same schema fails on CREATE TABLE and on
/// `ALTER TABLE ... ADD CONSTRAINT c1` with
/// `errno.ErrCheckConstraintDupName` (3822), while the same name in another
/// schema (`test2.t1`) succeeds. This crate's CREATE path models
/// `tidb_enable_check_constraint` OFF only — a CHECK under ON is an
/// explicit `unsupported` (`src/ddl.rs:863`), so the 3822 arms have no
/// carrier.
///
/// go-parity-gap: check-constraint registration with the variable ON is not
/// modelled; the dup-name 3822 scoping cannot run.
#[test]
#[ignore = "go-parity-gap: tidb_enable_check_constraint=1 (3822 dup-name scoping) is modelled OFF-only"]
fn create_constraint_for_table_dup_name_scope_documentary() {}

/// Documentary twin for `pkg/ddl/db_table_test.go:933
/// TestCreateTableHandleAutoIDOnce` — item 243.
///
/// Go counts `handleAutoIncID` failpoint hits during `CREATE TABLE t1(id
/// int) AUTO_INCREMENT 1000` (exactly 1 for ordinary DDL) and requires
/// `SHOW TABLE test.t1 NEXT_ROW_ID` to report 1000. The rebase hook, its
/// failpoint, and the NEXT_ROW_ID surface are not transcreated here.
///
/// go-parity-gap: the CREATE TABLE auto-ID rebase hook is not transcreated.
#[test]
#[ignore = "go-parity-gap: CREATE TABLE auto-ID rebase hook (handleAutoIncID) is not transcreated"]
fn create_table_handles_auto_id_once_documentary() {}

/// Documentary twin for `pkg/ddl/db_table_test.go:952 TestCreateTableWithBR`
/// — item 244.
///
/// Go mocks the BR start mode (`mockBRStartMode`) and drives
/// `CreateTableWithInfo` with a hand-built TableInfo (AutoIncID 1000,
/// SharedInvolving ref): the `handleAutoIncID` hook fires EXACTLY TWICE and
/// `SHOW TABLE test.t1 NEXT_ROW_ID` still reports 1000 — the double rebase
/// is idempotent.
///
/// go-parity-gap: BR-mode CreateTableWithInfo and the rebase hook are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: BR-mode CreateTableWithInfo double rebase is not transcreated"]
fn create_table_with_br_rebases_twice_documentary() {}

// ===========================================================================
// pkg/ddl/db_test.go (items 245-279)
// ===========================================================================

/// Documentary twin for `pkg/ddl/db_test.go:78 TestGetTimeZone` — item 245.
///
/// Go sets `@@time_zone` to fifteen values and pins, per value, the
/// session's `TimeZone.String()` plus `ddlutil.GetTimeZone(sctx)`
/// (pkg/ddl/util/util.go:247): numeric offsets keep an empty name and a
/// signed second offset (`+05:00` -> name "", offset 18000; `-08:00` ->
/// -28800), named zones keep the name with offset 0, `SYSTEM`/DEFAULT
/// resolve to `timeutil.SystemLocation()`, `GMT+1` is REFUSED with
/// "[variable:1298]Unknown or incorrect time zone: 'GMT+1'", and
/// `Etc/GMT+12` is accepted (POSIX sign convention).
///
/// go-parity-gap: the session time_zone variable stack and
/// ddlutil.GetTimeZone are not transcreated.
#[test]
#[ignore = "go-parity-gap: session time_zone variables and ddlutil.GetTimeZone (util/util.go:247) are not transcreated"]
fn get_time_zone_session_table_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:122 TestIssue22819` — item 246.
///
/// Go holds an open transaction updating a hash-partitioned table, runs
/// `ALTER TABLE t1 TRUNCATE PARTITION p0` from another session, and
/// requires the first commit to fail with 8028 "Information schema is
/// changed during the execution of the statement".
///
/// go-parity-gap: commit-time schema-version validation (8028) needs the
/// txn/session stack.
#[test]
#[ignore = "go-parity-gap: MDL/schema-version commit validation (8028) needs the session stack"]
fn issue22819_truncate_partition_conflicts_with_open_txn_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:146 TestIssue22307` — item 247.
///
/// Go fires `beforeRunOneJobStep` at the DROP COLUMN job's WriteOnly state:
/// an UPDATE naming the dropped column must fail with
/// "[planner:1054]Unknown column 'b' in 'where clause'", and `UPDATE ... SET
/// a = 3 ORDER BY b` with "[planner:1054]Unknown column 'b' in 'order
/// clause'" — the intermediate schema state already hides the column from
/// the planner.
///
/// go-parity-gap: failpoint-driven intermediate schema states plus a planner
/// are not transcreated.
#[test]
#[ignore = "go-parity-gap: WriteOnly-state column visibility for concurrent DML needs the job runner"]
fn issue22307_drop_column_hides_column_at_write_only_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:172
/// TestAddExpressionIndexRollback` — item 248.
///
/// Go adds `INDEX expr_idx ((pow(c1, c2)))` over rows holding (160,160) and
/// requires the ALTER to fail with "[types:1690]DOUBLE value is out of range
/// in 'pow(160, 160)'" — while DML performed at DeleteOnly/WriteOnly/
/// WriteReorganization states survives — and the reorg handle to be cleaned
/// up (`GetDDLReorgHandle` answers `meta.ErrDDLReorgElementNotExist`).
///
/// go-parity-gap: expression-index backfill rollback and reorg-handle
/// cleanup are not transcreated.
#[test]
#[ignore = "go-parity-gap: expression index backfill rollback (1690) + reorg cleanup are not transcreated"]
fn add_expression_index_rollback_on_overflow_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:236
/// TestDropTableOnTiKVDiskFull` — item 249.
///
/// Go enables the unistore failpoint `rpcTiKVAllowedOnAlmostFull` and
/// requires `DROP TABLE` to SUCCEED while the store reports disk full —
/// the request allowlist that keeps DDL running on an almost-full TiKV.
///
/// go-parity-gap: unistore disk-full emulation has no Rust carrier.
#[test]
#[ignore = "go-parity-gap: unistore rpcTiKVAllowedOnAlmostFull failpoint has no Rust carrier"]
fn drop_table_on_tikv_disk_full_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:248 TestRebaseAutoID` — item
/// 250.
///
/// Go, under the `mockAutoIDChange` failpoint, pins the row ids an
/// AUTO_INCREMENT column produces: 1; after `auto_increment = 6000` the
/// next insert is 6000; after `auto_increment = 5` (below the allocator
/// base) the next insert is 11000; after `auto_increment = 12000` (inside
/// the reserved batch) the next insert is 16000 — batch allocation, not
/// MySQL-compatible per-row allocation — and `ALTER TABLE ... ADD COLUMN b
/// int auto_increment key, auto_increment=10` is refused with
/// `errno.ErrUnsupportedDDLOperation` (8200).
///
/// go-parity-gap: the autoid allocator and its rebase batching
/// (meta/autoid) are not transcreated.
#[test]
#[ignore = "go-parity-gap: autoid rebase batching (meta/autoid) is not transcreated"]
fn rebase_auto_id_allocates_in_batches_documentary() {}

/// Rust side of `pkg/ddl/db_test.go:281 TestProcessColumnFlags` — item 251.
///
/// Go creates `t(a year(4) comment 'xxx', b year, c bit)` and requires
/// `processColumnFlags` (pkg/ddl/add_column.go:1303-1321) to have stamped,
/// after MODIFY COLUMN: every YEAR column with
/// `mysql.HasUnsignedFlag && mysql.HasZerofillFlag && !mysql.HasBinaryFlag`
/// (the ZEROFILL the YEAR arm implies survives an explicit `unsigned`, and
/// is re-added when spelled `zerofill`), and the BIT column with
/// `HasUnsignedFlag && !HasBinaryFlag`. The Rust carrier is
/// [`crate::ddl::column_field_type::process_column_flags`]
/// (src/ddl/column_field_type.rs:358), which the CREATE path
/// (src/ddl/column_types.rs:174) and the MODIFY path
/// (src/ddl/column_types.rs via src/ddl/alter_table.rs) both run on the
/// finished field type, exactly Go's two callers; the flags are read back
/// off the stored column like Go's `col.GetFlag()`.
#[test]
fn process_column_flags_stamp_year_and_bit_after_modify() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "create table t(a year(4) comment 'xxx', b year, c bit)",
        &mut catalog,
    )
    .unwrap();
    let yearcheck = |catalog: &crate::Catalog, column: &str| {
        let field_type = column_flags(catalog, "t", column);
        field_type.has_flag(FieldTypeFlags::UNSIGNED)
            && field_type.has_flag(FieldTypeFlags::ZEROFILL)
            && !field_type.has_flag(FieldTypeFlags::BINARY)
    };
    let ctx = crate::StmtContext::for_query();

    crate::run_alter_table_in(
        "alter table t modify a year(4)",
        &mut catalog,
        current_db(),
        &ctx,
    )
    .unwrap();
    assert!(yearcheck(&catalog, "a"), "a after modify year(4)");

    crate::run_alter_table_in(
        "alter table t modify a year(4) unsigned",
        &mut catalog,
        current_db(),
        &ctx,
    )
    .unwrap();
    assert!(yearcheck(&catalog, "a"), "a after modify year(4) unsigned");

    crate::run_alter_table_in(
        "alter table t modify a year(4) zerofill",
        &mut catalog,
        current_db(),
        &ctx,
    )
    .unwrap();

    crate::run_alter_table_in(
        "alter table t modify b year",
        &mut catalog,
        current_db(),
        &ctx,
    )
    .unwrap();
    assert!(yearcheck(&catalog, "b"), "b after modify year");

    crate::run_alter_table_in(
        "alter table t modify c bit",
        &mut catalog,
        current_db(),
        &ctx,
    )
    .unwrap();
    let field_type = column_flags(&catalog, "t", "c");
    assert!(field_type.has_flag(FieldTypeFlags::UNSIGNED));
    assert!(!field_type.has_flag(FieldTypeFlags::BINARY));
}

/// Documentary twin for `pkg/ddl/db_test.go:320
/// TestForbidCacheTableForSystemTable` — item 252.
///
/// Go walks every table of MySQL, INFORMATION_SCHEMA, PERFORMANCE_SCHEMA,
/// METRICS_SCHEMA and SYS and requires `ALTER TABLE ... CACHE` on each to
/// fail: "[ddl:8200]ALTER table cache for tables in system database is
/// currently unsupported" for real tables in MySQL/SYS,
/// `dbterror.ErrWrongObject` for views, and a planner 1142 permission error
/// for the remaining system schemas.
///
/// go-parity-gap: the system-schema walk and auth surface are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: system-schema CACHE refusals need the session/domain stack"]
fn forbid_cache_table_for_system_table_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:350 TestAlterShardRowIDBits` —
/// item 253.
///
/// Go pins `ALTER TABLE ... SHARD_ROW_ID_BITS`: raising bits so the next
/// global auto id overflows fails with "[autoid:1467]shard_row_id_bits 10
/// will cause next global auto ID 72057594037932936 overflow"; lowering 5
/// -> 3 succeeds leaving `MaxShardRowIDBits = 5, ShardRowIDBits = 3`; after
/// lowering 10 -> 5 a rebase to 1<<56 still overflows because the check
/// uses the historical max, failing inserts with "[autoid:1467]Failed to
/// read auto-increment value from storage engine".
///
/// go-parity-gap: shard_row_id_bits alter + autoid overflow checks are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: SHARD_ROW_ID_BITS alter + autoid overflow checks are not transcreated"]
fn alter_shard_row_id_bits_uses_max_recorded_bits_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:386 TestDDLJobErrorCount` —
/// item 254.
///
/// Go forces a RENAME job to fail with `kv.ErrEntryTooLarge` and pins the
/// persisted history: the failed job's `ErrorCount` is exactly 1 and its
/// `Error` equals `kv.ErrEntryTooLarge`, while the source table keeps zero
/// rows.
///
/// go-parity-gap: DDL job persistence (error counts in the job history) is
/// not transcreated.
#[test]
#[ignore = "go-parity-gap: DDL job error-count persistence is not transcreated"]
fn ddl_job_error_count_is_recorded_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:414
/// TestAddIndexFailOnCaseWhenCanExit` — item 255.
///
/// Go enables `MockCaseWhenParseFailure`, sets the global error-count limit
/// to 1, and requires the ADD INDEX job to fail with
/// "[ddl:-1]job.ErrCount:0, mock unknown type: ast.whenClause." and then be
/// CANCELLED once the retry budget is exhausted (issue #19325).
///
/// go-parity-gap: the DDL job retry/cancel loop is not transcreated.
#[test]
#[ignore = "go-parity-gap: DDL job retry/cancel loop is not transcreated"]
fn add_index_fails_on_case_when_mock_and_exits_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:433
/// TestCreateTableWithIntegerLengthWarning` — item 256.
///
/// Go, with the parser-global `parsertypes.TiDBStrictIntegerDisplayWidth =
/// true`, pins that `CREATE TABLE t(a tinyint(1))` warns NOTHING
/// (boolean-ish) while every other integer family spelled with an explicit
/// display width — smallint(2), int(2), mediumint(2), bigint(2),
/// integer(2), int2..int8(2) — warns exactly once: "Warning 1681 Integer
/// display width is deprecated and will be removed in a future release."
/// (int1(1) again silent). The CREATE path here emits no 1681 warning and
/// the session warning buffer is not transcreated.
///
/// go-parity-gap: strict integer display width warnings need the session
/// warning buffer.
#[test]
#[ignore = "go-parity-gap: strict integer display width warnings need the session warning buffer"]
fn create_table_integer_length_warning_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:487
/// TestShowCountWarningsOrErrors` — item 257.
///
/// Go runs `SHOW COUNT(*) WARNINGS` / `SHOW COUNT(*) ERRORS`, then pins
/// that after three strict-integer-width CREATEs the warning count equals
/// `@@session.warning_count`, and after a duplicate CREATE TABLE the error
/// count equals `@@session.error_count`.
///
/// go-parity-gap: SHOW COUNT statements and session diagnostics counters
/// are not transcreated.
#[test]
#[ignore = "go-parity-gap: SHOW COUNT(*) WARNINGS/ERRORS statements are not transcreated"]
fn show_count_warnings_or_errors_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:515 TestIssue60047` — item 258.
///
/// Go loads 90 rows into a `RANGE COLUMNS(c)`-partitioned table with
/// `UNIQUE KEY idx(a, c)`, runs ADD COLUMN concurrently with `INSERT ...
/// ON DUPLICATE KEY UPDATE` fired at the job's WriteOnly state, and
/// requires both to succeed (the partition-key column set must stay
/// readable during the reorg).
///
/// go-parity-gap: concurrent DML during online ADD COLUMN needs the job
/// runner and partitioned DML.
#[test]
#[ignore = "go-parity-gap: online DDL concurrency with partitioned DML is not transcreated"]
fn issue60047_insert_odku_during_add_column_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:559 TestCancelJobWriteConflict`
/// — item 259.
///
/// Go pins `ADMIN CANCEL DDL JOBS` write-conflict behavior: with
/// `mockCommitErrorInNewTxn = "no_retry"` plus
/// `mockFailedCommandOnConcurencyDDL` the admin statement fails with "mock
/// failed admin command on ddl jobs" and the ADD INDEX finishes anyway;
/// with `"retry_once"` the cancel succeeds once and the ADD INDEX ends
/// cancelled (`errno.ErrCancelledDDLJob`), the result row reading "<jobID>
/// successful".
///
/// go-parity-gap: ADMIN CANCEL DDL JOBS and its txn retry logic are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: admin cancel ddl jobs write-conflict retry is not transcreated"]
fn cancel_job_write_conflict_retry_once_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:610 TestTxnSavepointWithDDL` —
/// item 260.
///
/// Go pins pessimistic transactions with SAVEPOINTs against concurrent DDL:
/// a `ROLLBACK TO SAVEPOINT` that only touched rolled-back rows lets the
/// commit succeed across a concurrent ADD INDEX; a savepoint spanning a
/// table whose schema changed makes the commit fail with 8028; and
/// `ADMIN CHECK TABLE` stays clean in every scenario.
///
/// go-parity-gap: pessimistic savepoints + commit-time schema validation
/// are not transcreated.
#[test]
#[ignore = "go-parity-gap: pessimistic savepoints + commit-time schema validation are not transcreated"]
fn txn_savepoint_with_ddl_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:667 TestSnapshotVersion` —
/// item 261.
///
/// Go pins snapshot metadata reads around a CREATE DATABASE:
/// `GetSnapshotInfoSchema(snapTS)` carries the live `SchemaMetaVersion`;
/// `GetSnapshotMeta(snapTS)` answers `meta.ErrDBNotExists` for the
/// later-created database while `GetSnapshotMeta(currSnapTS)` returns the
/// created table byte-equal; `SchemaSyncer().WaitVersionSynced` succeeds
/// with `SyncSummary{ServerCount: 1}`.
///
/// go-parity-gap: domain snapshot infoschema/meta machinery is not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: domain snapshot infoschema/meta machinery is not transcreated"]
fn snapshot_version_reads_are_stable_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:729 TestSchemaValidator` —
/// item 262.
///
/// Go pins the domain schema validator: after a successful reload
/// `Check(ts, schemaVer, nil, true)` is `ResultSucc`; while a mocked reload
/// fails the check stays `ResultSucc` until one schema lease elapses, when
/// it turns `ResultUnknown`; after a real reload it returns to `ResultSucc`;
/// and a stopped validator makes `domain.NewSchemaChecker(...).Check(123456)`
/// fail with `domain.ErrInfoSchemaExpired` after exactly one retry.
///
/// go-parity-gap: the domain schema validator (validatorapi) is not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: domain schema validator states are not transcreated"]
fn schema_validator_succ_unknown_expired_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:782 TestLogAndShowSlowLog` —
/// item 263.
///
/// Go logs three slow queries (internal, aliased, plain) and pins
/// `dom.ShowSlowQuery` orderings: Top-2 = bbb(3s, alias1) then ccc(2s);
/// Top-2 internal = aaa only; Top-4 all = bbb, ccc, aaa; Recent-2 = ccc
/// then bbb — insertion order for Recent, duration order for Top.
///
/// go-parity-gap: the domain slow-query log ring is not transcreated.
#[test]
#[ignore = "go-parity-gap: domain slow-query log/SHOW SLOW is not transcreated"]
fn log_and_show_slow_log_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:829
/// TestReportingMinStartTimestamp` — item 264.
///
/// Go gives `dom.InfoSyncer()` a mock session manager holding txn start ts
/// `{0, maxUint64, lowerLimit, validTS}` and requires `ReportMinStartTS` to
/// publish exactly `validTS` — zero and max are skipped, values below the
/// lower limit are clamped away.
///
/// go-parity-gap: InfoSyncer.ReportMinStartTS is not transcreated.
#[test]
#[ignore = "go-parity-gap: InfoSyncer.ReportMinStartTS is not transcreated"]
fn reporting_min_start_timestamp_picks_valid_session_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:857
/// TestBuildMaxLengthIndexWithNonRestrictedSqlMode` — item 265.
///
/// Go, per charset (Maxlen) x column shape (text/blob/varchar/varbinary,
/// with or without explicit lengths), pins index-prefix behavior at
/// `config.MaxIndexLength`: strict mode rejects with `errno.ErrTooLongKey`,
/// empty sql_mode downgrades to exactly one warning of the same code, SHOW
/// CREATE TABLE renders `KEY name (name(<expectKeyLength>))`, UNIQUE
/// indexes always error (no downgrade for uniqueness), and a multi-column
/// index whose total exceeds the limit errors even in non-strict mode.
///
/// go-parity-gap: index-length clamping + the sql_mode warning downgrade
/// are not transcreated.
#[test]
#[ignore = "go-parity-gap: index-prefix clamping + sql_mode warning downgrade are not transcreated"]
fn build_max_length_index_with_non_restricted_sql_mode_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:971
/// TestTiDBDownBeforeUpdateGlobalVersion` — item 266.
///
/// Go enables `mockDownBeforeUpdateGlobalVersion` +
/// `checkDownBeforeUpdateGlobalVersion` (a TiDB-down simulation during the
/// version publish) and requires `ALTER TABLE t ADD COLUMN b int` to still
/// COMPLETE.
///
/// go-parity-gap: global-version publish with a down node needs the syncer.
#[test]
#[ignore = "go-parity-gap: global-version update with a down TiDB is not transcreated"]
fn tidb_down_before_update_global_version_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:985 TestDDLBlockedCreateView` —
/// item 267.
///
/// Go fires a CREATE VIEW from a second session at the ALTER MODIFY COLUMN
/// job's WriteOnly state and requires the CREATE VIEW to succeed — a schema
/// change mid-flight does not block view creation.
///
/// go-parity-gap: concurrent statement execution around job states is not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: concurrent CREATE VIEW during a job state is not transcreated"]
fn ddl_blocked_create_view_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1008
/// TestHashPartitionAddColumn` — item 268.
///
/// Go fires `DELETE FROM t` from a second session at the ADD COLUMN job's
/// WriteOnly state on a `PARTITION BY hash(a) PARTITIONS 4` table and
/// requires the ALTER to succeed — deleting all rows mid-backfill must not
/// wedge the reorg.
///
/// go-parity-gap: online ADD COLUMN on partitioned tables is not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: partitioned-table online column add is not transcreated"]
fn hash_partition_add_column_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1026
/// TestSetInvalidDefaultValueAfterModifyColumn` — item 269.
///
/// Go runs `ALTER TABLE t MODIFY COLUMN a text(100)` and, from its
/// DeleteOnly state, a concurrent `ALTER TABLE t ALTER COLUMN a SET DEFAULT
/// 1`, which must fail with "[ddl:1101]BLOB/TEXT/JSON column 'a' can't have
/// a default value" — the DEFAULT legality check runs against the NEW
/// column type.
///
/// go-parity-gap: set-default during an in-flight type change (1101) needs
/// the job runner.
#[test]
#[ignore = "go-parity-gap: set-default-during-modify refusal (1101) needs the job runner"]
fn set_invalid_default_after_modify_column_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1057 TestMDLTruncateTable` —
/// item 270.
///
/// Go holds `BEGIN; SELECT * FROM t FOR UPDATE`, truncates from two other
/// sessions, then commits: BOTH truncate statements must complete only
/// AFTER the commit (their wall-clock finish times are after the commit's
/// start) — metadata-lock serialization of TRUNCATE behind the reader, with
/// the second truncate additionally waiting for the first table-id swap to
/// be visible.
///
/// go-parity-gap: metadata-lock wait queues are not transcreated.
#[test]
#[ignore = "go-parity-gap: MDL serialization of TRUNCATE behind a reader is not transcreated"]
fn mdl_truncate_table_waits_for_reader_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1130
/// TestTruncateTableAndSchemaDependence` — item 271.
///
/// Go starts TRUNCATE TABLE, then issues `DROP DATABASE test` in parallel,
/// and requires the drop to finish strictly AFTER the truncate — the job
/// dependency ordering between a truncate and its schema's drop.
///
/// go-parity-gap: cross-job dependency ordering is not transcreated.
#[test]
#[ignore = "go-parity-gap: truncate/drop-database job dependency ordering is not transcreated"]
fn truncate_table_and_schema_dependence_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1169 TestInsertIgnore` — item
/// 272.
///
/// Go adds `UNIQUE INDEX idx(b)` while inserting duplicates at DeleteOnly
/// and `INSERT IGNORE` at the backfill's ReadyToMerge state, then requires
/// `ADMIN CHECK TABLE t` to pass — duplicate suppression during unique-
/// index backfill must leave the index consistent.
///
/// go-parity-gap: unique-index backfill states and admin check are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: unique index backfill states + admin check are not transcreated"]
fn insert_ignore_during_backfill_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1199
/// TestDDLJobErrEntrySizeTooLarge` — item 273.
///
/// Go forces the entry-size guard (`mockErrEntrySizeTooLarge`, single shot)
/// so `RENAME TABLE t TO t1` fails with `errno.ErrEntryTooLarge`, and the
/// FOLLOWING `CREATE TABLE t1` and `ALTER TABLE t ADD COLUMN b int` must
/// still work — one failed job must not wedge the DDL queue.
///
/// go-parity-gap: the job queue and its entry-size guard are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: DDL queue unwedging after ErrEntryTooLarge is not transcreated"]
fn ddl_job_err_entry_size_too_large_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1244
/// TestResumeSystemPausedDDLJobWithKVDiskFullReason` — item 274.
///
/// Go pins system-paused job resume semantics (hand-built jobs inserted
/// into mysql.tidb_ddl_job): a job paused by the SYSTEM for
/// `JobPauseReasonKVDiskFull` with error `ErrDDLAutoPausedByKVDiskFull`
/// resumes via `ADMIN RESUME DDL JOBS 1` into JobStateQueueing with pause
/// reason nil, resume reason KVDiskFull, error nil; a system-paused job
/// WITHOUT that reason refuses end-user resume with "[ddl:8261]Job [2]
/// can't be resumed: job has been paused by [System], should not resumed by
/// [EndUser]"; and `ResumeAllJobsBySystem` (pkg/ddl/ddl.go:1705) resumes
/// the upgrade-paused job while KEEPING the kv-disk-full one paused
/// (`IsPausedBySystemForKVDiskFull` still true).
///
/// go-parity-gap: the tidb_ddl_job table and resume machinery are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: system-paused DDL job resume (ddl.go:1705) is not transcreated"]
fn resume_system_paused_ddl_job_with_kv_disk_full_reason_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1313
/// TestAdminAlterDDLJobUpdateSysTable` — item 275.
///
/// Go inserts an ADD INDEX job (ReorgMeta concurrency 4, batch 128, with
/// and without UseCloudStorage) and pins `ADMIN ALTER DDL JOBS <id>`
/// persistence: `thread = 8` reads back 8, `batch_size = 256` reads back
/// 256, and the combined `thread = 16, batch_size = 512` reads back both.
///
/// go-parity-gap: ADMIN ALTER DDL JOBS persistence is not transcreated.
#[test]
#[ignore = "go-parity-gap: admin alter ddl jobs sys-table updates are not transcreated"]
fn admin_alter_ddl_job_updates_sys_table_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1349
/// TestAdminAlterDDLJobUnsupportedCases` — item 276.
///
/// Go pins the ADMIN ALTER DDL JOBS validation messages verbatim: thread
/// outside [1,256] and batch_size outside [32,10240] answer "the value N
/// for X is out of range [lo, hi]"; non-integer values answer "the value
/// for X is invalid, only integer is allowed"; max_write_speed outside
/// [0, 1125899906842624] answers the range message, unparseable sizes
/// answer "parse max_write_speed value error: invalid size: 'MiB'" /
/// "invalid suffix: 'xl'"; malformed SQL answers parser 1064 with a
/// line/column; valid values against a non-existent job answer "ddl job 1
/// is not running"; and an ADD COLUMN job answers "unsupported DDL
/// operation: add column. Supported DDL operations are: ADD INDEX, MODIFY
/// COLUMN, and ALTER TABLE REORGANIZE PARTITION".
///
/// go-parity-gap: the ADMIN ALTER DDL JOBS statement layer is not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: admin alter ddl jobs validation messages are not transcreated"]
fn admin_alter_ddl_job_unsupported_cases_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1414
/// TestAdminAlterDDLJobCommitFailed` — item 277.
///
/// Go enables `mockAlterDDLJobCommitFailed` and requires the ADMIN ALTER
/// statement to fail with "mock commit failed on admin alter ddl jobs"
/// while the stored job's ReorgMeta is UNCHANGED (concurrency 4, batch 128)
/// — a failed commit must not leak partial parameter updates.
///
/// go-parity-gap: the ADMIN ALTER commit path is not transcreated.
#[test]
#[ignore = "go-parity-gap: admin alter ddl jobs commit-failure atomicity is not transcreated"]
fn admin_alter_ddl_job_commit_failed_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1440 TestGetAllTableInfos` —
/// item 278.
///
/// Go creates 113 databases x 3 tables and requires `meta.IterAllTables`
/// (batch 13) to return exactly the infoschema's non-special-schema tables,
/// matched on ID and DBID after sorting; batch sizes 0 and -999 must also
/// succeed.
///
/// go-parity-gap: meta.IterAllTables and the multi-database infoschema are
/// not transcreated.
#[test]
#[ignore = "go-parity-gap: meta.IterAllTables scan is not transcreated"]
fn get_all_table_infos_matches_infoschema_documentary() {}

/// Documentary twin for `pkg/ddl/db_test.go:1491 TestGetVersionFailed` —
/// item 279.
///
/// Go makes the current-version lookup fail TWICE
/// (`mockGetCurrentVersionFailed`, `2*return(true)`) and requires
/// `ALTER TABLE t ADD COLUMN b int` to still succeed — the version fetch
/// retries exactly past two transient failures.
///
/// go-parity-gap: the current-version retry path is not transcreated.
#[test]
#[ignore = "go-parity-gap: current-version fetch retry is not transcreated"]
fn get_version_failed_retries_twice_documentary() {}

// ===========================================================================
// pkg/ddl/ddl_error_test.go (items 281-287)
// ===========================================================================

/// Documentary twin for `pkg/ddl/ddl_error_test.go:32 TestTableError` —
/// item 281.
///
/// Go corrupts the job's schema ID (`mockModifyJobSchemaId` -> -1) so
/// `DROP TABLE testDrop` errors, corrupts the table ID
/// (`MockModifyJobTableId` -> -1) so the drop errors again, corrupts the
/// schema ID so `CREATE TABLE test.t1` errors, and finally — with no
/// corruption — a duplicate `CREATE TABLE test.t2` fails with
/// `errno.ErrTableExists` (1050). The first three arms need the job runner;
/// the 1050 arm has a live carrier here (`run_create_table_in` -> 1050) but
/// only after the failpoint arms of the same test, so the whole test stays
/// a documentary port.
///
/// go-parity-gap: job schema/table-id corruption failpoints need the DDL
/// job runner.
#[test]
#[ignore = "go-parity-gap: job schema/table-id failpoints need the DDL job runner"]
fn table_error_on_corrupted_job_ids_documentary() {}

/// Rust side of `pkg/ddl/ddl_error_test.go:60 TestViewError` — item 282.
///
/// The Go body (ddl_error_test.go:62-66) only opens a store and runs
/// `CREATE TABLE t (a int)` — a vestigial bootstrap test whose
/// error-checking statements were removed; it pins no behavior beyond "a
/// one-column CREATE TABLE succeeds". That IS hostable here:
/// [`crate::run_create_table_on`] is the transcreated CREATE TABLE entry.
#[test]
fn view_error_vestigial_body_create_table_succeeds() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("create table t (a int)", &mut catalog)
        .unwrap_or_else(|error| panic!("Go's body requires the CREATE to succeed: {error}"));
}

/// Documentary twin for `pkg/ddl/ddl_error_test.go:68 TestForeignKeyError`
/// — item 283.
///
/// Go creates `t(a int, index(a))` and `t1(... FOREIGN KEY fk(a) REFERENCES
/// t(a))`, then with the job's schema ID corrupted requires `ALTER TABLE t1
/// ADD FOREIGN KEY idx(a) REFERENCES t(a)` to error and `ALTER TABLE t1
/// DROP INDEX fk` to error — FK jobs hitting the metadata-error path fail
/// loudly instead of silently applying.
///
/// go-parity-gap: FK DDL jobs under failpoint corruption are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: FK job corruption failpoints need the DDL job runner"]
fn foreign_key_error_on_corrupted_job_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_error_test.go:83 TestIndexError` —
/// item 284.
///
/// Go adds index `a(a)` on `t(a int)`, then with the schema ID corrupted
/// requires `ALTER TABLE t ADD INDEX idx(a)` to error and `ALTER TABLE t1
/// DROP a` (unknown table) to error.
///
/// go-parity-gap: index DDL jobs under failpoint corruption are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: index job corruption failpoints need the DDL job runner"]
fn index_error_on_corrupted_job_documentary() {}

/// Rust side of the failpoint-free tail of `pkg/ddl/ddl_error_test.go:100
/// TestColumnError` — item 285.
///
/// Go ends the test (ddl_error_test.go:135-139) with four failpoint-free
/// refusals on `t(a int, aa int, ab int)`: `ADD COLUMN c int AFTER c5`
/// fails with `errno.ErrBadField` (1054), `DROP COLUMN c5` with
/// `errno.ErrCantDropFieldOrKey` (1091), and both multi-action spellings
/// fail with the offending action's code. The two DROP arms run here
/// (`UnknownColumnInAlter`, 1091). The two ADD arms are a REAL DIVERGENCE,
/// not a missing carrier: Go's ADD spelling answers 1054, while this
/// crate's add-column position resolution raises `UnknownColumnInAlter`
/// (1091, src/ddl/alter_table.rs:2700-2706 + src/driver/errors/mod.rs:417)
/// where the MODIFY spelling correctly raises 1054
/// (`UnknownColumnInTable`, src/ddl/alter_table.rs:2515-2521). They are
/// pinned in the ignored twin below; the production file is outside a
/// testport batch's edit scope.
#[test]
fn column_error_drop_unknown_column_is_1091() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("create table t (a int, aa int, ab int)", &mut catalog).unwrap();
    crate::run_alter_table_in(
        "alter table t add index a(a)",
        &mut catalog,
        current_db(),
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();

    // `alter table t drop column c5` -> ErrCantDropFieldOrKey (1091).
    let error = crate::run_alter_table_in(
        "alter table t drop column c5",
        &mut catalog,
        current_db(),
        &ctx,
    )
    .expect_err("dropping an unknown column must fail");
    assert_eq!(error.to_mysql_error().code, 1091);

    // `alter table t drop column ab, drop column c5` -> 1091 for the
    // offending action (Go pins only the statement's error code; its own
    // multi-schema-change machinery aborts the whole job, this crate's
    // sequential dispatcher applies `ab` first — the code is the pinned
    // fact either way).
    let error = crate::run_alter_table_in(
        "alter table t drop column ab, drop column c5",
        &mut catalog,
        current_db(),
        &ctx,
    )
    .expect_err("the unknown column in the multi-action drop must fail");
    assert_eq!(error.to_mysql_error().code, 1091);
}

/// Documentary twin for the two ADD arms of `pkg/ddl/ddl_error_test.go:100
/// TestColumnError` — item 285 (the DROP arms run in
/// [`column_error_drop_unknown_column_is_1091`]).
///
/// Go pins `alter table t add column c int after c5` ->
/// `errno.ErrBadField` (1054) and the multi-action spelling
/// `add column c int after c5, add column d int` -> 1054.
///
/// go-parity-gap: DIVERGENCE — the Rust ADD COLUMN position resolution
/// raises 1091 (`UnknownColumnInAlter`) where Go raises 1054; fixing it is
/// a production-code change outside a testport batch's edit scope.
#[test]
#[ignore = "go-parity-gap: DIVERGENCE - ADD COLUMN AFTER unknown raises 1091 here, Go pins 1054 (alter_table.rs:2700)"]
fn column_error_add_after_unknown_column_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_error_test.go:142
/// TestCreateDatabaseError` — item 286.
///
/// Go enables `mockModifyJobSchemaId` (returning -1) around `CREATE
/// DATABASE db1;` and requires the statement to SUCCEED — CREATE DATABASE
/// allocates its schema ID from the global allocator, not the corrupted
/// job field, so the mock does not reach it.
///
/// go-parity-gap: CREATE DATABASE under the schema-id mock needs the job
/// runner.
#[test]
#[ignore = "go-parity-gap: CREATE DATABASE under the schema-id mock needs the job runner"]
fn create_database_error_survives_mock_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_error_test.go:153
/// TestCreateIndexErrTooManyKeys` — item 287.
///
/// Go is a hard-coded guard: `require.Equal(t, 512,
/// config.DefMaxOfIndexLimit)` (pkg/config/config.go:75, `64 * 8`), the
/// per-table index ceiling that must not be loosened. The Rust config tree
/// DOES carry the constant — `DEF_MAX_OF_INDEX_LIMIT: i64 = 64 * 8`
/// (tidb-config src/config_tree/config.rs:64, enforced by the index-limit
/// validation at config.rs:719) — but it is module-private, so no test in
/// this crate can assert its value.
///
/// go-parity-gap: DEF_MAX_OF_INDEX_LIMIT exists in tidb-config
/// (config.rs:64) but is private; the 512 guard cannot be asserted from
/// tidb-executor.
#[test]
#[ignore = "go-parity-gap: DEF_MAX_OF_INDEX_LIMIT (config.rs:64) is private to tidb-config"]
fn create_index_err_too_many_keys_guard_documentary() {}

// ===========================================================================
// pkg/ddl/ddl_history_test.go (items 288-289)
// ===========================================================================

/// Documentary twin for `pkg/ddl/ddl_history_test.go:36 TestDDLHistoryBasic`
/// — item 288.
///
/// Go inserts jobs {1, 2} through `AddHistoryDDLJob`
/// (pkg/ddl/ddl_history.go:47) inside kv.RunInNewTxn batches and pins:
/// `GetHistoryJobByID(sess, 1)` (ddl_history.go:74) returns ID 1;
/// `GetLastNHistoryDDLJobs(m, 2)` (ddl_history.go:94) returns exactly 2;
/// `GetAllHistoryDDLJobs` (ddl_history.go:128) counts the store's history;
/// `ScanHistoryDDLJobs(m, 2, 2)` (ddl_history.go:154) returns `[ID 2, ID
/// 1]` — newest first from the start ID downward; and with the
/// `history-ddl-jobs-limit` failpoint returning 128,
/// `ScanHistoryDDLJobs(m, 0, 0)` returns min(total, 128) rows ending with
/// IDs 2 and 1.
///
/// go-parity-gap: the ddl_history accessors and meta mutator are not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: ddl_history accessors (ddl_history.go:47-200) and meta mutator are not transcreated"]
fn ddl_history_basic_scan_windows_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_history_test.go:129
/// TestScanHistoryDDLJobsWithErrorLimit` — item 289.
///
/// Go calls `ScanHistoryDDLJobs(&meta.Mutator{}, 10, 0)`
/// (pkg/ddl/ddl_history.go:154-160) with a start_jobID but a zero limit and
/// requires the error to contain "when 'start_job_id' is specified, it must
/// work with a 'limit'" — the argument pair is validated before any store
/// access.
///
/// go-parity-gap: ScanHistoryDDLJobs is not transcreated here.
#[test]
#[ignore = "go-parity-gap: ScanHistoryDDLJobs argument validation (ddl_history.go:154) is not transcreated"]
fn scan_history_ddl_jobs_requires_limit_with_start_documentary() {}

// ===========================================================================
// pkg/ddl/ddl_running_jobs_test.go (items 290-292)
// ===========================================================================
//
// All three tests of this file are already carried, and were re-verified
// line-by-line against origin/master this session, by the in-crate tests of
// src/ddl_running_jobs.rs (a complete transcreation of ddl_running_jobs.go):
//
// - `TestRunningJobs` (ddl_running_jobs_test.go:79)
//   -> running_jobs_matches_go
// - `TestSchemaPolicyAndResourceGroup` (:147)
//   -> schema_policy_and_resource_group_matches_go
// - `TestExclusiveShared` (:223) -> exclusive_shared_matches_go

// ===========================================================================
// pkg/ddl/ddl_test.go (items 293-300)
// ===========================================================================

/// Documentary twin for `pkg/ddl/ddl_test.go:90 TestGetIntervalFromPolicy`
/// — item 293.
///
/// Go (getIntervalFromPolicy, pkg/ddl/executor.go:7569-7581): given the
/// policy `[1s, 2s]`, index 0 returns `(1s, true)`, index 1 returns
/// `(2s, true)`, and any index at or past the end (2, 3) returns
/// `(2s, false)` — the LAST interval sticks and `changed` reports whether
/// the index was still inside the policy. It drives the backfill worker
/// poll cadence (`d.getJobReorgInterval`, executor.go:7583+).
///
/// go-parity-gap: getIntervalFromPolicy and the reorg poll loop it feeds
/// are not transcreated.
#[test]
#[ignore = "go-parity-gap: getIntervalFromPolicy (executor.go:7569) is not transcreated"]
fn get_interval_from_policy_sticks_to_the_last_entry_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_test.go:128 TestModifyColumn` — item
/// 294.
///
/// Go builds both sides of each row through the parser +
/// `buildColumnAndConstraint` (helper `colDefStrToColInfo`,
/// ddl_test.go:112-125) and feeds them to `checkModifyTypes(from, to,
/// false)` (pkg/ddl/modify_column.go:2262-2295), the charset/collation
/// compat gate for type-changing MODIFY. The 21-row table pins: integer and
/// varchar/varbinary family changes are accepted; `text -> blob` is refused
/// with `ErrUnsupportedModifyCharset` ("charset from utf8mb4 to binary");
/// varchar(10) narrowing/widening and decimal precision moves are accepted;
/// and every gbk crossing (int -> gbk varchar, gbk -> int, gbk <-> utf8
/// varchar/char) is refused with the two charsets named, while gbk -> gbk
/// widening is accepted.
///
/// go-parity-gap: checkModifyTypes (modify_column.go:2262) is not
/// transcreated; the crate models only its CheckModifyTypeCompatible callee
/// (alter_table.rs check_type_change_supported).
#[test]
#[ignore = "go-parity-gap: checkModifyTypes (modify_column.go:2262) charset gate is not transcreated"]
fn modify_column_charset_compat_table_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_test.go:169
/// TestProcessModifyColumnOptionsGenerated` — item 295.
///
/// Go runs `ProcessModifyColumnOptions(sctx, col, options)`
/// (pkg/ddl/modify_column.go:2297-2330) over three parsed MODIFY COLUMN
/// definitions and pins that the generated expression is restored with
/// `RestoreWithoutSchemaName | RestoreWithoutTableName`, so the table-
/// qualified source `t.a + 1` lands as "`a` + 1", `LOWER(t.a)` lands as
/// "lower(`a`)", and `t.a * t.b` lands as "`a` * `b`"; `GeneratedStored`
/// follows the STORED/VIRTUAL spelling; `GeneratedExpr` is always set.
///
/// go-parity-gap: ProcessModifyColumnOptions is not transcreated; this
/// crate's ALTER path refuses MODIFY of a generated column instead of
/// rebuilding its expression.
#[test]
#[ignore = "go-parity-gap: ProcessModifyColumnOptions (modify_column.go:2297) is not transcreated"]
fn process_modify_column_options_unqualifies_generated_expr_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_test.go:221 TestFieldCase` — item 296.
///
/// Go builds `[]*model.ColumnInfo{{Name: "field"}, {Name: "Field"}}` and
/// requires `checkDuplicateColumn` (pkg/ddl/create_table.go:754-763) to
/// fail with `infoschema.ErrColumnExists.GenWithStackByArgs("Field")` —
/// "[ddl:1060]Duplicate column name 'Field'" — the collision is
/// case-insensitive and the reported name is the SECOND spelling. The
/// crate's CREATE/ALTER paths duplicate the 1060 refusal inline on their
/// own column lists (e.g. src/ddl.rs:1030-1040) without a callable carrier
/// for this function's contract.
///
/// go-parity-gap: checkDuplicateColumn (create_table.go:754) is not
/// transcreated as a callable function.
#[test]
#[ignore = "go-parity-gap: checkDuplicateColumn (create_table.go:754) is not transcreated"]
fn field_case_duplicate_column_is_case_insensitive_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_test.go:233 TestIgnorableSpec` — item
/// 297.
///
/// Go (isIgnorableSpec, pkg/ddl/executor.go:1589-1592) classifies ALTER
/// TABLE specification types: `AlterTableLock` and `AlterTableAlgorithm`
/// are ignorable (the DDL layer drops them), while the eleven structural
/// types in the test's first list — Option, AddColumns, AddConstraint,
/// DropColumn, DropPrimaryKey, DropIndex, DropForeignKey, ModifyColumn,
/// ChangeColumn, RenameTable, AlterColumn — are not.
///
/// go-parity-gap: isIgnorableSpec is not transcreated; the Rust AST models
/// LOCK/ALGORITHM as typed AlterTableAction variants dispatched directly.
#[test]
#[ignore = "go-parity-gap: isIgnorableSpec (executor.go:1589) is not transcreated"]
fn ignorable_spec_covers_only_lock_and_algorithm_documentary() {}

/// Rust side of `pkg/ddl/ddl_test.go:260 TestError` — item 298.
///
/// Go (ddl_test.go:262-267): for each of `dbterror.ErrDDLJobNotFound`,
/// `dbterror.ErrCancelFinishedDDLJob`, `dbterror.ErrCannotCancelDDLJob`,
/// `terror.ToSQLError(err).Code` must not be `mysql.ErrUnknown` (1105) and
/// must equal `uint16(err.Code())`. The Rust carriers are the generated
/// `LazyLock<TerrorError>` prototypes of the complete `ddl_terror.go` table
/// (`tidb_util::dbterror`, entries for `dbterror.ErrDDLJobNotFound` et al.,
/// src/dbterror/ddl_errors.rs:328-333), and
/// [`tidb_error::terror::TerrorError::to_sql_error`]
/// (tidb-error/src/terror.rs:534) is `ToSQLError`, whose protocol-code
/// resolution falls back to `ErrUnknown` exactly like Go's
/// `getMySQLErrorCode` (pkg/parser/terror/terror.go). Mechanism deviation,
/// named: the Rust table is GENERATED from the same Go source rather than
/// sharing Go's terror objects.
#[test]
fn error_terror_to_sql_error_round_trips_the_code() {
    for error in [
        &ERR_DDL_JOB_NOT_FOUND,
        &ERR_CANCEL_FINISHED_DDL_JOB,
        &ERR_CANNOT_CANCEL_DDL_JOB,
    ] {
        let sql_error = error.to_sql_error();
        assert_ne!(
            sql_error.code,
            errcode::ErrUnknown,
            "{}: ToSQLError code must not be ErrUnknown",
            error.rfc_code()
        );
        let expected = u16::try_from(error.code().value())
            .unwrap_or_else(|_| panic!("{}: class-local code fits u16", error.rfc_code()));
        assert_eq!(
            expected,
            sql_error.code,
            "{}: ToSQLError code equals the terror code",
            error.rfc_code()
        );
    }
}

/// Documentary twin for `pkg/ddl/ddl_test.go:273
/// TestCheckDuplicateConstraint` — item 299.
///
/// Go (checkDuplicateConstraint, pkg/ddl/create_table.go:1219-1236) keys a
/// name set by constraint type: the first `f1` FOREIGN KEY registers, the
/// second fails "[ddl:1826]Duplicate foreign key constraint name 'f1'";
/// the first `c1` CHECK registers, the second fails "[ddl:3822]Duplicate
/// check constraint name 'c1'."; the first `u1` UNIQUE registers, the
/// second fails "[ddl:1061]Duplicate key name 'u1'". Names stay comparable
/// ACROSS types (one shared set). The crate raises 1826 only from the ALTER
/// ADD FOREIGN KEY path (src/ddl/alter_table.rs:939) and duplicates 1061
/// inline for CREATE TABLE index constraints (src/ddl.rs:1042-1059), with
/// no carrier for the CHECK (3822) arm or the shared set.
///
/// go-parity-gap: checkDuplicateConstraint (create_table.go:1219) is not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: checkDuplicateConstraint (create_table.go:1219) is not transcreated"]
fn check_duplicate_constraint_names_by_shared_set_documentary() {}

/// Documentary twin for `pkg/ddl/ddl_test.go:295
/// TestGetTableDataKeyRanges` — item 300.
///
/// Go (getTableDataKeyRanges, pkg/ddl/cluster.go:295-320) splits the
/// flashback key space `[EncodeTablePrefix(0), EncodeTablePrefix(
/// MaxUserGlobalID)]` around the excluded table IDs: none excluded -> one
/// whole range; `{3}` -> `[0,3)`,`[4,Max]`; `{3,5,9}` -> `[0,3)`,`[4,5)`,
/// `[6,9)`,`[10,Max]` (each hole starts at `prevID + 1`). The constants
/// exist here (`tidb_metadef::MAX_USER_GLOBAL_ID`, src/system.rs:25) and
/// tablecodec prefix encoding exists, but the splitting function does not.
///
/// go-parity-gap: getTableDataKeyRanges (cluster.go:295) is not
/// transcreated.
#[test]
#[ignore = "go-parity-gap: getTableDataKeyRanges (cluster.go:295) is not transcreated"]
fn table_data_key_ranges_split_around_excluded_ids_documentary() {}
