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

//! Ports of the batch-b103 slice of Go `pkg/ddl/db_table_test.go`
//! (origin/master, functions 225-240 of the package's deterministic
//! file-then-line test order).
//!
//! The concurrency/failpoint halves of Go's DDL-job tests exist to exercise
//! the online schema-change state machine, which this tier does not run;
//! those slices are `#[ignore]`d with a `go-parity-gap` note and any
//! deterministic metadata behavior the same Go test pins is ported beside
//! them. The enum/set/integer DEFAULT matrix and the DROP TABLE error
//! matrix are ported in full.

use tidb_datatype::Datum;

use crate::{
    run_alter_table_in, run_create_table_on, run_drop_table_in, run_insert_on, run_select_on,
    Catalog, DriverError, StmtContext, TableEntry, DEFAULT_DATABASE,
};

/// A stock strict session for DML.
fn ctx() -> StmtContext {
    StmtContext::for_dml(false, true, false)
}

/// A read context.
fn read() -> StmtContext {
    StmtContext::for_query()
}

fn create_ok(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog).unwrap_or_else(|error| panic!("{sql} should create: {error:?}"));
}

fn insert_ok(catalog: &mut Catalog, sql: &str) {
    run_insert_on(sql, catalog, &ctx()).unwrap_or_else(|error| panic!("{sql} should insert: {error:?}"));
}

fn select_rows(catalog: &Catalog, sql: &str) -> Vec<Vec<Datum>> {
    run_select_on(sql, catalog, &read()).unwrap_or_else(|error| panic!("{sql} should select: {error:?}"))
}

fn sql_error(result: Result<impl Sized, DriverError>) -> (u16, String) {
    let error = result.err().expect("expected the statement to fail");
    let mysql = error.to_mysql_error();
    (mysql.code, mysql.message)
}


/// The settled `DEFAULT` a column stores, as text (Go's
/// `ColumnInfo.DefaultValue` string).
fn default_text(catalog: &Catalog, table: &str, column: &str) -> String {
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test(table) else {
        panic!("expected kv table {table}");
    };
    let column = table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .unwrap_or_else(|| panic!("column {column} exists"));
    match &column.default_value {
        Some(crate::column_default::ColumnDefault::Value(datum)) => match datum {
            Datum::Null => "NULL".to_owned(),
            Datum::Int(value) => value.to_string(),
            Datum::UInt(value) => value.to_string(),
            Datum::Real(value) => format!("{value}"),
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Datum::String(string) => String::from_utf8_lossy(string.bytes()).into_owned(),
            other => panic!("unexpected default datum {other:?}"),
        },
        other => panic!("expected a settled default, got {other:?}"),
    }
}


/// The code a failing CREATE TABLE reports, `None` when it succeeds.
fn create_err_code(catalog: &mut Catalog, sql: &str) -> Option<u16> {
    run_create_table_on(sql, catalog)
        .err()
        .map(|error| error.to_mysql_error().code)
}

/// A DROP TABLE through the owning entry point (session defaults).
fn drop_ok(catalog: &mut Catalog, sql: &str) {
    run_drop_table_in(sql, catalog, DEFAULT_DATABASE, tidb_parser::SqlMode::default(), true)
        .unwrap_or_else(|error| panic!("{sql} should drop: {error:?}"));
}

/// The `(code, message)` a failed DROP TABLE reports.
fn drop_err(catalog: &mut Catalog, sql: &str) -> (u16, String) {
    sql_error(run_drop_table_in(
        sql,
        catalog,
        DEFAULT_DATABASE,
        tidb_parser::SqlMode::default(),
        true,
    ))
}

/// Go `db_table_test.go::TestAddNotNullColumn` (origin/master:53), the
/// deterministic half: a NOT NULL DEFAULT column added over existing rows
/// reads its default for every row.
///
/// The Go test's other half races the DDL against concurrent updates to pin
/// the online-backfill interleave; that half is not portable here (no job
/// state machine) and is noted in the receipt.
#[test]
fn add_not_null_column_fills_existing_rows_with_its_default() {
    let mut catalog = Catalog::default();
    create_ok(&mut catalog, "CREATE TABLE tnn (c1 int primary key auto_increment, c2 int)");
    for _ in 0..100 {
        insert_ok(&mut catalog, "INSERT INTO tnn (c2) VALUES (0)");
    }
    run_alter_table_in(
        "ALTER TABLE tnn ADD COLUMN c3 int not null default 3",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    let rows = select_rows(&catalog, "SELECT c2, c3 FROM tnn WHERE c1 = 99");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Datum::Int(3), "the pre-existing row reads the default");
}

/// Go `db_table_test.go::TestAddNotNullColumnWhileInsertOnDupUpdate`
/// (origin/master:84), the deterministic half: the added column lands at its
/// `AFTER` position and existing rows read the default there.
#[test]
fn add_not_null_column_after_a_lands_at_the_right_offset() {
    let mut catalog = Catalog::default();
    create_ok(&mut catalog, "CREATE TABLE nn (a int primary key, b int)");
    insert_ok(&mut catalog, "INSERT INTO nn VALUES (1, 1)");
    run_alter_table_in(
        "ALTER TABLE nn ADD COLUMN c int not null default 3 AFTER a",
        &mut catalog,
        DEFAULT_DATABASE,
        &ctx(),
    )
    .unwrap();
    insert_ok(&mut catalog, "INSERT INTO nn (a, b) VALUES (2, 5)");
    let rows = select_rows(&catalog, "SELECT * FROM nn ORDER BY a");
    assert_eq!(rows[0].len(), 3, "c is a real column");
    assert_eq!(rows[0][1], Datum::Int(3), "c sits between a and b for the old row");
    assert_eq!(rows[1][1], Datum::Int(3));
}

/// Go `db_table_test.go::TestAddNotNullColumn` /
/// `TestTransactionOnAddDropColumn` / `TestAddColumn2`
/// (origin/master:53, :110, :810): the online-schema-change halves.
#[test]
#[ignore]
fn add_drop_column_interleaves_with_transactions_at_every_schema_state() {
    // go-parity-gap: Go drives the DDL job through WRITE-ONLY /
    // DELETE-ONLY reorganization states with failpoints (`beforeRunOneJobStep`)
    // and concurrent transactions; this tier applies DDL directly with no job
    // state machine or explicit transaction surface.
}

/// Go `db_table_test.go::TestCreateTableWithSetCol` (origin/master:172):
/// a SET column's DEFAULT resolves to the members the value names -- a
/// comma-joined string for the mask, an integer treated as the mask -- and
/// anything outside the member list is 1067.
#[test]
fn set_column_defaults_resolve_to_member_masks() {
    let mut catalog = Catalog::default();
    create_ok(&mut catalog, "CREATE TABLE t_set (a int, b set('e') default '')");
    create_ok(&mut catalog, "CREATE TABLE t_set2 (a set('a', 'b', 'c', 'd') default 'a,c,c')");
    // The written default folds DUPLICATES away: 'a,c,c' stores as 'a,c'.
    assert_eq!(default_text(&catalog, "t_set2", "a"), "a,c");

    // Failure rows: Go refuses each with ErrInvalidDefault.
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t_set_bad (a set('1', '4', '10') default '3')"), Some(1067));
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t_set_bad (a set('1', '4', '10') default '1,4,11')"), Some(1067));
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t_set_bad (a set('1', '4', '10') default 0)"), Some(1067));
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t_set_bad (a set('1', '4', '10') default 8)"), Some(1067));
    // A member with surrounding spaces still parses member-by-member.
    create_ok(&mut catalog, "CREATE TABLE t_set_sp (a set('1', '4', '10') default '1 ,4')");

    // Integer defaults are the BITMASK.
    let int_default = |catalog: &mut Catalog, value: &str| {
        let sql = format!("CREATE TABLE t_set_int (a set('1', '4', '10', '21') default {value})");
        run_create_table_on(&sql, catalog).unwrap();
        let stored = default_text(catalog, "t_set_int", "a");
        catalog.drop_table_in(DEFAULT_DATABASE, "t_set_int");
        stored
    };
    assert_eq!(int_default(&mut catalog, "1"), "1");
    assert_eq!(int_default(&mut catalog, "2"), "4");
    assert_eq!(int_default(&mut catalog, "3"), "1,4");
    // Go leaves the last table in place and inserts into it.
    create_ok(&mut catalog, "CREATE TABLE t_set_int (a set('1', '4', '10', '21') default 15)");
    assert_eq!(default_text(&catalog, "t_set_int", "a"), "1,4,10,21");
    insert_ok(&mut catalog, "INSERT INTO t_set_int VALUES ()");
    assert_eq!(select_rows(&catalog, "SELECT * FROM t_set_int").len(), 1);
}

/// Go `db_table_test.go::TestCreateTableWithEnumCol` (origin/master:229):
/// an ENUM column's integer DEFAULT names the member at that position, and
/// anything outside `1..n` is 1067.
#[test]
fn enum_column_defaults_resolve_to_members() {
    let mut catalog = Catalog::default();
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t_enum (a enum('1', '4', '10') default '3')"), Some(1067));
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t_enum (a enum('1', '4', '10') default '')"), Some(1067));
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t_enum (a enum('1', '4', '10') default 0)"), Some(1067));
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t_enum (a enum('1', '4', '10') default 8)"), Some(1067));

    // default 2 means the second member.
    create_ok(&mut catalog, "CREATE TABLE t_enum (a enum('2', '3', '4') default 2)");
    assert_eq!(default_text(&catalog, "t_enum", "a"), "3");
    catalog.drop_table_in(DEFAULT_DATABASE, "t_enum");
    create_ok(&mut catalog, "CREATE TABLE t_enum (a enum('a', 'c', 'd') default 2)");
    insert_ok(&mut catalog, "INSERT INTO t_enum VALUES ()");
    let rows = select_rows(&catalog, "SELECT * FROM t_enum");
    match &rows[0][0] {
        Datum::Enum(value, _) => assert_eq!(value.name_bytes(), b"c", "member two of ('a','c','d')"),
        Datum::Bytes(bytes) => assert_eq!(bytes.as_slice(), b"c"),
        Datum::String(string) => assert_eq!(string.bytes(), b"c"),
        other => panic!("expected the enum member c, got {other:?}"),
    }
}

/// Go `db_table_test.go::TestCreateTableWithIntegerColWithDefault`
/// (origin/master:261): fractional integer defaults truncate toward the
/// integer (1.25 -> 1, -2.8 -> -3) and out-of-range defaults are 1067.
#[test]
fn integer_column_defaults_truncate_or_refuse() {
    let mut catalog = Catalog::default();
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t1 (a tinyint unsigned default -1.25)"), Some(1067));
    assert_eq!(create_err_code(&mut catalog, "CREATE TABLE t1 (a tinyint default 999999999)"), Some(1067));

    let default_of = |catalog: &mut Catalog, sql: &str, column: &str| {
        run_create_table_on(sql, catalog).unwrap();
        let stored = default_text(catalog, "t1", column);
        catalog.drop_table_in(DEFAULT_DATABASE, "t1");
        stored
    };
    assert_eq!(default_of(&mut catalog, "CREATE TABLE t1 (a tinyint unsigned default 1.25)", "a"), "1");
    assert_eq!(default_of(&mut catalog, "CREATE TABLE t1 (a smallint default -1.25)", "a"), "-1");
    assert_eq!(default_of(&mut catalog, "CREATE TABLE t1 (a mediumint default 2.8)", "a"), "3");
    assert_eq!(default_of(&mut catalog, "CREATE TABLE t1 (a int default -2.8)", "a"), "-3");
    assert_eq!(default_of(&mut catalog, "CREATE TABLE t1 (a bigint unsigned default 0.0)", "a"), "0");
    assert_eq!(default_of(&mut catalog, "CREATE TABLE t1 (a float default '0012.43')", "a"), "12.43");
    assert_eq!(default_of(&mut catalog, "CREATE TABLE t1 (a double default '12.4300')", "a"), "12.43");
}

/// Go `db_table_test.go::TestCreateTableWithInfo` (origin/master:310).
#[test]
#[ignore]
fn batch_create_table_with_info_allocates_or_honors_table_ids() {
    // go-parity-gap: Go drives `DDLExecutor.BatchCreateTableWithInfo` with
    // pre-allocated and generated table ids; this tier has no
    // BatchCreateTableWithInfo surface (CREATE TABLE from SQL only).
}

/// Go `db_table_test.go::TestBatchCreateTable` (origin/master:349).
#[test]
#[ignore]
fn batch_create_table_records_one_job_for_three_names() {
    // go-parity-gap: the same BatchCreateTableWithInfo surface (plus
    // `admin show ddl jobs` and TTL external-workload registration) is not
    // modelled in this tier.
}

/// Go `db_table_test.go::TestTableLock` (origin/master:480).
#[test]
#[ignore]
fn lock_tables_write_then_unlock_and_drop() {
    // go-parity-gap: `LOCK TABLES`/`UNLOCK TABLES` metadata and its
    // interaction with DROP TABLE are not modelled in this tier.
}

/// Go `db_table_test.go::TestTableLocksLostCommit` (origin/master:515).
#[test]
#[ignore]
fn table_locks_are_lost_when_the_locking_session_closes() {
    // go-parity-gap: LOCK TABLES and session-close cleanup are not modelled.
}

/// Go `db_table_test.go::TestWriteLocal` (origin/master:556).
#[test]
#[ignore]
fn write_local_lock_allows_reads_and_forbids_writes() {
    // go-parity-gap: LOCK TABLES ... WRITE LOCAL is not modelled.
}

/// Go `db_table_test.go::TestLockTables` (origin/master:605).
#[test]
#[ignore]
fn lock_tables_matrix_across_sessions_and_statements() {
    // go-parity-gap: the full LOCK TABLES matrix (read/write/local mutexes,
    // admin cleanup locks, lock-refusing statements while locked) is not
    // modelled in this tier.
}

/// Go `db_table_test.go::TestTablesLockDelayClean` (origin/master:778).
#[test]
#[ignore]
fn table_lock_cleanup_waits_for_the_configured_delay() {
    // go-parity-gap: `config.DelayCleanTableLock` and session-close lock
    // cleanup are not modelled.
}

/// Go `db_table_test.go::TestDropTables` (origin/master:876): without
/// IF EXISTS every existing table in the list IS dropped and ONE 1051 names
/// every missing one.
#[test]
fn drop_tables_reports_every_missing_name_after_dropping_the_rest() {
    let mut catalog = Catalog::default();
    assert_eq!(drop_err(&mut catalog, "DROP TABLE t1").0, 1051);
    assert_eq!(drop_err(&mut catalog, "DROP TABLE test2.t1").0, 1051);

    // IF EXISTS in any position.
    create_ok(&mut catalog, "CREATE TABLE t1 (a int)");
    drop_ok(&mut catalog, "DROP TABLE IF EXISTS t1, t2");
    create_ok(&mut catalog, "CREATE TABLE t1 (a int)");
    drop_ok(&mut catalog, "DROP TABLE IF EXISTS t2, t1");

    // Without IF EXISTS the existing table still goes, and the error names
    // every missing table.
    create_ok(&mut catalog, "CREATE TABLE t1 (a int)");
    let (code, message) = drop_err(&mut catalog, "DROP TABLE t1, t2");
    assert_eq!(code, 1051);
    assert_eq!(message, "Unknown table 'test.t2'");
    assert!(!catalog.contains_in(DEFAULT_DATABASE, "t1"), "t1 was dropped despite the error");

    create_ok(&mut catalog, "CREATE TABLE t1 (a int)");
    let (code, message) = drop_err(&mut catalog, "DROP TABLE t2, t1");
    assert_eq!(code, 1051);
    assert_eq!(message, "Unknown table 'test.t2'");
    assert!(!catalog.contains_in(DEFAULT_DATABASE, "t1"));
}

/// Go `db_table_test.go::TestCreateConstraintForTable` (origin/master:909).
#[test]
#[ignore]
fn check_constraint_names_are_unique_across_tables() {
    // go-parity-gap: this tier models CHECK constraints only with
    // `tidb_enable_check_constraint` OFF (they are discarded at DDL time);
    // Go's ON mode, which stores named constraints and refuses a duplicate
    // name with ErrCheckConstraintDupName, is not modelled.
}
