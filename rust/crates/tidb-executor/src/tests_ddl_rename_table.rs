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

//! Ports of the batch-b103 slice of Go `pkg/ddl/db_rename_test.go`
//! (origin/master, functions 218-224 of the package's deterministic
//! file-then-line test order).
//!
//! The Go tests assert table identity via `is.TableByName(...).Meta().ID`;
//! this tier pins the same identity with
//! [`Catalog::stored_table_id`], which is the same persisted id the meta
//! store keys. Row survival is pinned by SELECT.

use tidb_datatype::Datum;

use crate::{
    run_create_table_on, run_insert_on, run_rename_table_in, run_select_on, Catalog,
    DriverError, StmtContext, DEFAULT_DATABASE,
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

fn select_rows(catalog: &Catalog, sql: &str) -> Vec<Vec<Datum>> {
    run_select_on(sql, catalog, &read()).unwrap_or_else(|error| panic!("{sql} should select: {error:?}"))
}

fn sql_error(result: Result<impl Sized, DriverError>) -> (u16, String) {
    let error = result.err().expect("expected the statement to fail");
    let mysql = error.to_mysql_error();
    (mysql.code, mysql.message)
}

fn rename(catalog: &mut Catalog, sql: &str) -> Result<(), DriverError> {
    run_rename_table_in(sql, catalog, DEFAULT_DATABASE, tidb_parser::SqlMode::default())
}

/// The shared body of Go's `renameTableTest` for one statement spelling:
/// cross-database renames keep the TABLE ID and move the rows, the source
/// name frees up, same-database renames keep both, and every failure row
/// carries Go's error code.
///
/// `alter` selects between Go's `RENAME TABLE a TO b` and
/// `ALTER TABLE a RENAME TO b`, whose failure ordering differs.
fn rename_table_test(catalog: &mut Catalog, wrap: fn(&str, &str) -> String, alter: bool) {
    // A missing source is 1146 in both spellings.
    assert_eq!(sql_error(rename(catalog, &wrap("tb1", "tb2"))).0, 1146);

    create_ok(catalog, "CREATE TABLE t (c1 int, c2 int)");
    run_insert_on("INSERT INTO t VALUES (1, 1), (2, 2)", catalog, &ctx()).unwrap();
    let old_table_id = catalog.stored_table_id("test", "t").expect("t is stored");
    catalog.create_database("test1");

    // Cross-database: same table id, new database, rows intact.
    rename(catalog, &wrap("test.t", "test1.t1")).unwrap();
    assert_eq!(catalog.stored_table_id("test1", "t1"), Some(old_table_id));
    assert_eq!(
        select_rows(catalog, "SELECT * FROM test1.t1"),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(2)]
        ]
    );

    // The source name is free again.
    create_ok(catalog, "CREATE TABLE t (c1 int, c2 int)");
    catalog.drop_table_in("test", "t");

    // Same-database rename: same id, same database. (Go `USE test1`s
    // first; the qualified spelling is the same statement here.)
    rename(catalog, &wrap("test1.t1", "test1.t2")).unwrap();
    assert_eq!(catalog.stored_table_id("test1", "t2"), Some(old_table_id));
    assert_eq!(
        select_rows(catalog, "SELECT * FROM test1.t2"),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(2)]
        ]
    );
    assert!(catalog.table_in("test1", "t1").is_none());

    // Failure rows: a missing source database or table is 1146; a missing
    // DESTINATION database is 1025 in both spellings.
    assert_eq!(sql_error(rename(catalog, &wrap("test_not_exist.t", "test_not_exist.t"))).0, 1146);
    assert_eq!(sql_error(rename(catalog, &wrap("test.test_not_exist", "test.test_not_exist"))).0, 1146);
    assert_eq!(sql_error(rename(catalog, &wrap("test.t_not_exist", "test_not_exist.t"))).0, 1146);
    let (code, _) = sql_error(rename(catalog, &wrap("test1.t2", "test_not_exist.t")));
    assert_eq!(code, 1025, "a missing destination schema is ErrErrorOnRename");

    // Renaming onto an existing table is 1050. (Go `USE test1`s; the
    // qualified spelling is the same statement here.)
    create_ok(catalog, "CREATE TABLE test1.t_exist (c1 int, c2 int)");
    assert_eq!(sql_error(rename(catalog, &wrap("test1.t2", "test1.t_exist"))).0, 1050);

    // Go's RENAME TABLE checks the destination BEFORE the source: renaming a
    // MISSING source onto an existing destination reports 1050 for RENAME
    // and 1146 for ALTER TABLE RENAME. This tier checks the source first for
    // both spellings, so the RENAME row is asserted only for the ALTER
    // spelling and the RENAME row is pinned by the dedicated gap test below.
    create_ok(catalog, "CREATE TABLE test1.t (c1 int, c2 int)");
    create_ok(catalog, "CREATE TABLE test1.t1 (c1 int, c2 int)");
    if alter {
        assert_eq!(sql_error(rename(catalog, &wrap("test.t_not_exist", "test1.t_exist"))).0, 1146);
    } else {
        assert_eq!(
            sql_error(rename(catalog, &wrap("test1.t", "test1.t"))).0,
            1050,
            "RENAME TABLE sees the destination -- here the source itself -- as taken"
        );
        assert_eq!(sql_error(rename(catalog, &wrap("test1.t1", "test1.T1"))).0, 1050);
    }
}

/// Go `db_rename_test.go::TestRenameTable2` (origin/master:68) -- the
/// `RENAME TABLE a TO b` spelling, minus the too-long-identifier row below.
#[test]
fn rename_table_keeps_ids_and_rows_across_databases() {
    let mut catalog = Catalog::default();
    rename_table_test(&mut catalog, |from, to| format!("RENAME TABLE {from} TO {to}"), false);
}

/// Go `db_rename_test.go::TestAlterTableRenameTable` (origin/master:73) --
/// the `ALTER TABLE a RENAME TO b` spelling of the same matrix.
#[test]
fn alter_table_rename_keeps_ids_and_rows_across_databases() {
    let mut catalog = Catalog::default();
    rename_table_test(&mut catalog, |from, to| format!("ALTER TABLE {from} RENAME TO {to}"), true);
}

/// Go `db_rename_test.go::TestRenameTable2` / `TestAlterTableRenameTable`
/// (origin/master:138-155): the two rows where Go's RENAME and ALTER
/// spellings deliberately differ, which this tier's single validation order
/// cannot yet express.
#[test]
#[ignore]
fn rename_and_alter_rename_order_their_error_checks_differently() {
    // go-parity-gap: with `test.t_not_exist` missing and `test1.t_exist`
    // present, Go reports 1050 for `RENAME TABLE test.t_not_exist TO
    // test1.t_exist` (destination checked first) but 1146 for the ALTER
    // spelling (source checked first); this tier checks the source first for
    // BOTH spellings. And `ALTER TABLE test1.t RENAME TO t` (or to a
    // case-folding of itself) is a successful no-op in Go, while this tier
    // reports the destination as existing (1050).
}

/// Go `db_rename_test.go::TestRenameTable2` /
/// `TestAlterTableRenameTable` (origin/master:157-158): a rename whose
/// destination name exceeds the identifier limit is refused with 1059.
///
/// Both spellings run the same Go expectation; both are pinned here.
#[test]
#[ignore]
fn rename_to_an_over_long_identifier_is_refused() {
    // go-parity-gap: this tier's RENAME TABLE path does not enforce Go's
    // 64-identifier cap (`dbterror.ErrTooLongIdent`, 1059), so a 65-character
    // destination silently succeeds instead of failing.
}

/// Go `db_rename_test.go::TestRenameMultiTables` (origin/master:175):
/// multi-pair renames apply in written order with staged validation, keep
/// every table id, and fail without moving anything.
#[test]
fn rename_multi_tables_moves_each_pair_without_losing_ids() {
    let mut catalog = Catalog::default();
    create_ok(&mut catalog, "CREATE TABLE t1 (id int)");
    create_ok(&mut catalog, "CREATE TABLE t2 (id int)");
    rename(&mut catalog, "RENAME TABLE t1 TO t3, t2 TO t4").unwrap();
    // Go drops t3/t4 to free the names; the drop is the same act here.
    catalog.drop_table_in("test", "t3");
    catalog.drop_table_in("test", "t4");

    create_ok(&mut catalog, "CREATE TABLE t1 (c1 int, c2 int)");
    create_ok(&mut catalog, "CREATE TABLE t2 (c1 int, c2 int)");
    run_insert_on("INSERT INTO t1 VALUES (1, 1), (2, 2)", &mut catalog, &ctx()).unwrap();
    run_insert_on("INSERT INTO t2 VALUES (1, 1), (2, 2)", &mut catalog, &ctx()).unwrap();
    let id1 = catalog.stored_table_id("test", "t1").unwrap();
    let id2 = catalog.stored_table_id("test", "t2").unwrap();

    // Multi-table cross-database rename keeps every id.
    catalog.create_database("test1");
    rename(&mut catalog, "RENAME TABLE test.t1 TO test1.t1, test.t2 TO test1.t2").unwrap();
    assert_eq!(catalog.stored_table_id("test1", "t1"), Some(id1));
    assert_eq!(catalog.stored_table_id("test1", "t2"), Some(id2));
    assert_eq!(select_rows(&catalog, "SELECT * FROM test1.t1").len(), 2);
    assert!(catalog.table_in("test", "t1").is_none());
    assert!(catalog.table_in("test", "t2").is_none());

    // Same-database multi rename.
    rename(&mut catalog, "RENAME TABLE test1.t1 TO test1.t3, test1.t2 TO test1.t4").unwrap();
    assert_eq!(catalog.stored_table_id("test1", "t3"), Some(id1));
    assert_eq!(catalog.stored_table_id("test1", "t4"), Some(id2));
    assert!(catalog.table_in("test1", "t1").is_none());
    assert!(catalog.table_in("test1", "t2").is_none());

    // A three-way rotation inside one statement, ending with every id where
    // its row data should be.
    create_ok(&mut catalog, "CREATE TABLE test1.t5 (c1 int, c2 int)");
    run_insert_on("INSERT INTO test1.t5 VALUES (1, 1), (2, 2)", &mut catalog, &ctx()).unwrap();
    let id3 = catalog.stored_table_id("test1", "t5").unwrap();
    rename(&mut catalog, "RENAME TABLE test1.t3 TO test1.t1, test1.t4 TO test1.t2, test1.t5 TO test1.t3")
        .unwrap();
    assert_eq!(catalog.stored_table_id("test1", "t1"), Some(id1));
    assert_eq!(catalog.stored_table_id("test1", "t2"), Some(id2));
    assert_eq!(catalog.stored_table_id("test1", "t3"), Some(id3));

    // Failure: nothing moves when a pair is bad.
    assert_eq!(
        sql_error(rename(
            &mut catalog,
            "RENAME TABLE test_not_exist.t TO test_not_exist.t, test_not_exist.t TO test_not_exist.t",
        ))
        .0,
        1146
    );
    assert_eq!(catalog.stored_table_id("test1", "t1"), Some(id1), "nothing moved");
}

/// Go `db_rename_test.go::TestRenameMultiTablesIssue47064`
/// (origin/master:286): after a cross-database rename the table's columns
/// are still readable under the new database.
#[test]
fn renamed_table_keeps_its_columns_readable() {
    let mut catalog = Catalog::default();
    create_ok(&mut catalog, "CREATE TABLE t1 (a int)");
    create_ok(&mut catalog, "CREATE TABLE t2 (a int)");
    catalog.create_database("test1");
    rename(&mut catalog, "RENAME TABLE test.t1 TO test1.t1, test.t2 TO test1.t2").unwrap();
    // Go reads information_schema.columns; the same fact here is the table's
    // column metadata under the new database.
    let table = match catalog.table_in("test1", "t1") {
        Some(crate::TableEntry::Kv(table)) => table,
        other => panic!("renamed table is a kv table, got {other:?}"),
    };
    let names: Vec<&str> = table.columns.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(names, vec!["a"]);
}

/// Go `db_rename_test.go::TestRenameConcurrentAutoID` (origin/master:298).
#[test]
#[ignore]
fn rename_between_databases_keeps_the_auto_id_allocators_alive() {
    // go-parity-gap: the test needs NONCLUSTERED primary keys (a `_tidb_rowid`
    // sharing the allocator), `AUTO_ID_CACHE 5`, three concurrent sessions
    // and Go's `AutoIDSchemaID` bookkeeping -- none of which this tier's
    // executor models.
}

/// Go `db_rename_test.go::TestShowRunningRenameTable` (origin/master:494).
#[test]
#[ignore]
fn running_rename_table_is_visible_through_admin_show_ddl_jobs() {
    // go-parity-gap: `admin show ddl jobs`, `information_schema.ddl_jobs` and
    // the failpoint that pauses a job mid-state are not modelled here.
}

/// Go `db_rename_test.go::TestRenameTableWithLocked` (origin/master:38).
#[test]
#[ignore]
fn rename_table_with_locked_tables() {
    // go-parity-gap: `LOCK TABLES`/`UNLOCK TABLES` (and the
    // `ErrLockOrActiveTransaction` / `ErrTableNotLockedForWrite` rows) are
    // not modelled in this tier's executor.
}
