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

//! `ADMIN CHECK TABLE` / `ADMIN CHECK INDEX` through a live [`Session`].
//!
//! Every expectation here is TiDB's own, taken from the statements and
//! outputs recorded in `tests/integrationtest/t/util/admin.test`,
//! `tests/integrationtest/t/executor/admin.test` and their `r/*.result`
//! files -- read, never written.
//!
//! # The test that matters most
//!
//! [`a_corrupted_index_is_caught`] deletes an index entry out from under a
//! table and requires `ADMIN CHECK` to fail. Without it, every other test
//! here would pass just as happily against a `check_table` that returned
//! `Ok(0)` without reading anything -- which is the failure mode this whole
//! statement exists to make impossible.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// The `(code, message)` a failed statement reports on the wire.
fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).expect_err(sql);
    let mysql = error.to_mysql_error();
    (mysql.code, mysql.message)
}

/// Asserts a statement passed `ADMIN CHECK` and produced NO OUTPUT.
///
/// Go's `CheckTable` plan is a `SimpleSchemaProducer` that never sets a
/// schema, so the server replies with an OK packet rather than a zero-column
/// result set -- which is why `r/util/admin.result` has nothing at all under
/// the statement, not even a blank header line.
fn assert_check_passes(session: &mut Session, sql: &str) {
    match session.run_with_columns(sql).expect(sql) {
        StmtOutput::Affected(0) => {}
        other => panic!("{sql} answered {other:?}"),
    }
}

/// `tests/integrationtest/t/util/admin.test`, `TestAdminCheckTable`'s first
/// case: an unsigned integer primary key with a composite secondary index.
#[test]
fn unsigned_primary_key_with_composite_index() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t(a bigint unsigned primary key, b int, c int, index idx(a, b))")
        .unwrap();
    session.run("insert into t values(1, 1, 1)").unwrap();
    assert_check_passes(&mut session, "admin check table t");
    assert_check_passes(&mut session, "admin check index t idx");
}

/// `TestAdminCheckTableClusterIndex`: a clustered composite primary key with
/// two secondary indexes, checked as rows accumulate.
#[test]
fn clustered_primary_key_with_two_indexes() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run(
            "create table t (a bigint, b varchar(255), c int, primary key (a, b), \
             index idx_0(a, b), index idx_1(b, c))",
        )
        .unwrap();
    session.run("insert into t values (1, '1', 1)").unwrap();
    session.run("insert into t values (2, '2', 2)").unwrap();
    assert_check_passes(&mut session, "admin check table t");
    for n in 3..=20 {
        session
            .run(&format!("insert into t values ({n}, '{n}', {n})"))
            .unwrap();
    }
    assert_check_passes(&mut session, "admin check table t");
    assert_check_passes(&mut session, "admin check index t idx_0");
    assert_check_passes(&mut session, "admin check index t idx_1");
}

/// `tests/integrationtest/t/executor/admin.test`: a unique index that holds
/// NULLs, which MySQL allows any number of. A NULL-bearing entry is stored
/// the NON-distinct way (handle appended to the key), so this is the case
/// that would break a check written against distinct entries only.
#[test]
fn unique_index_holding_nulls() {
    let mut session = Session::new();
    session.run("drop table if exists admin_test").unwrap();
    session
        .run("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2))")
        .unwrap();
    session
        .run(
            "insert admin_test (c1, c2) values (1, 1), (2, 2), (5, 5), (10, 10), (11, 11), \
             (NULL, NULL)",
        )
        .unwrap();
    assert_check_passes(&mut session, "admin check table admin_test");
    assert_check_passes(&mut session, "admin check index admin_test c1");
    assert_check_passes(&mut session, "admin check index admin_test c2");
}

/// The extremes of the integer handle domain, from `executor/admin.test`:
/// `bigint unsigned primary key` holding `9223372036854775807`.
#[test]
fn handles_at_the_domain_edge() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t(a bigint unsigned primary key, b int, c int, index idx(a, b))")
        .unwrap();
    session
        .run("insert into t values(1, 1, 1), (9223372036854775807, 2, 2)")
        .unwrap();
    assert_check_passes(&mut session, "admin check index t idx");
    assert_check_passes(&mut session, "admin check table t");
}

/// A table with no index at all: nothing to disagree with, and Go's
/// `CheckTableExec` returns immediately (`len(e.srcs) == 0`).
#[test]
fn a_table_with_no_index_passes_trivially() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session.run("create table t (a int, b int)").unwrap();
    session.run("insert into t values (1, 1), (2, 2)").unwrap();
    assert_check_passes(&mut session, "admin check table t");
}

/// An empty table, and a table whose rows were all deleted -- the second is
/// `executor/admin.test`'s `delete from t1; admin check table t1;`, which
/// catches a check that never re-reads the index after the deletes.
#[test]
fn an_emptied_table_passes() {
    let mut session = Session::new();
    session.run("drop table if exists t1").unwrap();
    session
        .run("create table t1 (a int, b varchar(10), index idx_a(a), index idx_b(b))")
        .unwrap();
    assert_check_passes(&mut session, "admin check table t1");
    session
        .run("insert into t1 values (1, 'x'), (2, 'y'), (3, 'z')")
        .unwrap();
    assert_check_passes(&mut session, "admin check table t1");
    session.run("delete from t1").unwrap();
    assert_check_passes(&mut session, "admin check table t1");
}

/// `UPDATE` moves a row's indexed value, so both the old entry must go and
/// the new one must appear. A check that only looked row -> index would pass
/// a stale leftover entry; a check that only looked index -> row would pass
/// a missing one.
#[test]
fn an_update_keeps_both_directions_consistent() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a int primary key, b varchar(10), index idx_b(b))")
        .unwrap();
    session
        .run("insert into t values (1, 'aa'), (2, 'bb'), (3, 'cc')")
        .unwrap();
    session.run("update t set b = 'zz' where a = 2").unwrap();
    assert_check_passes(&mut session, "admin check table t");
    session.run("delete from t where a = 1").unwrap();
    assert_check_passes(&mut session, "admin check table t");
}

/// A generated column in an index: the index stores the generated value, and
/// the row re-materializes it on read. `util/admin.test` covers this with a
/// JSON-extract table; this is the same question with an arithmetic
/// expression, which this tier's DDL admits.
#[test]
fn a_generated_column_in_an_index() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a int, b int as (a + 1), index idx_b(b))")
        .unwrap();
    session
        .run("insert into t (a) values (1), (2), (3)")
        .unwrap();
    assert_check_passes(&mut session, "admin check table t");
}

/// `admin check index t idx (begin, end)` -- the ONE `ADMIN CHECK` form that
/// returns rows.
///
/// Captured verbatim from `tests/integrationtest/r/executor/admin.result`:
///
/// ```text
/// create table check_index_test (a int, b varchar(10), index a_b (a, b), index b (b));
/// insert check_index_test values (3, "ab"),(2, "cd"),(1, "ef"),(-1, "hi");
/// admin check index check_index_test a_b (2, 4);
/// a       b       extra_handle
/// 1       ef      3
/// 2       cd      2
/// admin check index check_index_test a_b (3, 5);
/// a       b       extra_handle
/// -1      hi      4
/// 1       ef      3
/// ```
///
/// The interval bounds the HANDLE (half-open), and the rows come back in
/// INDEX order, not handle order -- which is why the two outputs are not
/// sorted the same way.
#[test]
fn check_index_with_handle_ranges_returns_rows() {
    let mut session = Session::new();
    session
        .run("drop table if exists check_index_test")
        .unwrap();
    session
        .run("create table check_index_test (a int, b varchar(10), index a_b (a, b), index b (b))")
        .unwrap();
    session
        .run("insert check_index_test values (3, 'ab'),(2, 'cd'),(1, 'ef'),(-1, 'hi')")
        .unwrap();

    let (columns, rows) = query_text(
        &mut session,
        "admin check index check_index_test a_b (2, 4)",
    );
    assert_eq!(columns, vec!["a", "b", "extra_handle"]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "ef".to_owned(), "3".to_owned()],
            vec!["2".to_owned(), "cd".to_owned(), "2".to_owned()],
        ]
    );

    let (_, rows) = query_text(
        &mut session,
        "admin check index check_index_test a_b (3, 5)",
    );
    assert_eq!(
        rows,
        vec![
            vec!["-1".to_owned(), "hi".to_owned(), "4".to_owned()],
            vec!["1".to_owned(), "ef".to_owned(), "3".to_owned()],
        ]
    );

    // Two intervals in one statement, from the same recording:
    //   admin check index executor__admin.check_index_test a_b (2, 3), (4, 5);
    //   -1  hi  4
    //   2   cd  2
    let (_, rows) = query_text(
        &mut session,
        "admin check index check_index_test a_b (2, 3), (4, 5)",
    );
    assert_eq!(
        rows,
        vec![
            vec!["-1".to_owned(), "hi".to_owned(), "4".to_owned()],
            vec!["2".to_owned(), "cd".to_owned(), "2".to_owned()],
        ]
    );
}

/// THE test this module exists for: with an index entry physically removed,
/// `ADMIN CHECK` must FAIL. A check that returns `Ok` here is a success
/// return that is not one, and every other test in this file would still
/// pass.
///
/// The removal goes through the catalog's own `KvTable`, so the statement
/// path sees exactly the store a corrupted table would have.
#[test]
fn a_corrupted_index_is_caught() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a int primary key, b varchar(10), index idx_b(b))")
        .unwrap();
    session
        .run("insert into t values (1, 'aa'), (2, 'bb'), (3, 'cc')")
        .unwrap();
    assert_check_passes(&mut session, "admin check table t");

    // Drop ONE entry of `idx_b`, leaving the rows untouched.
    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
            else {
                panic!("t is not stored as bytes");
            };
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.name.eq_ignore_ascii_case("idx_b"))
                .expect("idx_b");
            let entries = table.index_entries_for_check(index.id).expect("entries");
            assert_eq!(entries.len(), 3, "three rows, three entries");
            table
                .delete_raw_key_for_test(&entries[0].0)
                .expect("delete the entry");
            Ok(())
        })
        .unwrap();

    // Go reports the row/entry count difference as 8003 for `ADMIN CHECK
    // INDEX` (`admin.CheckIndicesCount`'s own error) ...
    let (code, message) = error_of(&mut session, "admin check index t idx_b");
    assert_eq!(code, 8003, "{message}");
    assert!(
        message.contains("table count 3 != index(idx_b) count 2"),
        "{message}"
    );

    // ... and drills into WHICH row lost its entry for `ADMIN CHECK TABLE`,
    // which is `ErrDataInconsistent`, 8223.
    let (code, message) = error_of(&mut session, "admin check table t");
    assert_eq!(code, 8223, "{message}");
    assert!(
        message.starts_with("data inconsistency in table: t, index: idx_b, handle: "),
        "{message}"
    );
}

/// The mirror of [`a_corrupted_index_is_caught`]: a row removed from under a
/// complete index. This is the direction a row -> index check alone cannot
/// see, so it proves the index -> row leg runs too.
#[test]
fn an_orphaned_index_entry_is_caught() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a int primary key, b varchar(10), index idx_b(b))")
        .unwrap();
    session
        .run("insert into t values (1, 'aa'), (2, 'bb'), (3, 'cc')")
        .unwrap();

    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
            else {
                panic!("t is not stored as bytes");
            };
            table
                .delete_record_for_test(&tidb_executor::kv_table::TableHandle::Int(2))
                .expect("delete the row");
            Ok(())
        })
        .unwrap();

    let (code, message) = error_of(&mut session, "admin check table t");
    assert_eq!(code, 8223, "{message}");
    assert!(message.contains("index: idx_b"), "{message}");
}

/// With equal counts Go's `CheckTableExec` checks INDEX -> ROW. Swapping two
/// unique-index handles therefore reports the first stored entry against the
/// row that entry names, as the column-level 8134 mismatch.
#[test]
fn a_unique_index_entry_naming_the_wrong_row_reports_both_records() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a int primary key, b int, unique index idx_b(b))")
        .unwrap();
    session
        .run("insert into t values (1, 10), (2, 20)")
        .unwrap();

    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
            else {
                panic!("t is not stored as bytes");
            };
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.name.eq_ignore_ascii_case("idx_b"))
                .expect("idx_b");
            let entries = table.index_entries_for_check(index.id).expect("entries");
            assert_eq!(entries.len(), 2);
            table
                .swap_raw_values_for_test(&entries[0].0, &entries[1].0)
                .expect("swap the unique-index handles");
            Ok(())
        })
        .unwrap();

    let (code, message) = error_of(&mut session, "admin check table t");
    assert_eq!(code, 8134, "{message}");
    assert!(
        message.contains("col: b, handle: \"2\", index-values:\"KindInt64 10\""),
        "{message}"
    );
    assert!(
        message.contains("record-values:\"KindInt64 20\", compare err:<nil>"),
        "{message}"
    );
}

/// Go `TestAdminCheckGlobalIndex`'s equal-count corruption: an index entry
/// still names a live row, but its indexed datum differs from that row. The
/// normal checker reports the column-level executor error 8134 rather than
/// the general missing-entry error 8223.
#[test]
fn an_index_value_mismatch_reports_the_column_and_both_values() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a int primary key, b int, unique index idx_b(b))")
        .unwrap();
    session
        .run("insert into t values (1, 10), (2, 20)")
        .unwrap();

    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
            else {
                panic!("t is not stored as bytes");
            };
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.name.eq_ignore_ascii_case("idx_b"))
                .expect("idx_b");
            let entries = table.index_entries_for_check(index.id).expect("entries");
            let (old_key, _) = entries
                .iter()
                .find(|(_, handle)| *handle == tidb_executor::kv_table::TableHandle::Int(2))
                .expect("row 2 index entry");
            let row = vec![tidb_datatype::Datum::Int(2), tidb_datatype::Datum::Int(100)];
            let (new_key, _) = table
                .index_key_for_check(
                    &index,
                    &row,
                    &tidb_executor::kv_table::TableHandle::Int(2),
                    &tidb_datatype::SessionTimeZone::utc(),
                )
                .expect("wrong index key");
            table
                .move_raw_value_for_test(old_key, new_key)
                .expect("move the index entry");
            Ok(())
        })
        .unwrap();

    let (code, message) = error_of(&mut session, "admin check table t");
    assert_eq!(code, 8134, "{message}");
    assert_eq!(
        message,
        "data inconsistency in table: t, index: idx_b, col: b, handle: \"2\", \
         index-values:\"KindInt64 100\" != record-values:\"KindInt64 20\", compare err:<nil>"
    );
}

/// `ADMIN CHECK INDEX` naming an index the table does not have reaches Go's
/// generic error boundary because the planner returns a plain `errors.Errorf`.
#[test]
fn an_unknown_index_is_named() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a int, index idx_a(a))")
        .unwrap();
    let error = session
        .run("admin check index t nosuch")
        .expect_err("the index is absent");
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1105, "{}", mysql.message);
    assert_eq!(mysql.state, *b"HY000");
    assert_eq!(mysql.message, "secondary index nosuch does not exist");
}

/// A view has no rows of its own, so `ADMIN CHECK` over one is refused by
/// name rather than answered OK.
#[test]
fn a_view_is_refused_not_passed() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a int, index idx_a(a))")
        .unwrap();
    session.run("create view v as select * from t").unwrap();
    let (_, message) = error_of(&mut session, "admin check table v");
    assert!(
        message.contains("storage-backed table"),
        "a view must be refused, not passed: {message}"
    );
}

/// `ADMIN CHECK TABLE t1, t2` is refused: Go's planner rejects more than one
/// table for the consistency check.
#[test]
fn a_table_list_is_refused() {
    let mut session = Session::new();
    session.run("drop table if exists t1").unwrap();
    session.run("drop table if exists t2").unwrap();
    session.run("create table t1 (a int, index i(a))").unwrap();
    session.run("create table t2 (a int, index i(a))").unwrap();
    let (_, message) = error_of(&mut session, "admin check table t1, t2");
    assert!(message.contains("one table at a time"), "{message}");
}
