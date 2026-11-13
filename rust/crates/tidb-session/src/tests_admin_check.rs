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

/// Go `set tidb_enable_fast_table_check`: `FastCheckTableExec` (the
/// default) or `CheckTableExec`.
fn set_fast_check(session: &mut Session, enabled: bool) {
    session
        .run(&format!("set tidb_enable_fast_table_check = {}", u8::from(enabled)))
        .unwrap();
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

/// `TestPrefixedClusteredIndexUniqueKeyWithNewCollation` and
/// `TestCollationPrefixClusteredIndex`: a prefix index over a column the
/// clustered key also holds decodes the column whole from the handle, and Go
/// `compareData` cuts that value to the index's prefix before comparing it
/// with the row's -- the check passes. An unnamed PRIMARY KEY spends its
/// column's anonymous name, so `key (k)` is `k_2`.
#[test]
fn a_prefix_index_over_a_clustered_key_column_checks_clean() {
    let mut session = Session::new();
    session
        .run(
            "create table t (a text collate utf8mb4_general_ci not null, b int(11) not null, \
             primary key (a(10), b) clustered, key idx(a(2)) ) default charset=utf8mb4 \
             collate=utf8mb4_bin",
        )
        .unwrap();
    session.run("insert into t values ('aaa', 2)").unwrap();
    assert_check_passes(&mut session, "admin check table t");
    session.run("drop table t").unwrap();

    session
        .run(
            "create table t (k char(20), v int, primary key (k(4)) clustered, key (k)) \
             collate utf8mb4_general_ci",
        )
        .unwrap();
    session.run("insert into t values('01233', 1)").unwrap();
    session.run("create index idx on t(k(2))").unwrap();
    assert_eq!(
        row_text(session.run("select * from t use index(k_2)")),
        [["01233", "1"]]
    );
    assert_eq!(
        row_text(session.run("select * from t use index(idx)")),
        [["01233", "1"]]
    );
    assert_check_passes(&mut session, "admin check table t");
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

/// Go's physical ADMIN CHECK reader skips a clustered PRIMARY KEY because its
/// key is the record key, not a stored `_i` entry. Naming that key directly
/// still succeeds, but it must not report a missing index entry.
#[test]
fn clustered_primary_key_is_not_scanned_as_an_index() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t (a bigint, b varchar(255), primary key (a, b))")
        .unwrap();
    session.run("insert into t values (1, '1')").unwrap();

    assert_check_passes(&mut session, "admin check table t");
    assert_check_passes(&mut session, "admin check index t `PRIMARY`");
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

/// `FastCheckTableExec` splices a partial index's condition into the SQL it
/// runs through the system session's `ExecuteInternal(ctx, sql)` with no
/// arguments, which Go's `ParseWithParams` leaves unescaped
/// (`if len(args) > 0`): a `%n` inside the condition's literal is text, and
/// the check passes (Go captured: OK).
#[test]
fn a_partial_index_condition_holding_a_percent_sign_checks_clean() {
    let mut session = Session::new();
    set_fast_check(&mut session, true);
    session.run("drop table if exists t").unwrap();
    session
        .run("create table t(a int primary key, b varchar(10), index ib(b) where b = 'x%n')")
        .unwrap();
    session
        .run("insert into t values (1, 'x%n'), (2, 'xyz'), (3, 'x%%')")
        .unwrap();
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
/// Go `CheckIndexRangeExec` reads the index's own entries (an index scan
/// over `ranger.FullRange()`), so an entry missing from the index is missing
/// from the answer, and an unknown index is `buildCheckIndexSchema`'s error
/// (Go-probed: `[[5 5] [10 1] [20 2]]`, then `[[5 5] [10 1]]` once row 2's
/// entry is deleted; `index nosuch not found`).
#[test]
fn check_index_with_handle_ranges_reads_the_index_entries_like_go() {
    let mut session = Session::new();
    session.run("drop table if exists t").unwrap();
    session.run("create table t (a int primary key, b int, index idx(b))").unwrap();
    session.run("insert into t values (1,10),(2,20),(3,30),(5,5)").unwrap();
    let (columns, rows) = query_text(&mut session, "admin check index t idx (1,3), (5,6)");
    assert_eq!(columns, vec!["b", "extra_handle"]);
    assert_eq!(rows, vec![vec!["5", "5"], vec!["10", "1"], vec!["20", "2"]]);
    assert_eq!(
        error_of(&mut session, "admin check index t nosuch (1,3)"),
        (1105, "index nosuch not found".to_owned())
    );

    let mut store = session.transaction_store().expect("a node session has a store");
    let entries = session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t") else {
                panic!("t is not stored as bytes");
            };
            let table = std::sync::Arc::make_mut(table);
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.read().name.original().eq_ignore_ascii_case("idx"))
                .expect("idx");
            let id = index.read().id;
            Ok(table.index_entries_for_check(&mut *store, id).expect("entries"))
        })
        .unwrap();
    // The entries in index order: b = 5, 10, 20, 30; row 2's is the third.
    assert_eq!(entries.len(), 4);
    session.delete_store_key_for_test(&entries[2].0).unwrap();
    let (_, rows) = query_text(&mut session, "admin check index t idx (1,3), (5,6)");
    assert_eq!(rows, vec![vec!["5", "5"], vec!["10", "1"]]);
}

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
/// The removal goes through the catalog's own `TableCommon`, so the statement
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
    let mut store = session.transaction_store().expect("a node session has a store");
    let entry = session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
            else {
                panic!("t is not stored as bytes");
            };
            let table = std::sync::Arc::make_mut(table);
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.read().name.original().eq_ignore_ascii_case("idx_b"))
                .expect("idx_b");
            let entries = table.index_entries_for_check(&mut *store, index.read().id).expect("entries");
            assert_eq!(entries.len(), 3, "three rows, three entries");
            Ok(entries[0].0.clone())
        })
        .unwrap();
    // The delete commits into the store, as Go's test does.
    session
        .delete_store_key_for_test(&entry)
        .expect("delete the entry");

    // Go `FastCheckTableExec` names the row whose entry is gone, for either
    // statement.
    let missing = "data inconsistency in table: t, index: idx_b, handle: 1, \
                   index-values:\"\" != record-values:\"handle: 1, values: [KindString aa]\"";
    assert_eq!(error_of(&mut session, "admin check index t idx_b"), (8223, missing.to_owned()));
    assert_eq!(error_of(&mut session, "admin check table t"), (8223, missing.to_owned()));

    // Go `CheckTableExec` reports the row/entry count difference as 8003 for
    // `ADMIN CHECK INDEX` (`admin.CheckIndicesCount`'s own error) ...
    set_fast_check(&mut session, false);
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

    let table_id = session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
            else {
                panic!("t is not stored as bytes");
            };
            Ok(table.table_id())
        })
        .unwrap();
    // The delete commits into the store, as Go's test does.
    session
        .delete_store_key_for_test(&tidb_tablecodec::table_key::encode_row_key_with_handle(
            table_id,
            &tidb_tablecodec::table_key::RecordHandle::Int(2),
        ))
        .expect("delete the row");

    assert_eq!(
        error_of(&mut session, "admin check table t"),
        (
            8223,
            "data inconsistency in table: t, index: idx_b, handle: 2, \
             index-values:\"handle: 2, values: [KindString bb]\" != record-values:\"\""
                .to_owned()
        )
    );
    set_fast_check(&mut session, false);
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

    let mut store = session.transaction_store().expect("a node session has a store");
    let swapped = session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
            else {
                panic!("t is not stored as bytes");
            };
            let table = std::sync::Arc::make_mut(table);
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.read().name.original().eq_ignore_ascii_case("idx_b"))
                .expect("idx_b");
            let entries = table.index_entries_for_check(&mut *store, index.read().id).expect("entries");
            assert_eq!(entries.len(), 2);
            Ok((entries[0].0.clone(), entries[1].0.clone()))
        })
        .unwrap();
    // Swap the unique-index handles in a committed write, as Go's test does.
    let (first, second) = swapped;
    let first_value = session.get_store_key_for_test(&first).unwrap().expect("first entry");
    let second_value = session.get_store_key_for_test(&second).unwrap().expect("second entry");
    session.put_store_key_for_test(&first, &second_value).unwrap();
    session.put_store_key_for_test(&second, &first_value).unwrap();

    // Go `FastCheckTableExec`: row 1's index columns differ from its entry's.
    assert_eq!(
        error_of(&mut session, "admin check table t"),
        (
            8223,
            "data inconsistency in table: t, index: idx_b, handle: 1, \
             index-values:\"handle: 1, values: [KindInt64 20]\" != record-values:\"handle: 1, values: [KindInt64 10]\""
                .to_owned()
        )
    );
    set_fast_check(&mut session, false);
    // Go `CheckTableExec` (probed): the table worker reads the rows in
    // handle order, so row 1 meets the entry that now holds 20.
    assert_eq!(
        error_of(&mut session, "admin check table t"),
        (
            8134,
            "data inconsistency in table: t, index: idx_b, col: b, handle: \"1\", \
             index-values:\"KindInt64 20\" != record-values:\"KindInt64 10\", compare err:<nil>"
                .to_owned()
        )
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

    let mut store = session.transaction_store().expect("a node session has a store");
    let moved = session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
            else {
                panic!("t is not stored as bytes");
            };
            let table = std::sync::Arc::make_mut(table);
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.read().name.original().eq_ignore_ascii_case("idx_b"))
                .expect("idx_b");
            let entries = table.index_entries_for_check(&mut *store, index.read().id).expect("entries");
            let (old_key, _) = entries
                .iter()
                .find(|(_, handle)| *handle == tidb_table::tables::TableHandle::Int(2))
                .expect("row 2 index entry");
            let row = vec![tidb_datatype::Datum::Int(2), tidb_datatype::Datum::Int(100)];
            let (new_key, _) = table
                .index_keys_for_check(
                    &index,
                    &row,
                    &tidb_table::tables::TableHandle::Int(2),
                    table.table_id(),
                    &tidb_datatype::SessionTimeZone::utc(),
                )
                .expect("wrong index key")
                .remove(0);
            Ok((old_key.clone(), new_key))
        })
        .unwrap();
    // Move the entry in a committed write, as Go's test does.
    let (old_key, new_key) = moved;
    let value = session.get_store_key_for_test(&old_key).unwrap().expect("row 2 index entry");
    session.delete_store_key_for_test(&old_key).unwrap();
    session.put_store_key_for_test(&new_key, &value).unwrap();

    // Go `TestAdminCheckGlobalIndex` with fast check: 8223 with both records.
    assert_eq!(
        error_of(&mut session, "admin check table t"),
        (
            8223,
            "data inconsistency in table: t, index: idx_b, handle: 2, \
             index-values:\"handle: 2, values: [KindInt64 100]\" != record-values:\"handle: 2, values: [KindInt64 20]\""
                .to_owned()
        )
    );
    set_fast_check(&mut session, false);
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

/// Go `RecoverIndexExec` and `CleanupIndexExec` repair the two corruptions
/// `ADMIN CHECK` finds: RECOVER writes back the entries a row lost
/// (`ADDED_COUNT` rows of `SCAN_COUNT` scanned) and CLEANUP deletes the
/// entries whose row is gone (`REMOVED_COUNT`), after which the check
/// passes again. A missing index is Go's plain `secondary index ... is not
/// found` error.
#[test]
fn recover_and_cleanup_index_repair_what_check_finds() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b varchar(10), index idx_b(b))")
        .unwrap();
    session
        .run("insert into t values (1, 'aa'), (2, 'bb'), (3, 'cc')")
        .unwrap();
    let corrupt = |session: &mut Session, drop_entry: bool| {
        let mut store = session.transaction_store().expect("a node session has a store");
        let key = session
            .with_committed_catalog_mut(|catalog| {
                let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t")
                else {
                    panic!("t is not stored as bytes");
                };
                let table = std::sync::Arc::make_mut(table);
                if drop_entry {
                    let index = table
                        .index_list_for_check()
                        .into_iter()
                        .find(|index| index.read().name.original().eq_ignore_ascii_case("idx_b"))
                        .expect("idx_b");
                    let entries = table.index_entries_for_check(&mut *store, index.read().id).expect("entries");
                    Ok(entries[0].0.clone())
                } else {
                    Ok(tidb_tablecodec::table_key::encode_row_key_with_handle(
                        table.table_id(),
                        &tidb_tablecodec::table_key::RecordHandle::Int(2),
                    ))
                }
            })
            .unwrap();
        // The delete commits into the store, as Go's test does through a
        // transaction of its own.
        session
            .delete_store_key_for_test(&key)
            .expect("delete the key");
    };
    assert_eq!(
        row_text(session.run("admin recover index t idx_b")),
        [["0", "3"]]
    );
    corrupt(&mut session, true);
    // Go `FastCheckTableExec` (the default) names the row (8223);
    // `CheckTableExec` reports `CheckIndicesCount`'s 8003.
    assert_eq!(error_of(&mut session, "admin check index t idx_b").0, 8223);
    set_fast_check(&mut session, false);
    assert_eq!(error_of(&mut session, "admin check index t idx_b").0, 8003);
    set_fast_check(&mut session, true);
    assert_eq!(
        row_text(session.run("admin recover index t idx_b")),
        [["1", "3"]]
    );
    assert_check_passes(&mut session, "admin check table t");

    corrupt(&mut session, false);
    assert_eq!(error_of(&mut session, "admin check table t").0, 8223);
    assert_eq!(
        row_text(session.run("admin cleanup index t idx_b")),
        [["1"]]
    );
    assert_check_passes(&mut session, "admin check table t");
    assert_eq!(
        row_text(session.run("admin cleanup index t idx_b")),
        [["0"]]
    );
    assert_eq!(
        error_of(&mut session, "admin recover index t nosuch"),
        (
            1105,
            "secondary index `nosuch` is not found in table `t`".to_owned()
        )
    );
}

/// Go `ChecksumTableExec` over the in-process engine, which answers as Go's
/// unistore `handleCopChecksumRequest`: `{1, 1, 1}` per request. A table's
/// requests are its own ID plus every partition, times one plus its public
/// indexes, and a table named twice answers once.
#[test]
fn admin_checksum_table_answers_unistores_stub_per_request() {
    let mut session = Session::new();
    session.run("create database ck").unwrap();
    session.run("use ck").unwrap();
    session
        .run("create table t (id int primary key, c int, key (c))")
        .unwrap();
    session
        .run("create table p (a int) partition by hash(a) partitions 4")
        .unwrap();
    session.run("insert into t values (1, 1), (2, 2)").unwrap();
    assert_eq!(
        row_text(session.run("admin checksum table t")),
        [["ck", "t", "0", "2", "2"]]
    );
    assert_eq!(
        row_text(session.run("admin checksum table p, ck.p")),
        [["ck", "p", "1", "5", "5"]]
    );
    assert_eq!(
        error_of(&mut session, "admin checksum table nosuch").0,
        1146
    );
}

/// Go `admin_test.go` `TestAdminCheckGeneratedColumns`: the virtual column's
/// `gen=10 -> handle 2` entry replaced by `gen=5 -> handle 2`; both checks
/// refuse, each with its own shape (Go-probed: the fast check's 8223 over
/// the whole record, the lookup check's 8134 naming the column).
#[test]
fn admin_check_detects_generated_column_index_corruption_like_go() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t(pk int PRIMARY KEY CLUSTERED, val int, \
             gen int GENERATED ALWAYS AS (val * pk) VIRTUAL, KEY idx_gen(gen))",
        )
        .unwrap();
    session.run("INSERT INTO t(pk, val) VALUES (2, 5)").unwrap();
    session.run("admin check table t").unwrap();

    let mut store = session.transaction_store().expect("a node session has a store");
    let (old_key, wrong_key) = session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t") else {
                panic!("t is not stored as bytes");
            };
            let table = std::sync::Arc::make_mut(table);
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.read().name.original().eq_ignore_ascii_case("idx_gen"))
                .expect("idx_gen");
            let entries = table.index_entries_for_check(&mut *store, index.read().id).expect("entries");
            assert_eq!(entries.len(), 1);
            let handle = tidb_table::tables::TableHandle::Int(2);
            let table_id = table.table_id();
            let wrong_row = [Datum::Int(2), Datum::Int(5), Datum::Int(5)];
            let (wrong_key, _) = table
                .index_keys_for_check(&index, &wrong_row, &handle, table_id, &tidb_datatype::SessionTimeZone::utc())
                .expect("the wrong entry's key")
                .remove(0);
            Ok((entries[0].0.clone(), wrong_key))
        })
        .unwrap();
    let value = session.get_store_key_for_test(&old_key).unwrap().expect("the stored entry");
    session.delete_store_key_for_test(&old_key).unwrap();
    session.put_store_key_for_test(&wrong_key, &value).unwrap();

    set_fast_check(&mut session, true);
    assert_eq!(
        error_of(&mut session, "admin check table t"),
        (
            8223,
            "data inconsistency in table: t, index: idx_gen, handle: 2, \
             index-values:\"handle: 2, values: [KindInt64 5]\" != record-values:\"handle: 2, values: [KindInt64 10]\""
                .to_owned()
        )
    );
    set_fast_check(&mut session, false);
    assert_eq!(
        error_of(&mut session, "admin check table t"),
        (
            8134,
            "data inconsistency in table: t, index: idx_gen, col: gen, handle: \"2\", \
             index-values:\"KindInt64 5\" != record-values:\"KindInt64 10\", compare err:<nil>"
                .to_owned()
        )
    );
}

/// Go `admin_test.go` `TestAdminCheckTableWithEnumAndPointGet`, data
/// halves: unique indexes over an ENUM column, a VARCHAR column and an
/// (int, ENUM) pair pass `ADMIN CHECK TABLE` and `ADMIN CHECK INDEX` under
/// both check settings.
#[test]
fn admin_check_passes_over_enum_unique_index_tables_like_go() {
    let mut session = Session::new();
    for (create, insert, index) in [
        (
            "create table admin_test (id int primary key, \
             status enum('active', 'inactive', 'pending'), unique key uk_status(status))",
            "insert into admin_test values (1, 'active'), (2, 'inactive'), (3, 'pending')",
            "uk_status",
        ),
        (
            "create table admin_test (id int primary key, name varchar(50), unique key uk_name(name))",
            "insert into admin_test values (1, 'alice'), (2, 'bob'), (3, 'charlie')",
            "uk_name",
        ),
        (
            "create table admin_test (id int, type enum('A', 'B', 'C'), value int, \
             unique key uk_composite(id, type))",
            "insert into admin_test values (1, 'A', 100), (2, 'B', 200), (3, 'C', 300)",
            "uk_composite",
        ),
    ] {
        session.run("drop table if exists admin_test").unwrap();
        session.run(create).unwrap();
        session.run(insert).unwrap();
        for fast in [true, false] {
            set_fast_check(&mut session, fast);
            session.run("admin check table admin_test").unwrap();
            session.run(&format!("admin check index admin_test {index}")).unwrap();
        }
    }
}

/// A partial index counts only the rows its condition holds for: the fast
/// check passes over a table whose one indexed row of three is the only
/// entry, and the lookup check refuses partial indexes (Go-probed: 8273
/// for both statements).
#[test]
fn admin_check_over_a_partial_index_like_go() {
    let mut session = Session::new();
    session.run("create table t (a int, b int, key idx_b (b) where a > 0)").unwrap();
    session.run("insert into t values (1, 10), (-1, 20), (null, 30)").unwrap();
    set_fast_check(&mut session, true);
    session.run("admin check table t").unwrap();
    session.run("admin check index t idx_b").unwrap();
    set_fast_check(&mut session, false);
    for sql in ["admin check table t", "admin check index t idx_b"] {
        assert_eq!(
            error_of(&mut session, sql),
            (8273, "Validation of partial indexes requires tidb_enable_fast_table_check=ON".to_owned())
        );
    }
}

/// Go `admin_test.go` `TestFastCheckTableConcurrent`: five sessions check a
/// 100-row table at once, and every check passes.
#[test]
fn admin_check_table_runs_concurrently_like_go() {
    let mut session = Session::new();
    session
        .run("create table t_concurrent (id int primary key, val int, key idx_val(val))")
        .unwrap();
    let values = (0..100).map(|i| format!("({i}, {})", i * 10)).collect::<Vec<_>>().join(", ");
    session.run(&format!("insert into t_concurrent values {values}")).unwrap();
    let workers: Vec<_> = (0..5)
        .map(|_| {
            let mut peer = Session::with_catalog(session.shared_catalog());
            std::thread::spawn(move || peer.run("admin check table t_concurrent").map(|_| ()).map_err(|error| format!("{error:?}")))
        })
        .collect();
    for worker in workers {
        worker.join().expect("check thread joins").expect("check passes");
    }
}

/// Go `executor_test.go` `TestCheckIndex`: `ADMIN CHECK INDEX` over a unique
/// index whose entries and records are corrupted step by step, each step
/// answering Go's message (the fast check, the default).
#[test]
fn admin_check_index_reports_each_corruption_like_go() {
    let mut session = Session::new();
    session
        .run("create table t (pk int primary key, c int default 1, c1 int default 1, unique key c(c))")
        .unwrap();
    session.run("admin check index t c").unwrap();
    session.run("admin check index t C").unwrap();
    session.run("insert into t values (1, 10, 11), (2, 20, 21), (3, 30, 31), (4, 40, 41)").unwrap();
    assert_eq!(
        error_of(&mut session, "admin check index t idx_inexistent"),
        (1105, "secondary index idx_inexistent does not exist".to_owned())
    );

    let mut store = session.transaction_store().expect("a node session has a store");
    let (table_id, entries) = session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t") else {
                panic!("t is not stored as bytes");
            };
            let table = std::sync::Arc::make_mut(table);
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.read().name.original().eq_ignore_ascii_case("c"))
                .expect("index c");
            let id = index.read().id;
            Ok((table.table_id(), table.index_entries_for_check(&mut *store, id).expect("entries")))
        })
        .unwrap();
    let entry = |handle: i64| {
        entries
            .iter()
            .find(|(_, entry_handle)| *entry_handle == tidb_table::tables::TableHandle::Int(handle))
            .map(|(key, _)| key.clone())
            .expect("entry for handle")
    };
    let record = |handle: i64| {
        tidb_tablecodec::table_key::encode_row_key_with_handle(
            table_id,
            &tidb_tablecodec::table_key::RecordHandle::Int(handle),
        )
    };
    let entry_4 = entry(4);
    let entry_4_value = session.get_store_key_for_test(&entry_4).unwrap().expect("entry 4");

    // index (1, 10), (2, 20), (3, 30); table (1, 10), (2, 20), (4, 40).
    session.delete_store_key_for_test(&record(3)).unwrap();
    session.delete_store_key_for_test(&entry_4).unwrap();
    assert_eq!(
        error_of(&mut session, "admin check index t c"),
        (
            8223,
            "data inconsistency in table: t, index: c, handle: 3, \
             index-values:\"handle: 3, values: [KindInt64 30]\" != record-values:\"\""
                .to_owned()
        )
    );

    // index (1, 10), (2, 20), (3, 30), (4, 40); table (1, 10), (2, 20), (4, 40).
    session.put_store_key_for_test(&entry_4, &entry_4_value).unwrap();
    assert_eq!(
        error_of(&mut session, "admin check index t c"),
        (
            8223,
            "data inconsistency in table: t, index: c, handle: 3, \
             index-values:\"handle: 3, values: [KindInt64 30]\" != record-values:\"\""
                .to_owned()
        )
    );

    // index (1, 10), (4, 40); table (1, 10), (2, 20), (4, 40).
    session.delete_store_key_for_test(&entry(3)).unwrap();
    session.delete_store_key_for_test(&entry(2)).unwrap();
    assert_eq!(
        error_of(&mut session, "admin check index t c"),
        (
            8223,
            "data inconsistency in table: t, index: c, handle: 2, \
             index-values:\"\" != record-values:\"handle: 2, values: [KindInt64 20]\""
                .to_owned()
        )
    );
}

/// DELETE and UPDATE maintain a prefix index's cut entries, so both checks
/// pass after each write (captured: every `admin check table` passes on
/// TiDB), and a multi-byte charset cuts by characters.
#[test]
fn prefix_index_writes_stay_admin_check_clean() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a VARCHAR(20), b INT, KEY idx (a(3)))").unwrap();
    session.run("INSERT INTO t VALUES ('abcdef', 1), ('abcxyz', 2), ('zzz', 3)").unwrap();
    let check = |session: &mut Session| {
        for fast in [true, false] {
            set_fast_check(session, fast);
            session.run("admin check table t").unwrap();
        }
    };
    check(&mut session);
    session.run("DELETE FROM t WHERE a = 'abcdef'").unwrap();
    check(&mut session);
    session.run("UPDATE t SET a = 'abcqqq' WHERE b = 2").unwrap();
    check(&mut session);
    assert_eq!(
        query_text(&mut session, "SELECT a FROM t ORDER BY a").1,
        vec![vec!["abcqqq"], vec!["zzz"]]
    );

    session.run("CREATE TABLE c (a VARCHAR(20), KEY idx (a(3)))").unwrap();
    session.run("INSERT INTO c VALUES ('世界你好啊'), ('世界你不好')").unwrap();
    assert_eq!(
        query_text(&mut session, "select a from c where a = '世界你好啊'").1,
        vec![vec!["世界你好啊"]]
    );
    for fast in [true, false] {
        set_fast_check(&mut session, fast);
        session.run("admin check table c").unwrap();
    }
}

/// Go `RecoverIndexExec` and `CleanupIndexExec` repair the index in
/// transactions of their own (`kv.RunInNewTxn`), reading the committed
/// table: inside an open transaction RECOVER does not see the
/// transaction's own row, the transaction stays open, and a ROLLBACK keeps
/// the repair (Go-probed: `[[1 3]]`, then the check passes; CLEANUP's
/// `[[1]]` survives its ROLLBACK too).
#[test]
fn admin_recover_and_cleanup_index_commit_on_their_own_like_go() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, b int, index idx(b))").unwrap();
    session.run("insert into t values (1,10),(2,20),(3,30)").unwrap();
    let mut store = session.transaction_store().expect("a node session has a store");
    let entries = session
        .with_committed_catalog_mut(|catalog| {
            let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t") else {
                panic!("t is not stored as bytes");
            };
            let table = std::sync::Arc::make_mut(table);
            let index = table
                .index_list_for_check()
                .into_iter()
                .find(|index| index.read().name.original().eq_ignore_ascii_case("idx"))
                .expect("idx");
            let id = index.read().id;
            Ok(table.index_entries_for_check(&mut *store, id).expect("entries"))
        })
        .unwrap();
    let (entry_2, _) = entries
        .iter()
        .find(|(_, handle)| *handle == tidb_table::tables::TableHandle::Int(2))
        .expect("entry 2");
    let entry_2 = entry_2.clone();
    let value = session.get_store_key_for_test(&entry_2).unwrap().expect("entry 2 value");
    session.delete_store_key_for_test(&entry_2).unwrap();

    session.run("begin").unwrap();
    session.run("insert into t values (4, 40)").unwrap();
    assert_eq!(query_text(&mut session, "admin recover index t idx").1, vec![vec!["1", "3"]]);
    session.run("rollback").unwrap();
    session.run("admin check index t idx").unwrap();
    assert_eq!(query_text(&mut session, "admin recover index t idx").1, vec![vec!["0", "3"]]);

    // A dangling entry: the entry of row 2 filed under handle 9.
    let mut dangling = entry_2[..entry_2.len() - 8].to_vec();
    dangling.extend_from_slice(&tidb_codec::encode_int_to_cmp_uint(9).to_be_bytes());
    session.put_store_key_for_test(&dangling, &value).unwrap();
    session.run("begin").unwrap();
    assert_eq!(query_text(&mut session, "admin cleanup index t idx").1, vec![vec!["1"]]);
    session.run("rollback").unwrap();
    session.run("admin check index t idx").unwrap();
}
