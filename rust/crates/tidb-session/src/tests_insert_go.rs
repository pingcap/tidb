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

//! Write answers `executor/insert.test` and `executor/write.test` record,
//! each checked against Go (testkit or server runs on this branch where the
//! recording does not settle it).

use crate::tests_support::{row_text, warnings_of};
use crate::Session;

fn error_of(session: &mut Session, sql: &str) -> String {
    session.run(sql).unwrap_err().to_string()
}

/// Go `TxnCtx.TemporaryTables`: a GLOBAL temporary table's allocator lives
/// with its rows, one per transaction, so every transaction starts at 1 for
/// the auto-increment column and `_tidb_rowid` alike. It had kept counting.
#[test]
fn a_global_temporary_table_allocates_afresh_in_each_transaction() {
    let mut session = Session::new();
    session
        .run("create global temporary table g(id int primary key auto_increment) on commit delete rows")
        .unwrap();
    session.run("insert into g(id) values (0)").unwrap();
    for _ in 0..2 {
        session.run("begin").unwrap();
        session.run("insert into g(id) values (0), (0)").unwrap();
        assert_eq!(
            row_text(session.run("select id from g order by id")),
            vec![vec!["1"], vec!["2"]]
        );
        session.run("commit").unwrap();
    }
    session
        .run("create global temporary table h(v int) on commit delete rows")
        .unwrap();
    for _ in 0..2 {
        session.run("begin").unwrap();
        session.run("insert into h values (7)").unwrap();
        assert_eq!(row_text(session.run("select _tidb_rowid from h")), vec![vec!["1"]]);
        session.run("commit").unwrap();
    }
}

/// Go `setDatumAutoIDAndCast` (issue 38950): under IGNORE an allocated id
/// past the column's range warns 1690 and clamps; ON DUPLICATE KEY UPDATE
/// goes on with the clamped id, while a plain insert ends with a 1467
/// warning and writes nothing. The statement had failed with 1690.
#[test]
fn an_overflowing_auto_id_under_ignore_follows_go() {
    let mut session = Session::new();
    session
        .run("create table t (id smallint auto_increment primary key, c1 int default 1)")
        .unwrap();
    session.run("insert ignore into t(id) values (194626268)").unwrap();
    session
        .run("insert ignore into t(id) values ('*') on duplicate key update c1 = 2")
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![
            (1366, "Incorrect smallint value: '*' for column 'id' at row 1".to_owned()),
            (1690, "constant 32768 overflows smallint".to_owned()),
        ]
    );
    assert_eq!(row_text(session.run("select * from t")), vec![vec!["32767", "2"]]);

    session.run("create table t0 (c0 smallint auto_increment primary key)").unwrap();
    session.run("insert into t0 values (32767)").unwrap();
    session.run("insert ignore into t0(c0) values ('*')").unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![
            (1366, "Incorrect smallint value: '*' for column 'c0' at row 1".to_owned()),
            (1690, "constant 32768 overflows smallint".to_owned()),
            (1467, "Failed to read auto-increment value from storage engine".to_owned()),
        ]
    );
    assert_eq!(row_text(session.run("select * from t0")), vec![vec!["32767"]]);
}

/// Go `initInsertColumns`' `CheckOnce`: a column named twice is 1110 in both
/// the column list and the SET form.
#[test]
fn a_column_named_twice_is_refused() {
    let mut session = Session::new();
    session.run("create table t(c1 int)").unwrap();
    for sql in ["insert t set c1 = 4, c1 = 5", "insert t (c1, c1) values (4, 5)"] {
        assert_eq!(error_of(&mut session, sql), "Column 'c1' specified twice");
    }
}

/// A DECIMAL datum carries its column's length and frac (Go `SetLength`,
/// `SetFrac`), and the key codec encodes under them: a zero value written
/// for a NULL, and a row ADD INDEX backfills, key exactly as a cast value
/// does. Both had keyed under the value's own shape, so the duplicate went
/// undetected and the backfilled entry could not be read or removed.
#[test]
fn decimal_index_keys_carry_the_column_shape() {
    let mut session = Session::new();
    session.run("create table n(c numeric primary key)").unwrap();
    session.run("insert ignore into n values (null)").unwrap();
    assert_eq!(
        error_of(&mut session, "insert into n values (0)"),
        "Duplicate entry '0' for key 'n.PRIMARY'"
    );

    session.run("create table t(c1 decimal(6,4))").unwrap();
    session.run("insert into t set c1 = '1.1'").unwrap();
    session.run("alter table t add index idx(c1)").unwrap();
    assert_eq!(
        row_text(session.run("select * from t use index(idx) where c1 = 1.1")),
        vec![vec!["1.1000"]]
    );
    session.run("update t set c1 = 2.2").unwrap();
    session.run("admin check table t").unwrap();
    session.run("delete from t").unwrap();
    session.run("admin check table t").unwrap();
}

/// Go's bad-NULL level reads `tidb_enable_strict_not_null_check`: with it
/// off, even a one-row insert takes the zero value and a warning.
#[test]
fn strict_not_null_check_off_admits_a_single_null_row() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, col1 varchar(10) not null default '')")
        .unwrap();
    session.run("set session tidb_enable_strict_not_null_check = off").unwrap();
    session.run("insert into t values (1, null)").unwrap();
    session.run("set session tidb_enable_strict_not_null_check = on").unwrap();
    assert_eq!(
        error_of(&mut session, "insert into t values (2, null)"),
        "Column 'col1' cannot be null"
    );
    assert_eq!(row_text(session.run("select * from t")), vec![vec!["1", ""]]);
}

/// Go `Names4OnDuplicate`: an assignment value resolves at plan time over
/// the target's columns, the SELECT fields Go appends for it, and the
/// SELECT outputs at the target column each fills. A name on both sides is
/// 1052, `VALUES()` names a target column, a source column the SELECT does
/// not output is appended, and a derived table's output reads the column it
/// fills. Each had been resolved only on a conflict, against a guessed
/// scope.
#[test]
fn on_duplicate_names_resolve_as_go_does() {
    let mut session = Session::new();
    session.run("create table t1(a bigint primary key, b bigint)").unwrap();
    session.run("create table t2(a bigint primary key, b bigint)").unwrap();
    assert_eq!(
        error_of(&mut session, "insert into t1 select * from t2 on duplicate key update a = b"),
        "Column 'b' in field list is ambiguous"
    );
    assert_eq!(
        error_of(&mut session, "insert into t1 select * from t2 on duplicate key update c = b"),
        "Unknown column 'c' in 'field list'"
    );
    assert_eq!(
        error_of(&mut session, "insert into t1 select * from t2 on duplicate key update a = values(z)"),
        "Unknown column 'z' in 'field list'"
    );

    session.run("create table a(x int primary key)").unwrap();
    session.run("create table b(x int, y int)").unwrap();
    session.run("insert into a values (1)").unwrap();
    session.run("insert into b values (1, 2)").unwrap();
    session
        .run("insert into a select x from b on duplicate key update a.x = b.y")
        .unwrap();
    assert_eq!(row_text(session.run("select * from a")), vec![vec!["2"]]);

    session
        .run("create table k(k1 bigint, k2 bigint, val bigint, primary key(k1, k2))")
        .unwrap();
    session.run("insert into k (val, k1, k2) values (3, 1, 2)").unwrap();
    session
        .run(
            "insert into k (val, k1, k2) select c, a, b from (select 1 as a, 2 as b, 4 as c) tmp \
             on duplicate key update val = tmp.c",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from k")), vec![vec!["1", "2", "4"]]);
}

/// Go's batch checker formats a binary key's duplicate value with
/// `dataToStrings`: trailing zero bytes dropped (one kept) and non-printable
/// bytes as `\xNN`.
#[test]
fn a_binary_duplicate_under_ignore_is_hex_escaped() {
    let mut session = Session::new();
    session.run("create table t (id binary(3) unique)").unwrap();
    session.run("insert ignore into t values (0x00000a)").unwrap();
    session.run("insert ignore into t values (0x00000a)").unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(1062, "Duplicate entry '\\x00\\x00\\x0A' for key 't.id'".to_owned())]
    );
    session.run("insert ignore into t values (0x01)").unwrap();
    session.run("insert ignore into t values (0x01)").unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(1062, "Duplicate entry '\\x01' for key 't.id'".to_owned())]
    );
}

/// Go renders a duplicate two ways, and both read a prefix key's cut value:
/// the batch checker (INSERT IGNORE) formats `FetchValues` after the index
/// key build cut them in place, and the table layer an UPDATE reaches
/// (`addIndices`, `getDuplicateError`) cuts them with `TruncateIndexValues`.
/// The UPDATE had reported the whole value.
#[test]
fn a_prefix_key_duplicate_reports_the_cut_value() {
    let mut session = Session::new();
    session
        .run("create table t(a varchar(20), b varchar(20), unique index idx_a(a(1)))")
        .unwrap();
    session.run("insert into t values ('qaa', 'abc'), ('rcc', 'xyz')").unwrap();
    let cut = vec![(1062, "Duplicate entry 'q' for key 't.idx_a'".to_owned())];
    session.run("insert ignore into t values ('qbb', 'x')").unwrap();
    assert_eq!(warnings_of(&session), cut);
    session.run("update ignore t set a = 'qcc' where a = 'rcc'").unwrap();
    assert_eq!(warnings_of(&session), cut);

    session.run("set @@tidb_enable_clustered_index = 'on'").unwrap();
    session
        .run("create table c(name varchar(255), b int, primary key(name(2)), index idx(b))")
        .unwrap();
    session.run("insert into c values ('aaa', 1), ('bbb', 1)").unwrap();
    session.run("update ignore c set name = 'aaaaa' where name = 'bbb'").unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(1062, "Duplicate entry 'aa' for key 'c.PRIMARY'".to_owned())]
    );
}

/// Go `evalRow` over `setValueForRefColumn`: a value naming a column of the
/// row reads what the statement already wrote to it, or else its default,
/// the zero value of a column without one, and the zero of the
/// auto-increment column, which still takes an id. These had been refused.
#[test]
fn insert_values_read_the_row_they_build() {
    let mut session = Session::new();
    session.run("create table t(a int default 100, b int)").unwrap();
    session.run("insert into t set b = a + 1, a = 1").unwrap();
    session.run("insert into t (b) value (a)").unwrap();
    session.run("insert into t set a = 2, b = a + 1").unwrap();
    assert_eq!(
        row_text(session.run("select a, b from t order by a")),
        [["1", "101"], ["2", "3"], ["100", "100"]]
    );

    session.run("create table n(a bigint not null, b bigint not null)").unwrap();
    session.run("insert into n value (b + 1, a)").unwrap();
    session.run("insert into n set a = b + a, b = a + 1").unwrap();
    session.run("insert into n value (1000, a)").unwrap();
    session.run("insert n set b = sqrt(a + 4), a = 10").unwrap();
    assert_eq!(
        row_text(session.run("select * from n order by a")),
        [["0", "1"], ["1", "1"], ["10", "2"], ["1000", "1000"]]
    );

    session.run("create table ai(a int auto_increment key, b int)").unwrap();
    session.run("insert into ai (b) value (a)").unwrap();
    session.run("insert into ai value (a, a + 1)").unwrap();
    assert_eq!(row_text(session.run("select * from ai order by a")), [["1", "0"], ["2", "1"]]);

    // A generated column reads as its default: zero when NOT NULL.
    session
        .run("create table g(j int generated always as (i + 1) stored not null, i int default 5)")
        .unwrap();
    session.run("insert into g set i = j + 9").unwrap();
    assert_eq!(row_text(session.run("select * from g")), [["10", "9"]]);
}

/// A column without a default is still refused when the statement leaves
/// it out, and under a non-strict mode warns 1364 once, from the seeding,
/// even when the statement then writes it (a testkit run on this branch).
#[test]
fn a_ref_column_insert_keeps_the_no_default_rules() {
    let mut session = Session::new();
    session.run("create table s(a int not null, b int)").unwrap();
    assert_eq!(
        error_of(&mut session, "insert into s (b) value (a)"),
        "Field 'a' doesn't have a default value"
    );
    session.run("set @@sql_mode = ''").unwrap();
    let no_default = vec![(1364, "Field 'a' doesn't have a default value".to_owned())];
    session.run("insert into s (b) value (a)").unwrap();
    assert_eq!(warnings_of(&session), no_default);
    session.run("insert into s set a = a + 1").unwrap();
    assert_eq!(warnings_of(&session), no_default);
    assert_eq!(
        row_text(session.run("select a, ifnull(b, 'null') from s order by a")),
        [["0", "0"], ["1", "null"]]
    );
}

/// Go casts each value as `evalRow` produces it, so the warnings follow the
/// statement's column list, and the row build's 1364 comes after them
/// (a testkit run on this branch).
#[test]
fn insert_cast_warnings_follow_the_column_list() {
    let mut session = Session::new();
    session.run("create table w(a int not null, b int, c varchar(2))").unwrap();
    session.run("insert ignore into w(c, b) values ('abc', 'x')").unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![
            (1406, "Data too long for column 'c' at row 1".to_owned()),
            (1366, "Incorrect int value: 'x' for column 'b' at row 1".to_owned()),
            (1364, "Field 'a' doesn't have a default value".to_owned()),
        ]
    );
}

/// Go `ResolveOnDuplicate` drops `b = DEFAULT(b)` for a generated `b`; with
/// nothing left to assign the statement is a plain INSERT and reports its
/// duplicate (a warning under IGNORE). It had updated nothing silently.
#[test]
fn an_on_duplicate_left_with_no_assignment_reports_the_duplicate() {
    let mut session = Session::new();
    session
        .run(
            "create table t2 (a int default 10 primary key, b int generated always as (-a) virtual, \
             c int generated always as (-a) stored)",
        )
        .unwrap();
    let sql = "insert into t2 set a = 3, b = default, c = default(c) on duplicate key update b = default(b)";
    session.run(sql).unwrap();
    assert_eq!(error_of(&mut session, sql), "Duplicate entry '3' for key 't2.PRIMARY'");
    session
        .run("insert ignore into t2 set a = 3 on duplicate key update b = default(b)")
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(1062, "Duplicate entry '3' for key 't2.PRIMARY'".to_owned())]
    );
}

/// Go `doDupRowUpdate`: `VALUES(col)` reads the would-be row whatever the
/// column's type, anywhere in the assignment. It had been substituted as a
/// literal, which a TIMESTAMP has no form for, so the statement failed.
#[test]
fn on_duplicate_values_read_the_would_be_row() {
    let mut session = Session::new();
    session
        .run("create table t(id int primary key, ts timestamp, n int, s varchar(10))")
        .unwrap();
    session
        .run("insert into t values (1, '2020-01-01 00:00:00', 1, 'a')")
        .unwrap();
    session
        .run(
            "insert into t values (1, '2020-05-03 05:58:45', 5, 'b') on duplicate key update \
             ts = values(ts), n = ifnull(values(n), 0) + n, s = concat(values(s), s)",
        )
        .unwrap();
    assert_eq!(
        row_text(session.run("select * from t")),
        [["1", "2020-05-03 05:58:45", "6", "ba"]]
    );
}

/// Go evaluates an uncorrelated subquery in a VALUES list or an ON
/// DUPLICATE KEY UPDATE assignment while planning (`EvalSubqueryFirstRow`),
/// conflict or not; one with no row is NULL. Both had been refused.
#[test]
fn insert_subqueries_are_evaluated_while_planning() {
    let mut session = Session::new();
    session.run("create table t(a int primary key, b int)").unwrap();
    session.run("create table s(x int)").unwrap();
    session.run("insert into s values (5)").unwrap();
    session.run("insert into t values (1, (select x from s))").unwrap();
    session
        .run("insert into t values (1, 2) on duplicate key update b = (select x + 1 from s)")
        .unwrap();
    session
        .run("insert into t values (2, 2) on duplicate key update b = (select x from s where x < 0)")
        .unwrap();
    assert_eq!(
        row_text(session.run("select * from t order by a")),
        [["1", "6"], ["2", "2"]]
    );
}

/// Go `buildUpdate`/`buildDelete` push the statement's hints for its read
/// (`pushTableHints(stmt.TableHints, 0)`): they apply and they warn. They had
/// been dropped.
#[test]
fn update_and_delete_hints_apply_and_warn() {
    let mut session = Session::new();
    session.run("create table t(a varchar(10), key(a))").unwrap();
    session.run("create table t1(a varchar(10), key(a))").unwrap();
    let deprecated = vec![(
        1815,
        "The INDEX MERGE JOIN hint is deprecated for usage, try other hints.".to_owned(),
    )];
    session
        .run("update /*+ INL_MERGE_JOIN(t) */ t, t1 set t.a = 'a' where t.a = t1.a")
        .unwrap();
    assert_eq!(warnings_of(&session), deprecated);
    session
        .run("delete /*+ INL_MERGE_JOIN(t) */ t from t, t1 where t.a = t1.a")
        .unwrap();
    assert_eq!(warnings_of(&session), deprecated);
    session
        .run("update /*+ use_index(t, nosuch) */ t set t.a = 'a' where t.a = 'b'")
        .unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![(1176, "Key 'nosuch' doesn't exist in table 't'".to_owned())]
    );
    let plan = row_text(session.run("explain delete /*+ ignore_index(t, a) */ from t where a = 'x'"));
    assert!(
        plan.iter().any(|row| row[0].contains("TableFullScan")),
        "IGNORE_INDEX left the index path: {plan:?}"
    );
    let plan = row_text(
        session.run("explain update /*+ inl_join(t1) */ t, t1 set t.a = 'a' where t.a = t1.a"),
    );
    assert!(
        plan.iter().any(|row| row[0].contains("IndexJoin")),
        "INL_JOIN was not applied: {plan:?}"
    );
}
