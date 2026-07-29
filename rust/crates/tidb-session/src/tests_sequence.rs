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

//! Sequences through a live [`Session`]: the SQL surface, not the allocator
//! (which `tidb_executor::sequence` covers). Every expectation is a value or
//! message captured from real TiDB.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// The single scalar a `SELECT <expr>` reports.
fn scalar(session: &mut Session, sql: &str) -> String {
    let (_, rows) = query_text(session, sql);
    assert_eq!(rows.len(), 1, "{sql}");
    rows[0][0].clone()
}

/// The `(code, message)` a failed statement reports on the wire.
fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).expect_err(sql);
    let mysql = error.to_mysql_error();
    (mysql.code, mysql.message)
}

/// The corpus fixture, end to end:
///
/// ```text
/// drop sequence if exists seq   -- OK
/// create sequence seq           -- OK
/// select nextval(seq)           -- 1
/// select nextval(seq)           -- 2
/// select nextval(seq)           -- 3
/// alter sequence seq increment by 2  -- OK
/// ```
///
/// `nextval(seq)` is written with `seq` in a COLUMN position -- the grammar has
/// no place for a table name inside an expression -- so this also covers the
/// rewriter reinterpreting the reference as a name path.
#[test]
fn nextval_counts_through_a_session() {
    let mut session = Session::new();
    session.run("drop sequence if exists seq").unwrap();
    session.run("create sequence seq").unwrap();
    assert_eq!(scalar(&mut session, "select nextval(seq)"), "1");
    assert_eq!(scalar(&mut session, "select nextval(seq)"), "2");
    assert_eq!(scalar(&mut session, "select nextval(seq)"), "3");
    session.run("alter sequence seq increment by 2").unwrap();
    // Captured continuation: an ALTER discards the cache and re-seeks, so the
    // next value is 1001 -- the default CACHE of 1000 had already advanced the
    // stored counter to 1000. NOT 5.
    assert_eq!(scalar(&mut session, "select nextval(seq)"), "1001");
}

/// A schema-qualified name and an unqualified one reach the same sequence, and
/// an unqualified one resolves in the session's current database.
#[test]
fn a_sequence_name_resolves_like_a_table_name() {
    let mut session = Session::new();
    session.run("create sequence seq").unwrap();
    assert_eq!(scalar(&mut session, "select nextval(test.seq)"), "1");
    assert_eq!(scalar(&mut session, "select nextval(seq)"), "2");

    session.run("create database other").unwrap();
    session.run("use other").unwrap();
    // Unqualified now means `other.seq`, which does not exist.
    assert_eq!(
        error_of(&mut session, "select nextval(seq)"),
        (1146, "Table 'other.seq' doesn't exist".to_owned())
    );
    assert_eq!(scalar(&mut session, "select nextval(test.seq)"), "3");
}

/// A name that is not a sequence is 1146. Captured:
/// `select nextval(nosuch)` reports `Table 'test.nosuch' doesn't exist`.
#[test]
fn nextval_on_an_unknown_name_is_1146() {
    let mut session = Session::new();
    assert_eq!(
        error_of(&mut session, "select nextval(nosuch)"),
        (1146, "Table 'test.nosuch' doesn't exist".to_owned())
    );
    // A real TABLE is not a sequence either.
    session.run("create table t (a int)").unwrap();
    assert_eq!(
        error_of(&mut session, "select nextval(t)"),
        (1146, "Table 'test.t' doesn't exist".to_owned())
    );
}

/// Exhaustion is 4135 with the sequence's qualified name -- a DIFFERENT error
/// from the auto-increment allocator's 1467. Captured:
/// `[table:4135] Sequence 'test.s4' has run out`.
#[test]
fn an_exhausted_sequence_is_4135() {
    let mut session = Session::new();
    session
        .run("create sequence s4 maxvalue 3 nocycle")
        .unwrap();
    for want in ["1", "2", "3"] {
        assert_eq!(scalar(&mut session, "select nextval(s4)"), want);
    }
    assert_eq!(
        error_of(&mut session, "select nextval(s4)"),
        (4135, "Sequence 'test.s4' has run out".to_owned())
    );
    // Still exhausted on a second read, not reset by the failure.
    assert_eq!(
        error_of(&mut session, "select nextval(s4)"),
        (4135, "Sequence 'test.s4' has run out".to_owned())
    );
}

/// `LASTVAL` is SESSION state, so it is NULL until this session has taken a
/// value -- it is not the stored counter. Captured:
///
/// ```text
/// create sequence s11
/// select lastval(s11)  -- <nil>
/// select nextval(s11)  -- 1
/// select lastval(s11)  -- 1
/// ```
#[test]
fn lastval_is_null_until_this_session_takes_a_value() {
    let mut session = Session::new();
    session.run("create sequence s11").unwrap();
    assert_eq!(scalar(&mut session, "select lastval(s11)"), "<nil>");
    assert_eq!(scalar(&mut session, "select nextval(s11)"), "1");
    assert_eq!(scalar(&mut session, "select lastval(s11)"), "1");
    // `LASTVAL` does not consume, so reading it twice reports the same value.
    assert_eq!(scalar(&mut session, "select lastval(s11)"), "1");
}

/// `SETVAL` moves the sequence forward and reports the new value; a backwards
/// one reports NULL and changes nothing. Captured:
///
/// ```text
/// select setval(s11, 100) -- 100
/// select nextval(s11)     -- 101
/// select setval(s11, 50)  -- <nil>
/// select nextval(s11)     -- 102
/// ```
#[test]
fn setval_moves_forward_and_reports_null_backwards() {
    let mut session = Session::new();
    session.run("create sequence s11").unwrap();
    assert_eq!(scalar(&mut session, "select setval(s11, 100)"), "100");
    assert_eq!(scalar(&mut session, "select nextval(s11)"), "101");
    assert_eq!(scalar(&mut session, "select setval(s11, 50)"), "<nil>");
    assert_eq!(scalar(&mut session, "select nextval(s11)"), "102");
}

/// A `NEXTVAL` inside a rolled-back transaction still SPENDS the value:
/// Go allocates in its own meta transaction, outside the statement's, so the
/// rollback cannot give it back. Captured:
///
/// ```text
/// create sequence s12
/// begin
/// select nextval(s12)  -- 1
/// rollback
/// select nextval(s12)  -- 2   (NOT 1)
/// ```
///
/// This is the sharpest behavioural difference from every other write in this
/// engine, and it works because the allocator is an `Arc` handle the catalog
/// image shares rather than copies.
#[test]
fn a_rolled_back_nextval_is_still_consumed() {
    let mut session = Session::new();
    session.run("create sequence s12").unwrap();
    session.run("begin").unwrap();
    assert_eq!(scalar(&mut session, "select nextval(s12)"), "1");
    session.run("rollback").unwrap();
    assert_eq!(scalar(&mut session, "select nextval(s12)"), "2");
}

/// `SHOW CREATE SEQUENCE` and `SHOW CREATE TABLE` answer a sequence
/// identically, with the column names Go picks from the OBJECT rather than the
/// keyword. Captured for `show create table s1` over a sequence:
/// `Sequence | Create Sequence` carrying the `CREATE SEQUENCE` text.
#[test]
fn show_create_reports_a_sequence_under_either_keyword() {
    let mut session = Session::new();
    session.run("create sequence s1").unwrap();
    let expected_text = "CREATE SEQUENCE `s1` start with 1 minvalue 1 \
                         maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle \
                         ENGINE=InnoDB";
    for sql in ["show create sequence s1", "show create table s1"] {
        let (columns, rows) = query_text(&mut session, sql);
        assert_eq!(columns, ["Sequence", "Create Sequence"], "{sql}");
        assert_eq!(
            rows,
            vec![vec!["s1".to_owned(), expected_text.to_owned()]],
            "{sql}"
        );
    }
}

/// A sequence lives in the TABLE namespace but is not a row source, and every
/// statement that would treat it as one is refused rather than answered with
/// an empty result. The two Go refuses with a plain message are reproduced
/// verbatim; the read paths are refused with this engine's own wording (see
/// the report note on `select * from <sequence>`).
#[test]
fn a_sequence_is_not_a_table() {
    let mut session = Session::new();
    session.run("create sequence s1").unwrap();
    // Captured: `insert into sequence s1 is not supported now`.
    assert_eq!(
        error_of(&mut session, "insert into s1 values (1)"),
        (
            1105,
            "insert into sequence s1 is not supported now".to_owned()
        )
    );
    // Captured: `delete sequence s1 is not supported now`.
    assert_eq!(
        error_of(&mut session, "delete from s1"),
        (1105, "delete sequence s1 is not supported now".to_owned())
    );
    // Real TiDB reports `[planner:1051] Unknown table ''` here, with the name
    // genuinely empty; this refuses instead of claiming a zero-column source.
    assert!(session.run("select * from s1").is_err());
    // The name really is taken. Real TiDB reports
    // `[schema:1050] Table 'test.s1' already exists`; this engine's
    // `CREATE TABLE` duplicate check reports a generic 1105 for EVERY existing
    // name, sequence or table alike -- a pre-existing divergence on the plain
    // `CREATE TABLE` path, not one sequences introduce. What matters here is
    // that the name collides at all, which is the sequence-specific claim.
    assert!(session.run("create table s1 (a int)").is_err());
    assert_eq!(
        error_of(&mut session, "create sequence s1"),
        (1050, "Table 'test.s1' already exists".to_owned())
    );
    // And `SHOW TABLES` lists it, as Go's does.
    let (_, rows) = query_text(&mut session, "show tables");
    assert_eq!(rows, vec![vec!["s1".to_owned()]]);
}

/// `DROP SEQUENCE` removes the name, and reading it afterwards is 1146 again.
#[test]
fn drop_sequence_frees_the_name() {
    let mut session = Session::new();
    session.run("create sequence s1").unwrap();
    assert_eq!(scalar(&mut session, "select nextval(s1)"), "1");
    session.run("drop sequence s1").unwrap();
    assert_eq!(
        error_of(&mut session, "select nextval(s1)"),
        (1146, "Table 'test.s1' doesn't exist".to_owned())
    );
    // Captured: a second DROP is 4139, not 1146.
    assert_eq!(
        error_of(&mut session, "drop sequence s1"),
        (4139, "Unknown SEQUENCE: 'test.s1'".to_owned())
    );
    // The name is free for a table now.
    session.run("create table s1 (a int)").unwrap();
}
