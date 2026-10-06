//! MIN/MAX over a VARCHAR compare lexicographically — 'aa' < 'ab' < 'b' —
//! and empty-string inputs answer as themselves.

use tidb_session::Session;

use crate::support::tagged_string_rows_with_sql as rows;

#[test]
fn string_min_max_lexicographic() {
    let mut session = Session::new();
    session.run("create table s (v varchar(8))").unwrap();
    session
        .run("insert into s values ('b'), ('aa'), ('ab')")
        .unwrap();

    assert_eq!(rows(&mut session, "select min(v), max(v) from s"), "s:aa|s:b");
}

#[test]
fn empty_and_null_extremes() {
    let mut session = Session::new();
    session.run("create table s (v varchar(8))").unwrap();
    session.run("insert into s values (''), (null), ('a')").unwrap();

    // MIN ignores NULLs; '' sorts before 'a'.
    assert_eq!(rows(&mut session, "select min(v), max(v) from s"), "s:|s:a");
}
