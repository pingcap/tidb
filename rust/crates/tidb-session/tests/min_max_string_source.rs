//! MIN/MAX over a VARCHAR compare lexicographically — 'aa' < 'ab' < 'b' —
//! and empty-string inputs answer as themselves.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

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
