//! `CREATE TEMPORARY TABLE` is session-local: another session cannot see it
//! at all, two sessions can each hold their own `tmp_t` with different data,
//! and neither shadows the other's.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
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
fn temporary_tables_are_session_local() {
    let mut first = Session::new();
    let mut second = Session::new();

    first.run("create temporary table tmp_t (a int primary key)").unwrap();
    first.run("insert into tmp_t values (100)").unwrap();
    assert_eq!(rows(&mut first, "select a from tmp_t"), "100");

    // The other session cannot see the first's temporary table at all.
    let error = second
        .run("select a from tmp_t")
        .expect_err("a temp table is invisible to other sessions");
    assert!(error.to_string().contains("doesn't exist"), "{error}");

    // The second session's own `tmp_t` is independent data under the same
    // name; the first session's is untouched.
    second.run("create temporary table tmp_t (a int primary key)").unwrap();
    second.run("insert into tmp_t values (200)").unwrap();
    assert_eq!(rows(&mut second, "select a from tmp_t"), "200");
    assert_eq!(rows(&mut first, "select a from tmp_t"), "100");
}
