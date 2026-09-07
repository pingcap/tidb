//! Schema lifecycle errors: `USE` on a missing database fails with Go's
//! 1049 ("Unknown database"), and dropping the CURRENT database leaves the
//! session with no current schema — the next query fails with 1046 ("No
//! database selected").

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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn error(session: &mut Session, sql: &str) -> String {
    session
        .run(sql)
        .expect_err(sql)
        .to_string()
}

#[test]
fn use_and_drop_database_errors() {
    let mut session = Session::new();

    // USE on a missing database: 1049.
    assert_eq!(
        error(&mut session, "use nope"),
        "Unknown database 'nope'"
    );

    // The full lifecycle inside a real schema.
    session.run("create database probe_db").unwrap();
    session.run("use probe_db").unwrap();
    session.run("create table t (a int primary key)").unwrap();
    session.run("insert into t values (1)").unwrap();
    assert_eq!(rows(&mut session, "select a from t"), "1");

    // DROP DATABASE removes the schema; the session's current-db is gone.
    session.run("drop database probe_db").unwrap();
    assert_eq!(
        error(&mut session, "select a from t"),
        "No database selected"
    );
}
