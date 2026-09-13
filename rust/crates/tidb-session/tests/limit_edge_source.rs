//! LIMIT edge semantics: LIMIT 0 yields an empty result, a limit beyond
//! the row count yields everything, `LIMIT offset, count` skips the offset
//! and keeps the remainder, and a negative bound refuses at parse.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

fn setup(session: &mut Session) {
    session.run("create table t (id int primary key)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();
}

#[test]
fn limit_bound_edges() {
    let mut session = Session::new();
    setup(&mut session);

    // Zero: empty result set.
    assert_eq!(rows(&mut session, "select id from t limit 0"), "");

    // Over the row count: everything.
    assert_eq!(rows(&mut session, "select id from t limit 100"), "i:1;i:2;i:3");

    // Offset 2 with a large count: only the remainder.
    assert_eq!(rows(&mut session, "select id from t limit 2, 100"), "i:3");

    // Negative bound refuses at parse.
    let error = rows(&mut session, "select id from t limit -1");
    assert!(error.contains("integer literal"), "{error}");
}
