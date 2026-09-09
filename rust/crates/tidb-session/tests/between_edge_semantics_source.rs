//! BETWEEN edge semantics: an empty range answers nothing (bounds are NOT
//! swapped), a NULL bound yields UNKNOWN (the row is dropped), and NOT
//! BETWEEN excludes the NULL row too.

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
                    .join(";")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn seed(session: &mut Session) {
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (1), (5), (10), (NULL)").unwrap();
}

#[test]
fn between_edge_semantics() {
    let mut session = Session::new();
    seed(&mut session);

    assert_eq!(rows(&mut session, "select a from t where a between 2 and 9"), "5");
    assert_eq!(
        rows(&mut session, "select a from t where a between 9 and 2"),
        "",
        "a reversed range is empty, never swapped"
    );
    assert_eq!(
        rows(&mut session, "select a from t where a between 2 and NULL"),
        "",
        "a NULL bound yields UNKNOWN"
    );
    assert_eq!(
        rows(&mut session, "select a from t where a not between 2 and 9 order by a"),
        "1;10",
        "NOT BETWEEN also excludes the NULL row"
    );
}
