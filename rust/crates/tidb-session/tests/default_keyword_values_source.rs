//! The DEFAULT keyword inside INSERT VALUES takes the column's default.

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

#[test]
fn default_keyword_takes_column_default() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int default 7)")
        .unwrap();

    session.run("insert into t (a, b) values (1, default)").unwrap();
    assert_eq!(rows(&mut session, "select b from t"), "7");
}
