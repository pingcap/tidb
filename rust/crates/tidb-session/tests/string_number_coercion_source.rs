//! Mixed string/number comparisons coerce the string to a number using the
//! leading numeric prefix: '10abc' = 10, 'abc' = 0, ' 5 ' = 5, scientific
//! notation ('1e2' = 100), and a column comparison matches '12' = 12 while
//! 'abc' matches 0.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn leading_prefix_coercion() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select '10abc' = 10, 'abc' = 0, ' 5 ' = 5"),
        "Int(1)|Int(1)|Int(1)"
    );
    assert_eq!(rows(&mut session, "select '1e2' = 100"), "Int(1)");

    session.run("create table t (v varchar(8))").unwrap();
    session.run("insert into t values ('12'), ('abc')").unwrap();
    assert_eq!(rows(&mut session, "select count(*) from t where v = 12"), "Int(1)");
    assert_eq!(rows(&mut session, "select count(*) from t where v = 0"), "Int(1)");
}
