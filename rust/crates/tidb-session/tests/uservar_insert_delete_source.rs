//! User variables across write statements: INSERT VALUES binds `@k`/`@v`
//! to the session's values, and a DELETE's WHERE predicate compares against
//! the same variables.

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
fn uservars_bind_in_insert_and_delete() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, v int)").unwrap();
    session.run("set @k = 5").unwrap();
    session.run("set @v = 50").unwrap();

    session.run("insert into t values (@k, @v)").unwrap();
    assert_eq!(rows(&mut session, "select a, v from t"), "5|50");

    let removed = match session.run("delete from t where a = @k").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(removed, 1);
    assert_eq!(rows(&mut session, "select count(*) from t"), "0");
}
