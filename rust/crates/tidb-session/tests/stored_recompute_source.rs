//! Updating a STORED generated column's base recomputes the stored value:
//! a=5 (b=10) updated to a=10 yields b=20 on disk.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run("create table t (id int primary key, a int, b int as (a * 2) stored)")
        .unwrap();
    session.run("insert into t (id, a) values (1, 5)").unwrap();
}

#[test]
fn base_update_recomputes_stored_value() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(rows(&mut session, "select a, b from t"), "i:5|i:10");

    session.run("update t set a = 10 where id = 1").unwrap();
    assert_eq!(rows(&mut session, "select a, b from t"), "i:10|i:20");
}
