//! Chained generated columns: a STORED column may reference another
//! STORED generated column (c = b + 1 where b = a * 2) — the dependency
//! order resolves and both compute from the single base insert.

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
        .run(
            "create table t (id int primary key, a int, \
             b int as (a * 2) stored, c int as (b + 1) stored)",
        )
        .unwrap();
    session.run("insert into t (id, a) values (1, 5)").unwrap();
}

#[test]
fn chained_generated_columns_resolve() {
    let mut session = Session::new();
    setup(&mut session);

    // a=5 -> b=10 -> c=11.
    assert_eq!(rows(&mut session, "select a, b, c from t"), "i:5|i:10|i:11");

    // Updating the base re-resolves the whole chain.
    session.run("update t set a = 7 where id = 1").unwrap();
    assert_eq!(rows(&mut session, "select a, b, c from t"), "i:7|i:14|i:15");
}
