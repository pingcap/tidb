//! DELETE with ORDER BY + LIMIT removes the highest-v rows first: the
//! rows-to-delete are materialized (ordered, limited) before the delete
//! pass, so `order by v desc limit 2` removes ids 3 and 4.

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
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn delete_honors_order_and_limit() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v int)")
        .unwrap();
    session
        .run("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)")
        .unwrap();

    session
        .run("delete from t order by v desc limit 2")
        .unwrap();

    assert_eq!(rows(&mut session, "select id from t order by id"), "Int(1);Int(2)");
}
