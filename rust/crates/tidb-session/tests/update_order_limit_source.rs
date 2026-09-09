//! UPDATE with ORDER BY + LIMIT: the ordered, limited row set is materialized
//! before the update pass — `order by v asc limit 2` touches only the two
//! smallest rows.

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
fn update_honors_order_and_limit() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v int)")
        .unwrap();
    session.run("insert into t values (1, 1), (2, 2), (3, 3)").unwrap();

    session
        .run("update t set v = 100 order by v asc limit 2")
        .unwrap();

    assert_eq!(
        rows(&mut session, "select id, v from t order by id"),
        "Int(1)|Int(100);Int(2)|Int(100);Int(3)|Int(3)"
    );
}
