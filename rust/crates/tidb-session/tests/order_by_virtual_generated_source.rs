//! ORDER BY and WHERE over a VIRTUAL generated column: the expression is
//! recomputed per row on read, so sorting and filtering see the derived
//! values exactly as a stored column would show them.

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
fn virtual_column_orders_and_filters() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, a int, b int as (a * 2) virtual)")
        .unwrap();
    session
        .run("insert into t (id, a) values (1, 10), (2, 2), (3, 6)")
        .unwrap();

    // b values are 20, 4, 12 — the sort follows them.
    assert_eq!(rows(&mut session, "select id from t order by b"), "Int(2);Int(3);Int(1)");
    assert_eq!(
        rows(&mut session, "select id from t order by b desc"),
        "Int(1);Int(3);Int(2)"
    );

    // b > 5 keeps rows 3 (12) and 1 (20).
    assert_eq!(
        rows(&mut session, "select id from t where b > 5 order by b"),
        "Int(3);Int(1)"
    );
}
