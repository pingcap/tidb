//! GROUP BY and DISTINCT over a VIRTUAL generated column: aggregation and
//! dedup run on the recomputed values — parity 0 gets the even rows (2),
//! parity 1 the odd ones (3).

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
fn virtual_column_groups_and_dedups() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, a int, parity int as (a % 2) virtual)")
        .unwrap();
    session
        .run("insert into t (id, a) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
        .unwrap();

    assert_eq!(
        rows(
            &mut session,
            "select parity, count(*) from t group by parity order by parity"
        ),
        "Int(0)|Int(2);Int(1)|Int(3)"
    );
    assert_eq!(
        rows(&mut session, "select distinct parity from t"),
        "Int(1);Int(0)"
    );
}
