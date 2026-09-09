//! Ordinal ORDER BY/GROUP BY (`order by 2, 1` / `group by 1` refer to
//! SELECT-list positions) and a UNION inside a derived table with an outer
//! ORDER BY/LIMIT.

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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn seed(session: &mut Session) {
    session.run("create table t (a int, b int)").unwrap();
    session.run("insert into t values (2, 1), (1, 2), (1, 1)").unwrap();
}

#[test]
fn ordinals_and_union_derived() {
    let mut session = Session::new();
    seed(&mut session);

    // Ordinal ORDER BY: position 2 (b) then position 1 (a).
    assert_eq!(rows(&mut session, "select a, b from t order by 2, 1"), "1|1;2|1;1|2");

    // Ordinal GROUP BY: position 1 (a).
    assert_eq!(rows(&mut session, "select a, count(*) from t group by 1 order by 1"), "1|2;2|1");

    // A UNION inside a derived table composes with the outer sort/limit:
    // a values {2,1,1} + b values {1,2,1} sorted = 1,1,1,1,2,2; limit 4.
    assert_eq!(
        rows(
            &mut session,
            "select v from (select a as v from t union all select b from t) d order by v limit 4"
        ),
        "1;1;1;1"
    );
}
