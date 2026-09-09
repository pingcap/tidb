//! DISTINCT aggregates: COUNT/SUM collapse duplicates, COUNT(DISTINCT a, b)
//! counts distinct TUPLES, and the semantics hold under GROUP BY.

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
    session.run("create table t (g int, a int, b int)").unwrap();
    session
        .run("insert into t values (1, 1, 10), (1, 1, 10), (1, 2, 20), (2, 1, 10), (2, 3, 30)")
        .unwrap();
}

#[test]
fn distinct_aggregate_forms() {
    let mut session = Session::new();
    seed(&mut session);

    // COUNT vs COUNT(DISTINCT): 3 rows but 2 distinct a values.
    assert_eq!(
        rows(&mut session, "select count(a), count(distinct a) from t where g = 1"),
        "3|2"
    );

    // SUM vs SUM(DISTINCT): 4 total but 3 distinct. SUM folds to a DECIMAL,
    // so assert the digit bytes ('4' = 52, '3' = 51).
    let sums = rows(&mut session, "select sum(a), sum(distinct a) from t where g = 1");
    assert!(sums.contains("52"), "{sums}");
    assert!(sums.contains("51"), "{sums}");

    // Multi-column DISTINCT counts distinct TUPLES: (1,10)x2 + (2,20) -> 2.
    assert_eq!(rows(&mut session, "select count(distinct a, b) from t where g = 1"), "2");

    // The semantics hold under GROUP BY.
    assert_eq!(
        rows(&mut session, "select g, count(distinct a) from t group by g order by g"),
        "1|2;2|2"
    );
}
