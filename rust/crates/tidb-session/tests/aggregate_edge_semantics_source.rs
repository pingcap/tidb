//! Aggregate edge semantics: over an EMPTY table the scalar aggregation
//! answers one row (COUNT 0, SUM/MAX NULL) while GROUP BY answers none;
//! HAVING without GROUP BY filters that scalar row.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
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

#[test]
fn empty_table_and_having_without_group_by() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();

    // COUNT is 0; SUM/MAX are NULL — but the row itself still exists.
    assert_eq!(
        rows(&mut session, "select count(*), count(a), sum(a), max(a) from t"),
        "0|0|NULL|NULL"
    );
    // With GROUP BY there are no groups, so no rows.
    assert_eq!(rows(&mut session, "select a, count(*) from t group by a"), "");

    session.run("insert into t values (5)").unwrap();
    // HAVING keeps the scalar row when it passes...
    assert_eq!(rows(&mut session, "select count(*) from t having count(*) > 0"), "1");
    // ...and drops it when it fails.
    assert_eq!(rows(&mut session, "select count(*) from t having count(*) > 5"), "");
}
