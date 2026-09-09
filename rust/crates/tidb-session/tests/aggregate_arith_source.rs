//! Aggregates inside arithmetic: sum(v)/count(*) computes the mean by hand
//! (DECIMAL division, extended scale), and sum(v)*1.0 forces the float
//! path — pinned with the rendered values.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Decimal(value) => format!("d:{}", value.to_string()),
                        tidb_datatype::Datum::Real(f) => format!("f:{f}"),
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
fn aggregate_arithmetic() {
    let mut session = Session::new();
    session.run("create table t (v int)").unwrap();
    session.run("insert into t values (1), (2), (4)").unwrap();

    // DECIMAL division keeps the extended scale: 7/3 = 3.3333...
    let mean = rows(&mut session, "select sum(v) / count(*) from t");
    assert!(mean.starts_with("d:2.333"), "{mean}");

    // SUM of an INT column is exact DECIMAL; COUNT stays integer.
    assert_eq!(rows(&mut session, "select sum(v), count(*) from t"), "d:7|i:3");
}
