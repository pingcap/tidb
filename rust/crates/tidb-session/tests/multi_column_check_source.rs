//! A multi-column CHECK (`check (hi >= lo)`) evaluates both columns
//! together: (1, 5) passes, (5, 1) violates, and a NULL column makes the
//! predicate UNKNOWN which passes (three-valued logic).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
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

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Affected(n)) => format!("affected {n}"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (lo int, hi int, check (hi >= lo))")
        .unwrap();
}

#[test]
fn multi_column_check_three_valued() {
    let mut session = Session::new();
    setup(&mut session);

    // lo < hi: passes.
    assert_eq!(try_sql(&mut session, "insert into t values (1, 5)"), "affected 1");

    // lo > hi: violates.
    let error = try_sql(&mut session, "insert into t values (5, 1)");
    assert!(error.contains("is violated"), "{error}");

    // NULL lo: UNKNOWN passes.
    assert_eq!(try_sql(&mut session, "insert into t values (null, 5)"), "affected 1");

    assert_eq!(rows(&mut session, "select lo, hi from t"), "i:1|i:5;Null|i:5");
}
