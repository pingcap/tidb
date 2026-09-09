//! A CHECK over a STORED generated column evaluates the RECOMPUTED value:
//! `check (b >= 0)` where b = a * 2 rejects a = -5 (b would be -10) while
//! a = 5 lands with b = 10.

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
        .run(
            "create table t (id int primary key, a int, \
             b int as (a * 2) stored, check (b >= 0))",
        )
        .unwrap();
}

#[test]
fn stored_generated_check_evaluates_the_recomputed_value() {
    let mut session = Session::new();
    setup(&mut session);

    // a = 5 -> b = 10: passes.
    assert_eq!(try_sql(&mut session, "insert into t (id, a) values (1, 5)"), "affected 1");

    // a = -5 -> b = -10: violates check (b >= 0).
    let error = try_sql(&mut session, "insert into t (id, a) values (2, -5)");
    assert!(error.contains("is violated"), "{error}");

    // Only the valid row landed.
    assert_eq!(rows(&mut session, "select id, a, b from t"), "i:1|i:5|i:10");
}
