//! UPDATE of a generated column's base re-evaluates the CHECK: moving
//! a = 5 to a = -10 would make b = -20, violating `check (b >= 0)` — the
//! UPDATE refuses and the stored row is untouched.

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
    session.run("insert into t (id, a) values (1, 5)").unwrap();
}

#[test]
fn base_update_recompute_violation_refuses() {
    let mut session = Session::new();
    setup(&mut session);

    let error = try_sql(&mut session, "update t set a = -10 where id = 1");
    assert!(error.contains("is violated"), "{error}");

    // The stored row keeps a = 5, b = 10.
    assert_eq!(rows(&mut session, "select id, a, b from t"), "i:1|i:5|i:10");
}
