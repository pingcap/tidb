//! CHECK (json_valid(j)) over a JSON column: the valid document inserts
//! (CHECK passes), and an invalid document refuses at the JSON CAST
//! (3140 "Invalid JSON text") before the CHECK can even see it.

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
            "create table t (id int primary key, j json, check (json_valid(j)))",
        )
        .unwrap();
}

#[test]
fn json_valid_check_and_cast_order() {
    let mut session = Session::new();
    setup(&mut session);

    // A valid document lands and satisfies the CHECK.
    assert_eq!(try_sql(&mut session, "insert into t values (1, '{\"a\": 1}')"), "affected 1");

    // An invalid document refuses at the JSON CAST (3140) — the CHECK never
    // sees it because the value never becomes a JSON at all.
    let error = try_sql(&mut session, "insert into t values (2, 'nope')");
    assert!(error.contains("Invalid JSON text"), "{error}");

    // Only the valid row landed.
    assert_eq!(rows(&mut session, "select id from t"), "i:1");
}
