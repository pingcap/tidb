//! A self-referencing CHECK (`check (a < a * 0)` — always false for any
//! non-NULL): only SQL NULL inserts, since CHECK treats UNKNOWN as pass.

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
        .run("create table t (a int, check (a < a * 0))")
        .unwrap();
}

#[test]
fn self_referencing_check_blocks_every_value() {
    let mut session = Session::new();
    setup(&mut session);

    // NULL passes (CHECK UNKNOWN = pass).
    assert_eq!(try_sql(&mut session, "insert into t values (null)"), "affected 1");

    // Every value >= 0 violates `a < a * 0` (i.e. `a < 0`); -1 would pass.
    for value in ["0", "1", "5"] {
        let error = try_sql(&mut session, &format!("insert into t values ({value})"));
        assert!(error.contains("is violated"), "{value}: {error}");
    }

    // Only the NULL row landed.
    assert_eq!(rows(&mut session, "select a from t"), "Null");
}
