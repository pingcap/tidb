//! CHECK constraints gate WRITES only: DELETE removes rows that would
//! violate without evaluation, and reads never re-check. Only the UPDATE
//! writing a violating value refuses.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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
        .run("create table t (id int primary key, a int, check (a >= 0))")
        .unwrap();
    session.run("insert into t values (1, 5), (2, 7)").unwrap();
}

#[test]
fn delete_bypasses_check_reads_do_not_recheck() {
    let mut session = Session::new();
    setup(&mut session);

    // Writing a violating value refuses.
    let error = try_sql(&mut session, "update t set a = -1 where id = 1");
    assert!(error.contains("is violated"), "{error}");

    // Deleting the would-violate row is allowed: DELETE has no CHECK pass.
    assert_eq!(try_sql(&mut session, "delete from t where id = 2"), "affected 1");
    assert_eq!(rows(&mut session, "select id, a from t"), "i:1|i:5");

    // Reads over the remaining (valid) rows are unaffected.
    assert_eq!(rows(&mut session, "select id, a from t where a < 0"), "");
}
