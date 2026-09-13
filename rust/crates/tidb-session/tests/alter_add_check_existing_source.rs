//! Standalone `ALTER TABLE ... ADD CONSTRAINT ... CHECK` validates existing
//! rows: a violating row refuses the ALTER with the named constraint's
//! violation text and leaves the data untouched; after the data is fixed
//! the constraint lands (affected 0) and is live against new writes.

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
    session.run("create table t (id int primary key, a int)").unwrap();
    session.run("insert into t values (1, 5), (2, -7)").unwrap();
}

#[test]
fn alter_add_check_validates_existing_rows() {
    let mut session = Session::new();
    setup(&mut session);

    // The violating existing row refuses, by the constraint's own name.
    let error = try_sql(&mut session, "alter table t add constraint pos check (a >= 0)");
    assert!(error.contains("Check constraint 'pos' is violated"), "{error}");
    assert_eq!(rows(&mut session, "select id, a from t"), "i:1|i:5;i:2|i:-7");

    // Fixing the data lets the constraint land.
    assert_eq!(try_sql(&mut session, "update t set a = 7 where id = 2"), "affected 1");
    assert_eq!(
        try_sql(&mut session, "alter table t add constraint pos check (a >= 0)"),
        "affected 0"
    );

    // The new constraint is live.
    let error = try_sql(&mut session, "insert into t values (3, -1)");
    assert!(error.contains("is violated"), "{error}");
}
