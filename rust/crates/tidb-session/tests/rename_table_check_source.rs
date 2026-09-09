//! RENAME TABLE carries attached CHECK constraints: after renaming, the
//! constraint still enforces under its original name.

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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int, constraint pos_b check (b > 0))")
        .unwrap();
    session.run("insert into t values (1, 5)").unwrap();
}

#[test]
fn renamed_table_still_enforces_the_check() {
    let mut session = Session::new();
    setup(&mut session);

    session.run("rename table t to t2").unwrap();
    let error = session
        .run("insert into t2 values (2, -5)")
        .expect_err("the constraint follows the renamed table");
    assert!(error.to_string().contains("'pos_b' is violated"), "{error}");
    assert_eq!(rows(&mut session, "select a, b from t2 order by a"), "1|5");
}
