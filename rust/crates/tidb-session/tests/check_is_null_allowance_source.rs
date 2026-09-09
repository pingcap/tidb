//! CHECK with an explicit NULL allowance: `check (b is null or b > 0)`
//! passes NULL rows (UNKNOWN satisfies the OR), refuses a real negative,
//! and accepts a positive.

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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int null, check (b is null or b > 0))")
        .unwrap();
}

#[test]
fn check_with_null_allowance() {
    let mut session = Session::new();
    setup(&mut session);

    // NULL passes the allowance arm (UNKNOWN is not a violation).
    session.run("insert into t values (1, NULL)").unwrap();
    // A negative fails.
    let error = session
        .run("insert into t values (2, -5)")
        .expect_err("a negative fails the allowance");
    assert!(error.to_string().contains("'t_chk_1' is violated"), "{error}");
    // A positive passes.
    session.run("insert into t values (3, 7)").unwrap();

    assert_eq!(
        rows(&mut session, "select a, b from t order by a"),
        "1|NULL;3|7"
    );
}
