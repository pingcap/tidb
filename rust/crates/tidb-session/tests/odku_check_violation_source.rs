//! ODKU whose UPDATE branch violates a CHECK (no IGNORE): Go's
//! `doDupRowUpdate` surfaces `ErrCheckConstraintViolated` and the statement
//! fails with the STORED row untouched (`insert_common.go`'s
//! `handleErr` path — the dup row is never updated).

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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int check (b > 0))")
        .unwrap();
    session.run("insert into t values (1, 5)").unwrap();
}

#[test]
fn odku_update_branch_violation_fails_the_statement() {
    let mut session = Session::new();
    setup(&mut session);

    let error = session
        .run("insert into t values (1, 99) on duplicate key update b = -1")
        .expect_err("the update branch's violation must fail the statement");
    assert!(
        error.to_string().contains("Check constraint 't_chk_1' is violated."),
        "{error}"
    );

    // The stored row is untouched: the ODKU update never applied.
    assert_eq!(rows(&mut session, "select a, b from t"), "1|5");
}
