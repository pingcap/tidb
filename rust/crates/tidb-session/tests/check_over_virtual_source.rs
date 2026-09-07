//! CHECK over a VIRTUAL generated column: the constraint is evaluated
//! against the materialized virtual value at write time — INSERT a=60
//! (virtual b=120) fails 3819 and stores nothing; a=40 (b=80) passes.

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
        .run("create table t (a int primary key, b int as (a * 2) virtual, check (b < 100))")
        .unwrap();
}

#[test]
fn check_follows_the_virtual_value() {
    let mut session = Session::new();
    setup(&mut session);

    session.run("insert into t (a) values (40)").unwrap();
    assert_eq!(rows(&mut session, "select a, b from t"), "40|80");

    let error = session
        .run("insert into t (a) values (60)")
        .expect_err("the virtual 120 violates the CHECK");
    assert!(
        error.to_string().contains("Check constraint 't_chk_1' is violated."),
        "{error}"
    );
    assert_eq!(rows(&mut session, "select a, b from t"), "40|80", "nothing stored");
}
