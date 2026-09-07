//! A CHECK-attached table cannot hold violating rows: the INSERT that would
//! seed one fails with 3819, so a later UPDATE over the table evaluates
//! cleanly and the data stays conformant.

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

#[test]
fn constrained_tables_stay_conformant() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int, constraint pos_b check (b > 0))")
        .unwrap();

    // The seeding attempt itself is refused (3819).
    let error = session
        .run("insert into t values (1, -5)")
        .expect_err("the violating seed is refused");
    assert!(error.to_string().contains("'pos_b' is violated"), "{error}");

    // Later UPDATEs over the table evaluate cleanly — nothing to violate.
    session.run("update t set a = a where 1 = 1").unwrap();
    assert_eq!(rows(&mut session, "select count(*) from t"), "0");
}
