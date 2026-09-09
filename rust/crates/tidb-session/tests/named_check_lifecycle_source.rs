//! Named CHECK constraint lifecycle: the GIVEN name appears in refusal
//! messages, NOT ENFORCED disables enforcement, re-ENFORCING over rows that
//! already violate fails with 3819 (Go validates existing data on the
//! ALTER), and DROP CHECK removes the constraint entirely.

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

#[test]
fn named_check_lifecycle() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int, constraint pos_b check (b > 0))")
        .unwrap();
    session.run("insert into t values (1, 5)").unwrap();

    // The refusal names the constraint as the user wrote it.
    let error = session
        .run("insert into t values (2, -5)")
        .expect_err("enforced by default");
    assert!(error.to_string().contains("'pos_b' is violated"), "{error}");

    // NOT ENFORCED disables it.
    session.run("alter table t alter check pos_b not enforced").unwrap();
    session.run("insert into t values (2, -5)").unwrap();

    // Re-ENFORCING validates the EXISTING rows: row 2 violates -> 3819.
    let error = session
        .run("alter table t alter check pos_b enforced")
        .expect_err("existing violations block the ALTER");
    assert!(error.to_string().contains("'pos_b' is violated"), "{error}");

    // Removing the violating row lets the ALTER succeed...
    session.run("delete from t where a = 2").unwrap();
    session.run("alter table t alter check pos_b enforced").unwrap();
    // ...and enforcement is back.
    let error = session
        .run("insert into t values (3, -6)")
        .expect_err("enforcement is restored");
    assert!(error.to_string().contains("'pos_b' is violated"), "{error}");

    // DROP CHECK removes it entirely.
    session.run("alter table t drop check pos_b").unwrap();
    session.run("insert into t values (4, -7)").unwrap();
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "1|5;4|-7");
}
