//! CHECK over a STORED generated column: the constraint is evaluated
//! against the REGENERATED value — INSERT and UPDATE both fail with 3819
//! when the generated result crosses the bound, and nothing is stored.

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
        .run("create table t (a int primary key, b int as (a * 2) stored, check (b < 100))")
        .unwrap();
}

#[test]
fn check_follows_the_generated_value() {
    // a=40 -> b=80: passes and stores.
    let mut session = Session::new();
    setup(&mut session);
    session.run("insert into t (a) values (40)").unwrap();
    assert_eq!(rows(&mut session, "select a, b from t"), "40|80");

    // INSERT a=60 -> b=120: the generated value violates; nothing stored.
    let error = session
        .run("insert into t (a) values (60)")
        .expect_err("the generated 120 violates the CHECK");
    assert!(
        error.to_string().contains("Check constraint 't_chk_1' is violated."),
        "{error}"
    );
    assert_eq!(rows(&mut session, "select a, b from t"), "40|80");

    // UPDATE a across the boundary: regenerates to 120 -> violation, no write.
    let error = session
        .run("update t set a = 60 where a = 40")
        .expect_err("the regenerated value still violates");
    assert!(
        error.to_string().contains("Check constraint 't_chk_1' is violated."),
        "{error}"
    );
    assert_eq!(rows(&mut session, "select a, b from t"), "40|80", "unchanged");
}
