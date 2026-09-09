//! Mixed CHECK enforcement: an ENFORCED constraint refuses violating
//! writes while a NOT ENFORCED sibling on the same table does not — each
//! constraint's state is evaluated independently.

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
        .run(
            "create table t (a int primary key, b int, c int, \
             constraint chk_b check (b > 0), \
             constraint chk_c check (c < 0) not enforced)",
        )
        .unwrap();
}

#[test]
fn enforced_and_not_enforced_coexist() {
    let mut session = Session::new();
    setup(&mut session);

    // chk_b is enforced: the violating row is refused.
    let error = session
        .run("insert into t values (1, -1, 5)")
        .expect_err("chk_b must refuse the write");
    assert!(error.to_string().contains("'chk_b' is violated"), "{error}");

    // chk_c is NOT enforced: a row violating only chk_c passes.
    session.run("insert into t values (2, 2, 5)").unwrap();
    assert_eq!(rows(&mut session, "select a, b, c from t order by a"), "2|2|5");
}
