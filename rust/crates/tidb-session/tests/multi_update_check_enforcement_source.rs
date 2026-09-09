//! CHECK enforcement in the multi-table UPDATE: a violation on a LATER
//! joined row fails the statement with Go's 3819 and leaves EVERY row
//! unchanged (the whole statement rolls back, matching the single-table
//! path's atomicity).

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
fn multi_table_update_check_violation_rolls_back() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int check (b > 0))")
        .unwrap();
    session.run("create table u (a int primary key)").unwrap();
    session.run("insert into t values (1, 1), (2, 2)").unwrap();
    session.run("insert into u values (1), (2)").unwrap();

    // The joined rows are t.a=1 then t.a=2; the CASE makes the SECOND row's
    // new value violate the CHECK.
    let error = session
        .run("update t, u set t.b = case t.a when 1 then 50 when 2 then -5 end where t.a = u.a")
        .expect_err("the violating row must fail the statement");
    assert!(
        error.to_string().contains("Check constraint 't_chk_1' is violated."),
        "{error}"
    );

    // Rolled back: neither row changed.
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "1|1;2|2");
}
