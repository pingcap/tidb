//! CHECK-clause expression restrictions (Go `pkg/ddl` constraint checks):
//! non-deterministic functions are named in 3814, subqueries hit 3815,
//! auto-increment columns hit 3818, and an ordinary comparison passes.

use tidb_session::Session;

#[test]
fn disallowed_check_expressions_are_refused() {
    // rand() is NAMED by 3814.
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    let error = session
        .run("create table t (a int, check (a > rand()))")
        .expect_err("rand() is disallowed");
    assert!(
        error
            .to_string()
            .contains("contains disallowed function: rand"),
        "{error}"
    );

    // now() likewise.
    let error = session
        .run("create table t (a int, check (a > now()))")
        .expect_err("now() is disallowed");
    assert!(
        error.to_string().contains("contains disallowed function: now"),
        "{error}"
    );

    // A subquery hits the generic 3815 refusal.
    let error = session
        .run("create table t (a int, check (a = (select 1)))")
        .expect_err("subqueries are disallowed");
    assert!(
        error
            .to_string()
            .contains("contains disallowed function."),
        "{error}"
    );

    // An auto-increment column cannot be referenced (3818).
    let error = session
        .run("create table t (id int auto_increment primary key, b int, check (id > 0))")
        .expect_err("auto-increment columns are disallowed");
    assert!(
        error
            .to_string()
            .contains("cannot refer to an auto-increment column"),
        "{error}"
    );

    // A plain comparison passes.
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int, b int, check (a > b))")
        .expect("an ordinary CHECK is accepted");
}
