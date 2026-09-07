//! RENAME COLUMN of a CHECK-dependent column: Go's 3959 "uses column ...
//! hence column cannot be dropped or renamed" refuses the rename — the
//! same guard the DROP COLUMN path raises.

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn renaming_a_check_dependent_column_is_refused() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int, constraint pos_b check (b > 0))")
        .unwrap();

    let error = session
        .run("alter table t rename column b to bb")
        .expect_err("the rename must be refused")
        .to_string();
    assert!(
        error.contains("Check constraint 'pos_b' uses column 'b', hence column cannot be dropped or renamed."),
        "{error}"
    );
}
