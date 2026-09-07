//! CHECK on an FK referential-action column: with `ON DELETE SET NULL`,
//! the FK's referential action needs the column, so a CHECK naming it is
//! refused with Go's exact 3823 text ("Column 'pa' cannot be used in a
//! check constraint 'c_chk_1': needed in a foreign key constraint
//! referential action.").

use tidb_session::Session;

#[test]
fn check_on_referential_action_column_is_refused() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session.run("create table p (a int primary key)").unwrap();

    let error = session
        .run(
            "create table c (x int primary key, pa int, \
             foreign key (pa) references p(a) on delete set null, \
             check (pa > 0))",
        )
        .expect_err("the referential action needs the column");
    assert_eq!(
        error.to_string(),
        "Column 'pa' cannot be used in a check constraint 'c_chk_1': needed in a foreign key constraint referential action.",
        "{error}"
    );
}
