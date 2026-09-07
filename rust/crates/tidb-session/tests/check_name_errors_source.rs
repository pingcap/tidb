//! CHECK constraint name errors: a duplicate CHECK name fails with Go's
//! `ErrCheckConstraintDupName` (3822, "Duplicate check constraint name
//! 'pos_b'."), and dropping a nonexistent CHECK fails with Go's
//! `ErrCheckConstraintNotExists`-shaped refusal ("Constraint 'nope' does
//! not exist.").

use tidb_session::Session;

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int, b int, constraint pos_b check (b > 0))")
        .unwrap();
}

#[test]
fn duplicate_name_and_missing_drop() {
    let mut session = Session::new();
    setup(&mut session);

    let error = session
        .run("alter table t add constraint pos_b check (a > 0)")
        .expect_err("the CHECK name is taken");
    assert_eq!(
        error.to_string(),
        "Duplicate check constraint name 'pos_b'.",
        "{error}"
    );

    let error = session
        .run("alter table t drop check nope")
        .expect_err("the constraint does not exist");
    assert_eq!(
        error.to_string(),
        "Constraint 'nope' does not exist.",
        "{error}"
    );
}
