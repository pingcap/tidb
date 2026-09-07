//! TRUNCATE on a parent referenced by an `ON DELETE CASCADE` child is still
//! refused with Go's exact `ErrTruncateIllegalForeignKey` text — the guard
//! applies regardless of the child's referential action.

use tidb_session::Session;

fn setup(session: &mut Session) {
    session.run("set global tidb_enable_check_constraint = off").unwrap();
    session.run("create table p (a int primary key)").unwrap();
    session
        .run("create table c (x int primary key, pa int, foreign key (pa) references p(a) on delete cascade)")
        .unwrap();
    session.run("insert into p values (1)").unwrap();
}

#[test]
fn cascade_child_still_blocks_truncate() {
    let mut session = Session::new();
    setup(&mut session);

    let error = session
        .run("truncate table p")
        .expect_err("a referenced parent cannot be truncated");
    assert!(
        error
            .to_string()
            .contains("Cannot truncate a table referenced in a foreign key constraint"),
        "{error}"
    );
    assert!(error.to_string().contains("`test`.`c`"), "{error}");
}
