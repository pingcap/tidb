//! A self-referencing foreign key: the root row takes a NULL manager,
//! child rows reference an existing id, a bad manager refuses with the
//! child-row FK text, and deleting the still-referenced root refuses with
//! the parent-row text.

use tidb_session::Session;

use crate::support::tagged_nullable_integer_rows as rows;

use crate::support::execute as try_sql;

fn setup(session: &mut Session) {
    session
        .run(
            "create table emp (id int primary key, mgr int, \
             foreign key (mgr) references emp(id))",
        )
        .unwrap();
    session.run("insert into emp values (1, null)").unwrap();
    session.run("insert into emp values (2, 1)").unwrap();
}

#[test]
fn self_referencing_fk_enforced() {
    let mut session = Session::new();
    setup(&mut session);

    // A bad manager refuses on insert.
    let error = try_sql(&mut session, "insert into emp values (3, 99)");
    assert!(error.contains("a foreign key constraint fails"), "{error}");

    // Deleting the referenced root refuses too.
    let error = try_sql(&mut session, "delete from emp where id = 1");
    assert!(error.contains("Cannot delete or update a parent row"), "{error}");

    // Both rows survive.
    assert_eq!(rows(&mut session, "select id, mgr from emp order by id"), "i:1|Null;i:2|i:1");
}
