//! TRUNCATE on an FK-referenced parent refuses with MySQL's 1701 text and
//! both tables stay untouched; truncating the referencing child is fine.

use tidb_session::Session;

use crate::support::tagged_integer_rows as rows;

use crate::support::execute as try_sql;

fn setup(session: &mut Session) {
    session.run("create table p (id int primary key)").unwrap();
    session
        .run("create table c (id int primary key, pid int, foreign key (pid) references p(id))")
        .unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (1, 1)").unwrap();
}

#[test]
fn truncate_fk_parent_refuses() {
    let mut session = Session::new();
    setup(&mut session);

    let error = try_sql(&mut session, "truncate table p");
    assert!(
        error.contains("referenced in a foreign key constraint"),
        "{error}"
    );

    // Both tables untouched by the failed truncate.
    assert_eq!(rows(&mut session, "select count(*) from p"), "i:2");
    assert_eq!(rows(&mut session, "select count(*) from c"), "i:1");

    // Truncating the child is allowed (DDL reports affected 0).
    assert_eq!(try_sql(&mut session, "truncate table c"), "affected 0");
    assert_eq!(rows(&mut session, "select count(*) from c"), "i:0");
}
