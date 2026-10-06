//! A NULL in an FK column is allowed — SQL semantics exempt NULL from the
//! referential obligation — while a non-NULL unmatched value refuses.

use tidb_session::Session;

use crate::support::tagged_nullable_integer_rows as rows;

use crate::support::execute as try_sql;

fn setup(session: &mut Session) {
    session.run("create table p (id int primary key)").unwrap();
    session
        .run("create table c (id int primary key, pid int, foreign key (pid) references p(id))")
        .unwrap();
    session.run("insert into p values (1)").unwrap();
}

#[test]
fn null_fk_column_is_exempt() {
    let mut session = Session::new();
    setup(&mut session);

    // NULL pid: no referential obligation.
    assert_eq!(try_sql(&mut session, "insert into c values (1, null)"), "affected 1");
    assert_eq!(rows(&mut session, "select id, pid from c"), "i:1|Null");

    // A concrete unmatched value still refuses.
    let error = try_sql(&mut session, "insert into c values (2, 99)");
    assert!(error.contains("a foreign key constraint fails"), "{error}");
}
