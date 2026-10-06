//! Multi-table UPDATE and DELETE: `update t1, t2 …` applies the joined
//! projection to the target rows, and `delete t1 from t1, t2 where …`
//! removes the joined t1 rows — both key off the join condition.

use tidb_session::Session;

use crate::support::tagged_integer_rows as rows;

use crate::support::execute as try_sql;

fn setup(session: &mut Session) {
    session.run("create table t1 (id int primary key, a int)").unwrap();
    session.run("create table t2 (id int primary key, b int)").unwrap();
    session.run("insert into t1 values (1, 1), (2, 2), (3, 3)").unwrap();
    session.run("insert into t2 values (2, 20), (3, 30)").unwrap();
}

#[test]
fn multi_table_update_and_delete() {
    let mut session = Session::new();
    setup(&mut session);

    // Joined rows 2 and 3 get bumped by 100.
    assert_eq!(
        try_sql(&mut session, "update t1, t2 set t1.a = t1.a + 100 where t1.id = t2.id"),
        "affected 2"
    );
    assert_eq!(rows(&mut session, "select id, a from t1 order by id"), "i:1|i:1;i:2|i:102;i:3|i:103");

    // Multi-table DELETE removes the joined t1 rows.
    assert_eq!(
        try_sql(&mut session, "delete t1 from t1, t2 where t1.id = t2.id"),
        "affected 2"
    );
    assert_eq!(rows(&mut session, "select id, a from t1 order by id"), "i:1|i:1");
}
