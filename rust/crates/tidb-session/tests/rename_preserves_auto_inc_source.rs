//! RENAME TABLE preserves the auto-increment counter: the renamed table
//! continues from the previous max (ids 1,2 exist → next id is 3).

use tidb_session::Session;

use crate::support::integer_rows as rows;

#[test]
fn rename_keeps_auto_inc_counter() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int)")
        .unwrap();

    session.run("insert into t (v) values (1), (2)").unwrap();
    session.run("rename table t to t2").unwrap();
    session.run("insert into t2 (v) values (3)").unwrap();

    assert_eq!(rows(&mut session, "select id from t2 order by id"), "1;2;3");
}
