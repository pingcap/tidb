//! TRUNCATE TABLE resets the auto-increment counter: the next implicit
//! insert starts from 1 again (Go semantics; DELETE does NOT reset).

use tidb_session::Session;

use crate::support::integer_rows as rows;

#[test]
fn truncate_resets_auto_inc_counter() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int)")
        .unwrap();

    session.run("insert into t (v) values (1), (2)").unwrap();
    session.run("truncate table t").unwrap();
    session.run("insert into t (v) values (3)").unwrap();

    assert_eq!(rows(&mut session, "select id from t"), "1");
}
