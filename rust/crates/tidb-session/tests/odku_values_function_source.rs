//! The VALUES(col) function inside ON DUPLICATE KEY UPDATE refers to the
//! NEW row's would-be value: `... values (1, 99) on duplicate key update
//! v = values(v) + 1` stores 100.

use tidb_session::Session;

use crate::support::debug_rows as rows;

#[test]
fn values_function_reads_the_new_row() {
    let mut session = Session::new();
    session.run("create table t (id int primary key, v int)").unwrap();
    session.run("insert into t values (1, 10)").unwrap();

    session
        .run("insert into t (id, v) values (1, 99) on duplicate key update v = values(v) + 1")
        .unwrap();

    // The stored v comes from the NEW row's 99, not the old 10.
    assert_eq!(rows(&mut session, "select id, v from t"), "Int(1)|Int(100)");
}
