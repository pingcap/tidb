//! DROP DATABASE with a CHECK-attached table inside: the drop succeeds and
//! the schema disappears — CHECK attachment doesn't block schema drops.

use tidb_session::Session;

use crate::support::byte_rows as strings;

#[test]
fn drop_database_removes_the_check_attached_schema() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session.run("create database probe_db").unwrap();
    session.run("use probe_db").unwrap();
    session
        .run("create table t (a int primary key, b int, check (b > 0))")
        .unwrap();

    session.run("drop database probe_db").unwrap();

    let dbs = strings(&mut session, "show databases");
    assert!(!dbs.iter().any(|db| db == "probe_db"), "{dbs:?}");
}
