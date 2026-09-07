//! `ALTER TABLE ADD/DROP UNIQUE INDEX`: after adding, a row duplicating the
//! composite key fails with the dash-separated entry format ("Duplicate
//! entry '5-7' for key 't.uq'"); dropping the index removes enforcement.

use tidb_session::Session;

#[test]
fn unique_index_enforcement_follows_add_and_drop() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int, c int)")
        .unwrap();
    session.run("insert into t values (1, 5, 7), (2, 6, 8)").unwrap();

    // The composite unique key refuses a duplicate with Go's entry format.
    session.run("alter table t add unique index uq (b, c)").unwrap();
    let error = session
        .run("insert into t values (3, 5, 7)")
        .expect_err("the composite key must refuse the duplicate");
    assert!(
        error.to_string().contains("Duplicate entry '5-7' for key 't.uq'"),
        "{error}"
    );

    // Dropping the index removes the enforcement.
    session.run("alter table t drop index uq").unwrap();
    session
        .run("insert into t values (3, 5, 7)")
        .expect("the same row inserts once the index is gone");
}
