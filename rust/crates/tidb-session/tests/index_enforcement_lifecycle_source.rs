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

/// Go `convertAddIdxJob2RollbackJob`: an ADD UNIQUE INDEX whose backfill
/// meets a duplicate turns into a rollback under a version of its own
/// (`updateVersionAndTableInfo`), so the failed statement publishes six
/// versions -- delete-only, write-only, write-reorganization, the
/// conversion, delete-reorganization, none -- as a TiDB does (59 -> 65 by
/// `ADMIN SHOW DDL`).
#[test]
fn failed_add_unique_index_publishes_go_s_versions() {
    let mut tk = crate::support::testkit::TestKit::new();
    tk.must_exec("create table t (a int)");
    tk.must_exec("insert into t values (1), (1)");
    let before = tk.schema_version();
    tk.must_get_err_code("alter table t add unique index i(a)", 1062);
    assert_eq!(tk.schema_version() - before, 6);
}
