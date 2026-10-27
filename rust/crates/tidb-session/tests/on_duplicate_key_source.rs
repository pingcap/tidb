//! `INSERT ... ON DUPLICATE KEY UPDATE`: a non-duplicate key takes the insert
//! branch (1 row), a duplicate takes the UPDATE branch (2 rows, Go's
//! accounting), and `VALUES(col)` refers to the would-be-inserted value.

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3))");
    tk
}

#[test]
fn non_duplicate_takes_the_insert_branch() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 'abc') on duplicate key update b = 'upd'");
    // The insert branch affects one row.
    assert_eq!(tk.affected_rows(), 1);
}

#[test]
fn duplicate_takes_the_update_branch() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 'abc')");
    tk.must_exec("insert into t values (1, 'xyz') on duplicate key update b = 'upd'");
    // Go counts the update as 2 affected rows.
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select b from t").check(["upd"]);
}

#[test]
fn values_alias_refers_to_the_inserted_value() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 'abc')");
    tk.must_exec("insert into t values (1, 'qrs') on duplicate key update b = values(b)");
    tk.must_query("select b from t").check(["qrs"]);
}
