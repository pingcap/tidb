//! `REPLACE INTO`: on a duplicate key the old row is deleted and the new one
//! inserted — Go reports 2 affected rows (the delete plus the insert), and
//! the surviving row carries the new values.

use crate::support::testkit::TestKit;

#[test]
fn replace_into_replaces_the_duplicate_row() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3))");
    tk.must_exec("insert into t values (1, 'abc')");

    // REPLACE succeeds on a duplicate key.
    tk.must_exec("replace into t values (1, 'xyz')");
    // Go counts the delete and the insert as 2 rows.
    assert_eq!(tk.affected_rows(), 2);

    // Exactly one row survives, carrying the new value.
    tk.must_query("select a, b from t").check(["1 xyz"]);
}
