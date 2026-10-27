//! Two UPDATE statement shapes: `ORDER BY ... LIMIT` updates only the first
//! n rows in the given order, and a multi-table `UPDATE t, u ... WHERE`
//! joins the tables and applies to the joined rows only.

use crate::support::testkit::TestKit;

#[test]
fn order_by_limit_updates_first_n_rows() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b int)");
    tk.must_exec("insert into t values (1, 0), (2, 0), (3, 0)");

    tk.must_exec("update t set b = 9 order by a desc limit 2");
    assert_eq!(tk.affected_rows(), 2);

    // The DESC order updates rows 3 and 2, leaving row 1.
    tk.must_query("select a, b from t order by a")
        .check(["1 0", "2 9", "3 9"]);
}

#[test]
fn multi_table_update_joins_and_filters() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b int)");
    tk.must_exec("create table u (a int primary key, label varchar(4))");
    tk.must_exec("insert into t values (1, 0), (2, 0)");
    tk.must_exec("insert into u values (1, 'x'), (2, 'y')");

    tk.must_exec("update t, u set t.b = 5 where t.a = u.a and u.label = 'y'");
    // Only the join row with label 'y' updates.
    assert_eq!(tk.affected_rows(), 1);

    tk.must_query("select a, b from t order by a").check(["1 0", "2 5"]);
}
