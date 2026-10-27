//! Correlated scalar subquery in an UPDATE's SET: each row's assignment
//! evaluates the subquery against that row's own values — `max(v)` per
//! matching `k` lands per row.

use crate::support::testkit::TestKit;

#[test]
fn correlated_subquery_drives_the_assignment() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b int)");
    tk.must_exec("create table s (k int, v int)");
    tk.must_exec("insert into t values (1, 0), (2, 0)");
    tk.must_exec("insert into s values (1, 111), (1, 5), (2, 222)");

    tk.must_exec("update t set b = (select max(v) from s where s.k = t.a)");
    assert_eq!(tk.affected_rows(), 2);

    tk.must_query("select a, b from t order by a").check(["1 111", "2 222"]);
}
