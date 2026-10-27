//! Subquery predicates in DELETE: `a IN (select ...)` removes exactly the
//! matching rows, `EXISTS` correlates per row, and `NOT IN` keeps the
//! matching rows — all evaluated against the subquery's own tables.

use crate::support::testkit::TestKit;

#[test]
fn in_and_exists_subqueries_drive_deletes() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key)");
    tk.must_exec("create table s (k int)");
    tk.must_exec("insert into t values (1), (2), (3)");
    tk.must_exec("insert into s values (2), (3)");

    tk.must_exec("delete from t where exists (select 1 from s where s.k = t.a)");
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select a from t order by a").check(["1"]);

    tk.must_exec("insert into t values (2), (3)");
    tk.must_exec("delete from t where a in (select k from s)");
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select a from t order by a").check(["1"]);

    tk.must_exec("delete from t where a not in (select k from s)");
    // Row 1 is the only one missing from the subquery.
    assert_eq!(tk.affected_rows(), 1);
    tk.must_query("select a from t order by a").check(Vec::<&str>::new());
}
