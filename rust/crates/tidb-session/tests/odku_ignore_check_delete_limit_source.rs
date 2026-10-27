//! Two remaining IGNORE/LIMIT flows: `insert ignore ... on duplicate key
//! update` whose UPDATE branch violates a CHECK downgrades to a warning and
//! leaves the stored row alone (insert.go:218; Go
//! `tests/integrationtest/t/ddl/constraint.test`
//! TestCheckConstraintOnDuplicateKeyUpdate), and `DELETE ... LIMIT n`
//! removes exactly the first n matching rows.

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_enable_check_constraint = on");
    tk.must_exec("create table t (a int primary key, b int check (b > 0))");
    tk
}

#[test]
fn odku_ignore_skips_a_check_violating_update_branch() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 5)");

    // IGNORE downgrades the update branch's 3819.
    tk.must_exec("insert ignore into t values (1, -1) on duplicate key update b = -1");

    // The stored row is untouched.
    tk.must_query("select b from t").check(["5"]);
}

#[test]
fn delete_limit_stops_after_n_rows() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3)");

    tk.must_exec("delete from t where a >= 1 limit 2");
    assert_eq!(tk.affected_rows(), 2);

    tk.must_query("select count(*) from t").check(["1"]);
}
