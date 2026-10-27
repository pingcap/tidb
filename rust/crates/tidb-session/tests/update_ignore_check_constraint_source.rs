//! `UPDATE IGNORE` on a CHECK-violating assignment: Go's update executor
//! downgrades `ErrCheckConstraintViolated` the same way it downgrades a
//! duplicate key — warning, row skipped, statement succeeds
//! (update.go:335-341, via `HandleErrorWithAlias`). A plain UPDATE fails
//! with 3819 (Go `tests/integrationtest/t/ddl/constraint.test`
//! TestInsertUpdateIgnoreWarningMessage).

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_enable_check_constraint = on");
    tk.must_exec("create table t (a int primary key, b int check (b > 0))");
    tk
}

#[test]
fn update_ignore_skips_check_violating_rows() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 5)");

    // The plain UPDATE fails the statement with Go's 3819.
    let error = tk.must_get_err_code("update t set b = -1 where a = 1", 3819);
    assert_eq!(error.message, "Check constraint 't_chk_1' is violated.");

    // UPDATE IGNORE downgrades: warning, row skipped, statement succeeds.
    tk.must_exec("update ignore t set b = -1 where a = 1");
    assert_eq!(tk.affected_rows(), 0, "the violating row is NOT changed");

    // The row is unchanged.
    tk.must_query("select b from t").check(["5"]);
}
