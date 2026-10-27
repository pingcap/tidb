//! `INSERT IGNORE` on a CHECK-violating row: Go's `batchCheckAndInsert`
//! downgrades `ErrCheckConstraintViolated` to a warning and SKIPS the row,
//! exactly like a duplicate key (`insert_common.go:1364-1370`); a plain
//! insert still fails the statement with 3819 (Go
//! `tests/integrationtest/t/ddl/constraint.test`
//! TestInsertUpdateIgnoreWarningMessage).

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("set global tidb_enable_check_constraint = on");
    tk.must_exec("create table t (a int check (a > 0))");
    tk
}

#[test]
fn insert_ignore_skips_check_violating_rows() {
    let mut tk = setup();

    // The plain insert rejects (pinned separately); IGNORE skips instead.
    tk.must_exec("insert into t values (1)");
    let error = tk.must_get_err_code("insert into t values (-5)", 3819);
    assert_eq!(error.message, "Check constraint 't_chk_1' is violated.");

    // IGNORE downgrades the 3819 to a warning.
    tk.must_exec("insert ignore into t values (-5)");
    // The conforming row under IGNORE passes.
    tk.must_exec("insert ignore into t values (2)");

    // Only the conforming rows are stored.
    tk.must_query("select a from t").check(["1", "2"]);
}
