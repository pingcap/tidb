//! `ROW_COUNT()` reads `PrevAffectedRows`, promoted at the statement
//! boundary: INSERT/UPDATE/DELETE publish their affected count, SELECT
//! publishes -1, everything else 0 (select.go:1234-1240; Go
//! `TestInfoBuiltin`, pkg/expression/integration_test/integration_test.go).
//! The published count is the same affected-rows accounting the OK packet
//! reports -- so an ODKU duplicate update answers 2 and a no-op UPDATE
//! answers 0.

use crate::support::testkit::TestKit;

#[test]
fn row_count_flow() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3))");

    // A two-row INSERT publishes 2.
    tk.must_exec("insert into t values (1, 'a'), (2, 'b')");
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select row_count()").check(["2"]);

    // An ODKU duplicate takes the update branch: 2 affected rows, and
    // ROW_COUNT() reports the same accounting.
    tk.must_exec("insert into t values (1, 'x') on duplicate key update b = 'upd'");
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select row_count()").check(["2"]);

    // A SELECT publishes -1.
    tk.must_exec("select * from t");
    tk.must_query("select row_count()").check(["-1"]);

    // A no-op UPDATE publishes 0.
    tk.must_exec("update t set b = b where a = 99");
    assert_eq!(tk.affected_rows(), 0);
    tk.must_query("select row_count()").check(["0"]);
}
