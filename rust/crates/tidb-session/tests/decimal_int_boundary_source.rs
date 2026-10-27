//! DECIMAL and unsigned-TINYINT boundary writes: scale rounding is silent
//! and accepted (1.005 -> 1.01 in DECIMAL(4,2)), an overflow fails strict
//! with Go's "Out of range value" (1264), and IGNORE clamps to the type
//! boundaries (99.99 / 255).

use crate::support::testkit::TestKit;

#[test]
fn rounding_and_boundary_clamping() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, d decimal(4,2), ti tinyint unsigned)");

    // Rounding to scale is silent and accepted.
    tk.must_exec("insert into t values (1, 1.005, 200)");
    tk.must_query("select d, ti from t").check(["1.01 200"]);

    // Overflow fails strict, naming the column (both values overflow).
    tk.must_contain_err_msg(
        "insert into t values (2, 100.00, 300)",
        "Out of range value for column 'd' at row 1",
    );

    // IGNORE clamps both to their boundaries.
    tk.must_exec("insert ignore into t values (2, 100.00, 300)");
    tk.must_query("select d, ti from t where a = 2").check(["99.99 255"]);
}
