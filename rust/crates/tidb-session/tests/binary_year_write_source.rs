//! BINARY and YEAR write casts: a short BINARY(4) value zero-pads, YEAR
//! accepts 1901-2155 (and 0), a strict out-of-range year fails with Go's
//! "Out of range value" (1264), and IGNORE clamps to the boundary 2155.

use crate::support::testkit::TestKit;

#[test]
fn binary_padding_and_year_boundaries() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, bin binary(4), y year)");

    tk.must_exec("insert into t values (1, 'ab', 2024)");
    // BINARY(4) pads with zero bytes.
    tk.must_query("select bin, y from t").check(["ab\0\0 2024"]);

    // 2156 is outside YEAR's range.
    tk.must_contain_err_msg(
        "insert into t values (2, 'xy', 2156)",
        "Out of range value for column 'y' at row 1",
    );

    // IGNORE clamps the year to the boundary.
    tk.must_exec("insert ignore into t values (2, 'xy', 2156)");
    tk.must_query("select y from t where a = 2").check(["2155"]);
}
