//! ENUM/SET write casts: string and numeric literals resolve to members
//! ('y'==2, 3=='z'; SET 'p,r'==5, 2=='q'), an out-of-range ENUM index fails
//! strict with Go's "Data truncated" (1265), IGNORE stores the empty enum,
//! and an unknown SET member truncates strictly.

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, e enum('x','y','z'), s set('p','q','r'))");
    tk
}

#[test]
fn enum_set_string_and_numeric_literals() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 'y', 'p,r')");
    tk.must_exec("insert into t values (2, 3, 2)");
    tk.must_query("select a, e, s from t order by a")
        .check(["1 y p,r", "2 z q"]);
    // The member indexes: ENUM 'y' == 2, 'z' == 3; SET 'p,r' == 5, 'q' == 2.
    tk.must_query("select e + 0, s + 0 from t order by a")
        .check(["2 5", "3 2"]);
}

#[test]
fn out_of_range_enum_truncates_or_stores_empty() {
    let mut tk = setup();

    // ENUM index 9 is out of range.
    tk.must_contain_err_msg(
        "insert into t values (3, 9, 'p')",
        "Data truncated for column 'e' at row 1",
    );

    // IGNORE stores the empty enum.
    tk.must_exec("insert ignore into t values (3, 9, 'p')");
    tk.must_query("select e, e + 0 from t where a = 3").check([" 0"]);
}

#[test]
fn unknown_set_member_truncates_strictly() {
    let mut tk = setup();
    // 'z' is not a SET member.
    tk.must_contain_err_msg(
        "insert into t values (4, 'x', 'p,z')",
        "Data truncated for column 's' at row 1",
    );
}
