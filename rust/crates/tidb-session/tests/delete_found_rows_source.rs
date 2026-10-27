//! DELETE affected-rows accounting and the FOUND_ROWS() flow: a DELETE
//! reports the rows it removed (0 when nothing matched), and FOUND_ROWS()
//! answers how many rows the PRECEDING SELECT actually returned.

use crate::support::testkit::TestKit;

#[test]
fn delete_accounting_and_found_rows() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3))");
    tk.must_exec("insert into t values (1, 'a'), (2, 'b'), (3, 'c')");

    // A DELETE reports the rows it removed.
    tk.must_exec("delete from t where a <= 2");
    assert_eq!(tk.affected_rows(), 2);

    // ... and 0 when nothing matched.
    tk.must_exec("delete from t where a = 99");
    assert_eq!(tk.affected_rows(), 0);

    // FOUND_ROWS() reads what the preceding SELECT returned.
    tk.must_query("select * from t").check(["3 c"]);
    tk.must_query("select found_rows()").check(["1"]);
}
