//! DML evaluates scalar expressions in VALUES and SET: constants compose
//! (`1 + 1`, `concat('ab', 'cd')`, `upper('xy')`), and an UPDATE may
//! reference its own column (`a = a * 3`) — the NEW value feeds later
//! assignments of the same row.

use crate::support::testkit::TestKit;

#[test]
fn computed_values_in_insert_and_update() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(8))");

    tk.must_exec("insert into t values (1 + 1, concat('ab', 'cd'))");
    tk.must_exec("update t set a = a * 3, b = concat(b, '!') where a = 2");
    tk.must_query("select b from t").check(["abcd!"]);

    tk.must_exec("insert into t values (9, upper('xy'))");
    tk.must_exec("update t set b = left(b, 2) where a = 9");
    tk.must_query("select b from t where a = 9").check(["XY"]);
}
