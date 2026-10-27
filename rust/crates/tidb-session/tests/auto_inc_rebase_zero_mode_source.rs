//! Two AUTO_INCREMENT statement flows: `UPDATE t SET id = 300` REBASES the
//! allocator (the next allocation lands past 300, Go `updateRecord`), and
//! `NO_AUTO_VALUE_ON_ZERO` stores an explicit 0 as 0 without allocation,
//! with the next allocation continuing above it.

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (id int auto_increment primary key, v int)");
    tk
}

#[test]
fn update_rebases_the_allocator() {
    let mut tk = setup();
    tk.must_exec("insert into t (v) values (1)");
    tk.must_exec("update t set id = 300 where id = 1");
    tk.must_exec("insert into t (v) values (2)");
    // The next id lands past 300.
    tk.must_query("select id from t order by id").check(["300", "301"]);
}

#[test]
fn no_auto_value_on_zero_stores_zero() {
    let mut tk = setup();
    tk.must_exec("set @@sql_mode = 'NO_AUTO_VALUE_ON_ZERO'");
    tk.must_exec("insert into t (id, v) values (0, 1)");
    // 0 is STORED, not allocated.
    tk.must_query("select id from t").check(["0"]);

    tk.must_exec("set @@sql_mode = default");
    tk.must_exec("insert into t (v) values (2)");
    // Allocation continues above 0.
    tk.must_query("select id from t order by id").check(["0", "1"]);
}
