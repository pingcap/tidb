//! The DEFAULT keyword and UPDATE IGNORE on NOT NULL: `VALUES (..., default)`
//! takes the column's declared default, while `UPDATE IGNORE ... SET col =
//! NULL` downgrades the 1048 and stores the type's IMPLICIT default (zero
//! value) -- not the declared column default, matching MySQL.

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3) not null default 'zz')");
    tk
}

#[test]
fn default_keyword_takes_the_declared_default() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, default)");
    tk.must_query("select b from t").check(["zz"]);
}

#[test]
fn update_ignore_null_stores_the_implicit_default() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 'abc')");
    // UPDATE IGNORE downgrades the 1048.
    tk.must_exec("update ignore t set b = NULL where a = 1");
    // The zero value, NOT the declared default.
    tk.must_query("select b from t").check([""]);
}
