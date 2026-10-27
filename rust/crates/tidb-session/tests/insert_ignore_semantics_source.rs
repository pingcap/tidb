//! INSERT/UPDATE IGNORE write semantics beyond the truncate downgrade:
//! duplicate-key rows are SKIPPED (not stored) with a warning, and NULL into
//! a nullable column stays NULL — Go `insert ignore` via `HandleErrorWithAlias`.

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3))");
    tk
}

#[test]
fn insert_ignore_duplicate_key_skips_the_row() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 'abc')");
    // IGNORE skips the duplicate with a warning.
    tk.must_exec("insert ignore into t values (1, 'xyz')");
    // The duplicate row is NOT stored.
    tk.must_query("select a, b from t").check(["1 abc"]);
}

#[test]
fn update_ignore_truncates_over_long_value() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 'zzz')");
    // UPDATE IGNORE truncates with a warning under the strict default mode.
    tk.must_exec("update ignore t set b = 'abcdef' where a = 1");
    tk.must_query("select b from t").check(["abc"]);
}
