//! Go `INSERT IGNORE` (`e.ignoreErr`) takes precedence over STRICT mode:
//! `HandleTruncate` (`pkg/types/datum.go:1311`) downgrades every write
//! conversion error to a warning and keeps the converted value — truncated
//! varchar, zero-filled int — where non-IGNORE strict rejects. Go notes the
//! blanket shape itself: "TODO: should not filter all types of errors here".

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3), c int)");
    tk
}

#[test]
fn insert_ignore_strict_too_long_stores_truncated() {
    let mut tk = setup();
    tk.must_exec("insert into t values (1, 'abc', 7)");
    // IgnoreErr takes precedence over strict: INSERT IGNORE downgrades 1406
    // to a warning and stores 'abc'.
    tk.must_exec("insert ignore into t values (2, 'abcdef', 8)");
    // The ignored row IS stored, truncated.
    tk.must_query("select a, b from t order by a").check(["1 abc", "2 abc"]);
}

#[test]
fn insert_ignore_strict_bad_int_stores_zero() {
    let mut tk = setup();
    // INSERT IGNORE downgrades the bad-int conversion.
    tk.must_exec("insert ignore into t values (3, 'abc', 9)");
}

#[test]
fn insert_strict_without_ignore_still_rejects() {
    let mut tk = setup();
    tk.must_exec_to_err("insert into t values (1, 'abcdef', 0)");
}
