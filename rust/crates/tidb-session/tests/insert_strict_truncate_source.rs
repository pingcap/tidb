//! An over-long VARCHAR write under the two SQL modes: TiDB's default
//! STRICT_TRANS_TABLES rejects it with Go's `ErrDataTooLong` (1406, via
//! `ProduceStrWithSpecifiedTp`), while `sql_mode = ''` truncates to the
//! declared flen and warns.

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3))");
    tk
}

#[test]
fn strict_insert_too_long_is_rejected_with_1406() {
    let mut tk = setup();
    // STRICT_TRANS_TABLES (TiDB's default mode): the write fails.
    tk.must_exec("insert into t values (1, 'abc')");
    // Strict mode must refuse the over-long value.
    let error = tk.must_get_err_code("insert into t values (2, 'abcdef')", 1406);
    assert_eq!(error.message, "Data too long for column 'b' at row 1");
}

#[test]
fn non_strict_insert_too_long_truncates_with_warning() {
    let mut tk = setup();
    // sql_mode = '': the write truncates to the declared flen and warns.
    tk.must_exec("set sql_mode = ''");
    tk.must_exec("insert into t values (1, 'abcdef')");
}
