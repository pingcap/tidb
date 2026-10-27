//! UPDATE affected-rows accounting: by default only rows whose new value
//! DIFFERS count; a client that negotiated `CLIENT_FOUND_ROWS` sees matching
//! rows instead (Go `tk.Session().SetClientCapability(mysql.ClientFoundRows)`,
//! `pkg/session/test/common/common_test.go:189`).

use crate::support::testkit::TestKit;

#[test]
fn update_accounting_modes() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3))");
    tk.must_exec("insert into t values (1, 'abc')");

    // A value that CHANGES: one row.
    tk.must_exec("update t set b = 'xyz' where a = 1");
    assert_eq!(tk.affected_rows(), 1);

    // The SAME value again: nothing counts by default.
    tk.must_exec("update t set b = 'xyz' where a = 1");
    assert_eq!(tk.affected_rows(), 0);

    // With CLIENT_FOUND_ROWS: the matching row counts even unchanged.
    tk.set_client_found_rows(true);
    tk.must_exec("update t set b = 'xyz' where a = 1");
    assert_eq!(tk.affected_rows(), 1);
}
