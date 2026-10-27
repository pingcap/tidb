//! NULL into a NOT NULL column: strict mode rejects with Go's 1048 message
//! ("Column 'b' cannot be null"); INSERT IGNORE downgrades the error to a
//! warning and stores the column's implicit default (`''` for varchar).

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, b varchar(3) not null)");
    tk
}

#[test]
fn strict_null_into_not_null_is_rejected() {
    let mut tk = setup();
    // Strict mode refuses NULL into a NOT NULL column. Go: ErrBadNull (1048).
    let error = tk.must_get_err_code("insert into t values (1, NULL)", 1048);
    assert_eq!(error.message, "Column 'b' cannot be null");
}

#[test]
fn ignore_null_into_not_null_stores_the_implicit_default() {
    let mut tk = setup();
    // IGNORE downgrades the 1048 to a warning.
    tk.must_exec("insert ignore into t values (1, NULL)");
    // The implicit default '' is stored.
    tk.must_query("select a, b from t").check_with_sep("|", ["1|"]);
}
