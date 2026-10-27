//! JSON and DATETIME write validation: a JSON literal parses to the binary
//! form (normalized, keys sorted), invalid JSON fails with Go's "Invalid
//! JSON text" (3141), loose temporal input normalizes ('2024-2-3' accepted),
//! and an impossible date fails with "Incorrect datetime value" (1292).

use crate::support::testkit::TestKit;

#[test]
fn json_and_datetime_writes_validate() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int primary key, j json, dt datetime)");

    tk.must_exec(r#"insert into t values (1, '{"b": 2, "a": 1}', '2024-2-3')"#);
    // Normalized to sorted-key JSON.
    tk.must_query("select dt, cast(j as char) from t")
        .check_with_sep("|", [r#"2024-02-03 00:00:00|{"a": 1, "b": 2}"#]);

    // Invalid JSON is rejected.
    tk.must_contain_err_msg(
        r#"insert into t values (2, '{bad}', '2024-01-01')"#,
        "Invalid JSON text",
    );

    // An impossible date is rejected.
    tk.must_contain_err_msg(
        r#"insert into t values (3, '{}', '2024-13-40')"#,
        "Incorrect datetime value: '2024-13-40' for column 'dt' at row 1",
    );
}
