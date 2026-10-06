//! The `DEFAULT(col)` function form in VALUES: each column resolves to its
//! own declared default (b -> 7, c -> 9).

use tidb_session::Session;

use crate::support::integer_rows as rows;

#[test]
fn default_function_resolves_each_column() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int default 7, c int default 9)")
        .unwrap();

    session
        .run("insert into t (a, b, c) values (1, default(b), default(c))")
        .unwrap();
    assert_eq!(rows(&mut session, "select a, b, c from t"), "1|7|9");
}
