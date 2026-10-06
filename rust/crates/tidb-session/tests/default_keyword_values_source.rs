//! The DEFAULT keyword inside INSERT VALUES takes the column's default.

use tidb_session::Session;

use crate::support::integer_rows as rows;

#[test]
fn default_keyword_takes_column_default() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int default 7)")
        .unwrap();

    session.run("insert into t (a, b) values (1, default)").unwrap();
    assert_eq!(rows(&mut session, "select b from t"), "7");
}
