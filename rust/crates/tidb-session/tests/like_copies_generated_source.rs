//! CREATE TABLE LIKE copies generated columns — the copy's stored column
//! computes on insert, exactly like the source table's.

use tidb_session::Session;

use crate::support::integer_rows as rows;

#[test]
fn like_copies_generated_column() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int as (a * 2) stored)")
        .unwrap();
    session.run("create table t2 like t").unwrap();

    // The copy's generated column still computes on insert.
    session.run("insert into t2 (a) values (3)").unwrap();
    assert_eq!(rows(&mut session, "select b from t2"), "6");
}
