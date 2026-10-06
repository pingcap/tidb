//! A theta self-join (an inequality in ON): `t a join t b on a.id < b.id`
//! emits exactly the strictly-increasing pairs — the nested-loop predicate
//! applies per pair without requiring an equality key.

use tidb_session::Session;

use crate::support::try_integer_rows_60 as rows;

#[test]
fn inequality_self_join_pairs() {
    let mut session = Session::new();
    session.run("create table t (id int primary key)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();

    assert_eq!(
        rows(
            &mut session,
            "select a.id, b.id from t a join t b on a.id < b.id order by a.id, b.id"
        ),
        "i:1|i:2;i:1|i:3;i:2|i:3"
    );
}
