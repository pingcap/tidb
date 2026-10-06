//! HASH-partition row movement on UPDATE: changing the key so the row's
//! hash bucket changes (`4` -> `5`, p1 -> p2) relocates it; the old
//! partition no longer answers for it.

use tidb_session::Session;

use crate::support::integer_rows_with_sql as rows;

#[test]
fn update_moves_a_hash_bucket() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key) partition by hash(a) partitions 3")
        .unwrap();
    session.run("insert into t values (1), (4)").unwrap();
    assert_eq!(rows(&mut session, "select a from t partition (p1) order by a"), "1;4");

    session.run("update t set a = 5 where a = 4").unwrap();
    assert_eq!(rows(&mut session, "select a from t partition (p1)"), "1");
    assert_eq!(rows(&mut session, "select a from t partition (p2)"), "5");
    assert_eq!(rows(&mut session, "select a from t order by a"), "1;5");
}
