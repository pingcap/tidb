//! REPLACE on a HASH-partitioned table: the replacement row is placed by
//! the partition function (id % 4 = 1 -> p1) and the partition-qualified
//! read agrees with the full-table scan.

use tidb_session::Session;

use crate::support::debug_rows as rows;

#[test]
fn replace_places_row_by_partition_function() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v int) partition by hash(id) partitions 4")
        .unwrap();

    session.run("insert into t values (1, 10)").unwrap();
    session.run("replace into t values (1, 20)").unwrap();

    assert_eq!(rows(&mut session, "select id, v from t"), "Int(1)|Int(20)");
    assert_eq!(
        rows(&mut session, "select id, v from t partition (p1)"),
        "Int(1)|Int(20)"
    );
}
