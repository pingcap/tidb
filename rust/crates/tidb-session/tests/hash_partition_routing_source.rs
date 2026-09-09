//! HASH-partition routing and qualified writes: `partition by hash(a)
//! partitions 3` places rows by `a % 3` (Go's `t.hashToPartition`), a
//! `PARTITION (p1)` read answers only that partition's rows, and a
//! p1-qualified DELETE removes exactly them.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn hash_routing_and_qualified_delete() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key) partition by hash(a) partitions 3")
        .unwrap();
    session
        .run("insert into t values (0), (1), (2), (3), (4), (5), (6)")
        .unwrap();

    assert_eq!(rows(&mut session, "select a from t partition (p0) order by a"), "0;3;6");
    assert_eq!(rows(&mut session, "select a from t partition (p1) order by a"), "1;4");
    assert_eq!(rows(&mut session, "select a from t partition (p2) order by a"), "2;5");
    assert_eq!(rows(&mut session, "select a from t order by a"), "0;1;2;3;4;5;6");

    let removed = match session.run("delete from t partition (p1)").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(removed, 2, "p1 holds rows 1 and 4");
    assert_eq!(rows(&mut session, "select a from t order by a"), "0;2;3;5;6");
}
