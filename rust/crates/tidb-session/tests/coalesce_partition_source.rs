//! `ALTER TABLE ... COALESCE PARTITION n` on a HASH table reduces the count
//! and re-hashes every row (Go `CoalescePartitions`, executor.go:2751 +
//! `hashPartitionManagement`). Refusals: non-partitioned (1505), non-HASH
//! (1509), count < 1 (1515), removing the last partition (1508).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn coalesce_rehashes_rows() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key) partition by hash(id) partitions 4")
        .unwrap();
    session.run("insert into t values (1), (2), (3), (4)").unwrap();

    session.run("alter table t coalesce partition 2").unwrap();

    // Two partitions remain; rows re-hash under id % 2.
    assert_eq!(rows(&mut session, "select count(*) from t"), "Int(4)");
    assert_eq!(rows(&mut session, "select id from t order by id"), "Int(1);Int(2);Int(3);Int(4)");
    // id=1 -> 1%2=1 -> p1; id=2 -> p0.
    assert_eq!(rows(&mut session, "select count(*) from t partition (p1)"), "Int(2)");
    assert_eq!(rows(&mut session, "select count(*) from t partition (p0)"), "Int(2)");
}

#[test]
fn coalesce_refusals() {
    let mut session = Session::new();
    session.run("create table plain (id int primary key)").unwrap();
    assert_eq!(
        error(&mut session, "alter table plain coalesce partition 2"),
        "Partition management on a not partitioned table is not possible"
    );

    session
        .run(
            "create table r (id int primary key) partition by range (id) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    assert_eq!(
        error(&mut session, "alter table r coalesce partition 1"),
        "COALESCE PARTITION can only be used on HASH/KEY partitions"
    );

    session
        .run("create table h (id int primary key) partition by hash(id) partitions 2")
        .unwrap();
    assert_eq!(
        error(&mut session, "alter table h coalesce partition 0"),
        "At least one partition must be coalesced"
    );
    assert_eq!(
        error(&mut session, "alter table h coalesce partition 2"),
        "Cannot remove all partitions, use DROP TABLE instead"
    );
}
