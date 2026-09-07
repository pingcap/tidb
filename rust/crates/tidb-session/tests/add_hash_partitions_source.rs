//! `ALTER TABLE ... ADD PARTITION PARTITIONS n` on a HASH table grows the
//! count and re-hashes every row (Go `AddTablePartitions`, executor.go:2297
//! -> hashPartitionManagement) — the mirror of COALESCE PARTITION.

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
fn add_partitions_rehashes_rows() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key) partition by hash(id) partitions 2")
        .unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();

    session.run("alter table t add partition partitions 2").unwrap();

    // Four partitions; every row still present under id % 4.
    assert_eq!(rows(&mut session, "select count(*) from t"), "Int(3)");
    assert_eq!(rows(&mut session, "select id from t order by id"), "Int(1);Int(2);Int(3)");
    assert_eq!(rows(&mut session, "select count(*) from t partition (p1)"), "Int(1)");
    assert_eq!(rows(&mut session, "select count(*) from t partition (p3)"), "Int(1)");
}

#[test]
fn add_partitions_to_non_hash_refuses() {
    let mut session = Session::new();
    session
        .run(
            "create table r (id int primary key) partition by range (id) \
             (partition p0 values less than (10))",
        )
        .unwrap();
    let error = error(&mut session, "alter table r add partition partitions 2");
    assert!(
        error.contains("not supported yet") || error.contains("REORGANIZE"),
        "{error}"
    );
}
