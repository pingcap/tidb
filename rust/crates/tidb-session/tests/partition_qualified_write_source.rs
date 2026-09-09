//! Partition-qualified writes through the session: `DELETE ... PARTITION
//! (p0)` removes only the rows living in p0; rows routed to other
//! partitions survive even though they match the WHERE.

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
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn delete_partition_p0_spares_other_partitions() {
    let mut session = Session::new();
    session
        .run(
            "create table t (a int primary key) partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1), (11), (12)").unwrap();

    println!("sel p0 first: {}", rows(&mut session, "select a from t partition (p0)"));
    let removed = match session.run("delete from t partition (p0)").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(removed, 1, "only p0's single row is in scope");

    assert_eq!(
        rows(&mut session, "select a from t order by a"),
        "11;12",
        "p1 rows survive a p0-qualified DELETE"
    );
}
