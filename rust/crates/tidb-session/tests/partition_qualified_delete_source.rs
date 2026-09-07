//! Partition-qualified DELETE with a wide WHERE: `PARTITION (p0)` restricts
//! the read to p0, so a WHERE matching rows in BOTH partitions still only
//! removes p0's — the p1 rows survive.

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

fn setup(session: &mut Session) {
    session
        .run(
            "create table t (a int primary key) partition by range (a) \
             (partition p0 values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    session.run("insert into t values (1), (2), (11), (12)").unwrap();
}

#[test]
fn qualified_delete_spares_other_partitions() {
    let mut session = Session::new();
    setup(&mut session);

    // The WHERE matches all 4 rows but the qualifier restricts to p0
    // (a=1, a=2).
    let removed = match session
        .run("delete from t partition (p0) where a >= 1")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(removed, 2);
    assert_eq!(rows(&mut session, "select a from t order by a"), "11;12");
}
