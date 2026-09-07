//! Partition-qualified UPDATE: the qualifier restricts which rows the
//! statement reads, and a new value that fits NO partition fails with Go's
//! "Table has no partition for value N" (1526) leaving the table unchanged.

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
fn qualified_update_spares_other_partitions_and_refuses_escaping_values() {
    let mut session = Session::new();
    setup(&mut session);

    // In-range qualified update: only p0's rows move (to 101, 102... wait —
    // 1+100=101 escapes p0's range, so use a value that stays).
    let changed = match session
        .run("update t partition (p0) set a = a + 5 where a >= 1")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(changed, 2, "only p0's two rows update");
    assert_eq!(rows(&mut session, "select a from t order by a"), "6;7;11;12");

    // An update whose new value fits NO partition: Go's 1526, table
    // unchanged.
    let error = session
        .run("update t partition (p0) set a = a + 100 where a = 6")
        .expect_err("the escaping value must fail the statement");
    assert!(
        error.to_string().contains("Table has no partition for value"),
        "{error}"
    );
    assert_eq!(rows(&mut session, "select a from t order by a"), "6;7;11;12");
}
