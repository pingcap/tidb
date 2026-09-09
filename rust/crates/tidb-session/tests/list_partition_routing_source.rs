//! LIST-partition routing and qualified writes: `partition by list (a)`
//! places rows by their value list (p0: 1,2 / p1: 3,4); a value no list
//! admits fails with Go's `ErrNoPartitionForGivenValue` (1526, "Table has
//! no partition for value %s"); a `PARTITION (p1)` DELETE removes exactly
//! p1's rows.

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
fn list_routing_and_qualified_delete() {
    let mut session = Session::new();
    session
        .run(
            "create table t (a int primary key) partition by list (a) \
             (partition p0 values in (1, 2), partition p1 values in (3, 4))",
        )
        .unwrap();
    session.run("insert into t values (1), (3), (2), (4)").unwrap();

    assert_eq!(rows(&mut session, "select a from t partition (p0) order by a"), "1;2");
    assert_eq!(rows(&mut session, "select a from t partition (p1) order by a"), "3;4");

    let error = session
        .run("insert into t values (5)")
        .expect_err("5 has no partition");
    assert!(
        error.to_string().contains("Table has no partition for value 5"),
        "{error}"
    );

    let removed = match session.run("delete from t partition (p1)").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(removed, 2);
    assert_eq!(rows(&mut session, "select a from t order by a"), "1;2");
}
