//! Partition-qualified writes honor the named set: an INSERT of a row whose
//! partition is outside the qualifier refuses ("Found a row not matching
//! the given partition set"), a DELETE scoped to the wrong partition sees
//! nothing, and a DELETE scoped to the right partition removes it.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Affected(n)) => format!("affected {n}"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

#[test]
fn qualifier_bounds_reads_and_writes() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key) partition by hash(id) partitions 2")
        .unwrap();
    session.run("insert into t values (1), (2)").unwrap(); // 1->p1, 2->p0

    // Insert naming the WRONG partition.
    let error = try_sql(&mut session, "insert into t partition (p0) values (3)");
    assert!(error.contains("not matching the given partition set"), "{error}");

    // Delete scoped to the WRONG partition: invisible, nothing removed.
    assert_eq!(
        try_sql(&mut session, "delete from t partition (p0) where id = 1"),
        "affected 0"
    );
    assert_eq!(rows(&mut session, "select count(*) from t"), "i:2");

    // Delete scoped to the RIGHT partition.
    assert_eq!(
        try_sql(&mut session, "delete from t partition (p1) where id = 1"),
        "affected 1"
    );
    assert_eq!(rows(&mut session, "select count(*) from t"), "i:1");
}
